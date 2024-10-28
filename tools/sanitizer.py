"""Module implementing checks and functions to mark and sanatize harmonized
    BOLD Database.
"""
import logging
import sqlite3
from typing import List, Set, Tuple
from sqlite.Bitvector import BitIndex, ChecksManager
from multiprocessing import Pool, cpu_count

logger = logging.getLogger(__name__)

# Characters we want to remove from start and end of our sequences
STRIP_CHARS = '_-N'

def _mark_duplicates(data: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    """ Finds duplicates and substrings in data and returns parameters
        for sql queries.

        Determines wether a string in data is unique or not.
        A string is considered unique when it is not contained inside
        another string (substring) and the string is the first one in the set
        to appear.
        E.g. if a string occures twice, only the first instance is considered
        unique.

        Return values are the specimenid and the bits to set in checks (e.g 
        duplicate or length-failed).

        Arguments:
            - data (List[Tuple[str, str]]): List of tuples with (specimenid, 
                                            sequence)

        Returns:
        List with Tuples (checks, specimenid) that can be written into database
       """

    # Purge all gaps and N to comapare data...
    data.sort(key=lambda x: len(x[1].strip(STRIP_CHARS).replace('-', '')), reverse=True)

    result = []
    seen = set()

    for i, (specimenid, nuc_raw) in enumerate(data):
        # Important: We can only set flags here, clearing is not possible.
        checks = []
        unique = True

        nuc_san = nuc_raw.strip(STRIP_CHARS).replace('-', '')

        if len(nuc_san) < 200:
            logger.debug("Specimenid %s failed length check.", specimenid)
            checks.append(BitIndex.FAILED_LENGTH)

        if i == 1:
            logger.debug("Specimenid %s was identified as unique.", specimenid)
            seen.add(nuc_san)
            result.append((nuc_san, ChecksManager.generate_mask(checks), specimenid))
            continue

        if nuc_san in seen:
            # Check for exact matches
            unique = False
            checks.append(BitIndex.DUPLICATE)
        else:
            # Check for matches in substings of the sets sequences
            for seq in seen:
                if nuc_san in seq:
                    unique = False
                    checks.append(BitIndex.DUPLICATE)
                    break

        if unique:
            logger.debug("Specimenid %s was identified as unique.", specimenid)
            seen.add(nuc_san)

        result.append((nuc_san, ChecksManager.generate_mask(checks), specimenid))

    return result

def purge_duplicates(db_handle: sqlite3.Connection,
                     duplicates: Set[Tuple[int]]) -> None:
    """ Finds and marks duplicates in database.

        This function writes the results directly into database
        using the checks column in table specimen.

        Arguments:
            - db_handle (sqlite3.Connection): Database connection
            - duplicates (Set[Tuple[int]]): Set with possible duplicates
                                            specimenids as Tuple
       """

    cursor = db_handle.cursor()

    command = ("UPDATE specimen SET nuc_san = ?, checks = checks | ? "
               "WHERE specimenid = ?;")

    parameters = []
    batch_size = 950

    #ToDO: Lower Priority:
    #   Maybee avoid using execute many and use single queries instead.
    #ToDo: HIGH PRIORITY:
    # Implement this in multithreading approach to spare a lot of time.
    for specimen_ids in duplicates:
        results = []
        for i in range(0, len(specimen_ids), batch_size):
            batch = specimen_ids[i:i + batch_size]
            placeholders = ', '.join(['?'] * len(batch))

            query = f"""SELECT specimenid, nuc_raw FROM specimen WHERE\
                        specimenid IN ({placeholders});"""
            cursor.execute(query, batch)
            batch_results = cursor.fetchall()
            results.extend(batch_results)

        parameters.extend(_mark_duplicates(results))

    cursor.executemany(command, parameters)
    db_handle.commit()

def purge_duplicates_multithreading(db_handle: sqlite3.Connection,
                                    duplicates: Set[Tuple[int]]) -> None:
    """ Finds and marks duplicates in database.

        This function writes the results directly into database
        using the checks column in table specimen.

        Arguments:
            - db_handle (sqlite3.Connection): Database connection
            - duplicates (Set[Tuple[int]]): Set with possible duplicates
                                            specimenids as Tuple
       """

    cursor = db_handle.cursor()

    command = ("UPDATE specimen SET nuc_san = ?, checks = checks | ? "
               "WHERE specimenid = ?;")

    parameters = []
    batch_size = 950

    # To-Do:
    # Do this in batches as this process consumes a huge amount of RAM
    # e.g. this might take up to 20 to 30 GB of RAM for the complete database
    all_results = []
    for specimen_ids in duplicates:
        results = []
        for i in range(0, len(specimen_ids), batch_size):
            batch = specimen_ids[i:i + batch_size]
            placeholders = ', '.join(['?'] * len(batch))

            query = f"""SELECT specimenid, nuc_raw FROM specimen WHERE\
                        specimenid IN ({placeholders});"""
            cursor.execute(query, batch)
            batch_results = cursor.fetchall()
            results.extend(batch_results)

        all_results.append(results)

    logger.info("Starting sequence cleaning in multithreading mode.")
    with Pool(processes=8) as pool:
        results = pool.map(_mark_duplicates, all_results)

    for batch in results:
        parameters.extend(batch)
    cursor.executemany(command, parameters)
    db_handle.commit()


def disclose_hybrids(db_handle: sqlite3.Connection) -> None:
    """ Finds and marks hybrid species in database. 

        This function marks hybrids direcly in checks column in the specimen
        table.
        Hybrid species are indicated by regex search for ' x ' or ' X ' in
        species name.

        Arguments:
            - db_handle(sqlite3.Connection): Database connection
    """

    # ToDo: Check if we can do this in a single query not fetching all results into memory
    command = ('SELECT specimenid FROM specimen WHERE taxon_species '
               'LIKE \'% x %\' OR taxon_species LIKE \'% X %\';')

    cursor = db_handle.cursor()
    cursor.execute(command)
    ids = cursor.fetchall()

    cmd = (f"UPDATE specimen SET checks = checks | (1 << {BitIndex.HYBRID.value}) "
            "WHERE specimenid = ?")
    cursor.executemany(cmd, ids)
    db_handle.commit()
