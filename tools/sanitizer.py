"""Module implementing checks and functions to mark and sanatize harmonized
    BOLD Database.
"""
import logging
import sqlite3
from typing import List, Set, Tuple
from sqlite.Bitvector import BitIndex, ChecksManager

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
            unique = False
            checks.append(BitIndex.DUPLICATE)
        else:
            for seq in seen:
                if nuc_san in seq:
                    unique = False
                    checks.append(BitIndex.DUPLICATE)
                    break

            # for j in range(i):
            #     if nuc_san in data[j][1].strip(STRIP_CHARS).replace('-', ''):
            #         unique = False
            #         v
            #         break

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

    for specimen_ids in duplicates:
        placeholders = ', '.join(['?'] * len(specimen_ids))

        query = f"""SELECT specimenid, nuc_raw FROM specimen WHERE\
                    specimenid IN ({placeholders});"""
        cursor.execute(query, specimen_ids)
        results = cursor.fetchall()

        parameters.extend(_mark_duplicates(results))

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

    command = ('SELECT specimenid FROM specimen WHERE taxon_species '
               'LIKE \'% x %\' OR taxon_species LIKE \'% X %\';')

    cursor = db_handle.cursor()
    cursor.execute(command)
    ids = cursor.fetchall()

    cmd = (f"UPDATE specimen SET checks = checks | (1 << {BitIndex.HYBRID.value}) "
            "WHERE specimenid = ?")
    cursor.executemany(cmd, ids)
    db_handle.commit()
