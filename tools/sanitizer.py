"""Module implementing checks and functions to mark and sanatize harmonized
    BOLD Database.
"""
import logging
import sqlite3
from typing import List, Set, Tuple
from sqlite.Bitvector import BitIndex, ChecksManager
from multiprocessing import Pool, cpu_count
from collections import deque

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

        # if i == 0:
        #     logger.debug("Specimenid %s was identified as unique.", specimenid)
        #     seen.add(nuc_san)
        #     result.append((nuc_san, ChecksManager.generate_mask(checks), specimenid))
        #     continue

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

def _mark_duplicates_presorted(data: List[Tuple[str, str]]) -> Tuple[List[Tuple[str, str]],
                                                                     List[Tuple[str, int, str]],
                                                                     List[Tuple[str, int, str]]]:
    """ Finds duplicates and substrings in data and returns unique strings
        for sql queries.

        Determines wether a string in data is unique or not.
        A string is considered unique when it is not contained inside
        another string (substring) and the string is the first one in the set
        to appear.
        E.g. if a string occures twice, only the first instance is considered
        unique.

        Return values are the specimenid and the bits to set in checks (e.g 
        duplicate or length-failed).

        Args::
            - data (List[Tuple[str, str]]): List of tuples with (specimenid, 
                                            sequence)

        Returns:
        List with Tuples (checks, specimenid) that can be written into database
       """

    unique_list = []
    dublicats_sql_params = []
    unique_sql_params = []
    #result = []
    seen = set()

    for i, (specimenid, nuc_san) in enumerate(data):
        # Important: We can only set flags here, clearing is not possible.
        checks = []
        unique = True

        if len(nuc_san) < 200:
            logger.debug("Specimenid %s failed length check.", specimenid)
            checks.append(BitIndex.FAILED_LENGTH)

        # if i == 0:
        #     logger.debug("Specimenid %s was identified as unique.", specimenid)
        #     seen.add(nuc_san)
        #     unique_list.append((specimenid, nuc_san))
        #     unique_sql_params.append((nuc_san, ChecksManager.generate_mask(checks), specimenid))
        #     continue

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
            unique_sql_params.append((nuc_san, ChecksManager.generate_mask(checks), specimenid))
            unique_list.append((specimenid, nuc_san))
        else:
            dublicats_sql_params.append((nuc_san, ChecksManager.generate_mask(checks), specimenid))

    return (unique_list, dublicats_sql_params, unique_sql_params)

def _combine_mark_duplicates_presorted(pair: Tuple[List[Tuple[str, str]],
                                       List[Tuple[str, str]]]) -> Tuple[List[Tuple[str, str]],
                                                                        List[Tuple[str, int, str]]]:
    """ Combines two lists of sequences and checks for duplicates. """

    helper, helper_1 = pair
    helper.extend(helper_1)
    helper.sort(key=lambda x: len(x[1]), reverse=True) 
    return _mark_duplicates_presorted(helper)

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

def purge_duplicates_multithreading_2(db_handle: sqlite3.Connection,
                                      duplicates: Set[Tuple[int]]) -> None:
    """ Finds and marks duplicates in database.

        This function writes the results directly into database
        using the checks column in table specimen.

        Args:
            - db_handle (sqlite3.Connection): Database connection
            - duplicates (Set[Tuple[int]]): Set with possible duplicates
                                            specimenids as Tuple
    """

    # Instances with over 50000 sequences are considered hard instances
    # We will process them in a separate loop
    hard_barrier = 50000
    subproblem_size = int(hard_barrier / cpu_count())
    hard_list = []
    simpel_list = []

    # Init process
    cursor = db_handle.cursor()
    command = ("UPDATE specimen SET nuc_san = ?, checks = checks | ? "
               "WHERE specimenid = ?;")
    parameters = []
    batch_size = 950 # Limit number of sql variables in query

    # Decide which instances are hard and which are simple
    for specimen_ids in duplicates:
        if len(specimen_ids) > hard_barrier:
            hard_list.append(specimen_ids)
        else:
            simpel_list.append(specimen_ids)

    logger.info("Got %s hard instances and %s simple instances...", len(hard_list), len(simpel_list))
    logger.info("Maximum size in hard instances is %s sequences.", max([len(x) for x in hard_list]) if hard_list else "0")
    logger.info("Maximum size in simple instances is %s sequences.", max([len(x) for x in simpel_list]) if simpel_list else "0")
    logger.info("Starting with simple problem instances.")
    # Straight forward processing each problem in a process
    with Pool(processes=cpu_count()) as pool:
        # We sort the instances by lentth so that the processes running in parallel
        # take around the same amount of time to finish. Otherwise we often end up
        # with one process taking much longer than the others, thus stalling the whole process.
        simpel_list.sort(key=lambda x: len(x))
        for i in range(0, len(simpel_list), cpu_count()):
            # This a a trade off, between memory and speed
            # With more data we easily run out of memory/ into caching stuff on disk
            # Thus we limit the number of data we process here to cpu_count()
            all_results = [] # Saves tuples of sequences
            instances  = simpel_list[i:i + cpu_count()]
            for instance in instances:
                sequences = []
                for i in range(0, len(instance), batch_size):
                    batch = instance[i:i + batch_size]
                    placeholders = ', '.join(['?'] * len(batch))

                    query = f"""SELECT specimenid, nuc_raw FROM specimen WHERE\
                                specimenid IN ({placeholders});"""
                    cursor.execute(query, batch)
                    batch_results = cursor.fetchall()
                    sequences.extend(batch_results)

                all_results.append(sequences)

            #with Pool(processes=cpu_count()) as pool:
            sequences = pool.map(_mark_duplicates, all_results)

            for result in sequences:
                parameters.extend(result)

            # Commit and clear parameters
            logging.info("Writing batch of simple instances into db...")
            cursor.executemany(command, parameters)
            db_handle.commit()
            parameters = []

        logger.info("Finished with simple problem instances.")

        logger.info("Starting with hard problem instances.")
        # Processing each problem on their own in multiple processes,
        # combining the reult at the end.
        for instance in hard_list:

            # Fetch all sequences for this instance
            sequences = []
            for i in range(0, len(instance), batch_size):
                batch = instance[i:i + batch_size]
                placeholders = ', '.join(['?'] * len(batch))

                query = f"""SELECT specimenid, nuc_raw FROM specimen WHERE\
                            specimenid IN ({placeholders});"""
                cursor.execute(query, batch)
                batch_results = cursor.fetchall()
                sequences.extend(batch_results)

            # Preprocessing data: Get rid of gaps and Ns, sort by length
            sequences = [(specimenid, nuc_raw.strip(STRIP_CHARS).replace('-', '')) for specimenid, nuc_raw in sequences]
            sequences.sort(key=lambda x: len(x[1]), reverse=True)

            # Split data into chunks of at most length hard_barrier for processing
            #chunks = [sequences[i:i + hard_barrier] for i in range(0, len(sequences), hard_barrier)]
            chunks = [sequences[i:i + subproblem_size] for i in range(0, len(sequences), subproblem_size)]
            results = pool.map(_mark_duplicates_presorted, chunks)

            uniques = []
            parameters = []
            for result in results:
                uniques.append(result[0]) # Uniques, we need to further process
                parameters.extend(result[1]) # Duplicates, we can write directly to db

            cursor.executemany(command, parameters)
            db_handle.commit()

            # helper = uniques[-1]
            # final_params = []
            # for i in range(len(uniques) - 2, -1, -1):
            #     helper.extend(uniques[i])
            #     helper.sort(key=lambda x: len(x[1]), reverse=True) 
            #     #= sorted(uniques[i], key=lambda x: len(x[1]), reverse=True)
            #     helper, parameters, final_params = _mark_duplicates_presorted(helper)

            #     # Directly write duplicates to db
            #     cursor.executemany(command, parameters)
            #     db_handle.commit()

            # # Write final results to db
            # cursor.executemany(command, final_params)
            # db_handle.commit()

            final_params = []
            while len(uniques) > 1:
                # Create pairs:
                pairs = [(uniques[i], uniques[i - 1]) for i in range(len(uniques) - 1, 0, -2)]

                if len(uniques) % 2 == 1:
                    # Appand first element to new unique list, if uneven number of elements
                    uniques = [uniques[0]]
                else:
                    uniques = []

                # Process pairs
                results = pool.map(_combine_mark_duplicates_presorted, pairs)
                for result in results:
                    uniques.append(result[0])
                    parameters.extend(result[1])
                    final_params = result[2]

                cursor.executemany(command, parameters)
                db_handle.commit()

            # Write final results to db
            cursor.executemany(command, final_params)
            db_handle.commit()


        logger.info("Finished with hard problem instances.")

    return
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
