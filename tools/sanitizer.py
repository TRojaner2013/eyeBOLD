"""Module implementing checks and functions to mark and sanatize harmonized
    BOLD Database.
"""
import logging
import sqlite3
from typing import List, Set, Tuple
from sqlite.Bitvector import BitIndex, ChecksManager
from multiprocessing import Pool, cpu_count
import common.constants as const

logger = logging.getLogger(__name__)

# Characters we want to remove from start and end of our sequences
STRIP_CHARS = '_-N'

def _mark_duplicates_own(chunk):
    # Marks duplicates inn a chunk, returning only the specimenids and the parameters of the duplicates
    duplicate_list = []
    parameters = []
    seen = set()

    for i, (specimenid, nuc_san) in enumerate(chunk):
        checks = []
        unique = True

        if len(nuc_san) < 200:
            checks.append(BitIndex.FAILED_LENGTH)

        if nuc_san in seen:
            # Check for exact matches
            unique = False
            checks.append(BitIndex.DUPLICATE)

        for seq in seen:
            if nuc_san in seq:
                checks.append(BitIndex.DUPLICATE)
                parameters.append((nuc_san, ChecksManager.generate_mask(checks), specimenid))
                duplicate_list.append(specimenid)
                unique = False
                break
        if unique:
            seen.add(nuc_san)

    return (duplicate_list, parameters)

def _mark_duplicates(data: List[Tuple[str, str]]) -> List[Tuple[str, int, str]]:
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

    helper = [(id, nuc_raw.strip(STRIP_CHARS).replace('-', '')) for id, nuc_raw in data]
    helper.sort(key=lambda x: len(x[1]), reverse=True)

    result = []
    seen = set()

    for specimenid, nuc_san in helper:
        # Important: We can only set flags here, clearing is not possible.
        checks = []
        unique = True

        #nuc_san = nuc_raw.strip(STRIP_CHARS).replace('-', '')

        if len(nuc_san) < 200:
            logger.debug("Specimenid %s failed length check.", specimenid)
            checks.append(BitIndex.FAILED_LENGTH)

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
    seen = set()

    for i, (specimenid, nuc_san) in enumerate(data):
        checks = []
        unique = True

        if len(nuc_san) < 200:
            logger.debug("Specimenid %s failed length check.", specimenid)
            checks.append(BitIndex.FAILED_LENGTH)

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

    # Need to batch this to spare memory when working
    # with datasets and 400k trivial instances
    for i in range(0, len(duplicates), const.TRIVIAL_PARALLEL_FACTOR):
        for specimen_ids in duplicates[i:i + const.TRIVIAL_PARALLEL_FACTOR]:
            results = []
            for i in range(0, len(specimen_ids), const.SQL_SAVE_NUM_VARS):
                batch = specimen_ids[i:i + const.SQL_SAVE_NUM_VARS]
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
    hard_barrier = const.SMALL_SIZE 
    hard_list = []
    simple_list = []

    simple_parallel_count = const.PHYSICAL_CORES_PER_CPU * const.SIMPLE_PARALLEL_FACTOR 


    # Init process
    cursor = db_handle.cursor()
    command = ("UPDATE specimen SET nuc_san = ?, checks = checks | ? "
               "WHERE specimenid = ?;")
    parameters = []

    # Decide which instances are hard and which are simple
    for specimen_ids in duplicates:
        if len(specimen_ids) > hard_barrier:
            hard_list.append(specimen_ids)
        else:
            simple_list.append(specimen_ids)

    logger.info("Got %s hard instances and %s simple instances...", len(hard_list), len(simple_list))
    logger.info("Maximum size in hard instances is %s sequences.", max([len(x) for x in hard_list]) if hard_list else "0")
    logger.info("Maximum size in simple instances is %s sequences.", max([len(x) for x in simple_list]) if simple_list else "0")
    logger.info("Starting with simple problem instances.")
    # Straight forward processing each problem in a process
    with Pool(processes=const.PHYSICAL_CORES_PER_CPU) as pool:
        # We sort the instances by lentth so that the processes running in parallel
        # take around the same amount of time to finish. Otherwise we often end up
        # with one process taking much longer than the others, thus stalling the whole process.

        simple_list.sort(key=lambda x: len(x))
        for i in range(0, len(simple_list), simple_parallel_count):
            # This a a trade off, between memory and speed
            # With more data we easily run out of memory/ into caching stuff on disk
            # Thus we limit the number of data we process here to cpu_count()
            # As we got a lot of very easy instances, its better to use a multiple of cpu_count()
            # To avoid stalling to to the frequent disk access.
            all_results = [] # Saves tuples of sequences
            instances  = simple_list[i:i + simple_parallel_count]
            for instance in instances:
                sequences = []
                for i in range(0, len(instance), const.SQL_SAVE_NUM_VARS):
                    batch = instance[i:i + const.SQL_SAVE_NUM_VARS]
                    placeholders = ', '.join(['?'] * len(batch))

                    query = f"""SELECT specimenid, nuc_raw FROM specimen WHERE\
                                specimenid IN ({placeholders});"""
                    cursor.execute(query, batch)
                    batch_results = cursor.fetchall()
                    sequences.extend(batch_results)

                all_results.append(sequences)

            results = pool.map(_mark_duplicates, all_results)

            for result in results:
                parameters.extend(result)

            # Commit and clear parameters
            cursor.executemany(command, parameters)
            db_handle.commit()
            parameters = []
        logger.info("Finished with simple problem instances.")

        logger.info("Starting with hard problem instances.")
        
        # Start with the biggest instances first
        hard_end_list = [] # Store reduced instances here.

        for i, instance in enumerate(hard_list):
            logger.info("Processing hard instance %s of %s.", i + 1, len(hard_list))
            # Fetch all sequences for this instance
            post_instance = []
            sequences = []
            for i in range(0, len(instance), const.SQL_SAVE_NUM_VARS):
                batch = instance[i:i + const.SQL_SAVE_NUM_VARS]
                placeholders = ', '.join(['?'] * len(batch))

                query = f"""SELECT specimenid, nuc_raw FROM specimen WHERE\
                            specimenid IN ({placeholders});"""
                cursor.execute(query, batch)
                batch_results = cursor.fetchall()
                sequences.extend(batch_results)

            # Preprocessing data: Get rid of gaps and Ns, sort by length
            sequences = [(specimenid, nuc_raw.strip(STRIP_CHARS).replace('-', '')) for specimenid, nuc_raw in sequences]
            sequences.sort(key=lambda x: len(x[1]), reverse=True)

            chunks = [sequences[i:i + const.SUBPROBLEM_SIZE_MIN] for i in range(0, len(sequences), const.SUBPROBLEM_SIZE_MIN)]
            adapted_size = const.SUBPROBLEM_SIZE_MIN

            # Start by trying to find as many duplicates as possible
            while adapted_size <= const.SUBPROBLEM_SIZE_MAX:
                delete_set = set()
                parameters = []
                results = []

                logger.info("Number of chunks: %s", len(chunks))
                logger.info("Adapted size: %s", adapted_size)

                results = pool.map(_mark_duplicates_own, chunks)
                for result in results:
                    delete_set.update(result[0])
                    parameters.extend(result[1])

                new_chunks = []
                for chunk in chunks:
                    filterd_chunk = [item for item in chunk if item[0] not in delete_set]
                    if filterd_chunk:
                        new_chunks.extend(filterd_chunk)
                del(chunks)
                cursor.executemany(command, parameters)
                logger.info(f"Discarded %s duplicates.", len(delete_set))

                adapted_size += const.SUBPROBLEM_SIZE_STEP
                chunks = [new_chunks[i:i + adapted_size] for i in range(0, len(new_chunks), adapted_size)]

            # Now we can just pray that we got no realy shitty instances

            # Make sure we are still sorted and appedn to hard_end_list
            new_chunks.sort(key=lambda x: len(x), reverse=True)
            hard_end_list.append(new_chunks)

        # Now we can process the reduced instances
        # As we got all the stuff in memory anyway, just process them in parallel
        # Sort them before hand to start the longest ones first
        parameters = []
        hard_end_list.sort(key=lambda x: len(x), reverse=True)
        results = pool.map(_mark_duplicates, hard_end_list)

        for result in results:
            parameters.extend(result)

        # Commit and clear parameters
        cursor.executemany(command, parameters)
        db_handle.commit()
        parameters = []

    # Done
    logger.info("Finished with hard problem instances.")

    return
def purge_duplicates_multithreading(db_handle: sqlite3.Connection,
                                    duplicates: List[Tuple[int]]) -> None:
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

    simple_parallel_count = const.PHYSICAL_CORES_PER_CPU * const.SIMPLE_PARALLEL_FACTOR
    with Pool(processes=const.PHYSICAL_CORES_PER_CPU) as pool:

        for i in range(0, len(duplicates), simple_parallel_count):
            all_results = []
            for specimen_ids in duplicates:
                results = []
                for i in range(0, len(specimen_ids), const.SQL_SAVE_NUM_VARS):
                    batch = specimen_ids[i:i + const.SQL_SAVE_NUM_VARS]
                    placeholders = ', '.join(['?'] * len(batch))

                    query = f"""SELECT specimenid, nuc_raw FROM specimen WHERE\
                                specimenid IN ({placeholders});"""
                    cursor.execute(query, batch)
                    batch_results = cursor.fetchall()
                    results.extend(batch_results)

                all_results.append(results)

            logger.info("Starting sequence cleaning in multithreading mode.")
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
