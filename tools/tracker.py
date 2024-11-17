""" Module implementing location tracking and evaluation for specimen """

from collections import defaultdict
import csv
from itertools import chain
import logging
import multiprocessing as mp
import os
import sqlite3
from typing import List, Tuple, Set, Dict
import zipfile

from sqlite.Bitvector import BitIndex
from gbif.gbif import get_locations
import common.constants as const

import kgcpy # type: ignore # pylint: disable=import-error
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

#ToDo: Move to constants?
KOPPEN_ZONES = [
    'af', 'am', 'as', 'aw', 'bsh', 'bsk', 'bwh', 'bwk', 
    'cfa', 'cfb', 'cfc', 'csa', 'csb', 'csc', 'cwa', 'cwb',
    'cwc', 'dfa', 'dfb', 'dfc', 'dfd', 'dsa', 'dsb', 'dsc', 
    'dsd', 'dwa', 'dwb', 'dwc', 'dwd', 'ef', 'et', 'ocean'
]

def _get_keys(db_handle: sqlite3.Connection) -> List[int]:
    """ Returns list of uncheked gbif_keys from the database

        Args:
            - db_handle (sqlite3.Connection): Connection to the database

        Returns:
            List[int]: List of gbif_keys to check
    """

    # We only want to include entires we have not checked before.
    query = ("SELECT DISTINCT gbif_key FROM specimen WHERE"
             f"(checks & (1 << {BitIndex.INCL_SPECIES.value}) ="
             f" (1 << {BitIndex.INCL_SPECIES.value}))"
             f"AND (checks & (1 << {BitIndex.LOC_CHECKED.value})) = 0;")

    cursor = db_handle.cursor()
    cursor.execute(query)
    keys = list(chain(*cursor.fetchall()))

    return keys

def _get_file_name(zip_file: str) -> str:
    """ Validates contents of zip-file

        Args:
            - zip_file (str): Path to the zip-file
    
        Returns:
            Tuple[bool, str]: Tuple with status and file name
    """

    #ToDo: Raise exception instead of returning tuple
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_handle:
            file_list = zip_handle.namelist()

            if len(file_list) == 1:
                return (True, file_list[0])

            logger.error("Downloaded file %s contains to many files:\n%s",
                         zip_file, file_list)
            return (False, "")

    except zipfile.BadZipFile:
        logger.error("Downloaded file %s is invalid.", zip_file)
        return (False, "")

def _get_new_keys(loc_db_handle: sqlite3.Connection, keys: List[int]) -> Tuple[List[int],List[int]]:
    """ Returns all gibf_keys in keys that are not already in the location database 

        Args:
            - loc_db_handle (sqlite3.Connection): Connection to the location database
            - keys (List[int]): List of gbif_keys to check

        Returns:
            List[int]: List of gbif_keys that are not already in the database
            List[int]: List of gbif_keys that are already in the database

    """
    cursor = loc_db_handle.cursor()

    cursor.execute("SELECT taxon_key FROM climate_data")
    existing_keys = set(row[0] for row in cursor.fetchall())
    new_keys = [key for key in keys if key not in existing_keys]

    return new_keys, list(existing_keys)

def _mark_keys_as_checked(db_handle: sqlite3.Connection,
                          loc_db_handle: sqlite3.Connection,
                          keys: List[int]) -> None:
    """ Marks a gbif_key as checked in the database 

        Note:
            We mark gbif_keys as checked, so specimens without any occurrence data are not
            downloaded again and again.

        Args:
            - db_handle (sqlite3.Connection): Connection to the database
            - loc_db_handle (sqlite3.Connection): Connection to the location database
            - keys (List[int]): gbif_keys to mark as checked
    """

    _evaluate_location(db_handle, loc_db_handle, keys)

    command = f"""UPDATE specimen SET checks = checks | (1 << {BitIndex.LOC_CHECKED.value}) WHERE gbif_key=?;"""
    cursor = db_handle.cursor()
    cursor.executemany(command, [(key,) for key in keys])
    db_handle.commit()


def validate_location(db_file: str, loc_db_file: str, batch_size: int):
    """ Downloads and evaluates occurrence data

    Note:
        The download consumes a lot of time and resources (disk space).

    Args:
        - db_file (str): Path to the specimen database file
        - loc_db_file (str): Path to the location database file
        - batch_size (int): Number of gbif_keys to download at once
    """

    db_handle = sqlite3.connect(db_file)
    loc_db_handle = sqlite3.connect(loc_db_file)

    keys = _get_keys(db_handle)
    logger.info("Stating downloading for %s keys", len(keys))

    keys, old_keys = _get_new_keys(loc_db_handle, keys)
    logger.info("Checking keys reduced keys to download to %s keys", len(keys))

    # Make sure that all already downloaded keys are marked as checked
    _mark_keys_as_checked(db_handle, loc_db_handle, old_keys)
    logging.info("Marked %s keys as checked", len(old_keys))

    extract_path = os.path.join(".", "locationdata")
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)

    # Use a already downloaded file for debugging
    #debug_file = os.path.join(".", '0005124-241024112534372.zip')
    #for zip_file in (debug_file,):

    for zip_file, incl_keys in get_locations(keys, batch_size):

        status, csv_file = _get_file_name(zip_file)
        if not status:
            logger.error("Unable to access location data in file %s", zip_file)
            continue
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_handle:
                logger.info("Extracting file %s from %s...", csv_file, zip_file)
                zip_handle.extract(csv_file, extract_path)

            file_path = os.path.join(extract_path, csv_file)
            _extract_information_2(file_path, loc_db_handle)
            _mark_keys_as_checked(db_handle, loc_db_handle, incl_keys)

            # Cleanup disk space for obviouse reasons
            os.remove(file_path)
            os.remove(zip_file)

        except zipfile.BadZipFile:
            logger.error("Unable to extract zip-file %s", zip_file)
            continue

#ToDo: Define column names as constants
def _get_kg_zone_vec(data: pd.DataFrame, epsilon=1e-6) -> pd.DataFrame:
    """ Returns the Koeppen-Geiger zone for given coordinates

        Args:
            - data (pd.DataFrame): Dataframe with columns decimallatitude 
                                   and decimallongitude
            - epsilon (float): Small value to avoid providing exact values
                               at the poles and the dateline
        Returns:
            pd.DataFrame: Dataframe with additional column kg_zone containing
                          the Koeppen-Geiger zone
    """

    helper = data.copy()

    lat_col = 'decimalLatitude'
    lon_col = 'decimalLongitude'
    if const.USE_GBIF_SQL:
        lat_col = 'decimallatitude'
        lon_col = 'decimallongitude'

    # Constrain latitude to (-90 + epsilon, 90 - epsilon)
    helper[lat_col] = np.clip(data[lat_col].astype(float),
                                        -90.0 + epsilon, 90.0 - epsilon)

    # Constrain longitude to (-180 + epsilon, 180 - epsilon)
    helper[lon_col] = np.clip(data[lon_col].astype(float),
                                         -180.0 + epsilon, 180.0 - epsilon)

    helper['kg_zone'] = kgcpy.vectorized_lookupCZ(helper[lat_col],
                                                  helper[lon_col])

    # This is needed to make the insertion process smother and faster
    helper['kg_zone'] = helper['kg_zone'].astype(str).str.lower()
    return helper

def _get_kg_zone(row, epsilon=1e-6) -> str:
    """ Returns the Koeppen-Geiger zone for given coordinate

        Note: This funciton is not used anymore, but kept for reference.
    """

    lat = float(row['decimallatitude'])
    lon = float(row['decimallongitude'])

    # Constrain latitude to (-90 + epsilon, 90 - epsilon)
    lat = max(-90.0 + epsilon, min(90.0 - epsilon, lat))

    # Constrain longitude to (-180 + epsilon, 180 - epsilon)
    lon = max(-180.0 + epsilon, min(180.0 - epsilon, lon))

    #logger.debug("Checking location: ", lat, lon)
    return kgcpy.lookupCZ(lat, lon)

def _extract_information(tsv_file: str, db_handle: sqlite3.Connection) -> None:
    """ Extracts relevant information from downloaded data

        Note: This function is not used anymore, but kept for reference.
    """
    chunk_size = 10000000
    cursor = db_handle.cursor()
    # gbif_id seems to be not unique  thus use update to avoid failures.
    command = ("INSERT INTO gbif_info (taxon_key, occurrenceid, long, latt, kg_zone)"
               "VALUES (?, ?, ?, ?, ?);")

    logger.info("Starting to insert location data into DB with chunk_size of %s", chunk_size)
    with pd.read_csv(tsv_file, sep='\t',
                     usecols=[
                                "acceptedtaxonkey", "decimallatitude",
                                "decimallongitude", "countrycode"
                             ],
                     encoding='utf-8',
                     quoting= csv.QUOTE_NONE,
                     on_bad_lines='warn',
                     chunksize=chunk_size) as tsv_reader:
        for chunk in tsv_reader:
            chunk = chunk.dropna()
            # Add additional column with loaction information
            logger.debug("Checking locations...")
            chunk = _get_kg_zone_vec(chunk)
            #chunk['lookupCZ'] = chunk.apply(_get_kg_zone, axis=1)
            logger.debug("PASSED checking locations...")
            data = list(zip(chunk["acceptedtaxonkey"],
                            chunk["occurrenceid"],
                            chunk["decimallongitude"],
                            chunk["decimallatitude"],
                            chunk['kg_zone']))
            logger.info("Dropped %s locations due to NAN/Null values.", chunk_size-len(data))
            # Ensure there is still data left to insert.
            if len(data) > 0:
                cursor.executemany(command, data)
                db_handle.commit()

def _pickable_defaultdict_creator() -> defaultdict:
    """ Returns a default dictionary with int as default value

        Note: This is needed to make multiprocessing work

        Returns:
            defaultdict: Default dictionary with int as default
    """
    return defaultdict(int)

def process_chunk(chunk: pd.DataFrame) -> Tuple[Dict[int, Dict[str, int]], Dict[int, Set[str]]]:
    """ Processes a single chunk of data, returning aggregated results for each
        taxon.

        Args:
            - chunk (pd.DataFrame): Dataframe with columns acceptedtaxonkey,
                                    kg_zone and countrycode

        Returns:
            Tuple[Dict[int, Dict[str, int]], Dict[int, Set[str]]]: Tuple containing
            aggregated data for each taxon and a set of country codes for each taxon
            provided in a dictionary.
    """
    chunk = chunk.dropna()
    chunk = _get_kg_zone_vec(chunk)
    taxon_data = defaultdict(_pickable_defaultdict_creator)
    country_codes = defaultdict(set)

    key_col = 'speciesKey'
    country_col = 'countryCode'
    if const.USE_GBIF_SQL:
        key_col = 'acceptedtaxonkey'
        country_col = 'countrycode'

    for _, row in chunk.iterrows():
        taxon_id = row[key_col]
        kg_zone = row['kg_zone']
        country_code = row[country_col]

        taxon_data[taxon_id][kg_zone] += 1
        country_codes[taxon_id].add(country_code)

    return taxon_data, country_codes

def combine_results(results: List[Tuple[Dict[int, Dict[str, int]], Dict[int, Set[str]]]]):
    """ Combines results from multiple processed chunks into a single dictionary.

        Note: This function is mandatory as data in download is not sorted and
              thus each chunk can contain data from any gbif_key the same taxon
        Args:
            - results (List[Tuple[Dict[int, Dict[str, int]], Dict[int, Set[str]]]): 
                List of results from process_chunk

        Returns:
            Tuple[Dict[int, Dict[str, int]], Dict[int, Set[str]]]: Tuple containing
            aggregated data for each taxon and a set of country codes for each taxon
            provided in a dictionary.
    """
    aggregated_data = defaultdict(_pickable_defaultdict_creator)
    aggregated_countries = defaultdict(set)

    for taxon_data, country_codes in results:
        for taxon_id, zones in taxon_data.items():
            for zone, count in zones.items():
                aggregated_data[taxon_id][zone] += count
        for taxon_id, codes in country_codes.items():
            aggregated_countries[taxon_id].update(codes)

    return aggregated_data, aggregated_countries

def _extract_information_2(tsv_file: str, db_handle: sqlite3.Connection,
                           num_processes: int = const.PHYSICAL_CORES_PER_CPU) -> None:
    """ Extracts relevant information from downloaded data and aggregates it using
        multiprocessing.

        Args:
            - tsv_file (str): Path to the TSV file containing location data
            - db_handle (sqlite3.Connection): Connection to the location database
            - num_processes (int): Number of processes to use for parallel processing
    """
    chunk_size = const.TRACKER_CHUNK_SIZE
    pool = mp.Pool(num_processes)

    results = []

    logger.info("Starting to process location data from TSV file with chunk_size of %s", chunk_size)

    usecols = ["speciesKey", "decimalLatitude",
                "decimalLongitude", "countryCode"]

    if const.USE_GBIF_SQL:
        usecols = ["acceptedtaxonkey", "decimallatitude",
                   "decimallongitude", "countrycode"]

    with pd.read_csv(tsv_file, sep='\t',
                     usecols=usecols,
                     encoding='utf-8',
                     quoting=csv.QUOTE_NONE,
                     on_bad_lines='warn',
                     chunksize=chunk_size) as tsv_reader:

        for chunk in tsv_reader:
            result = pool.apply_async(process_chunk, args=(chunk,))
            results.append(result)

    pool.close()
    pool.join()

    # Retrieve results and combine them
    processed_results = [result.get() for result in results]
    taxon_data, country_codes = combine_results(processed_results)

    # Prepare to write to the location database
    command = """
        INSERT OR REPLACE INTO climate_data (taxon_key, kg_af, kg_am, kg_as, kg_aw,
                                            kg_bsh, kg_bsk, kg_bwh, kg_bwk, kg_cfa,
                                            kg_cfb, kg_cfc, kg_csa, kg_csb, kg_csc,
                                            kg_cwa, kg_cwb, kg_cwc, kg_dfa, kg_dfb,
                                            kg_dfc, kg_dfd, kg_dsa, kg_dsb, kg_dsc,
                                            kg_dsd, kg_dwa, kg_dwb, kg_dwc, kg_dwd,
                                            kg_ef, kg_et, kg_ocean, country_codes)
        VALUES (:taxon_key, :af, :am, :as, :aw, :bsh, :bsk, :bwh, 
                :bwk, :cfa, :cfb, :cfc, :csa, :csb, :csc, :cwa, 
                :cwb, :cwc, :dfa, :dfb, :dfc, :dfd, :dsa, :dsb, 
                :dsc, :dsd, :dwa, :dwb, :dwc, :dwd, :ef, :et, 
                :ocean, :country_codes)
    """

    data_to_insert = []
    for taxon_id, zones in taxon_data.items():
        row_data = {'taxon_key': taxon_id}

        for zone in KOPPEN_ZONES:
            row_data[zone] = zones.get(zone, 0)

        row_data['country_codes'] = ','.join(sorted(country_codes[taxon_id]))

        data_to_insert.append(row_data)

    cursor = db_handle.cursor()
    cursor.executemany(command, data_to_insert)
    db_handle.commit()
    logger.info("Inserted aggregated data for %d taxon records into database.",
                len(data_to_insert))

def _evaluate_location(db_handle: sqlite3.Connection, loc_db_handle: sqlite3.Connection,
                       keys: List[int]) -> None:
    """ Evaluates the likelihood of a location being correct 
        The result is written into the database.

    Args:
        - db_handle (sqlite3.Connection): Connection to the location database
        - loc_db_handle (sqlite3.Connection): Connection to the location database
        - keys (List[int]): List of gbif_keys to evaluate
    """

    # Create a dictionary cursor to store the results
    loc_db_handle.row_factory = sqlite3.Row
    loc_cursor = loc_db_handle.cursor()

    db_cursor = db_handle.cursor()
    # Get all taxon keys we have data on
    for key in keys:

        loc_cursor.execute("SELECT * FROM climate_data WHERE taxon_key = ?;", (key,))
        data = loc_cursor.fetchone()

        if not data:
            # Species not in database --> No locaton data in GBIF
            # -> Mark as not verifiable
            # ToDo: Keep a trace of this in location database to avoid redownloading on new build.
            command = f"UPDATE specimen SET geo_info = -1,  checks = checks | (1 << {BitIndex.LOC_EMPTY.value}) WHERE gbif_key = ?;"
            db_cursor.execute(command, (key,))
            loc_db_handle.commit()
            continue

        # Process data
        country_list = set(data['country_codes'].split(',')) if data['country_codes'] else set()
        total_occurrences = sum(data["kg_"+zone] for zone in KOPPEN_ZONES)

        # Get all occurrences for the taxon key
        db_cursor.execute("SELECT specimenid, country_iso, kg_zone FROM specimen WHERE gbif_key = ?;", (key,))
        occurrences = db_cursor.fetchmany(const.TRACKER_INSERT_CHUNK_SIZE)

        # Store results
        command = f"""UPDATE specimen SET geo_info = ?, checks = checks | (? << {BitIndex.LOC_PASSED.value}) WHERE specimenid = ?;"""
        while occurrences:
            results = []
            for occurrence in occurrences:
                score = 0
                flag = 0
                specimen_id, country_iso, kg_zone = occurrence

                if country_iso is not None and country_iso.upper() in country_list:
                    score = score + 2

                if kg_zone is not None and data['kg_'+kg_zone.lower()] > 0:
                    score = score + 1
                    score = score + data['kg_'+kg_zone.lower()] / total_occurrences

                if score > 0:
                    flag = 1

                results.append((score, flag, specimen_id))

            db_cursor.executemany(command, results)
            db_handle.commit()
            occurrences = db_cursor.fetchmany(const.TRACKER_INSERT_CHUNK_SIZE)

    # Reset behavior of the cursor
    loc_db_handle.row_factory = None
