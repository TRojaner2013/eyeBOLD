""" Module implementing location tracking and evaluation for specimen """


import csv
import logging
import os
import pandas as pd
import numpy as np
import sqlite3
import zipfile
from collections import defaultdict

from itertools import chain
from typing import Any, List, Tuple, Set, Dict
import kgcpy

from sqlite.Bitvector import BitIndex
from gbif.gbif import get_locations_sql
from gbif.gbif import get_locations
import multiprocessing as mp
import numpy as np

from multiprocessing import Pool
import time

logger = logging.getLogger(__name__)
WORKER_THREADS = 1
KOPPEN_ZONES = [
    'af', 'am', 'as', 'aw', 'bsh', 'bsk', 'bwh', 'bwk', 
    'cfa', 'cfb', 'cfc', 'csa', 'csb', 'csc', 'cwa', 'cwb',
    'cwc', 'dfa', 'dfb', 'dfc', 'dfd', 'dsa', 'dsb', 'dsc', 
    'dsd', 'dwa', 'dwb', 'dwc', 'dwd', 'ef', 'et', 'ocean'
]

def _get_keys(db_handle: sqlite3.Connection) -> List[int]:
    """Returns a list of GBIF keys for queries """

    # query = ("SELECT DISTINCT gbif_key FROM specimen WHERE"
    #          f" ((checks & (1 << {BitIndex.INCL_SUBSPECIES.value}) ="
    #          f" (1 << {BitIndex.INCL_SUBSPECIES.value}))"
    #          f" OR (checks & (3 << {BitIndex.INCL_SPECIES.value}) ="
    #          f" (1 << {BitIndex.INCL_SPECIES.value})));")
    
    query = ("SELECT DISTINCT gbif_key FROM specimen WHERE"
             f"(checks & (3 << {BitIndex.INCL_SPECIES.value}) ="
             f" (1 << {BitIndex.INCL_SPECIES.value}));")
    
    cursor = db_handle.cursor()
    cursor.execute(query)
    keys = list(chain(*cursor.fetchall()))

    return keys

def _get_file_name(zip_file: str) -> str:
    """ Validates contend of zip file and returns """
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

def validate_location(db_file: str, loc_db_file: str, batch_size: int):

    #ToDo:
    #   1. Get relevant keys from database [x]
    #   2. Create multiprocessing pool []
    #   3. Query GBIF and download data []
    #   4. Extract arcives []
    #   5. ??? (Extract relevant information) []
    #   6. PROFIT (Insert relevant information to DB) []
    #
    db_handle = sqlite3.connect(db_file)
    loc_db_handle = sqlite3.connect(loc_db_file)
    keys = _get_keys(db_handle)

    extract_path = os.path.join(".", "locationdata")
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)

    # ToDo: Async handle to download and insert at same time
    debug_file = os.path.join(".", '0005124-241024112534372.zip')
    #for zip_file in (debug_file,):
    
    #for zip_file in get_locations_sql(keys, batch_size):
    
    for zip_file in (debug_file,):
        # Unzip file
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

            # Cleanup disk space for obviouse reasons
            # os.remove(file_path)
            # os.remove(zip_file)

        except zipfile.BadZipFile:
            logger.error("Unable to extract zip-file %s", zip_file)
            continue

#ToDo: HIGH IMPORANT
# Define column names as constants
def _get_kg_zone_vec(df, epsilon=1e-6) -> str:
    """ Returns the Koeppen-Geiger zone for given coordinates """

    helper = df.copy()
    # Constrain latitude to (-90 + epsilon, 90 - epsilon)
    helper['decimallatitude'] = np.clip(df['decimallatitude'].astype(float), -90.0 + epsilon, 90.0 - epsilon)
    
    # Constrain longitude to (-180 + epsilon, 180 - epsilon)
    helper['decimallongitude'] = np.clip(df['decimallongitude'].astype(float), -180.0 + epsilon, 180.0 - epsilon)
        
    helper['kg_zone'] = kgcpy.vectorized_lookupCZ(helper['decimallatitude'], helper['decimallongitude'])

    # This is needed to make the insertion process smother and faster
    helper['kg_zone'] = helper['kg_zone'].astype(str).str.lower()
    return helper

def _get_kg_zone(row, epsilon=1e-6) -> str:
    """ Returns the Koeppen-Geiger zone for given coordinates """

    lat = float(row['decimallatitude'])
    lon = float(row['decimallongitude'])

    # Constrain latitude to (-90 + epsilon, 90 - epsilon)
    lat = max(-90.0 + epsilon, min(90.0 - epsilon, lat))
    
    # Constrain longitude to (-180 + epsilon, 180 - epsilon)
    lon = max(-180.0 + epsilon, min(180.0 - epsilon, lon))

    #logger.debug("Checking location: ", lat, lon)
    return kgcpy.lookupCZ(lat, lon)

def _extract_information(tsv_file: str, db_handle: sqlite3.Connection) -> None:
    """ Extracts relevant information from downloaded data"""
    chunk_size = 10000000
    cursor = db_handle.cursor()
    # gbif_id seems to be not unique  thus use update to avoid failures.
    command = ("INSERT INTO gbif_info (taxon_key, occurrenceid, long, latt, kg_zone)"
               "VALUES (?, ?, ?, ?, ?);")

    logger.info("Starting to insert location data into DB with chunk_size of %s", chunk_size)
    with pd.read_csv(tsv_file, sep='\t',
                     usecols=["acceptedtaxonkey", "decimallatitude", "decimallongitude", "countrycode"],
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

def _pickable_defaultdict_creator():
    return defaultdict(int)

def process_chunk(chunk: pd.DataFrame) -> Tuple[Dict[int, Dict[str, int]], Dict[int, Set[str]]]:
    """
    Processes a single chunk of data, returning aggregated results for each taxon.
    """
    chunk = chunk.dropna()
    chunk = _get_kg_zone_vec(chunk)
    taxon_data = defaultdict(_pickable_defaultdict_creator)
    country_codes = defaultdict(set)
    
    for _, row in chunk.iterrows():
        taxon_id = row['acceptedtaxonkey']
        kg_zone = row['kg_zone']
        country_code = row['countrycode']

        taxon_data[taxon_id][kg_zone] += 1
        country_codes[taxon_id].add(country_code)

    
    # print(taxon_data, country_codes)
    return taxon_data, country_codes

# Combine results from parallel processing
def combine_results(results: List[Tuple[Dict[int, Dict[str, int]], Dict[int, Set[str]]]]):
    """
    Combines results from multiple processed chunks into a single dictionary.
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

# Main function to extract and process information with multiprocessing
def _extract_information_2(tsv_file: str, db_handle: sqlite3.Connection, num_processes: int = 8) -> None:
    """
    Extracts relevant information from downloaded data and aggregates it using parallel processing.
    """
    chunk_size = 1000000
    pool = mp.Pool(num_processes)

    # List to hold future results for each processed chunk
    results = []

    logger.info("Starting to process location data from TSV file with chunk_size of %s", chunk_size)

    with pd.read_csv(tsv_file, sep='\t',
                     usecols=["acceptedtaxonkey", "decimallatitude", "decimallongitude", "countrycode"],
                     encoding='utf-8',
                     quoting=csv.QUOTE_NONE,
                     on_bad_lines='warn',
                     chunksize=chunk_size) as tsv_reader:
        
        # Send each chunk to the pool for parallel processing
        for chunk in tsv_reader:
            result = pool.apply_async(process_chunk, args=(chunk,))
            results.append(result)

    # Close pool and wait for all processes to complete
    pool.close()
    pool.join()

    # Retrieve results and combine them
    processed_results = [result.get() for result in results]
    #print(processed_results)
    taxon_data, country_codes = combine_results(processed_results)
    #print(taxon_data, country_codes)

    # Prepare to write to the database
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

    # Prepare the data for bulk insertion as a list of dictionaries
    data_to_insert = []
    for taxon_id, zones in taxon_data.items():
        # Create a dictionary for the current taxon
        row_data = {'taxon_key': taxon_id}
        
        # Fill in climate zone counts, ensuring names match the SQL placeholders
        for zone in KOPPEN_ZONES:
            row_data[zone] = zones.get(zone, 0)
            
        # Add the country codes as a comma-separated string
        row_data['country_codes'] = ','.join(sorted(country_codes[taxon_id]))
        
        # Append the dictionary to data_to_insert
        data_to_insert.append(row_data)

    # Execute the insert command using named parameters for each row
    cursor = db_handle.cursor()
    cursor.executemany(command, data_to_insert)
    db_handle.commit()
    logger.info("Inserted aggregated data for %d taxon records into database.", len(data_to_insert))