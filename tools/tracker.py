""" Module implementing location tracking and evaluation for specimen """


import csv
import logging
import os
import pandas as pd
import sqlite3
import zipfile

from itertools import chain
from typing import Any, List

from sqlite.Bitvector import BitIndex
from gbif.gbif import get_locations

logger = logging.getLogger(__name__)
WORKER_THREADS = 1

def _get_keys(db_handle: sqlite3.Connection) -> List[int]:
    """Returns a list of GBIF keys for queries """

    query = ("SELECT DISTINCT gbif_key FROM specimen WHERE"
             f" ((checks & (1 << {BitIndex.INCL_SUBSPECIES.value}) ="
             f" (1 << {BitIndex.INCL_SUBSPECIES.value}))"
             f" OR (checks & (3 << {BitIndex.INCL_SPECIES.value}) ="
             f" (1 << {BitIndex.INCL_SPECIES.value})));")
    
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

def validate_location(db_file: str, batch_size: int):

    #ToDo:
    #   1. Get relevant keys from database [x]
    #   2. Create multiprocessing pool []
    #   3. Query GBIF and download data []
    #   4. Extract arcives []
    #   5. ??? (Extract relevant information) []
    #   6. PROFIT (Insert relevant information to DB) []
    #
    db_handle = sqlite3.connect(db_file)
    keys = _get_keys(db_handle)

    extract_path = os.path.join(".", "locationdata")
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)

    # ToDo: Async handle to download and insert at same time
    for zip_file in get_locations(keys, batch_size):
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
            _extract_information(file_path, db_handle)

            # Cleanup disk space for obviouse reasons
            os.remove(file_path)
            os.remove(zip_file)

        except zipfile.BadZipFile:
            logger.error("Unable to extract zip-file %s", zip_file)
            continue

def _extract_information(tsv_file: str, db_handle: sqlite3.Connection) -> None:
    """ Extracts relevant information from downloaded data"""
    chunk_size = 10000000
    cursor = db_handle.cursor()
    command = ("INSERT INTO gbif_info (gbif_id, gbif_key, long, latt)"
               "VALUES (?, ?, ?, ?);")

    logging.info("Starting to insert location data into DB with chunk_size of %s", chunk_size)
    with pd.read_csv(tsv_file, sep='\t',
                     usecols=["gbifID", "decimalLatitude", "decimalLongitude", "speciesKey"],
                     encoding='utf-8',
                     quoting= csv.QUOTE_NONE,
                     on_bad_lines='warn',
                     chunksize=chunk_size) as tsv_reader:
        for chunk in tsv_reader:
            chunk = chunk.dropna()
            data = list(zip(chunk["gbifID"],
                            chunk["speciesKey"],
                            chunk["decimalLongitude"],
                            chunk["decimalLatitude"]))
            logging.info("Dropped %s locations due to NAN/Null values.", chunk_size-len(data))
            cursor.executemany(command, data)
            db_handle.commit()
