"""Module that builds our databse """

import logging
import sqlite3
from typing import Dict, Tuple, List

from common.helper import file_exist
import common.constants as const
from sqlite.db_layout import CreateCommands
from sqlite.parser import get_data_layout, get_create_command, TsvParser, TsvUpdateParser

logger = logging.getLogger(__name__)

def execute_batches(db_handle:sqlite3.Connection,
                    commands: List[Tuple[str,List[str]]],
                    retrive: bool=False) -> List:
    """ Executes a batch of commands one after another

        Note:
            This does not uses the executemany method provided by sqlite3.
            The mere intention is to collect and then execute statments to work a
            bit more efficently on the database.

        Args:
            - db_handle (sqlite3.Connection): Connection to database
            - commands (List[Tuple[str, List[str]]]): List of tuples with
                                                      command and values that
                                                      are inserted.
            - retrive: Fetches results of commands

        Returns:
            List of fetched data if any. Empty list on retrive = False.
    """
    cursor = db_handle.cursor()
    results = []
    try:
        for command, values in commands:
            cursor.execute(command, values)
            if retrive:
                result = cursor.fetchone()
                results.append(result[0])
        db_handle.commit()
    except Exception as err:
        db_handle.rollback()
        print(f"Error executing commands: {err}")
        raise

    return results

def open_db_file(path: str) -> sqlite3.Connection:
    """ Opens database file and returns connection handle.

    Args:
        - path (str): Path to database file

    Returns:
        sqlite3 connection on success.

    Raises:
        FileNotFoundError: if path is not existing 
    """

    if file_exist(path):
        return sqlite3.connect(path)

    raise FileNotFoundError("Database is not available.")

def create_db_file(path: str) -> bool:
    """Creates a new database.

        Args:
            - path (str): Path to database file

        Returns:
            True on success.

        Raises:
            FileExistsError: If file exists
            IOError: On IOError during creation.
    """

    if file_exist(path):
        logger.error("Database %s already exists.", path)
        raise FileExistsError

    try:
        with open(path, 'w+', encoding='utf-8') as handle:
            handle.close()
    except IOError as err:
        logger.error("Unable to create database %s: %s", path, err)
        raise

    return True

def create_database(db_handle: sqlite3.Connection,
                    tsv_file: str,
                    datapackage: str,
                    marker_code: str) -> bool:
    """
    Creates database for eyebold.

    Args:
        - db_handle (sqlite3.Connecton): Connection to database
        - tsv_file (str): Path to tsv_file
        - datapackage (str): Path to datapackage file
        - marker_code (str): Marker code to use

    Returns:
        True on success

    Raises:
        ValueError: If some file does not exist.
    """

    logger.info("Starting to build a new database...")

    if not file_exist(tsv_file):
        logger.critical("Input tsv file %s does not exists.", tsv_file)
        raise ValueError(f"File {tsv_file} does not exists.")

    if not file_exist(datapackage):
        logger.critical("Input datapackage file %s does not exists.", datapackage)
        raise ValueError(f"File {datapackage} does not exists.")

    layout, parser_dict = get_data_layout(datapackage)
    if layout is False:
        logger.critical('Unexpected problems reading datapackage file...')
        raise ValueError('Unable to read datapackage file.')

    command = get_create_command("processing_input", layout)

    try:
        db_handle.execute(command)

    except sqlite3.Error as err:
        logger.critical("Unable to create database: %s", err)
        logger.info("Command: %s", command)
        return False

    if not _create_table(db_handle, CreateCommands.SPECIMEN_CMD):
        return False

    db_handle.commit()

    # Create index on gbif_key
    idx_cmd = "CREATE INDEX idx_gbif_key ON specimen(gbif_key);"
    db_handle.execute(idx_cmd)
    db_handle.commit()

    logger.info("Created tables for new database...")
    logger.info("Inserting data into new database with a batch size of %s.",
                const.BUILD_CHUNK_SIZE)

    # Read datapackage file and insert tsv_data:
    tables = TsvParser(tsv_file, marker_code, parser_dict)
    table1_batch, table2_batch = [], []

    for table_dict in tables:
        for table_name, row in table_dict.items():
            # Assuming table1 and table2 based on table_name logic
            if table_name == "processing_input":
                table1_batch.append(row)
            elif table_name == "specimen":
                table2_batch.append(row)

            if (len(table1_batch) == const.BUILD_CHUNK_SIZE or
                len(table2_batch) == const.BUILD_CHUNK_SIZE):

                logger.info("Inserting batch into database.")
                _insert_batch(db_handle, "processing_input", table1_batch)
                _insert_batch(db_handle, "specimen", table2_batch)

                table1_batch, table2_batch = [], []

    if table1_batch:
        logger.info("Inserting last batch into processing_input.")
        _insert_batch(db_handle, "processing_input", table1_batch)
    if table2_batch:
        logger.info("Inserting last batch into specimen.")
        _insert_batch(db_handle, "specimen", table2_batch)

    logger.info("Successfully created new database...")
    return True

def _insert_batch(db_handle: sqlite3.Connection,
                  table_name: str,
                  batch: List[Dict[str, str]]) -> bool:
    """ Inserts a batch of data into database

        Args:
            - db_handle (sqlite3.Connection): Connection to database
            - table_name (str): Name of table
            - batch (List[Dict[str, str]]): List of dictionaries with data

        Returns:
            True on success, False otherwise
    """

    try:
        columns = ', '.join(batch[0].keys())
        placeholders = ', '.join(['?'] * len(batch[0]))
        command = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = [tuple(x.values()) for x in batch]
        values = tuple(values)
        values = iter(values)
        db_handle.executemany(command, values)
        db_handle.commit()
        return True

    except sqlite3.Error as err:
        logger.critical("Unable to insert data in processing_input: %s", err)
        logger.critical("Command: %s", command)
        logger.critical("Values: %s", values)
        db_handle.rollback()
        #db_handle.close()
        return False

def _create_table(db_handle: sqlite3.Connection, command: CreateCommands) -> bool:
    """ Creates a table with a provided command

        Args:
            - db_handle (sqlite3.Connection): Connection to database
            - command (CreateCommands): Command to execute

        Returns:
            True on success, False otherwise
    """

    try:
        cursor = db_handle.cursor()
        cursor.execute(command.value)
        db_handle.commit()
        return True

    except sqlite3.Error as err:
        logger.error("An error occurred: %s", err)
        logger.info("Perfoming database rollback...")
        db_handle.rollback()
        return False

def insert_updates(db_file: str, tsv_file: str, datapackage: str,
                   marker: str) -> Tuple[List[int], List[Tuple[int,int]]]:
    """ Creates a new, temporary table to store potential updates.

        Args:
            - db_file (str): Path to database file.
            - tsv_file (str): Path to TSV file.
            - datapackage (str): Path to datapackage file.

        Returns:
            Lists with new and updated specimen ids.
    """

    if not file_exist(tsv_file):
        logger.critical("Input tsv file %s does not exists.", tsv_file)
        raise FileNotFoundError(f"File {tsv_file} does not exists.")

    if not file_exist(datapackage):
        logger.critical("Input datapackage file %s does not exists.", datapackage)
        raise FileNotFoundError(f"File {datapackage} does not exists.")

    logger.info("Starting to build update table database...")

    # Open connection, and parse datapackage file to check laycout compadiablity
    db_handle = sqlite3.connect(db_file)
    cursor = db_handle.cursor()

    layout, parser_dict = get_data_layout(datapackage)
    if layout is False:
        logger.critical('Unexpected problems reading datapackage file...')
        raise ValueError('Unable to process datapackage file.')

    logger.info("Inserting updated infos with a batch size of %s.", const.UPDATE_CHUNK_SIZE)

    # Read datapackage file and insert tsv_data:
    possible_updates = TsvUpdateParser(tsv_file, marker, parser_dict)

    new_ids = []
    updated_ids = []

    table_batch = []
    check_cmd = "SELECT gbif_key, hash FROM specimen WHERE specimenid = ?"

    #ToDo: Chanke this to work on batches...
    for id_, hash_, row in possible_updates:

        cursor.execute(check_cmd, (id_,))
        result = cursor.fetchone()

        if result:
            if hash_ != result[1]:
                updated_ids.append([id_, result[0]])
            else:
                continue
        else:
            new_ids.append(id_)

        table_batch.append(row)

        if len(table_batch) == const.UPDATE_CHUNK_SIZE:
            logger.info("Inserting batch into database.")
            _insert_batch(db_handle, "specimen", table_batch)

    if table_batch:
        logger.info("Inserting last batch into specimen.")
        _insert_batch(db_handle, "specimen", table_batch)

    logger.info("Successfully updated database entries...")
    logger.info("New entries: %s", len(new_ids))
    logger.info("Updated entries: %s", len(updated_ids))
    return (new_ids, updated_ids)
