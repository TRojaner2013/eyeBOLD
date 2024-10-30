"""Module that build a databse """

import sqlite3
import warnings
import logging
from typing import Dict, Tuple, List, Any

from sqlite.db_layout import CreateCommands
from sqlite.parser import get_data_layout, get_create_command, TsvParser
from common.helper import file_exist

logger = logging.getLogger(__name__)

# ToDo: Create helper module for this:
def execute_batches(db_handle:sqlite3.Connection,
                    commands: List[Tuple[str,List[str]]],
                    retrive: bool=False) -> List:
    """ Executes a batch of commands

        Note that this does not uses the executemany method provided by sqlite3.
        The mere intention is to collect and then execute statments to work a
        bit more efficently on the database.

        Arguments:
            - db_handle(sqlite3.Connection): Connection to database
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
    except Exception as e:
        db_handle.rollback()
        print(f"Error executing commands: {e}")
        raise

    return results

def open_db_file(path: str) -> sqlite3.Connection:
    """ Opens database file and returns connection handle.

    Arguments:
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

        Arguments:
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
    except IOError as e:
        logger.error("Unable to create database %s: %s", path, e)
        raise

    return True

def create_database(db_handle: sqlite3.Connection,
                    tsv_file: str,
                    datapackage: str,
                    marker_code: str) -> bool:
    """
    Creates database for eyebold.

    Arguments:
        - db_handle (sqlite3.Connecton): Connection to database
        - tsv_file (str): Path to tsv_file
        - datapackage (str): Path to datapackage file
        - marker_code (str): Marker code to use

    Returns: True on success

    Raises:
        ValueError: If files do not exist.
    """

    logger.info("Starting to build a new database...")

    if not file_exist(tsv_file):
        logger.critical("Input tsv file %s does not exists.", tsv_file)
        raise ValueError(f"File {tsv_file} does not exists.")

    if not file_exist(datapackage):
        logger.critical("Input datapackage file %s does not exists.", datapackage)
        raise ValueError(f"File {datapackage} does not exists.")

    # We need to create the database in the following order:
    # 1. input database
    # 2. GBIF data
    # 3. Taxonomy data
    # 4. Specimen data

    layout, parser_dict = get_data_layout(datapackage)
    if layout is False:
        logger.critical('Unexpected problems reading datapackage file...')
        raise ValueError('Unable to read datapackage file.')

    command = get_create_command("processing_input", layout)

    try:
        db_handle.execute(command)

    except sqlite3.Error as e:
        logger.critical("Unable to create database: %s", e)
        logger.info("Command: %s", command)
        return False

    if not _create_table(db_handle, CreateCommands.SPECIMEN_CMD):
        return False

    db_handle.commit()

    # Create index on gbif_key
    idx_cmd = "CREATE INDEX idx_gbif_key ON specimen(gbif_key);"
    db_handle.execute(idx_cmd)
    db_handle.commit()

    batch_size = 1000000
    logger.info("Created tables for new database...")
    logger.info("Inserting data into new database with a batch size of %s.", batch_size)

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

            if len(table1_batch) == batch_size or len(table2_batch) == batch_size:
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

    except sqlite3.Error as e:
        logger.critical("Unable to insert data in processing_input: %s", e)
        logger.critical("Command: %s", command)
        logger.critical("Values: %s", values)
        db_handle.rollback()
        #db_handle.close()
        return False

def _create_table( db_handle: sqlite3.Connection, command: CreateCommands) -> bool:
    """ Creates a table with command"""

    try:
        cursor = db_handle.cursor()
        cursor.execute(command.value)
        db_handle.commit()
        return True

    except sqlite3.Error as e:
        logger.error("An error occurred: %s", e)
        logger.info("Perfoming database rollback...")
        db_handle.rollback()
        return False

def _generate_insert_statement(table_name: str, data: dict) -> Tuple[str, Tuple[Any]]:
    """ Generates inset statments for sql"""

    warnings.warn("Function will be removed.", DeprecationWarning)
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?' for _ in data])
    values = tuple(data.values())

    sql = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'

    return sql, values

def create_in_tables(db_handle: sqlite3.Connection,
                     name_list: List[str],
                     layout_dict: Any) -> None:
    """Creates tables from a single column layout template.

    Args:
        db_handle (sqlite3.Connection): SQLite3 database connection handle.
        name_list (List[str]): List of table names to be created.
        layout_dict (List[ColumnInfo]): List containing the schema for all tables.
    """

    warnings.warn("Function will be removed.", DeprecationWarning)
    cursor = db_handle.cursor()

    try:
        for table_name in name_list:
            # Start constructing the CREATE TABLE statement
            create_table_query = f"CREATE TABLE {table_name} ("

            # Add column definitions
            column_definitions = []
            primary_keys = []
            for column in layout_dict:
                column_def = f"{column.col_name} {column.data_format}"
                column_definitions.append(column_def)
                if column.is_primary:
                    primary_keys.append(column.col_name)

            # Combine column definitions and primary keys
            create_table_query += ", ".join(column_definitions)
            if primary_keys:
                create_table_query += f", PRIMARY KEY ({', '.join(primary_keys)})"

            create_table_query += ");"

            # Execute the CREATE TABLE statement
            cursor.execute(create_table_query)

        # Commit the changes
        db_handle.commit()
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        db_handle.rollback()
    finally:
        # Close the cursor
        cursor.close()
