""" Module implementing a database for location data"""


import logging
import sqlite3
from typing import Tuple, List

from sqlite.builder import open_db_file, create_db_file
from sqlite.db_layout import CreateCommands

from common.helper import file_exist

logger = logging.getLogger(__name__)

#ToDo: Need to implement checks if db_handle is not None.
class LocationDatabase():
    """ Defines the location data database"""

    # ToDo: Find a better way to store table names
    TABLE_NAME = 'climate_data'

    def __init__(self, db_file: str):
        """ Class constructor for class LocationDatabase

            Args:
                -db_file (str): Location of location database

            Returns:
                Instance of class LocationDatabase
        """

        self._db_file: str = db_file
        self._valid_db: bool = False

        try:
            self._db_handle: sqlite3.Connection = open_db_file(self._db_file)
            self._valid_db = True
        except FileNotFoundError:
            self._db_handle = None

    def __del__(self) -> None:
        self._close()

    def _close(self) -> None:
        """ Close handler for database"""
        if self._db_handle:
            self._db_handle.commit()
            self._db_handle.close()

        self._db_handle = None

    def check_db(self) -> bool:
        """ Checks if database is ready for use

            Returns:
                True if database is usable
        """

        if not file_exist(self._db_file):
            logger.info('Database file %s does not exist.', self._db_file)
            self.create()

        try:
            self._db_handle = open_db_file(self._db_file)
            self._valid_db = True
            return True
        except FileNotFoundError:
            logger.error("Unable to find and create database %s.", self._db_file)
            return False

    def create(self) -> Tuple[bool, str]:
        """ Crates a new location database

            Returns:
                Tuple[bool,str]: True on success or False with error messsage on
                                 error.
        """
        if self._valid_db:
            return False, f"Database at {self._db_file} is a valid database."

        try:
            create_db_file(self._db_file)
            self._db_handle = open_db_file(self._db_file)
        except FileExistsError:
            logger.error("Unable to create databse file at %s.", self._db_file)
            return False, f"Database {self._db_file} already exists."
        except FileNotFoundError:
            logger.error("Unable to find database %s.", self._db_file)
            return False, f"Unable to find database file at {self._db_file}."
        except IOError as err:
            logger.critical("Unexpected IOError on file %s\n%s.",
                            self._db_file, err)
            return False, "IOError on database file."

        try:
            cursor = self._db_handle.cursor()
            cursor.execute(CreateCommands.GBIF_DB_CMD.value)
            self._db_handle.commit()
            return True

        except sqlite3.Error as err:
            logging.critical("Unexpected database error: %s", err)
            return False, "Unable to perform actions on database."

    def close(self) -> None:
        """ Closes database after commiting all data"""

        if not self._valid_db:
            raise AttributeError

        if self._db_handle is None:
            return

        self._close()

    def _query_database(self, query: str, params: Tuple[str]|None=None) -> List:
        """ Executes quert on database an returns results

            Args:
                - query (str): Valid SQL query
                - params (Tuple[str]): Parameters for SQL query, if any.

            Returns:
                List off all rows returned
            """

        cur = self._db_handle.cursor()
        if params is not None:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur.fetchall()
