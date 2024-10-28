"""Module implementing a database for location data """


import logging
import sqlite3
from typing import Any, Tuple, Dict, Set, List
from multiprocessing import Process

from common.helper import file_exist
from sqlite.builder import open_db_file, create_db_file
from sqlite.db_layout import CreateCommands
from tools.tracker import validate_location

logger = logging.getLogger(__name__)

class LocationDatabase():
    """Defines the location data database """

    # ToDo: Find a better spot in sqlite module for table names.
    TABLE_NAME = 'climate_data'

    def __init__(self, db_file: str) -> None:

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
        if self._db_handle:
            self._db_handle.commit()
            self._db_handle.close()

        self._db_handle = None

    def check_db(self) -> bool:
        """ Checks if database is ready for use """

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
        """Crates a new valid database from scratch"""
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
        except IOError as e:
            logger.critical("Unexpected IOError on file %s\n%s.",
                            self._db_file, e)
            return False, "IOError on database file."

        try:
            cursor = self._db_handle.cursor()
            cursor.execute(CreateCommands.GBIF_DB_CMD.value)
            self._db_handle.commit()
            return True

        except sqlite3.Error as e:
            logging.critical("Unexpected database error: {e}")
            return False, "Unable to perform actions on database."

    def close(self) -> None:

        if not self._valid_db:
            raise AttributeError

        if self._db_handle is None:
            return

        self._db_handle.close()

    def _query_database(self, query: str, params: Tuple[str]|None=None) -> List:
        """Returns queries from database"""

        cur = self._db_handle.cursor()
        if params is not None:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur.fetchall()
