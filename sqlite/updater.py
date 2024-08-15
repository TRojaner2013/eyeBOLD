"""Module implementing updating mechanism for eyeBOLD"""

import sqlite3
import logging
from typing import List

from sqlite.parser import TsvParser, get_data_layout

logger = logging.getLogger(__name__)

class Updater():
    """Class representing updater mechanism"""

    def __init__(self, db_handle: sqlite3.Connection,
                 marker_code: str, tsv_file, datapackage: str) -> None:

        self._db_handle = db_handle
        self._tsv_file = tsv_file
        self._marker_code = marker_code
        _, self._datapackage = get_data_layout(datapackage)

        self._tsv_parser = TsvParser(self._tsv_file,
                                     self._marker_code,
                                     self._datapackage)

    def _check_requirements(self) -> bool:
        """ Checks requirements"""
        return True

    def report_updated_records(self) -> List[int]:
        """Reports list of updated specimen ids. """
        raise NotImplementedError

    def apply_update(self):
        """ Applies update to database """
        self._check_requirements()
        self._update()

    def _update(self):
        """ Update function """

        logging.info("Starting database update procedure...")

        cursor = self._db_handle.cursor()

        try:
            for row in self._tsv_parser:
                new_hash = row.get('hash', None)
                specimen_id = row.get('specimenid', None)

                command = "SELECT hash FROM processing_input WHERE specimenid = ?"
                # Sanity Check:
                if new_hash is None:
                    raise ValueError
                if specimen_id is None:
                    raise ValueError

                cursor.execute(command, (specimen_id,))

                existing_hash = cursor.fetchone()
                if existing_hash is None or existing_hash[0] != new_hash:
                    # Insert or update the record
                    columns = ', '.join(row.keys())
                    placeholders = ', '.join(['?' for _ in row])
                    command = (f"INSERT OR REPLACE INTO processing_input ({columns}) "
                               f"VALUES ({placeholders})")
                    cursor.execute(command, tuple(row.values()))

            self._db_handle.commit()

        except sqlite3.Error as e:
            logging.critical("Unable to update database due to error:\n%s", e)
            logging.critical("Error caused by command:\n%s", command)
            logging.critical("Parameters: %s" ,specimen_id)
            self._db_handle.rollback()
