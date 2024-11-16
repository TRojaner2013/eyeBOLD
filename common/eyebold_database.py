""" Module implementing database handler"""

from datetime import datetime
import logging
import sqlite3
import csv
from typing import Tuple, Dict, Set, List, Any
from enum import Enum
from collections import defaultdict

from sqlite.builder import open_db_file, create_database, create_db_file
from sqlite.builder import execute_batches, insert_updates
from sqlite.parser import DB_MAP
from sqlite.Bitvector import BitIndex
from tools.harmonizer import harmonize_b2t, raxtax_entry
from tools.sanitizer import purge_duplicates, disclose_hybrids, purge_duplicates_multithreading_2
from tools.tracker import validate_location
from common.helper import file_exist
from common.location_database import LocationDatabase
import common.constants as const

logger = logging.getLogger(__name__)

class ExportFormats(Enum):
    """ Enumeration of exportable formats """
    FASTA =     0
    RAXTAX =    1
    TSV =       2
    CSV =       3

    @classmethod
    def from_str(cls, format_str: str):
        """ Converts a string to the corresponding ExportFormats Enum.

            Args:
                format_str (str): String representation of the format
        """
        normalized_str = format_str.strip().upper()

        # Map the normalized string to the corresponding Enum value
        if normalized_str == 'FASTA':
            return cls.FASTA

        if normalized_str == 'RAXTAX':
            return cls.RAXTAX

        if normalized_str == 'TSV':
            return cls.TSV

        if normalized_str == 'CSV':
            return cls.CSV

        raise ValueError(f"Unknown format: {format_str}")

class EyeBoldDatabase():
    """Defines the EyeBold database """

    # ToDo: Find a better spot in sqlite module for table names.
    TABLE_NAME = 'processing_input'

    def __init__(self, db_file: str, marker_code: str,
                 location_db_file: str) -> 'EyeBoldDatabase':
        """ Constructor for EyeBoldDatabase class

            Args:
                - db_file (str): Path to database file
                - marker_code (str): Marker used for database
                - location_db_file (str): Path to location database file

            Returns:
                Instance of class EyeBoldDatabase
        """

        self._db_file: str = db_file
        self._loc_db_file: str = location_db_file
        self._loc_db: LocationDatabase = LocationDatabase(location_db_file)
        self._loc_db.check_db()

        self._marker_code : str = marker_code
        self._valid_db: bool = False
        try:
            self._db_handle: sqlite3.Connection = open_db_file(self._db_file)
            self._valid_db = True
        except FileNotFoundError:
            self._db_handle = None
        self._processes = []

    def __del__(self) -> None:
        """ Destructor for EyeBoldDatabase class """
        self._close()

    def _close(self) -> None:
        """ Closes the database connection """
        if self._db_handle:
            self._db_handle.commit()
            self._db_handle.close()

        self._db_handle = None

    def review(self) -> None:
        """ Automated review processs

            Note: This process is mandatory as we might fail to check all
            names on first try due to e.g. timeout errors.
        """
        if not self._valid_db:
            raise AttributeError
        self.curate()

    def update(self, tsv_file: str, datapackage: str) -> None:
        """ Updates the database with data from new tsv file

            Args:
                - tsv_file (str): Path to new data file
                - datapackage (str): Path to datapackage file
        """

        if not self._valid_db:
            raise AttributeError

        if not file_exist(self._db_file):
            logger.error("Unable to find database at %s.", self._db_file)
            raise FileNotFoundError

        if not file_exist(tsv_file):
            logger.error("Unable to find tsv file at %s.", tsv_file)
            raise FileNotFoundError

        if not file_exist(datapackage):
            logger.error("Unable to find datapackage at %s.", datapackage)
            raise FileNotFoundError

        logger.info("Starting update process for database %s", self._db_file)

        # Find all entries that are changed and new
        new_ids, changed_ids =  insert_updates(self._db_file, tsv_file,
                                               datapackage, self._marker_code)
        all_ids = [values[0] for values in changed_ids]
        all_ids.extend(new_ids)

        # 0. Sanity check
        if len(all_ids) == 0:
            logger.info("No new or updated entries found.")
            return

        # 1. Check names for all new/updated entries
        logger.info("Starting taxonomic harmonization process...")

        #ToDo: Check with curate function which functions to create,
        #      then update this procedure accordingly.
        cursor = self._db_handle.cursor()
        helper = []
        levels = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species', 'subspecies']
        levels.reverse()

        for level in levels:
            helper.append(self.get_unsanatized_taxonomy_b2t(level))

        for info_dict in helper:
            data = harmonize_b2t(info_dict)
            cmd_batch = []

            for datum in data:
                command_tuples = datum.to_sql_command()
                if command_tuples:
                    cmd_batch.extend(command_tuples)

            if cmd_batch:
                logger.info("Executing %s sql commands...", len(cmd_batch))
                cursor.execute_batches(self._db_handle, cmd_batch)

        logger.info("Finished taxonomic harmonization process...")

        # Find the taxonomy_keys for all updated entries.
        gbif_keys = set()
        for i in range(0, len(all_ids), const.SQL_SAVE_NUM_VARS):
            chunk = all_ids[i:i + const.SQL_SAVE_NUM_VARS]
            query = f"""SELECT gbif_key FROM specimen WHERE specimenid IN ({','.join(['?'] * len(chunk))});"""
            cursor.execute(query, chunk)

            for row in cursor.fetchall():
                gbif_keys.add(row[0])

        # Remove None and duplicate keys
        gbif_keys.discard(None)
        if len(gbif_keys) == 0:
            # Sanity check, if we end up here something went wrong...
            logger.error("Stopping update process: No gbif_keys found for updated entries.")
            logger.error("PLEASE CONTACT DEVELOPER OR OPEN AN ISSUE ON GITHUB.")
            return

        save_keys = list(gbif_keys)

        # Clear all flags for the updated entries
        mask = BitIndex.get_update_clear_mask()
        cmd = f"""UPDATE specimen SET include = False, checks = checks & {mask} WHERE gbif_key = ?;"""
        self._db_handle.executemany(cmd, save_keys)
        self._db_handle.commit()

        # 2. Get list of duplicates
        duplicates = []
        for i in range(0, len(gbif_keys), const.SQL_SAVE_NUM_VARS):
            chunk = gbif_keys[i:i + const.SQL_SAVE_NUM_VARS]
            query = f"""SELECT GROUP_CONCAT(specimenid) AS specimen_ids
                        FROM specimen
                        WHERE gbif_key IN ({','.join(['?'] * len(chunk))});"""
            cursor.execute(query, chunk)
            for row in cursor.fetchall():
                specimen_ids = list(map(int, row[0].split(',')))
                duplicates.append(specimen_ids)

        #Presort instances list.
        logger.info("Starting sorting all instances at %s",
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        duplicates.sort(key=lambda x: len(x))
        trivial_instances = [x for x in duplicates if len(x) <= const.TRIVIAL_SIZE]
        larger_instances = [x for x in duplicates if len(x) > const.TRIVIAL_SIZE]

        logger.info("Purging duplicates from database...")
        logger.info("Starting trivial instances at %s",
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        purge_duplicates(self._db_handle, trivial_instances)
        logger.info("Starting larger instances at %s",
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        purge_duplicates_multithreading_2(self._db_handle, larger_instances)
        logger.info("Finished larger instances at %s",
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        # Flag hybrid species (assuming they are marked with an 'x' in the species field)
        logger.info("Flagging hybrid species in database")
        #ToDo: add update flag to check only new entries...
        disclose_hybrids(self._db_handle)

        # Set include flag in bitvector for entries that passed all checks untill now
        read_mask, golden_mask = BitIndex.get_golden()
        command = f"""UPDATE specimen
                      SET checks = checks | {1 << BitIndex.SELECTED.value}
                      WHERE (checks & {read_mask}) = {golden_mask};"""
        cursor = self._db_handle.cursor()
        cursor.execute(command)
        self._db_handle.commit()

        # Find misclassified species with raxtax
        bad_entries = self._invoke_raxtax()
        self._update_raxtax(bad_entries)

        command = """UPDATE specimen SET review = False WHERE (checks & ?) = 2;"""
        cursor.execute(command, (1 << BitIndex.NAME_CHECKED.value,))
        self._db_handle.commit()

        command = """UPDATE specimen SET include = True WHERE (checks & ?) = 1;"""
        cursor.execute(command, (1 << BitIndex.SELECTED.value,))
        self._db_handle.commit()

        logger.info("Finished updating process")

    #ToDo: Think about better return type for this function...
    def create(self, tsv_file: str, datapackage: str) -> Tuple[bool, str]:
        """ Creates a new database

            Args:
                - tsv_file (str): Path to file containing data
                - datapackage (str): Path to datapackage file describing data

            Returns:
                Tuple[bool, str], True on success, False and error message on error.
        """
        if self._valid_db:
            return False, f"Database at {self._db_file} is a valid database."

        logger.info("Starting database creation process at %s",
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

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
            done = create_database(self._db_handle, tsv_file,
                           datapackage, self._marker_code)
            if done:
                logger.info("Finished database creation process at %s",
                            datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

                return True, "Succesfully created database!"
        except ValueError:
            logger.error("Unable to open %s  or %s.", datapackage, tsv_file)
            return False, f"Unable to open {datapackage} or {tsv_file}."
        except sqlite3.Error as err:
            logger.critical("Unexpected database error: %s", err)
            return False, "Unable to perform actions on database."

        logger.error("Unable to create database %s.", self._db_file)
        return False, "Unable to create Database."

    #ToDo: Create a constant for batch_size
    def invoke_tracker(self, batch_size: int=const.TRACKER_DOWNLOAD_CHUNK_SIZE) -> None:
        """ Invokes tracker feature to build location database

            Args:
                - batch_size: Number of species to download at once
        """

        if not self._valid_db:
            raise AttributeError

        logger.info("Starting tracker process...")
        validate_location(self._db_file, self._loc_db_file, batch_size)

    def _invoke_raxtax(self) -> List:
        """ Invokes raxtax

            Returns:
                List of specimeni ids that are misclassified.
        """
        #ToDo: Perfom only one export for first build, as files are identical.
        logger.info("Starting raxtax process...")
        self._export_raxtax_db_file(const.RAXTAX_DB_IN)
        self._export_raxtax_query_file(const.RAXTAX_QUERY_IN)
        return raxtax_entry()

    def curate(self) -> None:
        """ Starts curation process of database

            Note:
                This process runs for a long time (~2 days)
        """

        logger.info("Starting curating process for database %s",
                     self._db_file)

        # 1. Harmonize names
        logger.info("Starting taxonomic harmonization process at %s",
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        #ToDo Q1: Make this a function
        helper = []
        #ToDo: Assign this to a constant...
        levels = ['kingdom', 'phylum', 'class', 'order',
                  'family', 'genus', 'species', 'subspecies']
        levels.reverse()

        for level in levels:
            helper.append(self.get_unsanatized_taxonomy_b2t(level))

        for info_dict in helper:
            data = harmonize_b2t(info_dict)
            cmd_batch = []

            for datum in data:
                command_tuples = datum.to_sql_command()
                if command_tuples:
                    cmd_batch.extend(command_tuples)

            if cmd_batch:
                logger.info("Executing %s sql commands...", len(cmd_batch))
                execute_batches(self._db_handle, cmd_batch)

        logger.info("Finished taxonomic harmonization process at %s",
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        # ToDo Q1: Make this part a function
        # Get all unique gbif_keys, each representing one distict taxon
        cursor = self._db_handle.cursor()
        cmd = """SELECT DISTINCT gbif_key FROM specimen;"""
        cursor.execute(cmd)
        gbif_keys = [row[0] for row in cursor.fetchall() if row[0] is not None]

        if not gbif_keys:
            # Sanity check, if we end up here something went wrong...
            logger.error("No gbif_keys found in database.")
            logger.error("Please check input data or conatct developer.")
            return

       # 2. Get list of duplicates
        duplicates = []
        for i in range(0, len(gbif_keys), const.SQL_SAVE_NUM_VARS):
            chunk = gbif_keys[i:i + const.SQL_SAVE_NUM_VARS]
            query = f"""SELECT GROUP_CONCAT(specimenid) AS specimen_ids
                        FROM specimen
                        WHERE gbif_key IN ({','.join(['?'] * len(chunk))})
                        GROUP BY gbif_key;"""
            cursor.execute(query, chunk)
            for row in cursor.fetchall():
                specimen_ids = list(map(int, row[0].split(',')))
                duplicates.append(specimen_ids)

        # Check duplicates we just extracted...
        duplicates.sort(key=lambda x: len(x))
        tiny_instances = [x for x in duplicates if len(x) < const.TRIVIAL_SIZE]
        larger_instances = [x for x in duplicates if len(x) >= const.TRIVIAL_SIZE]

        logger.info("Purging duplicates from database...")

        if tiny_instances:
            logger.info("Starting with %s tiny instances at %s", len(tiny_instances),
                        datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

            purge_duplicates(self._db_handle, tiny_instances)
            logger.info("Finished trivial instances at %s",
                        datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        if larger_instances:
            logger.info("Starting %s larger instances at %s",
                        len(larger_instances), datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

            purge_duplicates_multithreading_2(self._db_handle, larger_instances)
            logger.info("Finished larger instances at %s",
                        datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        logger.info("Purging duplicates finished...")

        # Flag hybrid species (assuming they are marked with an 'x' in the species field)
        logger.info("Flagging hybrid species in database")
        disclose_hybrids(self._db_handle)

        # Set include flag in bitvector for entries that passed all checks untill now
        read_mask, golden_mask = BitIndex.get_golden()
        command = f"""UPDATE specimen
                      SET checks = checks | {1 << BitIndex.SELECTED.value}
                      WHERE (checks & {read_mask}) = {golden_mask};"""
        cursor.execute(command)
        self._db_handle.commit()

        # Find misclassified species with raxtax
        logger.info("Starting raxtax process at %s",
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        bad_entries = self._invoke_raxtax()

        # Update the database with the results from raxtax
        self._update_raxtax(bad_entries)
        logger.info("Finished raxtax process at %s",
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        logger.info("Flagging results in database at %s",
                    datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

        # Set review flag to false for all curated entries
        # Note: This enables us to check failed name lookups again
        command = """UPDATE specimen SET review = False WHERE (checks & ?) = 2;"""
        cursor.execute(command, (1 << BitIndex.NAME_CHECKED.value,))
        self._db_handle.commit()

        # Set data to be included in standard export
        command = """UPDATE specimen SET include = True WHERE (checks & ?) = 1;"""
        cursor = self._db_handle.cursor()
        cursor.execute(command, (1 << BitIndex.SELECTED.value,))
        self._db_handle.commit()

        logger.info("Finished curating process at %s", datetime.now().strftime('%Y-%m-%d_%H_%M_%S'))

    def _update_raxtax(self, bad_entries: List) -> None:
        """ Updates the database with the results from the raxtax process

            Args:
                - bad_entries (List): List of specimen ids marked as misclassified
        """

        command = f"""UPDATE specimen SET checks = (checks & ~1) | (1 << {BitIndex.BAD_CLASSIFICATION.value}) WHERE specimenid=?;"""

        parameters = [(entry,) for entry in bad_entries]
        cursor = self._db_handle.cursor()
        cursor.executemany(command, parameters)
        self._db_handle.commit()

    def close(self) -> None:
        """ Closes database

            Raises:
                AttributeError: If database is not valid
        """

        if not self._valid_db:
            raise AttributeError

        if self._db_handle is None:
            return

        self._db_handle.close()

    def _query_database(self, query: str, params: Tuple[str]|None=None) -> List:
        """ Queries database and returns result

            Args:
                - query (str): SQL query
                - params (Tuple[str]): Tuple of parameter for query, if any

            Returns:
                List of all selected rows from database
        """

        cur = self._db_handle.cursor()
        if params is not None:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur.fetchall()

    def get_unsanatized_taxonomy_b2t(self, level: str) -> List[Dict[Any, Any]]:
        """ Returns all taxonomy data for unsanatized entries at the specified level

            Args:
                - level (str): Taxonomy level

            Returns:
                List of dictionary with taxonomy data of all unsantized rows at
                the specified level

            Raises:
                ValueError: If an invalid level is provided
        """

        levels = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species', 'subspecies']

        if level not in levels:
            raise ValueError(f"Invalid level: {level}. Must be one of {levels}")

        level_index = levels.index(level)

        # Build the query to fetch data at the specified level
        conditions = [f"{DB_MAP[previous]} IS NULL" for previous in levels[level_index+1:]]
        conditions.append(f"{DB_MAP[level]} IS NOT NULL")
        conditions_str = " AND ".join(conditions)

        query = f"""
            SELECT 
                {DB_MAP['kingdom']},
                {DB_MAP['phylum']},
                {DB_MAP['class']},
                {DB_MAP['order']},
                {DB_MAP['family']},
                {DB_MAP['genus']},
                {DB_MAP['species']},
                {DB_MAP['subspecies']},
                specimenid
            FROM specimen
            WHERE review = ? AND {conditions_str}
        """

        params = (True,)
        data = self._query_database(query, params)
        result_dict = defaultdict(lambda: {"specimenids": []})


        for entry in data:
            key = tuple(entry[:8])  # Create a key using all taxonomy fields
            result_dict[key]["specimenids"].append(entry[8])
            for i, field in enumerate(levels):
                result_dict[key][field] = entry[i] if entry[i] else None
            result_dict[key]["query"] = entry[level_index] if entry[level_index] else None
            result_dict[key]["rank"] = level
            result_dict[key][level] = None # Remove enty to use as query

        # Convert the result_dict to a list of values
        result = list(result_dict.values())

        return result


    def get_unsanatized_taxonomy(self) -> Dict[str, Set]:
        """Returns all taxonomy data for unsanatized entries

        Note:
            This function is deprecated and will be removed in future versions.

        Arguments:
            - db_handle: Database connection handle

        Returns:
            Dictionary with taxonomy data of all unsantized rows
        """

        query = (f"SELECT {DB_MAP['kingdom']}, {DB_MAP['phylum']},"
                 f" {DB_MAP['class']}, {DB_MAP['order']}, {DB_MAP['family']},"
                 f" {DB_MAP['subfamily']}, {DB_MAP['genus']}, {DB_MAP['species']},"
                 f" {DB_MAP['subspecies']} FROM specimen WHERE review = ?")

        taxonomy_data = self._query_database(query, (True,))

        kingdoms = set()
        phyla = set()
        classes = set()
        orders = set()
        families = set()
        subfamilies = set()
        genera = set()
        species = set()
        subspecies = set()

        # Populate the sets with unique values
        for entry in taxonomy_data:
            kingdom, phylum, class_, order, family, subfamily, genus, species_, subspecies_ = entry
            kingdoms.add(kingdom)
            phyla.add(phylum)
            classes.add(class_)
            orders.add(order)
            families.add(family)
            subfamilies.add(subfamily)
            genera.add(genus)
            species.add(species_)
            subspecies.add(subspecies_)

        return {"kingdom": kingdoms, "phylum": phyla, "class": classes,
                'order': orders, 'family': families, 'subfamily': subfamilies,
                'genus': genera, 'species': species, 'subspecies': subspecies}

    def _export_raxtax_db_file(self, out_file: str) -> None:
        """ Exports database file for raxtax

            Args:
                - out_file (str): Path where export is saved
        """
        cursor = self._db_handle.cursor()

        query = (f"SELECT checks, specimenid, nuc_san,  {DB_MAP['phylum']}, "
                 f"{DB_MAP['class']}, {DB_MAP['order']}, {DB_MAP['family']}, "
                 f"{DB_MAP['genus']}, {DB_MAP['species'] }"
                 f" FROM specimen WHERE (checks & 1 = 1);")

        cursor.execute(query)
        rows = cursor.fetchall()

        self._export_fasta_raxtax(rows, out_file)

    def _export_raxtax_query_file(self, out_file: str) -> None:
        """ Exports query file for raxtax

            Args:
                - out_file (str): Path where export is saved
        """
        cursor = self._db_handle.cursor()

        query = (f"SELECT checks, specimenid, nuc_san,  {DB_MAP['phylum']}, "
                 f"{DB_MAP['class']}, {DB_MAP['order']}, {DB_MAP['family']}, "
                 f"{DB_MAP['genus']}, {DB_MAP['species'] }"
                 f" FROM specimen WHERE ((checks & 1 = 1) AND (review = 1));")

        cursor.execute(query)
        rows = cursor.fetchall()

        #header = ["checks", "specimenid", "nuc_san", "phylum",
        #           "class", "order", "family", "genus", "species"]

        self._export_fasta_raxtax(rows, out_file)

    def export(self, format_: ExportFormats, out_file: str) -> None:
        """ Exports selected data from databse into a file of provided format

            Args:
                format_ (ExportFormat): Format of exported file
                out-file (str): Path where export is saved
        """

        cursor = self._db_handle.cursor()

        query = (f"SELECT checks, specimenid, nuc_san,  {DB_MAP['phylum']}, "
                 f"{DB_MAP['class']}, {DB_MAP['order']}, {DB_MAP['family']}, "
                 f"{DB_MAP['genus']}, {DB_MAP['species'] }"
                 f" FROM specimen WHERE checks & 1;")

        cursor.execute(query)
        rows = cursor.fetchall()

        header = ["checks", "specimenid", "nuc_san", "phylum",
                  "class", "order", "family", "genus", "species"]

        if format_ == ExportFormats.FASTA:
            self._export_fasta(rows, out_file)
        elif format_ == ExportFormats.RAXTAX:
            self._export_fasta_raxtax(rows, out_file)
        elif format_ == ExportFormats.TSV:
            self._export_csv(rows, out_file, '\t', header)
        elif format_ == ExportFormats.CSV:
            self._export_csv(rows, out_file, ';', header)
        else:
            raise ValueError(f"Invalid export format provided: {format_}")

    def query_export(self, query: str, out_file: str, format_: ExportFormats):
        """ Executes a query and exports the data to out_file in the specified format

            Args:
                - queryy (str): SQL Query
                - out_file (str): Path to exported file
                - format_ (ExportFormats): Fortmat of export

            Raises:
                ValueError: If an invalid export format is provided
        """
        rows = self._query_database(query)

        if format_ == ExportFormats.FASTA:
            self._export_fasta(rows, out_file)
        elif format_ == ExportFormats.RAXTAX:
            self._export_fasta_raxtax(rows, out_file)
        elif format_ == ExportFormats.TSV:
            self._export_csv(rows, out_file, '\t')
        elif format_ == ExportFormats.CSV:
            self._export_csv(rows, out_file, ';')
        else:
            raise ValueError(f"Invalid export format provided: {format_}")

    def query_print(self, query: str) -> None:
        """ Executes a query and prints the results to the console

            Args:
                - query (str): SQL query
        """
        rows = self._query_database(query)

        for row in rows:
            print(row)

    def _export_fasta_raxtax(self, rows: List[Tuple], out_file: str) -> None:
        """ Exports data in raxtaxs fasta format

            Note:
                Any row must contain the values in the order below:
                [checks, specimen-id, sequence, phylum, class, oder, family,
                 genus, species]
            Args:
                - rows (List): Rows to export
                - out_file (str): Path to exported file
        
        """

        with open(out_file, 'w', encoding="utf-8") as file:
            for row in rows:
                checks, specimenid, seq, phylum, class_, order, family, genus, species = row

                tax_parts = []
                # Works for earlier versions of raxtax.
                # if checks & (1 << BitIndex.INCL_PHYLUM.value):
                #     tax_parts.append(f"p:{phylum}")
                # if checks & (1 << BitIndex.INCL_CLASS.value):
                #     tax_parts.append(f"c:{class_}")
                # if checks & (1 << BitIndex.INCL_ORDER.value):
                #     tax_parts.append(f"o:{order}")
                # if checks & (1 << BitIndex.INCL_FAMILY.value):
                #     tax_parts.append(f"f:{family}")
                # if checks & (1 << BitIndex.INCL_GENUS.value):
                #     tax_parts.append(f"g:{genus}")
                # if checks & (1 << BitIndex.INCL_SPECIES.value):
                #     tax_parts.append(f"s:{species}")

                # ToDo: Edit raxtax export query.
                # ToDo: Remove character restriction once raxtax is fixedl.
                valid_chars = {'A', 'G', 'C', 'T'}
                if checks & (1 << BitIndex.INCL_PHYLUM.value):
                    tax_parts.append(f"{phylum.replace(' ', '_')}")
                    if checks & (1 << BitIndex.INCL_CLASS.value):
                        tax_parts.append(f"{class_.replace(' ', '_')}")
                        if checks & (1 << BitIndex.INCL_ORDER.value):
                            tax_parts.append(f"{order.replace(' ', '_')}")
                            if checks & (1 << BitIndex.INCL_FAMILY.value):
                                tax_parts.append(f"{family.replace(' ', '_')}")
                                if checks & (1 << BitIndex.INCL_GENUS.value):
                                    tax_parts.append(f"{genus.replace(' ', '_')}")
                                    if checks & (1 << BitIndex.INCL_SPECIES.value):
                                        tax_parts.append(f"{species.replace(' ', '_')}")
                                        tax_string = ','.join(tax_parts)
                                        if not set(seq).issubset(valid_chars):
                                            continue
                                        raxtax_string = f">{specimenid};tax={tax_string};\n{seq}\n"
                                        file.write(raxtax_string)

    #ToDo: implement function
    def _export_fasta(self, rows: List[Tuple], out_file: str) -> None:
        """ Exports data in fasta format
        
            Args:
                - rows (List): Rows to export
                - out_file: String to exported file
            """

        raise NotImplementedError
        # with open(out_file, 'w', encoding="utf-8") as file:
        #     for row in rows:
        #         checks, specimenid, nuc_raw, phylum, class_, order, family, genus, species = row


    def _export_csv(self, rows: List[Tuple], out_file: str, delimiter: str,
                    header: List[str]=None) -> None:
        """ Exports the data to a csv file with the specified delimiter

            Args:
                rows (List[str]): Rows to export
                out_file (str): Path to file where export is saved
                delimiter (str): Delimiter to use
                header (List[str]): Headers for csv file
        """

        with open(out_file, 'w', encoding="utf-8", newline='') as file:
            writer = csv.writer(file, delimiter=delimiter)

            if header:
                writer.writerow(header)

            writer.writerows(rows)
