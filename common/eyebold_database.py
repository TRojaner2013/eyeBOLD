""" Module that defines the database object """

import logging
import sqlite3
import sys
import csv
from typing import Any, Tuple, Dict, Set, List
from enum import Enum
from collections import defaultdict
from multiprocessing import Process, Manager

from common.helper import file_exist
from common.location_database import LocationDatabase
import common.constants as ebc
from sqlite.builder import open_db_file, create_database, create_db_file, execute_batches
from sqlite.parser import DB_MAP
from sqlite.Bitvector import BitIndex
from tools.harmonizer import harmonize, harmonize_b2t, raxtax_entry
from tools.sanitizer import purge_duplicates, disclose_hybrids, purge_duplicates_multithreading
from tools.tracker import validate_location

logger = logging.getLogger(__name__)

class ExportFormats(Enum):
    """ Enumeration of exportable formats """
    FASTA =     0
    RAXTAX =    1
    TSV =       2
    CSV =       3

    @classmethod
    def from_str(cls, format_str: str):
        """ Converts a string to the corresponding ExportFormats Enum. """
        normalized_str = format_str.strip().upper()

        # Map the normalized string to the corresponding Enum value
        if normalized_str == 'FASTA':
            return cls.FASTA
        elif normalized_str == 'RAXTAX':
            return cls.RAXTAX
        elif normalized_str == 'TSV':
            return cls.TSV
        elif normalized_str == 'CSV':
            return cls.CSV

        raise ValueError(f"Unknown format: {format_str}")

class EyeBoldDatabase():
    """Defines the EyeBold database """

    # ToDo: Find a better spot in sqlite module for table names.
    TABLE_NAME = 'processing_input'

    def __init__(self, db_file: str, marker_code: str, location_db_file: str) -> None:

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
        self._close()

    def _close(self) -> None:
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
        raise NotImplementedError

    def update(self, tsv_file: str, datapackage: str) -> None:
        """ Updates the database with data from new tsv file"""

        logger.info("Starting update process for database %s", self._db_file)
        if self._valid_db:
            raise AttributeError

        if not file_exist(tsv_file):
            raise ValueError

        if not file_exist(datapackage):
            raise ValueError

        raise NotImplementedError

    def create(self, tsv_file: str, datapackage: str) -> Tuple[bool, str]:
        """Crates a new valid database from scratch"""
        if self._valid_db:
            #ToDo: Maybee return true here.
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
            done = create_database(self._db_handle, tsv_file,
                           datapackage, self._marker_code)
            if done:
                return True, "Succesfully created database!"
        except ValueError:
            logger.error("Unable to open %s  or %s.", datapackage, tsv_file)
            return False, f"Unable to open {datapackage} or {tsv_file}."
        except sqlite3.Error as e:
            logger.critical("Unexpected database error: {e}")
            return False, "Unable to perform actions on database."

        logger.error("Unable to create database %s.", self._db_file)
        return False, "Unable to create Database."

    def invoke_tracker(self) -> None:
        """ Invokes raxtax """

        if not self._valid_db:
            raise AttributeError

        logger.info("Starting tracker process...")
        validate_location(self._db_file, self._loc_db_file, 100)
        # tracker_process = Process(target=validate_location,
        #                           args=(self._db_file, self._loc_db_file, 5000))

        # self._processes.append(tracker_process)

    def _invoke_raxtax(self,) -> List:
        """ Invokes tracker """
        logger.info("Starting raxtax process...")
        self.export(ExportFormats.RAXTAX, ebc.RAXTAX_IN)
        return raxtax_entry()
        # raxtax_process = Process(target=raxtax_entry,
        #                          args=(self._db_file, return_list))

        # self._processes.append(raxtax_process)

    # ToDo:
    #   Rewrrite this function to perform subfuction calls
    #   Add skip/ommit flags for debugging purposes.
    def curate(self) -> None:
        """ Curates data in database. This process consumes a lot of time."""
        # Find all elements flagged checkable in database.

        logger.info("Starting curating process for database %s",
                     self._db_file)

        # 1. Harmonize names
        logger.info("Starting taxonomic harmonization process...")


        helper = []
        levels = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species', 'subspecies']
        levels.reverse()

        dublicates = set()

        for level in levels:
            helper.append(self.get_unsanatized_taxonomy_b2t(level))

        max_parameters = 999
        for info_dict in helper:
            data = harmonize_b2t(info_dict)
            cmd_batch = []
            for datum in data:
                #command, values = datum.to_sql_command()
                command_tuples = datum.to_sql_command()
                if command_tuples:
                    cmd_batch.extend(command_tuples)

                dublicates.add(tuple(datum.specimenids))
                #if values:
                    # We need to limit the length of the parametes to fit to 
                    # the sqlite3 limit of 999 parameters.
                    # The limit might depend on build settings and might be higher or lower.
                #    for i in range(0, len(values), max_parameters):
                #        chunk = values[i:i + max_parameters]
                #        cmd_batch.append((command, chunk))
            if cmd_batch:
                logger.info("Executing %s sql commands...", len(cmd_batch))
                execute_batches(self._db_handle, cmd_batch)

        # Check dublicates we just extracted...
        logger.info("Purging duplicates from database...")
        purge_duplicates_multithreading(self._db_handle, dublicates)
        logger.info("Finished purging duplicates from database.")
        # cmd_batch = []

        # for rank, taxaon_dict in helper.items():
        #     _, data = harmonize(taxaon_dict, rank)

        #     for datum in data:
        #         command, values = datum.to_sql_command()
        #         if values:
        #             cmd_batch.append((command, values))

        logger.info("FINISHED taxonomic harmonization process.")
        logger.info("Flagging hybrid species in database")
        # 2. Flag special cases
        # After sanatizing names we mark all hybrid species.
        disclose_hybrids(self._db_handle)

        # Set selected flag before raxtax export
        read_mask, golden_mask = BitIndex.get_golden()
        #ToDo: add a include column for faster selection after build process
        command = f"""UPDATE specimen
                      SET checks = checks | {1 << BitIndex.SELECTED.value}
                      WHERE (checks & {read_mask}) = {golden_mask};"""
        cursor = self._db_handle.cursor()
        cursor.execute(command)
        self._db_handle.commit()

        ###
        ### MULTIPROCESSING SECTION
        ### From here we work with multiple processes to spare time.

        # manager = Manager()
        # bad_entries = manager.list()
        # if False:
        #     # Disable tracker for now...
        #self._invoke_tracker()
        bad_entries = self._invoke_raxtax()

        # for process in self._processes:
        #     process.start()

        # 4. Check ORF in Sequences

        # Wait for all processes to finish
        # for process in self._processes:
        #     process.join()
        
        # Update the database with the results from the raxtax process
        self._update_raxtax(bad_entries)

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

        logger.info("Finished curating process")

    def _update_raxtax(self, bad_entries: List) -> None:
        """ Updates the database with the results from the raxtax process """

        command = """UPDATE specimen SET checks = (checks & ~1) | (1 << 15) WHERE specimenid=?;"""

        parameters = [(entry,) for entry in bad_entries]
        cursor = self._db_handle.cursor()
        cursor.executemany(command, parameters)
        self._db_handle.commit()

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

    def get_unsanatized_taxonomy_b2t(self, level: str) -> Dict[str, Set]:
        """Returns all taxonomy data for unsanatized entries at the specified level"""

        levels = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species', 'subspecies']

        if level not in levels:
            raise ValueError(f"Invalid level: {level}. Must be one of {levels}")

        # Determine the index of the input level
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
        #result = []
        result_dict = defaultdict(lambda: {"specimenids": []})


        for entry in data:
            key = tuple(entry[:8])  # Create a key using all taxonomy fields
            result_dict[key]["specimenids"].append(entry[8])
            for i, field in enumerate(levels):
                result_dict[key][field] = entry[i] if entry[i] else None
            result_dict[key]["query"] = entry[level_index] if entry[level_index] else None
            result_dict[key]["rank"] = level
            result_dict[key][level] = None # Remove enty to use as query

        # Convert the result_dict to a list of dictionaries
        result = list(result_dict.values())

        # for entry in data:
        #     entry_dict = {
        #         "query": entry[level_index] if entry[level_index] else None,
        #         "rank": level,
        #         "kingdom":      entry[0] if entry[0] else None,
        #         "phylum":       entry[1] if entry[1] else None,
        #         "class":        entry[2] if entry[2] else None,
        #         "order":        entry[3] if entry[3] else None,
        #         "family":       entry[4] if entry[4] else None,
        #         "genus":        entry[5] if entry[5] else None,
        #         "species":      entry[6] if entry[6] else None,
        #         "subspecies":   entry[7] if entry[7] else None,
        #         "specimenid":   entry[8] if entry[8] else None
        #     }
        #     entry_dict[level] = None
        #     result.append(entry_dict)

        return result


    def get_unsanatized_taxonomy(self) -> Dict[str, Set]:
        """Returns all taxonomy data for unsanatized entries

        Arguments:
            -db_handle: Database connection handle

        Returns:
            List with taxonomy data of all unsantized rows
        """

        # ToDo: Create better implementation here.
        # Tthis takes only around 1 min
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

    def export(self, format: ExportFormats, out_file: str) -> None:
        """ Exports selected data from databse into a file of provided format. """

        cursor = self._db_handle.cursor()

        query = (f"SELECT checks, specimenid, nuc_san,  {DB_MAP['phylum']}, "
                 f"{DB_MAP['class']}, {DB_MAP['order']}, {DB_MAP['family']}, "
                 f"{DB_MAP['genus']}, {DB_MAP['species'] }"
                 f" FROM specimen WHERE checks & 1;")

        cursor.execute(query)
        rows = cursor.fetchall()

        header = ["checks", "specimenid", "nuc_san", "phylum", "class", "order", "family", "genus", "species"]

        if format == ExportFormats.FASTA:
            self._export_fasta(rows, out_file)
        elif format == ExportFormats.RAXTAX:
            self._export_fasta_raxtax(rows, out_file)
        elif format == ExportFormats.TSV:
            self._export_csv(rows, out_file, '\t', header)
        elif format == ExportFormats.CSV:
            self._export_csv(rows, out_file, ';', header)
        else:
            raise ValueError(f"Invalid export format provided: {format}")

    def query_export(self, query: str, out_file: str, format: ExportFormats):
        """ Executes a query and exports the data to out_file in the specified format

            Raises:
                ValueError: If an invalid export format is provided
        """
        rows = self._query_database(query)

        if format == ExportFormats.FASTA:
            self._export_fasta(rows, out_file)
        elif format == ExportFormats.RAXTAX:
            self._export_fasta_raxtax(rows, out_file)
        elif format == ExportFormats.TSV:
            self._export_csv(rows, out_file, '\t')
        elif format == ExportFormats.CSV:
            self._export_csv(rows, out_file, ';')
        else:
            raise ValueError(f"Invalid export format provided: {format}")

    def query_print(self, query: str) -> None:
        """ Executes a query and prints the results to the console """
        rows = self._query_database(query)

        for row in rows:
            print(row)

    def _export_fasta_raxtax(self, rows, out_file) -> None:

        with open(out_file, 'w', encoding="utf-8") as file:
            for row in rows:
                checks, specimenid, nuc_raw, phylum, class_, order, family, genus, species = row

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
                                        if not set(nuc_raw).issubset(valid_chars):
                                            #logger.error("Invalid nucleotide sequence in specimen %s", specimenid)
                                            continue
                                        raxtax_string = f">{specimenid};tax={tax_string};\n{nuc_raw}\n"
                                        file.write(raxtax_string)

    def _export_fasta(self, rows, out_file) -> None:

        with open(out_file, 'w', encoding="utf-8") as file:
            for row in rows:
                checks, specimenid, nuc_raw, phylum, class_, order, family, genus, species = row

    def _export_csv(self, rows, out_file: str, delimiter: str, header: List=None) -> None:
        """Exports the data to a CSV file with the specified delimiter."""

        with open(out_file, 'w', encoding="utf-8", newline='') as file:
            writer = csv.writer(file, delimiter=delimiter)

            if header:
                writer.writerow(header)

            writer.writerows(rows)
