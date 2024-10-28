""" This module contains information to parse the .tsv database
    provided by bold into a sqlite table.
"""

import csv
import json
import logging
import copy
import kgcpy

import hashlib

from dataclasses import dataclass
from typing import List, Dict, Any, Tuple, Callable
from datetime import datetime

from common.helper import file_exist, parse_date

logger = logging.getLogger(__name__)

# This is an inital map for BOLDs tsv files to coloum names in our
# SQL-Databases
@dataclass
class ColumnInfo():
    """ Defines Database info"""
    index: int
    col_name: str
    parser: callable
    data_format: str
    is_primary: bool
    uses_idx: bool

@dataclass
class GbifName:
    """ Dataclass representing parsed named from GBIF Database. """
    old_name: str
    rank: str
    result: dict
    insert_dict: dict
    specimenids: List[int]

    def to_sql_command(self) -> List[Tuple[str, int]]:#Tuple[str, List[str]]:
        """Returns SQL update command for instance"""
        update_columns = []
        parameters = []
        checks_value = None
        max_varialbe_count = 950 # Make sure to keep this below 999 to account for update_columns andn not just specimenids
        commands = []

        for column, value in self.insert_dict.items():
            if column == "checks":
                checks_value = value
            else:
                update_columns.append(f"{column} = ?")
                parameters.append(value)

        if checks_value is not None:
            update_columns.append("checks = checks | ?")
            parameters.append(checks_value)

        if not update_columns:
            # These are failed GBIF queries
            # ToDo -- HIGH PRIORITY:
            #   Set flag in database!
            commands.append(("", []))
            return commands

        common_parameters = copy.deepcopy(parameters)
        for i in range(0, len(self.specimenids), max_varialbe_count):
            batch = self.specimenids[i:i + max_varialbe_count]
            placeholders = ', '.join(['?'] * len(batch))
            command = f"""
                UPDATE specimen
                SET {', '.join(update_columns)}
                WHERE specimenid IN ({placeholders});
            """
            parameters.extend(batch)
            assert command.count('?') == len(parameters)
            commands.append((command, parameters))
            parameters = copy.deepcopy(common_parameters)  # Reset parameters for the next batch

        return commands

DB_MAP = {
    'processid': 'processid',
    'sampleid': 'sampleid',
    'fieldid': 'fieldid',
    'museumid': 'museumid',
    'record_id': 'record_id',
    'specimenid': 'specimenid',
    'processid_minted_date': 'processid_minted_date',
    'bin_uri': 'bin_uri',
    'bin_created_date': 'bin_created_date',
    'collection_code': 'collection_code',
    'inst': 'inst',
    'taxid': 'taxid',
    'taxon_name': 'taxon_name',
    'taxon_rank': 'taxon_rank',
    'kingdom': 'taxon_kingdom',
    'phylum': 'taxon_phylum',
    'class': 'taxon_class',
    'order': 'taxon_order',
    'family': 'taxon_family',
    'subfamily': 'taxon_subfamily',
    'tribe': 'taxon_tribe',
    'genus': 'taxon_genus',
    'species': 'taxon_species',
    'subspecies': 'taxon_subspecies',
    'species_reference': 'species_reference',
    'identification': 'identification',
    'identification_method': 'identification_method',
    'identification_rank': 'identification_rank',
    'identified_by': 'identified_by',
    'identifier_email': 'identifier_email',
    'taxonomy_notes': 'taxonomy_notes',
    'sex': 'sex',
    'reproduction': 'reproduction',
    'life_stage': 'life_stage',
    'short_note': 'short_note',
    'notes': 'notes',
    'voucher_type': 'voucher_type',
    'tissue_type': 'tissue_type',
    'specimen_linkout': 'specimen_linkout',
    'associated_specimens': 'associated_specimens',
    'associated_taxa': 'associated_taxa',
    'collectors': 'collectors',
    'collection_date_start': 'collection_date_start',
    'collection_date_end': 'collection_date_end',
    'collection_event_id': 'collection_event_id',
    'collection_time': 'collection_time',
    'collection_notes': 'collection_notes',
    'geoid': 'geoid',
    'country/ocean': 'country_ocean',
    'country_iso': 'country_iso',
    'region': 'region',
    'province/state': 'province_state', # Real name in tsv
    'province': 'province_state', # Format in datapackage description
    'sector': 'sector',
    'site': 'site',
    'site_code': 'site_code',
    'coord': 'coord',
    'coord_accuracy': 'coord_accuracy',
    'coord_source': 'coord_source',
    'elev': 'elev',
    'elev_accuracy': 'elev_accuracy',
    'depth': 'depth',
    'depth_accuracy': 'depth_accuracy',
    'habitat': 'habitat',
    'sampling_protocol': 'sampling_protocol',
    'nuc': 'nuc',
    'nuc_basecount': 'nuc_basecount',
    'insdc_acs': 'insdc_acs',
    'funding_src': 'funding_src',
    'marker_code': 'marker_code',
    'primers_forward': 'primers_forward',
    'primers_reverse': 'primers_reverse',
    'sequence_run_site': 'sequence_run_site',
    'sequence_upload_date': 'sequence_upload_date',
    'bold_recordset_code_arr': 'bold_recordset_code_arr',
}

# Defines all columns used as primary key
PRIMARY_MAP = {
    "specimenid": True
}

INDEXING_MAP = {
    'none': True
}

# Defines all columns that are not allowed to be NULL
NOT_NULL_MAP = {
    'specimenid': True,
    'nuc': True
}

# Defines parser for data in BOLD DB
DB_PARSERS = {
    "default": lambda value: str(value), # Fallback to string
    "string:date": parse_date,
    "float": lambda value: float(value),
    "number": lambda value: int(value),
    "integer": lambda value: int(value),
    # ToDo: Add integer etc.
}

# Manual overwrite functions for data in BOLD DB
DB_PARSERS_M_OVERRIDE = {
    'processid': lambda value: value
}

# ToDo: Add special attributes e.g. not null.

# Defines datatypes to use in sql database
SQL_TYPES = {
    "default": "STRING",
    "string": "STRING",
    "string:date": "DATE",
    "array": "JSON",
    "float": "FLOAT",
    "number": "INTEGER",
    "integer": "INTEGER",
    "char": "CHAR"
}

TAXONOMY_MAP = {
    'kingdom':      'taxon_kingdom',
    'phylum':       'taxon_phylum',
    'class':        'taxon_class',
    'order':        'taxon_order',
    'family':       'taxon_family',
    'subfamily':    'taxon_subfamily',
    'tribe':        'taxon_tribe',
    'genus':        'taxon_genus',
    'species':      'taxon_species',
    'subspecies':   'taxon_subspecies'
}

TAXONOMY_TO_INT = {
    'kingdom':       1,
    'phylum':        2,
    'class':         3,
    'order':         4,
    'family':        5,
    'subfamily':     6,
    'tribe':         7,
    'genus':         8,
    'species':       9,
    'subspecies':   10
}

INT_TO_TAXONOMY = {
    1:   'kingdom',
    2:   'phylum',
    3:   'class',
    4:   'order',
    5:   'family',
    6:   'subfamily',
    7:   'tribe',
    8:   'genus',
    9:   'species',
    10:  'subspecies'
}

REV_AXONOMY_MAP = {
    'taxon_kingdom':    'kingdom',
    'taxon_phylum':     'phylum',
    'taxon_class':      'class',
    'taxon_order':      'order',
    'taxon_family':     'family',
    'taxon_subfamily':  'subfamily',
    'taxon_tribe':      'tribe',
    'taxon_genus':      'genus',
    'taxon_species':    'species',
    'taxon_subspecies': 'subspecies'
}
def _parse_database_layout(json_data: str) -> Tuple[List[ColumnInfo], Dict[str, Callable]]:
    """ Returns name of columns for database

        Args:

        Returns:

        Raises:
            ValueError: On wrong format or illegal data
    """
    layout = []
    parser_dict = {}

    resources = json_data.get("resources", [])

    for resource in resources:
        schema_fields = resource.get("schema", {}).get("fields", [])

        # First sanity check:
        # if len(schema_fields) != len(DB_MAP.keys()):
        #     raise ValueError(f"Json file has {len(DB_MAP.keys()) }"\
        #                         f"columns, expected: {len(schema_fields)}")

        for field in schema_fields:
            name = field.get("name")
            index = field.get("index")
            format_type = field.get("type", "default")

            # Handle bad json provided by bold.
            if name == "province":
                index = 50

            # Sanity check for data:
            if name is None:
                raise ValueError('Unable to extract column name')
            if format_type is None:
                raise ValueError(f"No format_type provided at column {name}")
            if index is None:
                raise ValueError(f"No index provided at column {name}")

            mapped_name = DB_MAP.get(name, None)
            if mapped_name is None:
                raise ValueError(f"Unexpected database format detected: {name} is not in map.")

            parser = DB_PARSERS.get(format_type, DB_PARSERS["default"])
            data_format = SQL_TYPES.get(format_type, SQL_TYPES["default"])
            is_primary = PRIMARY_MAP.get(name, False)
            uses_idx = INDEXING_MAP.get(name, False)
            infos = ColumnInfo(index, mapped_name, parser, data_format, is_primary, uses_idx)
            layout.append(infos)

            parser_dict[mapped_name] = parser

    layout.sort(key=lambda x: x.index)

    return layout, parser_dict


def get_data_layout(file: str) -> Tuple[List[ColumnInfo], Dict[str, Callable]] | bool:
    """ Returns column names for database """

    # ToDo: Better, more consistent return type

    try:
        with open(file, 'r', encoding='utf-8') as handle:
            json_data = json.load(handle)
            layout, parser_dict = _parse_database_layout(json_data)
            return layout, parser_dict
    except FileNotFoundError:
        print(f"Error: File {file} was not found.")
        return False
    except json.JSONDecodeError:
        print(f"Error: File {file} is not a valid JSON file.")
        return False
    except IOError:
        return False

def get_create_command(table_name: str, layout: List[ColumnInfo]) -> str:
    #ToDo: NOT NULL data enforcement
    """ Returns the table layout to use """
    column_definitions = []
    indices = []

    for column in layout:
        col_def = (f"{column.col_name} {column.data_format} "
                   f"{' PRIMARY KEY' if column.is_primary else ''}")
        column_definitions.append(col_def)
        if column.uses_idx:
            indices.append(column.col_name)

    create_table_sql = f"CREATE TABLE {table_name} ({', '.join(column_definitions)});"

    index_statements = [f"CREATE INDEX idx_{col} ON {table_name}({col});" for col in indices]

    if index_statements:
        return create_table_sql + "\n" + "\n".join(index_statements)

    return create_table_sql

class TsvParser():
    """ TSV Parser for BOLD """

    def __init__(self, tsv_file: str, marker_code: str,
                 data_parser: dict) -> None:

        if not file_exist(tsv_file):
            raise ValueError(f"File {tsv_file} does not exist")

        self._path = tsv_file
        self._marker_code = marker_code
        self._file = open(tsv_file, newline='\n', encoding='utf-8')
        self._tsv_reader = csv.DictReader(self._file,
                                          delimiter='\t',
                                          quoting=csv.QUOTE_NONE)

        # We need to rename the headers to match the database column names here
        self._tsv_reader.fieldnames = [DB_MAP.get(header, header) for header
                                       in self._tsv_reader.fieldnames]

        self._data_parser = data_parser

    @staticmethod
    def _null_data(row: str, value: str) -> bool:
        """ Returns True if a mandatory field is NULL """
        if NOT_NULL_MAP.get(row, False):
            return value is None

        return False

    def _transform_value(self, value: Any, pars_func: callable) -> Any:
        """ Transform the value for SQL insertion """
        if (value is None) or (value.strip() == '') or (value.strip() == 'None'):
            return None

        return pars_func(value)

    def __iter__(self) -> 'TsvParser':
        return self

    def __next__(self) -> Dict[str, Dict[str, str]]:
        """ Returns next element in tsv_file with marker

        The marker object is not checked for validity.

        Returns:
            Dict[str, str]: The next row as a dictionary if the marker matches.
        """

        # This are the columns we need to just copy to the new table
        # If you need to add more collums, add them here.
        column_names = [
            "taxon_rank",
            "taxon_kingdom",
            "taxon_phylum",
            "taxon_class",
            "taxon_order",
            "taxon_family",
            "taxon_subfamily",
            "taxon_tribe",
            "taxon_genus",
            "taxon_species",
            "taxon_subspecies",
            "identification_rank",
            "specimenid",
            "country_iso"
        ]

        for row in self._tsv_reader:
            marker = row.get('marker_code', None)
            if marker is not None and self._marker_code == marker:

                # Fill information for table processing_input
                # Check if any mandatory information is missing
                # E.g. we need a sequence (nuc) and a specimenid.
                # if either of these entries is NULL, we discard the record
                transformed_row = {key: self._transform_value(value, self._data_parser.get(key, lambda x: x)) for key, value in row.items()}
                null_checks = {self._null_data(key, value)
                               for key, value in transformed_row.items()}

                if True in null_checks:
                    logger.debug("Found an invalid row in tsv file: %s", row)
                    continue

                # Manually add hash and update indication.
                values_for_hash = [str(value) for value in transformed_row.values()]

                # Fill information for Table specimin
                specimen_rows = {column: transformed_row[column] for column in
                                 column_names if column in transformed_row}

                specimen_rows['nuc_raw'] = transformed_row['nuc']
                specimen_rows['hash'] = hashlib.sha256(''.join(values_for_hash).encode()).hexdigest()
                specimen_rows['last_updated'] = datetime.today().strftime('%Y-%m-%d')
                specimen_rows['include'] = False

                # Process coordinate and insert klimate zone if any coordinates
                # are available.
                coords = transformed_row.get('coord', None)
                specimen_rows['kg_zone'] = None
                if coords is not None:
                    # There is an entry we can process, extract the coordinates
                    # and insert the correct climate zone
                    lati = transformed_row.get('coord').split(',')[0].strip()[1:] # Remove leading []
                    long = transformed_row.get('coord').split(',')[1].strip()[:-1] # Remove trailing []
                    try:
                        lati = float(lati)
                        long = float(long)
                        specimen_rows['kg_zone'] = kgcpy.vectorized_lookupCZ([long], [lati])[0]
                    except ValueError:
                        logger.critical("Unable to convert coordinates to float: %s, %s", lati, long)
                        logger.critical("Original coordinates: %s", transformed_row.get('coord'))
                        pass

                # Set review to True, so we can simply rewrite row after
                # checking hash value for changes.
                specimen_rows['review'] = True

                return {'processing_input': transformed_row,
                        'specimen': specimen_rows
                }

        self._file.close()
        raise StopIteration
