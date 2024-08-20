"""
This module includes fixed information for our database
"""

from enum import StrEnum

class CreateCommands(StrEnum):
    # Information regarding SPECIMEN table
    SPECIMEN_CMD = '''CREATE TABLE IF NOT EXISTS specimen (
                    specimenid INTEGER PRIMARY KEY NOT NULL,
                    nuc_raw TEXT NOT NULL,
                    nuc_san TEXT,
                    geo_info TEXT,
                    hash VARCHAR(64) NOT NULL,
                    last_updated DATE NOT NULL,
                    review BOOLEAN NOT NULL,
                    processing_info JSON,
                    include BOOLEAN,
                    gbif_key INTEGER,
                    taxon_rank VARCHAR(255),
                    taxon_kingdom VARCHAR(255),
                    taxon_phylum VARCHAR(255),
                    taxon_class VARCHAR(255),
                    taxon_order VARCHAR(255),
                    taxon_family VARCHAR(255),
                    taxon_subfamily VARCHAR(255),
                    taxon_tribe VARCHAR(255),
                    taxon_genus VARCHAR(255),
                    taxon_species VARCHAR(255),
                    taxon_subspecies VARCHAR(255),
                    identification_rank VARCHAR(255),
                    checks INTEGER DEFAULT 0,
                    FOREIGN KEY (specimenid) REFERENCES processing_input(specimenid),
                    FOREIGN KEY (gbif_key) REFERENCES gbif_info(gbif_key)
                    );'''

    # Information regarding GBIF_INFO Table
    # Create Index on GBIF_KEY
    GBIF_INFO_CMD = '''CREATE TABLE IF NOT EXISTS gbif_info (
                    gbif_id PRIMARY KEY NOT NULL,
                    gbif_key INTEGER NOT NULL,
                    long FLOAT NOT NULL,
                    latt FLOAT NOT NULL
                    );'''

    # Mock commands for testing purposes...
    PROCESSING_INPUT_CMD =  '''CREATE TABLE IF NOT EXISTS processing_input (
                               specimenid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                               input_data TEXT
                               );'''

    INV_SQL_CMD = '''CREATE TABLE Invalid (id INTEGER,
                     name TEXT PRIMARY KEY,
                     PRIMARY KEY (id, name));'''
