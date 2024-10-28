"""
This module includes fixed information for our database
"""

from enum import StrEnum

class CreateCommands(StrEnum):
    # Information regarding SPECIMEN table
    # ToDo: Rename gbif_key to gbif_accepted_taxon_key!
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
                    country_iso VARCHAR(3),
                    kg_zone VARCHR(5),
                    FOREIGN KEY (specimenid) REFERENCES processing_input(specimenid)
                    );'''

    # Information regarding GBIF_INFO Table
    # Create Index on GBIF_KEY
    GBIF_INFO_CMD = '''CREATE TABLE IF NOT EXISTS gbif_info (
                    ebc_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    taxon_key INTEGER NOT NULL,
                    occurrenceid INTEGER NOT NULL,
                    long FLOAT NOT NULL,
                    latt FLOAT NOT NULL,
                    kg_zone TEXT
                    );'''

    # Alternative GBIF IMplementation
    GBIF_DB_CMD = '''CREATE TABLE climate_data (
                    taxon_key INTEGER PRIMARY KEY,
                    kg_af INTEGER DEFAULT 0,
                    kg_am INTEGER DEFAULT 0,
                    kg_as INTEGER DEFAULT 0,
                    kg_aw INTEGER DEFAULT 0,
                    kg_bsh INTEGER DEFAULT 0,
                    kg_bsk INTEGER DEFAULT 0,
                    kg_bwh INTEGER DEFAULT 0,
                    kg_bwk INTEGER DEFAULT 0,
                    kg_cfa INTEGER DEFAULT 0,
                    kg_cfb INTEGER DEFAULT 0,
                    kg_cfc INTEGER DEFAULT 0,
                    kg_csa INTEGER DEFAULT 0,
                    kg_csb INTEGER DEFAULT 0,
                    kg_csc INTEGER DEFAULT 0,
                    kg_cwa INTEGER DEFAULT 0,
                    kg_cwb INTEGER DEFAULT 0,
                    kg_cwc INTEGER DEFAULT 0,
                    kg_dfa INTEGER DEFAULT 0,
                    kg_dfb INTEGER DEFAULT 0,
                    kg_dfc INTEGER DEFAULT 0,
                    kg_dfd INTEGER DEFAULT 0,
                    kg_dsa INTEGER DEFAULT 0,
                    kg_dsb INTEGER DEFAULT 0,
                    kg_dsc INTEGER DEFAULT 0,
                    kg_dsd INTEGER DEFAULT 0,
                    kg_dwa INTEGER DEFAULT 0,
                    kg_dwb INTEGER DEFAULT 0,
                    kg_dwc INTEGER DEFAULT 0,
                    kg_dwd INTEGER DEFAULT 0,
                    kg_ef INTEGER DEFAULT 0,
                    kg_et INTEGER DEFAULT 0,
                    kg_ocean INTEGER DEFAULT 0,
                    country_codes TEXT);'''

    # Mock commands for testing purposes...
    PROCESSING_INPUT_CMD =  '''CREATE TABLE IF NOT EXISTS processing_input (
                               specimenid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                               input_data TEXT
                               );'''

    INV_SQL_CMD = '''CREATE TABLE Invalid (id INTEGER,
                     name TEXT PRIMARY KEY,
                     PRIMARY KEY (id, name));'''
