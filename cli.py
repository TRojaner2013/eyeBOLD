"""Command line interface for eyeBOLD.

This module is a command line interface for eyebold.
It provides serveral subcommands to interact with eyeBOLD.

Usage:
    eyebold.py create 
    eyebold.py update
    eyebold.py get
    eyebold.py export
    eyebold.py curate

            --- DEPRECATION WARNING -- 
THIS FILE AND ALL ITS CONTENTS WILL BE REPLACED / RESTRUCTURED IN A FUTURE RELEASE.
DO NOT RELY ON THE CONTENTS AND METHODS OF THIS FILE.

"""
from datetime import datetime
import argparse
import sys
import logging

from common.dbHandler import open_db, get_unsanatized_taxonomy
from common.csvTools import export_dict_to_csv
#from eyebold.tools.harmonizer import harmonize, harmonize_stats

logger = logging.getLogger(__name__)

#ToDo: Get these from yaml file
DB_FILE = "removed"
TSV_FILE = "removed"

def _init_argparse() -> argparse.ArgumentParser:
    """Creates and returns argument parser"""
    logger.debug("Called _init_argparse()")

    my_parser = argparse.ArgumentParser(prog='eyeBOLD',
                                        description=('Currating tool for the'
                                                     ' Biodiversity of Life Database (BOLD)'),
                                        epilog=' -- ')

    my_parser.add_argument('-v', '--verbose', action='count', default=0)
    # Crate sbuparser to distinguish between
    # eyebold create --> Creates new database & config
    # eyebold update --> Updates database
    # eyebold get --> Allows queries
    # eyebold export --> Exports database
    # eyebold curate --> Curated database

    #ToDo: Add a help text here.
    subpuarser = my_parser.add_subparsers(title='Subparser',
                                       description='create, update, get, export',
                                       help='',
                                       dest='sub')

    create_parser = subpuarser.add_parser('create')
    create_parser.add_argument('tsv_file')
    create_parser.add_argument('json_file')
    create_parser.add_argument('database_file')

    update_parser = subpuarser.add_parser('update')
    update_parser.add_argument('tsv_file')
    update_parser.add_argument('json_file')

    get_parser = subpuarser.add_parser('get')
    get_parser.add_argument('taxonomy')

    export_parser = subpuarser.add_parser('export')
    export_parser.add_argument("format")

    export_parser = subpuarser.add_parser('curate')

    return my_parser

def _log_success():
    """Logs success on programm exit."""
    _time_str = datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
    logger.info("All actions succeded.")
    logger.info("Terminating eyeBOLD with exit code 0"\
                " at %s", _time_str)

def _create_handle() -> bool:
    """Handles create subcommand"""
    logger.debug("Called _create_handle()")

    # Connect to database
    try:
        db_handle = open_db(DB_FILE)
    except FileNotFoundError:
        logging.critical("Unable to open database %s."\
                         "File does not exists.", DB_FILE)
        sys.exit(3)

    return True


def _update_handle() -> bool:
    """Handles create subcommand"""
    logger.debug("Called _update_handle()")

    # Connect to database
    try:
        db_handle = open_db(DB_FILE)
    except FileNotFoundError:
        logging.critical("Unable to open database %s."\
                         "File does not exists.", DB_FILE)
        sys.exit(3)

    return True


def _get_handle() -> bool:
    """Handles create subcommand"""
    logger.debug("Called _get_handle()")

    # Connect to database
    try:
        db_handle = open_db(DB_FILE)
    except FileNotFoundError:
        logging.critical("Unable to open database %s."\
                         "File does not exists.", DB_FILE)
        sys.exit(3)

    return True


def _export_handle() -> bool:
    """Handles create subcommand"""
    logger.debug("Called _export_handle()")

    # Connect to database
    try:
        db_handle = open_db(DB_FILE)
    except FileNotFoundError:
        logging.critical("Unable to open database %s."\
                         "File does not exists.", DB_FILE)
        sys.exit(3)

    return True

def _curate_handle() -> bool:
    """Handles curate subcommand"""
    logger.debug("Called _get_curate()")

    # Connect to database
    try:
        db_handle = open_db(DB_FILE)
    except FileNotFoundError:
        logging.critical("Unable to open database %s."\
                         "File does not exists.", DB_FILE)
        sys.exit(3)

    # Perfom taxonomic harmonization...
    taxon_data = get_unsanatized_taxonomy(db_handle)
    #harmonize_stats(taxon_data.get('subspecies'), 'subspecies')
    export_dict_to_csv(taxon_data, "all_records_csv")
    #harmonize_stats(taxon_data.get('species'), 'species')
    #harmonize(taxon_data.get('subspecies'), 'subspecies')
    return True

def cli_main(*args):

    parser = _init_argparse()
    args = parser.parse_args()
    print(args)

    # Set logging verbosity
    if args.verbose == 0:
        logging.info("Setting logging level to CRITICAL.")
        logger.setLevel(logging.CRITICAL)
    elif args.verbose == 1:
        logging.info("Setting logging level to ERROR.")
        logger.setLevel(logging.ERROR)
    elif args.verbose == 1:
        logging.info("Setting logging level to WARNING.")
        logger.setLevel(logging.WARNING)
    elif args.verbose == 3:
        logging.info("Setting logging level to INFO.")
        logger.setLevel(logging.INFO)
    elif args.verbose >= 4:
        logging.info("Setting logging level to DEBUG.")
        logger.setLevel(logging.DEBUG)

    if args.sub == 'create':
        if _create_handle():
            _log_success()
            sys.exit(0)
    elif args.sub == 'update':
        if _update_handle():
            _log_success()
            sys.exit(0)
    elif args.sub == 'get':
        if _get_handle():
            _log_success()
            sys.exit(0)
    elif args.sub == 'export':
        if _export_handle():
            _log_success()
            sys.exit(0)
    elif args.sub == 'curate':
        if _curate_handle():
            _log_success()
            sys.exit(0)
    else:
        print('Invalid command.\n')
        parser.print_help()
        logging.critical("Invalid argument passed: %s", args.sub)
        logging.info("EyeBOLD terminated due to error at"\
                     "%s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        sys.exit(2) # See bash documentation

    # We ran into some kind of error
    logging.critical('Eyebold ran into an problem.')
    logging.critical("Terminating eyeBOLD with exit code 1 at"\
                     "%s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sys.exit(1)
