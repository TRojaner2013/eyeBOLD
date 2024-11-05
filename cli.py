"""Command line interface for eyeBOLD.

This module is a command line interface for eyebold.
It provides serveral subcommands to interact with eyeBOLD.

"""
# ToDo: Make handles return false instead of terminating program
# ToDo: Write a better module description
from datetime import datetime
import argparse
import sys
import logging

from common.eyebold_database import EyeBoldDatabase
from common.eyebold_database import ExportFormats

logger = logging.getLogger(__name__)

def _init_argparse() -> argparse.ArgumentParser:
    """Creates and returns argument parser

        Retrurns:
            ArgumentParser for eyeBOLD
    """

    #ToDo: Create useful help messages here.
    logger.debug("Called _init_argparse()")

    my_parser = argparse.ArgumentParser(prog='eyeBOLD',
                                        description=('Currating tool for the'
                                                     ' Biodiversity of Life Database (BOLD)'),
                                        epilog=' -- ')

    my_parser.add_argument('db_file')
    my_parser.add_argument('loc_db_file')
    my_parser.add_argument('marker')
    my_parser.add_argument('-v', '--verbose', action='count', default=0)

    subpuarser = my_parser.add_subparsers(title='Subparser',
                                          description='build, update, export, query, review',
                                          help='',
                                          dest='sub')

    create_parser = subpuarser.add_parser('build')
    create_parser.add_argument('tsv_file',
                               help="Specify the input data from bold as .tsv")
    create_parser.add_argument('datapackage_file',
                               help="Specify the datapackage file from bold as .json")

    # Add parser for update command
    update_parser = subpuarser.add_parser('update')
    update_parser.add_argument('tsv_file',
                               help="Specify the input data from bold as .tsv")
    update_parser.add_argument('datapackage_file',
                               help="Specify the datapackage file from bold as .json")

    # Add parser for query command
    query = subpuarser.add_parser('query')
    query.add_argument('sql_query',
                            help="SQL query to execute on the database")
    query.add_argument('-o', '--output', type=str, default=None,
                           help="Specify the output file name or path")
    query.add_argument('-f', '--format', type=str, default=None,
                           help="Specify the output format: TSV, CSV")

    # Add parser for export command
    export_parser = subpuarser.add_parser('export')
    export_parser.add_argument("format",
                               help="Specify the output format: TSV, CSV, RAXTAX or FASTA")
    export_parser.add_argument('output', type=str,
                               help="Specify the output file name or path")

    # Add parser for build-location-db command
    build_loc_db_parser = subpuarser.add_parser('build-location-db')
    build_loc_db_parser.add_argument("-s", '--batch_size', type=int, default=1000,
                                     help="Specify the batch size for the download process")

    # Add parser for review command
    subpuarser.add_parser('review')

    return my_parser

def _log_success() -> None:
    """Logs success on programm exit."""
    _time_str = datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
    logger.info("All actions succeded.")
    logger.info("Terminating eyeBOLD with exit code 0"\
                " at %s", _time_str)

def _build_handle(db_file: str, loc_db_file: str, marker: str,
                   tsv_file: str, dtpkg_file: str) -> bool:
    """Handles build command

        Args:
            - db_file (str): Location of db-file
            - loc_db_file (str): Location of location-db-file
            - marker (str)" Marker used in database
            = tsv_file (str): Location of databse input file
            - dtpkg_file (str): Location of database input description file

        Returns:
            bool: True on success
    """
    logger.debug("Called _build-handle()")

    try:
        my_db = EyeBoldDatabase(db_file, marker, loc_db_file)
        my_db.create(tsv_file, dtpkg_file)
        my_db.curate()
    except FileNotFoundError:
        logging.critical("Unable to open database %s."\
                         "File does not exists.", db_file)
        sys.exit(3)

    return True


def _update_handle(db_file: str, loc_db_file: str, marker: str,
                   tsv_file: str, dtpkg_file: str) -> bool:
    """Handles update command

        Args:
            - db_file (str): Location of db-file
            - loc_db_file (str): Location of location-db-file
            - marker (str)" Marker used in database
            = tsv_file (str): Location of databse input file
            - dtpkg_file (str): Location of database input description file

        Returns:
            bool: True on success
    """
    logger.debug("Called _update_handle()")

    try:
        my_db = EyeBoldDatabase(db_file, marker, loc_db_file)
        my_db.update(tsv_file, dtpkg_file)
    except FileNotFoundError:
        logging.critical("Unable to open database %s." \
                         "File does not exists.", db_file)
        sys.exit(3)
    return True

def _review_handle(db_file: str, loc_db_file: str, marker: str) -> bool:
    """Handles review command

        Args:
            - db_file (str): Location of db-file
            - loc_db_file (str): Location of location-db-file
            - marker (str)" Marker used in database

        Returns:
            bool: True on success
    """
    logger.debug("Called _review_handle()")
    try:
        my_db = EyeBoldDatabase(db_file, marker, loc_db_file)
        my_db.review()
    except FileNotFoundError:
        logging.critical("Unable to open database %s."\
                         "File does not exists.", db_file)
        sys.exit(3)
    return True

def _build_location_db_handle(db_file: str, loc_db_file: str, marker: str,
                              batch_size: int=500) -> bool:
    """Handles build-location-db command

        Args:
            - db_file (str): Location of db-file
            - loc_db_file (str): Location of location-db-file
            - marker (str)" Marker used in database
            = batch_size (int): Number of keys to download at once

        Returns:
            bool: True on success
    """
    logger.debug("Called _create_handle()")

    try:
        my_db = EyeBoldDatabase(db_file, marker, loc_db_file)
        my_db.invoke_tracker(batch_size)
    except FileNotFoundError:
        logging.critical("Unable to open database %s."\
                         "File does not exists.", db_file)
        sys.exit(3)

    return True


def _query_handle(db_file: str, loc_db_file: str, marker: str,
                  query: str, format_: str|None=None, out_file: str|None=None) -> bool:
    """Handles query command

        Note:
            Format can only be supplied with out-file option.
        Args:
            - db_file (str): Location of db-file
            - loc_db_file (str): Location of location-db-file
            - marker (str)" Marker used in database
            - query (str): Full SQL-Query for database
            = format_ (str): Export format
            - out_file (str): Location of output file

        Returns:
            bool: True on success
    """
    logger.debug("Called _get_handle()")

    if format_ is not None:
        try:
            format_ = ExportFormats.from_str(format_)
            if format_ in (ExportFormats.RAXTAX, ExportFormats.FASTA):
                logger.critical("Invalid format specified: %s", format_)
                logger.critical("Valid formats are: TSV, CSV")
                logger.debug("Terminating program...")
                sys.exit(3)
        except ValueError:
            logger.critical("Invalid format specified: %s", format_)
            logger.critical("Valid formats are: TSV, CSV, RAXTAX, FASTA")
            logger.debug("Terminating program...")
            sys.exit(2)

    try:
        my_db = EyeBoldDatabase(db_file, marker, loc_db_file)
        if out_file is not None:
            my_db.query_export(query, out_file, format_)
        else:
            my_db.query_print(query)

    except FileNotFoundError:
        logger.critical("Unable to open database %s."\
                         "File does not exists.", db_file)
        sys.exit(3)
    return True


def _export_handle(db_file: str, loc_db_file: str, marker: str,
                   format_: str, out_file: str) -> bool:
    """Handles export command

        Args:
            - db_file (str): Location of db-file
            - loc_db_file (str): Location of location-db-file
            - marker (str)" Marker used in database
            = format_ (str): Export format
            - out_file (str): Location of output file

        Returns:
            bool: True on success
    """
    logger.debug("Called _export_handle()")

    try:
        format_ = ExportFormats.from_str(format_)
    except ValueError:
        logger.critical("Invalid format specified: %s", format_)
        logger.critical("Valid formats are: TSV, CSV, RAXTAX, FASTA")
        logger.debug("Terminating program...")
        sys.exit(2)

    try:
        format_ = ExportFormats.from_str(format_)
        my_db = EyeBoldDatabase(db_file, marker, loc_db_file)
        my_db.export(format_, out_file)
    except FileNotFoundError:
        logging.critical("Unable to open database %s."\
                         "File does not exists.", db_file)
        sys.exit(3)

    return True


def cli_main(*args) -> None:
    """Entry point for command line interface

        Args:
            - *args: Arguments passed by command line
    """

    parser = _init_argparse()
    args = parser.parse_args()

    db_file = args.db_file
    loc_db_file = args.loc_db_file
    marker = args.marker

    # Set logging verbosity
    #ToDo Q1: Make this a function
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

    if args.sub == 'build':
        logger.debug("Invoking build handle.")
        if _build_handle(db_file, loc_db_file, marker,
                          args.tsv_file, args.datapackage_file):
            _log_success()
            sys.exit(0)
    elif args.sub == 'update':
        logger.debug("Invoking update handle.")
        if _update_handle(db_file, loc_db_file, marker,
                          args.tsv_file, args.datapackage_file):
            _log_success()
            sys.exit(0)
    elif args.sub == 'query':
        logger.debug("Invoking query handle.")

        if args.format is None and args.output is not None:
            logger.critical("Output file specified without format.")
            logger.debug("Terminating program...")
            sys.exit(2)
        elif args.format is not None and args.output is None:
            logger.critical("Output format specified without output file.")
            logger.debug("Terminating program...")
            sys.exit(2)
        elif args.format is None and args.output is None:
            logger.debug("No output file or format specified.")
            logger.debug("Printing query to console.")
            if _query_handle(db_file, loc_db_file, marker, args.sql_query):
                _log_success()
                sys.exit(0)
        else:
            if _query_handle(db_file, loc_db_file, marker,
                             args.sql_query, args.format, args.output):
                _log_success()
                sys.exit(0)
    elif args.sub == 'export':
        logger.debug("Invoking export handle.")
        if _export_handle(db_file, loc_db_file, marker, args.format, args.output):
            _log_success()
            sys.exit(0)
    elif args.sub == 'build-location-db':
        logger.debug("Invoking build-location-db handle.")
        if _build_location_db_handle(db_file, loc_db_file, marker, args.s__batch_size):
            _log_success()
            sys.exit(0)
    elif args.sub == 'review':
        logger.debug("Invoking review handle.")
        if _review_handle(db_file, loc_db_file, marker):
            _log_success()
            sys.exit(0)
    else:
        print('Invalid command.\n')
        parser.print_help()
        logger.critical("Invalid argument passed: %s", args.sub)
        logger.info("EyeBOLD terminated due to error at"\
                     "%s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        sys.exit(2) # See bash documentation

    # We ran into some kind of error
    logging.critical('Eyebold ran into an problem.')
    logging.critical("Terminating eyeBOLD with exit code 1 at"\
                     "%s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sys.exit(1)
