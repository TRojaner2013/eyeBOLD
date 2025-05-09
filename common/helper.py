""" Module implementing helper methods """

from  datetime import datetime
import os

def file_exist(path: str) -> bool:
    """ Checks if file at path exists.

        Args:
            - path (str): Path to file

        Returns:
            True if file exists.
    """

    if not os.path.exists(path):
        return False

    return True

def parse_date(value: str) -> str:
    """ Returns sting date/time form BOld to an SQL date string

        Args:
            - value(str): Date/Time string in BOLD Format

        Returns:
            Date in SQL formatted string. 'NULL' on error.
    """
    if not value:
        return "NULL"

    date_formats = ["%Y-%m-%d", "%y-%m-%d", "%Y-%b-%d", "%y-%b-%d", "%Y-%m-%d", "%Y-%m-%d"]

    for date_format in date_formats:
        try:
            parsed_date = datetime.strptime(value, date_format)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return "NULL"
