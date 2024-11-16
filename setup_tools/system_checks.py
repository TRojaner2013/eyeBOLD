""" Module to check if system requirements are met. """

import os
import logging
import subprocess

#import common.constants as const

logger = logging.getLogger(__name__)

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

def check_env() -> int:
    """Checks if environmental variables are set.

    Returns:
        int: Bitvector encoding environment information.
        0 equals no error/fault.
    """

    result = 0

    # Check GBIF information for download
    if os.environ.get('GBIF_USER', None) is None:
        logger.error("GBIF_USER is not set! Please set environmental variable!")
        result |= 1 << 0

    if os.environ.get('GBIF_PWD', None) is None:
        logger.error("GBIF_PWD is not set! Please set environmental variable!")
        result |= 1 << 1

    return result

def check_raxtax_bin() -> int:
    """ Check if raxtax binary is available """

    try:
        # Try to run the raxtax command and check if it's valid
        result = subprocess.run(["raxtax", '--help'],
                                cwd=os.path.join(ROOT_DIR, ".."),
                                check=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        if result.returncode == 0:
            return 0
        return 1

    except subprocess.CalledProcessError as exc:
        logging.error("Unable to execute RaxTax: %s", exc)
        return False

def startup_checks() -> int:
    """ Run all startup checks

        Returns:
            int: 0 on success, error code otherwise
    """

    result = check_env()
    if result:
        logging.info("System Check: FAIL")
        logging.error("Environment check failed with code: %s", result)
        return result

    result = check_raxtax_bin()
    if result:
        logging.info("System Check: FAIL")
        logging.error("RaxTex check failed with code: %s", result)
        return result

    logging.info("System Check: PASS")

    return 0
