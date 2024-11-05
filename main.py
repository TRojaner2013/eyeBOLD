""" Entry point for eyeBold.

This script will either invoke an GUI or use a CLI.
"""

from datetime import datetime
import logging
import sys
#from PyQt6.QtWidgets import QApplication, QWidget

from cli import cli_main

# ToDo: Implement GUI
# ToDo: Perform system checks on startup
if __name__ == "__main__":

    # Setup logger
    time_str = datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
    logging.basicConfig(filename=f"log_eyebold_{time_str}.log",
                        level=logging.NOTSET)
    logger = logging.getLogger(__name__)
    logger.info("EyeBOLD started at %s", time_str)
    logger.debug("Command line arguments: %s", sys.argv)
    logger.debug("Number of arguments: %d", len(sys.argv))

    if len(sys.argv) == 1:
        # Start GUI if non arguments provided
        logger.debug("Starting GUI...")
        raise NotImplementedError("GUI not implemented yet.")
        # eyebold = QApplication([])
        # main_window = QWidget()
        # main_window.show()
        # eyebold.exec()
        # sys.exit(0)

    # Run CLI when arguments are provided
    logger.debug("Invoking CLI...")
    cli_main(sys.argv)
