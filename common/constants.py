"""Module defining constants for eyeBold """

import os

# MEMORY RELATED
INSERT_CHUNIK_SIZE = 1000

# MARK DUBLICATES RELATED
TRIVIAL_SIZE = 500 # Max. number of sequences in instances,that are checked sequentially
SMALL_SIZE = 10000 # Max number of sequences that are checkes in parallel, but not splitted
SUBPROBLEM_SIZE = 1000 # Number of sequences in subproblem instances for large instances


# RAXTAX RELATED -- Do not change any of this
RAXTAX_CMD = "raxtax"
RAXTAX_ARGS = []
RAXTAX_DB_IN = os.path.join(".", "raxtax_db.fasta")
RAXTAX_QUERY_IN = os.path.join(".", "raxtax_query.fasta")
RAXTAX_OUT = "./"

# SQL RELATED
SQL_SAVE_NUM_VARS = 950 # Max. number of variables for SQL query. Do not change unless you know that your SQL implementation can handle more.