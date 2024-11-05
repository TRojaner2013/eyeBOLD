"""Module defining constants for eyeBold """

import os

# MAIN SETTING
USE_GBIF_SQL = True # Use GBIF SQL Downloads
USE_GPU = False # Use GPU for speedup

# BUILD RELATED
INSERT_CHUNIK_SIZE = 1000

NUM_CPUS = 1 # Number of CPUs
PHYSICAL_CORES_PER_CPU = 8 # Physical cores per CPU

# MARK DUBLICATES RELATED
TRIVIAL_SIZE = 5000 # Max. sequences in sqeuentially checked instances
SMALL_SIZE = 50000 # Max sequences for instance to be small
TRIVIAL_PARALLEL_FACTOR = 50000 # Instances loaded at once for trivial instances

# Larger instances are splitted for a precheck
# This defines the size of subproblems and splitting behaviour.
SUBPROBLEM_SIZE = 2000 # Sequences in subproblem
SUBPROBLEM_SIZE_MIN = 1000 # Min. sequences in subproblem sweep
SUBPROBLEM_SIZE_MAX = 5000 # Max.sequences in subproblem sweep
SUBPROBLEM_SIZE_STEP = 1000 # Step size for sweep

# This one affects memory usage!
# Defines how many instances are loaded into memory at once.
SIMPLE_PARALLEL_FACTOR = 32
HARD_PARALLEL_FACTOR = 16

# DEBUGGING SIZES FOR MARK DUBLICATES
# This is designed to test if the algorithm works correctly without
# spending too much time for large instances
# TRIVIAL_SIZE = 200
# SMALL_SIZE = 100
# SUBPROBLEM_SIZE = 100
# SUBPROBLEM_SIZE_MIN = 100
# SUBPROBLEM_SIZE_MAX = 500
# SUBPROBLEM_SIZE_STEP = 100
# TRIVIAL_PARALLELL_FACTOR = 1000

# RAXTAX RELATED -- Do not change any of this
RAXTAX_CMD = "raxtax"
RAXTAX_ARGS = []
RAXTAX_DB_IN = os.path.join(".", "raxtax_db.fasta")
RAXTAX_QUERY_IN = os.path.join(".", "raxtax_query.fasta")
RAXTAX_OUT = "./"

# SQL RELATED

# Max. number of variavles for SQL commands.
# Make sure this works with your SQL implementation.
SQL_SAVE_NUM_VARS = 950
