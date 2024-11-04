"""Module defining constants for eyeBold """

import os

# MEMORY / PROCESSOR RELATED
INSERT_CHUNIK_SIZE = 1000

NUM_CPUS = 1 # Number of CPUs
PHYSICAL_CORES_PER_CPU = 8 # Number of physical cores per CPU

# MARK DUBLICATES RELATED
TRIVIAL_SIZE = 5000 # Max. number of sequences in instances,that are checked sequentially
SMALL_SIZE = 50000 # Max number of sequences that are checkes in parallel, but not splitted
SUBPROBLEM_SIZE = 2000 # Number of sequences in subproblem instances for large instances
SUBPROBLEM_SIZE_MIN = 1000 # Number of sequences in subproblem instances for large instances
SUBPROBLEM_SIZE_MAX = 5000 # Number of sequences in subproblem instances for large instances
SUBPROBLEM_SIZE_STEP = 1000 # Number of sequences in subproblem instances for large instances
TRIVIAL_PARALLEL_FACTOR = 50000 # How many instances are loaded in memory at once for trivial instances

# DEBUGGING SIZES FOR MARK DUBLICATES
# This is designed to test if algorighm works correctly without spending too much time for large instances
# TRIVIAL_SIZE = 200
# SMALL_SIZE = 100
# SUBPROBLEM_SIZE = 100
# SUBPROBLEM_SIZE_MIN = 100
# SUBPROBLEM_SIZE_MAX = 500
# SUBPROBLEM_SIZE_STEP = 100
# TRIVIAL_PARALLELL_FACTOR = 1000

# This one affects memory usage!
SIMPLE_PARALLEL_FACTOR = 32 # How many instances are checked in parallel (multiplyed by number of physical cores)
HARD_PARALLEL_FACTOR = 16



# RAXTAX RELATED -- Do not change any of this
RAXTAX_CMD = "raxtax"
RAXTAX_ARGS = []
RAXTAX_DB_IN = os.path.join(".", "raxtax_db.fasta")
RAXTAX_QUERY_IN = os.path.join(".", "raxtax_query.fasta")
RAXTAX_OUT = "./"

# SQL RELATED
SQL_SAVE_NUM_VARS = 950 # Max. number of variables for SQL query. Do not change unless you know that your SQL implementation can handle more.