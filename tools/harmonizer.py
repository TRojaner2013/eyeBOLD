"""Harmonizer module"""

import csv
import shutil
import subprocess
import logging
import os
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import common.constants as const
from gbif.gbif import query_name_backbone_b2t
from sqlite.parser import GbifName

logger = logging.getLogger(__name__)

def _harmonize_names_b2t(taxon_data: List[Dict]) -> List[GbifName]:
    """Checks naming of taxons and returns matches as dictionary"""

    result = []

    with ThreadPoolExecutor(max_workers=const.GBIF_NAME_QUERY_THREADS) as thread_pool:

        future_to_query = {thread_pool.submit(query_name_backbone_b2t, query): query
                           for query in taxon_data}

        for future in as_completed(future_to_query):
            result.append(future.result())

    return result

def harmonize_b2t(taxon_data: List[Dict]) -> List[GbifName]:
    """Harmonizes taxonomical data in passed dictionary

    Arguments:
        -taxon_data: Dictionary containing data from taxon

    Returns:
        - List of GBIF Names
    """

    harmonized_names = _harmonize_names_b2t(taxon_data)

    return harmonized_names

def raxtax_entry() -> List:
    """Entry point for raxtax process"""
    handle:RaxTaxer = RaxTaxer(const.RAXTAX_DB_IN, const.RAXTAX_QUERY_IN)
    return handle.run()

# Deprecated
# # RaxTax specific classes
# @dataclass
# class RaxTaxTaxonomy:
#     p: Optional[str] = None  # Phylum
#     c: Optional[str] = None  # Class
#     o: Optional[str] = None  # Order
#     f: Optional[str] = None  # Family
#     g: Optional[str] = None  # Genus
#     s: Optional[str] = None  # Species

# Deprecated
# @dataclass
# class RaxTaxData:
#     specimen_id: str
#     taxonomy: RaxTaxTaxonomy
#     score_f: Optional[float] = None
#     score_g: Optional[float] = None
#     score_o: Optional[float] = None
#     score_c: Optional[float] = None
#     score_s: Optional[float] = None
#     local_signal: Optional[float] = None
#     global_signal: Optional[float] = None

class RaxTaxer():
    """ Class handling raxtax data """

    def __init__(self, db_in_file: str=None, query_file: str=None) -> None:
        """ Creates instance of RaxTaxer class"""
        self._db_in_file = db_in_file
        self._query_file = query_file
        self._out_path = os.path.join(".", f"{self._query_file.split('.', 2)[1][1:]}.out")

        # Make sure to delete all old files, otherwise we can't detect errors in the process
        if os.path.exists(self._out_path):
            shutil.rmtree(self._out_path)

        self._out_file = os.path.join(self._out_path, "raxtax.out")
        self._chunk_size = const.RAXTAX_BATCH_SIZE

    def _clean(self) -> None:
        """ Cleans up the raxtax inputs and the output directory """
        if os.path.exists(self._out_path):
            shutil.rmtree(self._out_path)

        if os.path.exists(self._query_file):
            os.remove(self._query_file)

        if os.path.exists(self._db_in_file):
            os.remove(self._db_in_file)


    def run(self) -> List[int]:
        """ Runs raxtax process and returns the results

            Note:
                Calls raxtax in subprocess, reads the output file, inserts data
                and finally cleans up the files.

            Returns:
                Lits[int]: List of specimenids of marked records

        """

        results = []
        self._invoke_raxtax()
        results.extend(self._retrieve_result())
        self._clean()
        return results

    def _invoke_raxtax(self) -> None:
        """ Calls raxtax in subprocess and waits for it to finish """
        try:
            logger.info("Invoking RaxTax...")
            subprocess.run(["raxtax", "-d", self._db_in_file, "-i", self._query_file,
                            "--skip-exact-matches", "--redo"], check=True)

            if not os.path.exists(self._out_file):
                logger.critical("Unable to trace raxtax output at %s",
                                 self._out_file)
                raise FileNotFoundError

            logger.info("RaxTax finished...")
        except subprocess.CalledProcessError as exc:
            logger.error("Failed to execure raxtax: %s", exc)
            raise

    def _retrieve_result(self) -> List[int]:
        """ Reads result data from raxtax in batches and returns entries to mark.

        Note:
            Entries are markd based on a score criteria

        Returns:
            List[int]: specimenids of marked records
        """
        last_id = None
        marked_records = []


        len_file = 0
        with open(self._out_file, 'r', encoding='utf-8') as file_handle:
            reader = csv.reader(file_handle, delimiter='\t')

            batch = []
            last_id = 0
            for row in reader:
                batch.append(row)
                if len(batch) >= self._chunk_size:
                    len_file += len(batch)
                    self._process_batch(batch, last_id, marked_records)
                    # Update last ID after processing batch
                    # This is necessary to avoid processing the same record twice
                    last_id = int(batch[-1][0].split(';')[0])
                    batch = []

            if batch:
                len_file += len(batch)
                self._process_batch(batch, last_id, marked_records)

        return marked_records

    def _process_batch(self, batch: List[int], last_id: int|None,
                       marked_records: List[int]) -> None:
        """ Processes a batch of records

            Args:
                - batch (List[int]): List of records
                - last_id (int|None): Last specimenid processed, if any
                - marked_records (List[int]): List of marked records
        """
        for row in batch:
            try:
                record_id = int(row[0].split(';')[0]) # Extracts the ID

                if record_id == last_id:
                    continue
                last_id = record_id

                original_taxonomy = row[0].split('=')[1].split(',') # Original taxonomy lineage
                compared_taxonomy = row[1].split(',')               # Vompared taxonomy lineage
                scores = list(map(float, row[2].split(',')))        # Scores per lineage level
                # local_signal = float(row[3])                      # Extract overall score
                # global_signal = float(row[4])                     # Extract overall score

                if self._mark_entry(original_taxonomy, compared_taxonomy, scores):
                    marked_records.append(record_id)

            except (IndexError, ValueError) as err:
                print(f"Error parsing row: {row}, Error: {err}")
                continue  # Skip rows with parsing issues

    def _mark_entry(self, original_taxonomy: List[str], compared_taxonomy: List[str],
                    scores: List[float]) -> bool:
        """
        Determines if an entry should be marked based on lineage difference and score criteria.
        
        Args:
            - original_taxonomy (List[str]): Original taxonomy lineage list.
            - compared_taxonomy (List[str]): Compared taxonomy lineage list.
            - scores (List[floatr]): Scores corresponding to each level in the lineage.
            
        Returns:
            bool: True if entry should be marked, False otherwise.
        """
        for i, (original, compared) in enumerate(zip(original_taxonomy, compared_taxonomy)):
            if original != compared and i < len(scores) - 1:  # Ignore species level difference
                assert i < 5
                if scores[i] >= const.RAXTAX_SCORE_THRESHOLD:
                    return True
        return False

    # Depreacted
    # def _extract_specimen_data(self) -> List[RaxTaxData]:
    #     """Extracts specimen id, taxonomy, and score values from a TSV file.

    #         Note: This method is deprecated and should not be used.
    #     """
    #     results = []
    #     seen_set = set()

    #     with pd.read_csv(self._out_file, sep='\t',
    #                      encoding='utf-8',
    #                      quoting=csv.QUOTE_NONE,
    #                      header=None,
    #                      on_bad_lines='warn',
    #                      dtype=object,
    #                      chunksize=self._chunk_size) as tsv_reader:

    #         write_list = []

    #         for chunk in tsv_reader:
    #             for _, row in chunk.iterrows():
    #                 row_data = row.iloc[0].split(';')

    #                 # Extract specimen id and check if it has been seen before
    #                 specimen_id = row_data[0].strip()
    #                 if specimen_id in seen_set:
    #                     continue

    #                 seen_set.add(specimen_id)

    #                 write_helper = row.dropna().iloc[:].tolist()
    #                 write_list.append(write_helper)

    #                 # Extract taxonomy
    #                 taxonomy_string = row_data[1].strip() if len(row_data) > 1 else ""
    #                 taxonomy = RaxTaxTaxonomy()
    #                 taxonomy_fields = taxonomy_string[4:].split(',')

    #                 for field in taxonomy_fields:
    #                     if ':' in field:
    #                         rank, value = field.split(':')
    #                         if rank == 'p':
    #                             taxonomy.p = value
    #                         elif rank == 'c':
    #                             taxonomy.c = value
    #                         elif rank == 'o':
    #                             taxonomy.o = value
    #                         elif rank == 'f':
    #                             taxonomy.f = value
    #                         elif rank == 'g':
    #                             taxonomy.g = value
    #                         elif rank == 's':
    #                             taxonomy.s = value

    #                 specimen_data = RaxTaxData(specimen_id=specimen_id, taxonomy=taxonomy)
    #                 # Extract scores starting from the second column after the taxonomy
    #                 # Need to drop NaN values first, to make sure we can easily
    #                 # access local and global signal values with indexing.
    #                 scores = row.dropna().iloc[1:].tolist()

    #                 helepr_scores = scores[:-3]
    #                 try:
    #                     for i in range(0, len(helepr_scores), 2):  # Scores are in pairs
    #                         rank = scores[i].split(':')[0].strip()
    #                         score_value = float(scores[i + 1].strip())
    #                         if rank == 'f':
    #                             specimen_data.score_f = score_value
    #                         elif rank == 'g':
    #                             specimen_data.score_g = score_value
    #                         elif rank == 'o':
    #                             specimen_data.score_o = score_value
    #                         elif rank == 'c':
    #                             specimen_data.score_c = score_value
    #                         elif rank == 's':
    #                             specimen_data.score_s = score_value
    #                 except (IndexError, ValueError):
    #                     pass

    #                 specimen_data.local_signal = float(scores[-3])
    #                 specimen_data.global_signal = float(scores[-2])
    #                 results.append(specimen_data)

    #     # Write data to a TSV file -- for debugging purposes
    #     # with open('raxtax_unique_hits.tsv', mode='w', newline='', encoding='utf-8') as file:
    #     #     writer = csv.writer(file, delimiter='\t', quoting=csv.QUOTE_NONE)

    #     #     # Write each list in the data to a row in the TSV file
    #     #     for row in write_list:
    #     #         writer.writerow(row)

    #     return results
