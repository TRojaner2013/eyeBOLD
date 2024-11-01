"""Harmonizer module"""

import csv
import warnings
import shutil
import subprocess
import logging
import os
import pandas as pd
from collections import defaultdict

from typing import Dict, Set, Tuple, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass#, field


import common.constants as const
from gbif.gbif import query_name_backbone, name_backbone_stat, query_name_backbone_b2t
from sqlite.parser import GbifName

logger = logging.getLogger(__name__)
WORKER_THREADS = 30

def _harmonize_names(taxon_data: set, rank: str) -> List[GbifName]:
    """Checks naming of taxons and returns matches as dictionary"""

    warnings.warn("Function will be removed.", DeprecationWarning)
    result = []

    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as thread_pool:

        future_to_query = {thread_pool.submit(query_name_backbone, query, rank): query
                           for query in taxon_data}

        for future in as_completed(future_to_query):
            #future_to_query[future]
            result.append(future.result())

    return result

def _harmonize_names_b2t(taxon_data: List[Dict]) -> List[GbifName]:
    """Checks naming of taxons and returns matches as dictionary"""

    result = []

    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as thread_pool:

        future_to_query = {thread_pool.submit(query_name_backbone_b2t, query): query
                           for query in taxon_data}

        for future in as_completed(future_to_query):
            #query = future_to_query[future]
            result.append(future.result())

    return result

def _harmonize_name_stats(taxon_data: set) -> List[
                                                        Tuple[str, str, int, bool]]:
    """Checks naming of taxons and returns matches as dictionary"""
    # THIS FUNCTION IS FOR STATISTICS ONLY
    warnings.warn("Function will be removed.", DeprecationWarning)

    result = []

    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as thread_pool:

        result = list(thread_pool.map(name_backbone_stat, taxon_data))

    return result

def harmonize(taxon_data: Set, rank:str) -> List[GbifName]:
    """Harmonizes taxonomical data in passed databank

    Arguments:
        -db_handle: sqlite3 Connection handle to database

    Returns:
        True on success.
    """

    warnings.warn("Function will be removed.", DeprecationWarning)
    harmonized_names = _harmonize_names(taxon_data, rank)

    return harmonized_names

def harmonize_b2t(taxon_data: List[Dict]) -> List[GbifName]:
    """Harmonizes taxonomical data in passed dictionary

    Arguments:
        -taxon_data: Dictionary containing data from taxon

    Returns:
        - List of GBIF Names
    """

    harmonized_names = _harmonize_names_b2t(taxon_data)

    return harmonized_names

def harmonize_stats(taxon_data: Set, rank:str) -> None:
    """Harmonizes taxonomical data in passed databank

    Arguments:
        -db_handle: sqlite3 Connection handle to database

    Returns:
        True on success.
    """

    warnings.warn("Function will be removed.", DeprecationWarning)

    filename = f"{rank}.csv"
    stats = _harmonize_name_stats(taxon_data)
    headers = ['query', 'rank', 'match', 'status', 'confidence', 'synonym']
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers)
        csvwriter.writerows(stats)


def raxtax_entry() -> List:
    """Entry point for raxtax process"""
    handle:RaxTaxer = RaxTaxer(const.RAXTAX_DB_IN, const.RAXTAX_QUERY_IN)
    return handle.run()

# RaxTax specific classes
@dataclass
class RaxTaxTaxonomy:
    p: Optional[str] = None  # Phylum
    c: Optional[str] = None  # Class
    o: Optional[str] = None  # Order
    f: Optional[str] = None  # Family
    g: Optional[str] = None  # Genus
    s: Optional[str] = None  # Species

@dataclass
class RaxTaxData:
    specimen_id: str
    taxonomy: RaxTaxTaxonomy
    score_f: Optional[float] = None
    score_g: Optional[float] = None
    score_o: Optional[float] = None
    score_c: Optional[float] = None
    score_s: Optional[float] = None
    local_signal: Optional[float] = None
    global_signal: Optional[float] = None

class RaxTaxer():
    """ Class handling raxtax data """

    def __init__(self, db_in_file: str=None, query_file: str=None) -> None:
        self._db_in_file = db_in_file
        self._query_file = query_file
        self._out_path = os.path.join(".", f"{self._query_file.split('.', 2)[1][1:]}.out")

        # Make sure to delete all old files, otherwise we can't detect errors in the process
        if os.path.exists(self._out_path):
            shutil.rmtree(self._out_path)

        self._out_file = os.path.join(self._out_path, "raxtax.out")
        self._chunk_size = 10000

    def run(self) -> List:
        """ Runs raxtax """

        # ToDo: Enable Clean List
        results = []
        self._invoke_raxtax()
        results.extend(self._retrieve_result())
        #self._clean()
        return results

    def _invoke_raxtax(self) -> None:
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
        """
        Reads the data in batches from a TSV file, identifies entries to mark based on given criteria, and returns marked entries.
        
        Args:
            file_path (str): Path to the input file.
            batch_size (int): Number of rows to process in each batch.
            
        Returns:
            list: Marked records with IDs
        """
        last_id = None
        marked_records = []


        len_file = 0
        with open(self._out_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')

            batch = []
            last_id = 0
            for row in reader:
                batch.append(row)
                if len(batch) >= self._chunk_size:
                    len_file += len(batch)
                    self._process_batch(batch, last_id, marked_records)
                    last_id = int(batch[-1][0].split(';')[0])  # Update last ID after processing batch
                    batch = []

            if batch:
                len_file += len(batch)
                self._process_batch(batch, last_id, marked_records)

        return marked_records

    def _process_batch(self, batch:List, last_id:int|None, marked_records: List) -> None:
        """Processes a batch of records and applies marking criteria."""
        for row in batch:
            try:
                record_id = int(row[0].split(';')[0])  # Extracts the ID

                if record_id == last_id:
                    continue
                last_id = record_id

                original_taxonomy = row[0].split('=')[1].split(',')  # Parse original taxonomy lineage
                compared_taxonomy = row[1].split(',')                # Parse compared taxonomy lineage
                scores = list(map(float, row[2].split(',')))         # Parse scores per lineage level
                # local_signal = float(row[3])                         # Extract overall score
                # global_signal = float(row[4])                        # Extract overall score

                if self._mark_entry(original_taxonomy, compared_taxonomy, scores):
                    marked_records.append(record_id)

            except (IndexError, ValueError) as e:
                print(f"Error parsing row: {row}, Error: {e}")
                continue  # Skip rows with parsing issues

    def _mark_entry(self, original_taxonomy, compared_taxonomy, scores):
        """
        Determines if an entry should be marked based on lineage difference and score criteria.
        
        Args:
            original_taxonomy (list): Original taxonomy lineage list.
            compared_taxonomy (list): Compared taxonomy lineage list.
            scores (list): List of scores corresponding to each level in the lineage.
            
        Returns:
            bool: True if entry should be marked, False otherwise.
        """
        for i, (original, compared) in enumerate(zip(original_taxonomy, compared_taxonomy)):
            if original != compared and i < len(scores) - 1:  # Ignore species level difference
                assert i < 5
                if scores[i] >= 0.9:
                    return True
        return False

    def _extract_specimen_data(self) -> List[RaxTaxData]:
        """Extracts specimen id, taxonomy, and score values from a TSV file."""
        results = []
        seen_set = set()
        
        with pd.read_csv(self._out_file, sep='\t', 
                         encoding='utf-8',
                         quoting=csv.QUOTE_NONE,
                         header=None,
                         on_bad_lines='warn',
                         dtype=object,
                         chunksize=self._chunk_size) as tsv_reader:

            # Write subroutine
            write_list = []

            for chunk in tsv_reader:
                for _, row in chunk.iterrows():
                    # Split the first column by ';'
                    row_data = row.iloc[0].split(';')
                    
                    # Extract specimen id and check if it has been seen before
                    specimen_id = row_data[0].strip()
                    if specimen_id in seen_set:
                        continue

                    seen_set.add(specimen_id)

                    write_helper = row.dropna().iloc[:].tolist()
                    #write_helper = write_helper[:-1]
                    write_list.append(write_helper)

                    # Extract taxonomy
                    taxonomy_string = row_data[1].strip() if len(row_data) > 1 else ""
                    taxonomy = RaxTaxTaxonomy()
                    taxonomy_fields = taxonomy_string[4:].split(',')

                    for field in taxonomy_fields:
                        if ':' in field:
                            rank, value = field.split(':')
                            if rank == 'p': taxonomy.p = value
                            elif rank == 'c': taxonomy.c = value
                            elif rank == 'o': taxonomy.o = value
                            elif rank == 'f': taxonomy.f = value
                            elif rank == 'g': taxonomy.g = value
                            elif rank == 's': taxonomy.s = value

                    specimen_data = RaxTaxData(specimen_id=specimen_id, taxonomy=taxonomy)
                    # Extract scores starting from the second column after the taxonomy
                    # Need to drop NaN values first, to make sure we can easily
                    # access local and global signal values with indexing.
                    scores = row.dropna().iloc[1:].tolist()

                    helepr_scores = scores[:-3]
                    try:
                        for i in range(0, len(helepr_scores), 2):  # Scores are in pairs
                            rank = scores[i].split(':')[0].strip()
                            score_value = float(scores[i + 1].strip())
                            if rank == 'f': specimen_data.score_f = score_value
                            elif rank == 'g': specimen_data.score_g = score_value
                            elif rank == 'o': specimen_data.score_o = score_value
                            elif rank == 'c': specimen_data.score_c = score_value
                            elif rank == 's': specimen_data.score_s = score_value
                    except (IndexError, ValueError):
                        pass  # Handle potential misformatted rows gracefully

                    specimen_data.local_signal = float(scores[-3])
                    specimen_data.global_signal = float(scores[-2])
                    results.append(specimen_data)


        
        # Write data to a TSV file
        with open('raxtax_unique_hits.tsv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter='\t', quoting=csv.QUOTE_NONE)
            
            # Write each list in the data to a row in the TSV file
            for row in write_list:
                writer.writerow(row)

        return results
