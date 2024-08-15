"""Harmonizer module"""

import csv
import warnings

from typing import Dict, Set, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from gbif.gbif import query_name_backbone, name_backbone_stat, query_name_backbone_b2t
from sqlite.parser import GbifName

WORKER_THREADS = 50


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
