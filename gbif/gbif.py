"""Moduel providing an interface to GBIF.
REFACTOR TODO

- Seperate this file into two files
    --> gbif.py for offical use
    --> gbif_stats.py for stat and evaluation purposes
"""

import logging
import re

from typing import Any, Dict, Tuple
from pygbif.species import name_backbone
from pygbif import occurrences as occ
from pygbif import maps

from sqlite.parser import GbifName, TAXONOMY_MAP
from sqlite.Bitvector import BitIndex, ChecksManager

logger = logging.getLogger(__name__)

SCORE_BOARD = {'kingdom':       {'EXACT': 1, 'HIGHERRANK': 100,
                                 'FUZZY': 0, 'NONE': -100},
               'phylum':        {'EXACT': 1, 'HIGHERRANK': 100,
                                 'FUZZY': 0, 'NONE': -100},
               'class':         {'EXACT': 1, 'HIGHERRANK': 100,
                                 'FUZZY': 0, 'NONE': -100},
               'order':         {'EXACT': 1, 'HIGHERRANK': 100,
                                 'FUZZY': 0, 'NONE': -100},
               'family':        {'EXACT': 5, 'HIGHERRANK': 100,
                                 'FUZZY': 0, 'NONE': -50},
               'subfamily':     {'EXACT': 5, 'HIGHERRANK': 100,
                                 'FUZZY': 0, 'NONE': -50},
               'tribe':         {'EXACT': 5, 'HIGHERRANK': 100,
                                 'FUZZY': 0, 'NONE': -20},
               'genus':         {'EXACT': 10, 'HIGHERRANK': 100,
                                 'FUZZY': 0, 'NONE': -20},
               'species':       {'EXACT': 15, 'HIGHERRANK': 100,
                                 'FUZZY': 0, 'NONE': -10},
               'subspecies':    {'EXACT': 15, 'HIGHERRANK': 100,
                                 'FUZZY': 0, 'NONE': -10}
              }

def _evaluate_verbose_response(rank: str, result: Dict) -> Dict:
    """Evaluates verbose responses from GBIF"""

    logging.debug('Evaluating verbose output in result: %s at rank %s',
                   result, rank)

    res = result
    score = 0

    for alt_result in result.get("alternatives", []):
        match_type = alt_result.get("matchType")
        status = alt_result.get("status")
        res_rank = alt_result.get('rank')

        if ((match_type != 'EXACT') or
            (status != 'ACCEPTED') or
            (res_rank != rank.upper())):
            # We consider the result to be shitty
            continue

        notes = alt_result.get('note', '')
        new_score = int(re.search(r'score=(\d+)', notes).group(1))
        if new_score > score:
            score = new_score
            res = alt_result

    return res

def query_name_backbone_b2t(query: Dict) -> GbifName:
    """Queries GBIF name backbone service"""

    # GBIF Returns one of the following matchTypes:
    # 1. NONE -- NO MATCH
    # 2. HIGHERRANK -- NO MATCH on provided rank
    # 3. FUZZY -- TYPO or something similiar
    # 4. EXACT -- Exact match

    query_rank = query.get('rank')
    try:
        result = name_backbone(name=query.get('query'),
                               rank=query.get('rank'),
                               kingdom=query.get('kingddom'),
                               phylum=query.get('phylum'),
                               clazz=query.get('class'),
                               order=query.get('order'),
                               family=query.get('family'),
                               genus=query.get('genus'),
                               verbose=True)

    except Exception as e:
        # Most likely this will be a Connection timeout error.
        # ToDo: Do not raise but return error value in production
        logging.error(('Name backbone querry failed.\n\t'
                       'Query: %s \n\tRank: %s \n\t'
                       'Error: %s') , query, query_rank, e)

        return GbifName(query.get('query'), query_rank,  {}, {}, query.get('specimenids', []))

    new_data = GbifName(query.get('query'), query_rank,  result, {}, query.get('specimenids', []))
    new_data.insert_dict['processing_info'] = str(result)

    #ToDo: Implement better logic here:
    # 1. get match type
    match_type = result.get('matchType')
    confidence = result.get('confidence', 100)
    #score = -100
    status = result.get('status')
    match_rank = result.get('rank')


    # ToDo: Make this a dataclass for better handling...
    #name = 'Error'
    #score = SCORE_BOARD.get(query_rank).get(match_type)
    #key = -1

    index_list = [BitIndex.from_string(query_rank)]

    if match_type == 'NONE' and confidence == 100:
        # No result in GBIF
        return new_data

    if match_type in {'EXACT', 'FUZZY', "HIGHERRANK"}:
        # ToDo: Find better solution for HIGHERRANK -- we must not set all flags there..
        index_list.append(BitIndex.NAME_CHECKED)

        #ToDo: Need to check for cf. f. in name

        for res_rank in ['kingdom', 'phylum', 'class', 'order', 'family', 'subfamily', 'tribe', 'genus', 'species', 'subspecies']:
            if res_rank in result:
                new_data.insert_dict[TAXONOMY_MAP[res_rank]] = result[res_rank]
                index_list.append(BitIndex.from_string(res_rank))

        # if status != 'SYNONYM':
        #     # Gbif returns the synonym name as cannonical Name
        #     new_data.insert_dict[TAXONOMY_MAP[query_rank]] = result.get('canonicalName', query.get('query'))

        if status == 'HIGHERRANK':
            # Higher rank means we did not check the query rank...
            index_list.remove(BitIndex.from_string(query_rank))

        new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)
        new_data.insert_dict['gbif_key'] = result.get('usageKey', None)
    # elif match_type == 'FUZZY':
    #     new_data.insert_dict[TAXONOMY_MAP[query_rank]] = result.get('canonicalName')
    #     new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)
    # elif match_type == 'HIGHERRANK':
    #     # These  results might be shitty and we need to evaluate verbose output.
    #     result = _evaluate_verbose_response(query_rank, result)
    #     #ToDo: Extract this as a function to reuse on unexpected matches.
    #     for res_rank in ['kingdom', 'phylum', 'class', 'order', 'family', 'subfamily', 'tribe', 'genus', 'species', 'subspecies']:

    #         if res_rank in result:
    #             new_data.insert_dict[TAXONOMY_MAP[res_rank]] = result[res_rank]
    #             index_list.append(BitIndex.from_string(res_rank))

    #         SYNONYM
    #         new_data.insert_dict[TAXONOMY_MAP[query_rank]] = result.get('canonicalName', query.get('query'))
    #         new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)

    else:
        logger.error(('Undefinded match_type: %s from GBIF.\n\t'
                      'Querey: %s\n\tRank %s\n\tResult: %s'),
                      match_type, query, query, result)

        return new_data

    # Here a some sanity checks to make sure we only return valid data:
    if match_rank is None:
        # This is a serious problem
        logger.error(('Unexpected rank data from GBIF.\n\t'
                      'Query: %s \n\tRank: %s \n\t'
                      'Result: %s') , query, query_rank, result)
        return new_data

    if match_rank.upper() != query_rank.upper() and match_type != 'HIGHERRANK':
        logger.debug(('Got an exact match in different rank.\n\t'
                      'Query: %s\n\tRank: %s\n\t Rank in match: %s\n\tResult: %s'),
                      query, query_rank, match_rank, result)
        # We got an match in another class
        # We need to decide which rank is useful and what to do with these data
        # Need to check if we got a match in higher of lower rank...

        sane_name = result.get(query_rank.lower())
        if sane_name is not None:
            new_data.insert_dict[TAXONOMY_MAP[query_rank]] = sane_name
        else:
            # Match incorrect, need to remove checked names...
            index_list.remove(BitIndex.from_string(query_rank))
            new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)
            #ToDo: Check if we need to adapt usageKey here.

    return new_data

def query_name_backbone(query: str, rank: str) -> GbifName:
    """Queries GBIF name backbone service"""

    # GBIF Returns one of the following matchTypes:
    # 1. NONE -- NO MATCH
    # 2. HIGHERRANK -- NO MATCH on provided rank
    # 3. FUZZY -- TYPO or something similiar
    # 4. EXACT -- Exact match

    try:
        result = name_backbone(name=query, rank=rank, verbose=True)
    except Exception as e:
        # Most likely this will be a Connection timeout error.
        #ToDo: Do not raise but return error value in production
        logging.error(('Name backbone querry failed.\n\t'
                       'Query: %s \n\tRank: %s \n\t'
                       'Error: %s') , query, rank, e)
        return GbifName(query, rank,  {}, {})

    new_data = GbifName(query, rank,  result, {})
    #ToDo: Implement better logic here:
    # 1. get match type
    match_type = result.get('matchType')
    confidence = result.get('confidence', 100)
    #score = -100
    #status = result.get('status')
    match_rank = result.get('rank')


    # ToDo: Make this a dataclass for better handling...
    #name = 'Error'
    #score = SCORE_BOARD.get(rank).get(match_type)
    #key = -1

    index_list = [BitIndex.from_string(rank)]

    if match_type == 'NONE' and confidence == 100:
        # No result in GBIF
        return new_data

    if match_type == 'EXACT':
        new_data.insert_dict[TAXONOMY_MAP[rank]] = result.get('canonicalName')
        new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)
    elif match_type == 'FUZZY':
        new_data.insert_dict[TAXONOMY_MAP[rank]] = result.get('canonicalName')
        new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)
    elif match_type == 'HIGHERRANK':
        # These  results might be shitty and we need to evaluate verbose output.
        result = _evaluate_verbose_response(rank, result)
        #ToDo: Extract this as a function to reuse on unexpected matches.
        for res_rank in ['kingdom', 'phylum', 'class', 'order', 'family', 'subfamily', 'tribe', 'genus', 'species', 'subspecies']:

            if res_rank in result:
                new_data.insert_dict[TAXONOMY_MAP[res_rank]] = result[res_rank]
                index_list.append(BitIndex.from_string(res_rank))

            new_data.insert_dict[TAXONOMY_MAP[rank]] = result.get('canonicalName', query)
            new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)

    else:
        logger.error(('Undefinded match_type: %s from GBIF.\n\t'
                      'Querey: %s\n\tRank %s\n\tResult: %s'),
                      match_type, query, query, result)
        return new_data


    # Here a some sanity checks to make sure we only return valid data:
    if match_rank is None:
        # This is a serious problem
        logger.error(('Unexpected rank data from GBIF.\n\t'
                      'Query: %s \n\tRank: %s \n\t'
                      'Result: %s') , query, rank, result)
        return new_data

    if match_rank.upper() != rank.upper() and match_type != 'HIGHERRANK':
        logger.debug(('Got an exact match in different rank.\n\t'
                      'Query: %s\n\tRank: %s\n\t Rank in match: %s\n\tResult: %s'),
                      query, rank, match_rank, result)
        # We got an match in another class
        # We need to decide which rank is useful and what to do with these data
        sane_name = result.get(rank.lower())
        if sane_name is not None:
            new_data.insert_dict[TAXONOMY_MAP[rank]] = sane_name

    return new_data

def name_backbone_stat(query: str, rank: str='species') -> Tuple[str, str, str, str, int, bool]:
    """Queries GBIF name backbone service"""
    # THIS FUNCTION IS FOR STATISTICS ONLY
    try:
        result = name_backbone(name=query, rank=rank)
    except Exception:
        return (query, rank, 'EXCEPTION', 'EXCEPTION', 'EXCEPTION', 'EXCEPTION')

    match_type = result.get('matchType', '')
    status = result.get('status', '')
    confidence = result.get('confidence', '')
    synonym = result.get('synonym', '')

    return (query, result, match_type, status, confidence, synonym)


def query_location(gbif_key: int) -> Any:
    """ Queries GBIF for geo data 

        Taoxa keys from gbif are obtained and stored during names harmonization.

        Arguments:
            - gbif_key: Taxon key in GBIF

        Returns:
            Geo location of taxa or something similiar.
    """
    a = occ.download(f"taxonKey={gbif_key}")
    print(a)
    res = occ.search(taxonKey=gbif_key, limit=300, )
    print(res)

def get_map(gbif_key: int) -> None:
    """Plots map of provided gbif_key"""
    data = maps.map(taxonKey=gbif_key)
    # a = data.response
    # b = data.path
    # c = data.img
    data.plot()
