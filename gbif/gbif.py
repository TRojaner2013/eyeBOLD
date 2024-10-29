"""Moduel providing an interface to GBIF.
REFACTOR TODO

- Seperate this file into two files
    --> gbif.py for offical use
    --> gbif_stats.py for stat and evaluation purposes
"""

import logging
import re
import time
import os
import requests
import json

from typing import Any, Dict, List, Tuple, Generator
from pygbif.species import name_backbone
from pygbif import occurrences as occ
from pygbif import maps

from sqlite.parser import GbifName, TAXONOMY_MAP, TAXONOMY_TO_INT, INT_TO_TAXONOMY
from sqlite.Bitvector import BitIndex, ChecksManager

logger = logging.getLogger(__name__)

GBIF_LOC_PARA_LIMIT = 1
GBIF_LOC_QUERY_LIMIT = 101000
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

    logger.debug('Evaluating verbose output in result: %s at rank %s',
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
        logger.error(('Name backbone querry failed.\n\t'
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


    if match_type == 'NONE' and confidence == 100:
        # No result in GBIF
        # Make sure that we return a checked and failed flag here
        # Otherwise we would keep the value as needs to be reviewed.
        index_list.append(BitIndex.NAME_CHECKED)
        index_list.append(BitIndex.NAME_FAILED)
        new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)
        return new_data

    index_list = [BitIndex.from_string(query_rank)]


    if match_type in {'EXACT', 'FUZZY', "HIGHERRANK"}:
        # Bug: SPECIES Flag set on HIGHERANK match
        # With species names and higher ranks there are a few cases where we 
        # get a match and keep the species name but set species flag.
        index_list.append(BitIndex.NAME_CHECKED)

        #ToDo: Need to check for cf. f. in name -- why?

        for res_rank in ['kingdom', 'phylum', 'class', 'order', 'family', 'subfamily', 'tribe', 'genus', 'species', 'subspecies']:
            if res_rank in result:
                new_data.insert_dict[TAXONOMY_MAP[res_rank]] = result[res_rank]
                index_list.append(BitIndex.from_string(res_rank))

        # ToDo: Maybee we need to make sure all ranks are unset set here...
        if status == 'HIGHERRANK' or match_type == 'HIGHERRANK':
            # Higher rank means we did not check the query rank...
            index_list.remove(BitIndex.from_string(query_rank))

            current_rank = TAXONOMY_TO_INT['subspecies']
            end_rank = TAXONOMY_TO_INT[match_rank.lower()]
            while  current_rank > end_rank:
                # Remove all ranks above match rank
                try:
                    index_list.remove(BitIndex.from_string(INT_TO_TAXONOMY[current_rank]))
                except ValueError:
                    pass
                finally:
                    current_rank = current_rank-1

        new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)
        new_data.insert_dict['gbif_key'] = result.get('usageKey', None)

    else:
        logger.error(('Undefinded match_type: %s from GBIF.\n\t'
                      'Querey: %s\n\tRank %s\n\tResult: %s'),
                      match_type, query, query, result)

        return new_data

    # Here a some sanity checks to make sure we only return valid data:
    if match_rank is None:
        # This is a serious problem
        logger.debug(('Unexpected rank data from GBIF.\n\t'
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

            current_rank = TAXONOMY_TO_INT['subspecies']
            end_rank = TAXONOMY_TO_INT[match_rank.lower()]
            while  current_rank > end_rank:
                try:
                    index_list.remove(BitIndex.from_string(INT_TO_TAXONOMY[current_rank]))
                except ValueError:
                    pass
                finally:
                    current_rank = current_rank-1

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
        logger.error(('Name backbone querry failed.\n\t'
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


def handle_error(response, batch_index, json_payload):
    if response.status_code == 400:
        logger.error(f"Batch {batch_index} failed: SQL syntax error. {response.text}")
        raise ValueError("SQL syntax error")
    elif response.status_code == 401:
        logger.error("Unauthorized: Check your username or password.")
        raise ValueError("Unauthorized")
    elif response.status_code == 403:
        logger.error("Forbidden: Your account does not have access to this feature.")
        raise ValueError("Forbidden")
    else:
        logger.error(f"Batch {batch_index} failed with HTTP {response.status_code}: {response.text}")
        raise ValueError("Request failed")


def get_locations_sql(keys: List[int], batch_size: int) -> Generator[Tuple[str, List[int]], Any, Any]:
    """ Creates large download object for occurrences 

        Note:
            This function isn experimental and reflects GBIFs current implementation
            for SQL downloads.
            This feature is only available to invites users and not open to public.
    """

    GBIF_USER = os.getenv('GBIF_USER')
    GBIF_PWD = os.getenv('GBIF_PWD')
    API_URL = "https://api.gbif.org/v1/occurrence/download/request"

    query_size = min(batch_size, GBIF_LOC_QUERY_LIMIT)
    # Note:
    # GBIF displays public information for all downloads.
    # Avoid using a real account for this!
    # We need to limit the number of open downloads to a single one
    # here as we need to stay in GBIF limits
    # Information can be found here: https://techdocs.gbif.org/en/openapi/v1/occurrence#/
    file_path = "."
    logger.info("Location download will be executed in %s steps...",
                -(len(keys)//-query_size))

    for i in range(0,len(keys), query_size):
        logger.info("Downloading keys %i through to %s..", i, i+query_size)

        # Construct sql query and json payload
        batch = keys[i:i+query_size]
        #batch = [f"'{key}'" for key in batch]  # Enclose each gbifid in single quotes

        #sql_query = f'SELECT gbifid, decimallatitude, decimallongitude, v_decimallatitude, decimallongitude, v_verbatimlatitude, v_verbatimlongitude, occurrenceid, datasetkey FROM occurrence WHERE gbifid IN ({", ".join(batch)}) AND hascoordinate = TRUE;'
        #sql_query = f'SELECT acceptedtaxonkey, decimallatitude, decimallongitude, occurrenceid, datasetkey FROM occurrence WHERE acceptedtaxonkey IN ({", ".join(map(str, batch))}) AND hascoordinate = TRUE;'
        sql_query = f'SELECT acceptedtaxonkey, decimallatitude, decimallongitude, countrycode FROM occurrence WHERE acceptedtaxonkey IN ({", ".join(map(str, batch))}) AND hascoordinate = TRUE;'
        json_payload = {
            "sendNotification": False,
            "notificationAddresses": ["none@provided.com"],
            "format": "SQL_TSV_ZIP",
            "sql": sql_query
        }

        response = requests.post(
            API_URL,
            auth=(GBIF_USER, GBIF_PWD),
            headers={"Content-Type": "application/json"},
            data=json.dumps(json_payload)
        )
        if response.status_code == 201:
            req_id = response.text.splitlines()[-1]  # Download key is on the last line
            logger.info(f"Batch {i} submitted successfully: {req_id}")
        else:
            handle_error(response, i, json_payload)
    
        #ToDo: Good Implementation.
        # This is just a first draft to see if thinks work out as expected.
        meta = occ.download_meta(req_id)
        while meta["status"] != "SUCCEEDED":
            # We need to check if the request was killes.
            # Better approach: Check if still running, if not check if Success
            # if not, raise error!
            if meta["status"] == "KILLED":
                logger.error("Batch %i failed: Request was killed by server", i)
                raise ValueError("Request was killed")

            logger.info("Wating download... Metainfo:\n%s", meta['status'])
            time.sleep(60)
            meta = occ.download_meta(req_id)

        # The time has come to download the file. 
        logger.info("Starting download for request: %s", req_id)
        occ.download_get(req_id, file_path)
        yield (os.path.join(file_path, f"{req_id}.zip"), batch)


# ToDo: This should yield results so all other logic can be implemented in 
def get_locations(keys: List[int], batch_size: int) -> Generator[str, Any, Any]:
    """ Creates large download object for occurrences """

    query_size = min(batch_size, GBIF_LOC_QUERY_LIMIT)
    # Note:
    # GBIF displays public information for all downloads.
    # Avoid using a real account for this!
    # We need to limit the number of open downloads to a single one
    # here as we need to stay in GBIF limits
    # Information can be found here: https://techdocs.gbif.org/en/openapi/v1/occurrence#/
    file_path = "."
    logger.info("Location download will be executed in %s steps...",
                -(len(keys)//-query_size))
    for i in range(0,len(keys), query_size):
        logger.info("Downloading keys %i through to %s..", i, i+query_size)
        batch = keys[i:i+query_size]
        batch = [f"\"{key}\"" for key in batch]
        key_str = f"taxonKey in [{', '.join(key for key in batch)}]"
        # We can mock pygbif by providign False to email in order to skip email
        # notifications.
        a = occ.download([key_str, "hasCoordinate = TRUE"], email=False)
        req_id = a[0]

        #ToDo: Good Implementation.
        # This is just a first draft to see if thinks work out as expected.
        meta = occ.download_meta(req_id)
        while meta["status"] != "SUCCEEDED":
            logger.info("Still not ready. Metainfo:\n%s", meta['status'])
            time.sleep(60)
            meta = occ.download_meta(req_id)

        # The time has come to download the file. 
        logger.info("Starting download for request: %s", req_id)
        occ.download_get(req_id, file_path)
        yield os.path.join(file_path, f"{req_id}.zip")

    # This part should be implemented as an asyn procedure to make sure we do
    # not wait for any loger data...


def query_location(gbif_key: int) -> Any:
    """ Queries GBIF for geo data 

        Taoxa keys from gbif are obtained and stored during names harmonization.

        Arguments:
            - gbif_key: Taxon key in GBIF

        Returns:
            Geo location of taxa or something similiar.
    """
    a = occ.download([f"taxonKey = {gbif_key}", "hasCoordinate = TRUE"])
    print(a)
    #res = occ.search(taxonKey=gbif_key, limit=300, )
    #print(res)

def get_map(gbif_key: int) -> None:
    """Plots map of provided gbif_key"""
    data = maps.map(taxonKey=gbif_key)
    # a = data.response
    # b = data.path
    # c = data.img
    data.plot()
