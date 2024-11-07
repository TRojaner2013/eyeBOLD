"""Moduel providing an interface to GBIF."""

import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Tuple, Generator

import requests

from pygbif.species import name_backbone # type: ignore # pylint: disable=import-error
from pygbif import occurrences as occ # type: ignore # pylint: disable=import-error

from sqlite.parser import GbifName, TAXONOMY_MAP, TAXONOMY_TO_INT, INT_TO_TAXONOMY
from sqlite.Bitvector import BitIndex, ChecksManager

logger = logging.getLogger(__name__)

# ToDo: Move these to common.constants?
GBIF_LOC_QUERY_LIMIT = 101000

def _evaluate_verbose_response(rank: str, result: Dict) -> Dict:
    """ Evaluates verbose responses from GBIF

        Args:
            - rank (str): Rank of the query
            - result (dict): Result from GBIF

        Returns:
            - dict: Evaluated result
    """

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
            # There is some result with an bettter score than the original

            score = new_score
            res = alt_result

    return res

def query_name_backbone_b2t(query: Dict, retries: int=3) -> GbifName:
    """ Queries GBIF name backbone service

        Args:
            - query (dict): Query to GBIF
            - retries (int): Number of retries on failure

        Returns:
            - GbifName: Result from GBIF
    """

    # GBIF Returns one of the following matchTypes:
    # 1. NONE -- NO MATCH
    # 2. HIGHERRANK -- NO MATCH on provided rank
    # 3. FUZZY -- TYPO or something similiar
    # 4. EXACT -- Exact match

    query_rank = query.get('rank')

    try:
        # Note: We added all ranks to the query to get a better result.
        # and to avoid issues with the api when a query has only two characters.
        result = name_backbone(name=query.get('query'),
                               rank=query.get('rank'),
                               kingdom=query.get('kingddom'),
                               phylum=query.get('phylum'),
                               clazz=query.get('class'),
                               order=query.get('order'),
                               family=query.get('family'),
                               genus=query.get('genus'),
                               verbose=True)

    except Exception as err:
        # Most likely this will be a Connection timeout error.
        if retries > 0:
            # Time-out error, retry after waiting for a while
            logger.debug('Name harmonization failed, retrying in 30 seconds.')
            time.sleep(30)
            return query_name_backbone_b2t(query, retries-1)

        logger.warning('Name harmonization failed, please run review command.')
        logger.debug(('Name backbone querry failed.\n\t'
                       'Query: %s \n\tRank: %s \n\t'
                       'Error: %s') , query, query_rank, err)
        return GbifName(query.get('query'), query_rank,  {}, {}, query.get('specimenids', []))

    new_data = GbifName(query.get('query'), query_rank,  result, {}, query.get('specimenids', []))
    new_data.insert_dict['processing_info'] = str(result)

    match_type = result.get('matchType')
    confidence = result.get('confidence', 100)
    status = result.get('status')
    match_rank = result.get('rank')

    if match_type == 'NONE' and confidence == 100:
        # No result in GBIF
        # Make sure that we return a checked and failed flag here
        # Otherwise we would keep the value as needs to be reviewed.
        index_list = []
        index_list.append(BitIndex.NAME_CHECKED)
        index_list.append(BitIndex.NAME_FAILED)
        new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)
        new_data.insert_dict['identification_rank'] = 'Failed'
        return new_data

    index_list = [BitIndex.from_string(query_rank)]

    if match_type in {'EXACT', 'FUZZY', "HIGHERRANK"}:
        # Bug: SPECIES Flag set on HIGHERANK match
        # With species names and higher ranks there are a few cases where we
        # get a match and keep the original species name but set species flag.
        index_list.append(BitIndex.NAME_CHECKED)
        new_data.insert_dict['identification_rank'] = match_rank.lower()

        for res_rank in ['kingdom', 'phylum', 'class', 'order', 'family',
                         'subfamily', 'tribe', 'genus', 'species', 'subspecies']:

            if res_rank in result:
                new_data.insert_dict[TAXONOMY_MAP[res_rank]] = result[res_rank]
                index_list.append(BitIndex.from_string(res_rank))

        if status == 'HIGHERRANK' or match_type == 'HIGHERRANK':
            # Higher rank means we did not check the query rank...
            index_list.remove(BitIndex.from_string(query_rank))

            current_rank = TAXONOMY_TO_INT['subspecies']
            end_rank = TAXONOMY_TO_INT.get(match_rank.lower(), TAXONOMY_TO_INT['kingdom'])
            new_data.insert_dict['identification_rank'] = INT_TO_TAXONOMY[end_rank]
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
            # Bug: match_rank might be something else than we expect.
            # This might e.g be 'form' for plants. Untill fix, we just
            # remove these entries.
            # ToDo: Fix-me (Discovered 30-10-2024)
            end_rank = TAXONOMY_TO_INT.get(match_rank.lower(), TAXONOMY_TO_INT['kingdom'])
            new_data.insert_dict['identification_rank'] = INT_TO_TAXONOMY[end_rank]
            while  current_rank > end_rank:
                try:
                    index_list.remove(BitIndex.from_string(INT_TO_TAXONOMY[current_rank]))
                except ValueError:
                    pass
                finally:
                    current_rank = current_rank-1

            new_data.insert_dict['checks'] = ChecksManager.generate_mask(index_list)

    return new_data

def handle_error(response: requests.Response, batch_index: int) -> None:
    """Handles errors from GBIF API by raising exceptions

    Args:
        - response: Response object from requests
        - batch_index: Index of the batch that failed
    """
    if response.status_code == 400:
        logger.error("Batch %s failed: SQL syntax error. %s", batch_index, response.text)
        raise ValueError("SQL syntax error")

    if response.status_code == 401:
        logger.error("Unauthorized: Check your username or password.")
        raise ValueError("Unauthorized")

    if response.status_code == 403:
        logger.error("Forbidden: Your account does not have access to this feature.")
        raise ValueError("Forbidden")

    logger.error("Batch %s failed with Code %s: %s", batch_index,
                response.status_code, response.text)
    raise ValueError("Request failed")

def _gibf_status_handler(req_id: int, retries: int=3) -> dict:
    """ Handles status requests to GBIF

        Args:
            - id (int): ID of the download request
            - retries (int): Number of retries on failure

        Returns:
            - Dictonary with status information

        Raises:
            - ValueError: If download fails
    """
    try:
        meta = occ.download_meta(req_id)
        return meta
    except Exception as err:
        if retries > 0:
            # Time-out error, retry after waiting for a while
            logger.debug('Status update failed, retrying in 30 seconds.')
            time.sleep(30)
            return _gibf_status_handler(id, retries-1)

        logger.warning('Download failed, please run review command.')
        logger.debug(('Download failed.\n\t'
                       'Error: %s') , err)
        raise ValueError("Download failed")

def _gbif_download_handler(req_id: int, file_path: str, retries: int=3) -> bool:
    """ Handles download requests to GBIF

        Args:
            - id (int): ID of the download request
            - retries (int): Number of retries on failure

        Returns:
            - True on success

        Raises:
            - ValueError: If download fails
    """
    try:
        occ.download_get(req_id, file_path)
        return True
    except Exception as err:
        if retries > 0:
            # Time-out error, retry after waiting for a while
            logger.debug('Status update failed, retrying in 30 seconds.')
            time.sleep(30)
            return _gbif_download_handler(id, file_path, retries-1)

        logger.warning('Download failed, please run review command.')
        logger.debug(('Download failed.\n\t'
                       'Error: %s') , err)
        raise ValueError("Download failed")


def get_locations_sql(keys: List[int], batch_size: int) -> Generator[Tuple[str, List[int]],
                                                                     Any, Any]:
    """ Downloads locations from GBIF using SQL queries

        Note:
            This function is experimental and reflects GBIFs current implementation
            for SQL downloads.
            This feature is only available to invites users and not open to public.

        Args:
            - keys: List of GBIF taxon-keys to download
            - batch_size: Number of keys to download in each batch

        Yields:
            - Tuple[str, List[int]]: Path to the downloaded file and list of keys in the batch
    """

    gbif_user = os.getenv('GBIF_USER')
    gbif_pwd = os.getenv('GBIF_PWD')
    api_url = "https://api.gbif.org/v1/occurrence/download/request"

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
        batch_id = int((i/batch_size) + 1)
        sql_query = f'SELECT acceptedtaxonkey, decimallatitude, decimallongitude, countrycode FROM occurrence WHERE acceptedtaxonkey IN ({", ".join(map(str, batch))}) AND hascoordinate = TRUE;'
        json_payload = {
            "sendNotification": False,
            "notificationAddresses": ["none@provided.com"],
            "format": "SQL_TSV_ZIP",
            "sql": sql_query
        }

        # ToDo: Set a reasonable timeout here
        response = requests.post(
            api_url,
            auth=(gbif_user, gbif_pwd),
            headers={"Content-Type": "application/json"},
            data=json.dumps(json_payload)
        )
        if response.status_code == 201:
            req_id = response.text.splitlines()[-1]  # Download key is on the last line
            logger.info("Batch %s submitted successfully: %s", batch_id, req_id)
        else:
            handle_error(response, i)

        meta = _gibf_status_handler(req_id)
        while meta["status"] != "SUCCEEDED":
            # We need to check if the request was killed.
            if meta["status"] == "KILLED":
                logger.error("Batch %i failed: Request was killed by server", batch_id)
                raise ValueError("Request was killed")

            # logger.info("Wating download... Metainfo:\n%s", meta['status'])
            time.sleep(60)
            meta = _gibf_status_handler(req_id)
            #meta = occ.download_meta(req_id)

        # The time has come to download the file.
        logger.info("Starting download for request: %s", req_id)
        #occ.download_get(req_id, file_path)
        _gbif_download_handler(req_id, file_path)
        yield (os.path.join(file_path, f"{req_id}.zip"), batch)

def get_locations(keys: List[int], batch_size: int) -> Generator[Tuple[str, List[int]], Any, Any]:
    """ Downloads locations from GBIF using its API

        Note:
            These downloads are not specific and download all available information.
            Thus, size of the file can be quite large.
            Limit the number of keys to download in each batch to avoid issues.
            We recommend using the SQL download function for more specific downloads.

        Args:
            - keys: List of GBIF taxon-keys to download
            - batch_size: Number of keys to download in each batch

        Yields:
            - Tuple[str, List[int]]: Path to the downloaded file and list of keys in the batch
    """

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
        batch_id = int((i/batch_size) + 1)
        batch = [f"\"{key}\"" for key in batch]
        key_str = f"taxonKey in [{', '.join(key for key in batch)}]"

        dwn_info = occ.download([key_str, "hasCoordinate = TRUE"], email=False)
        req_id = dwn_info[0]

        meta = occ.download_meta(req_id)
        while meta["status"] != "SUCCEEDED":
            #logger.info("Still not ready. Metainfo:\n%s", meta['status'])
            if meta["status"] == "KILLED":
                logger.error("Batch %i failed: Request was killed by server", batch_id)
                raise ValueError("Request was killed")
            time.sleep(60)
            meta = occ.download_meta(req_id)

        # The time has come to download the file.
        logger.info("Starting download for request: %s", req_id)
        occ.download_get(req_id, file_path)
        yield (os.path.join(file_path, f"{req_id}.zip"), batch)
