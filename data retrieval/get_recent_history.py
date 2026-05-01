import requests
import logging
import os
import sys
import pendulum
import time
import duckdb
from typing import Dict
from pathlib import Path
from requests.exceptions import RequestException
from loading import save_to_jsonl

sys.path.insert(0, str(Path(__file__).parent.parent))

from logging_utils.logging_utils import setup_logging, get_current_filename

BASE_URL = "http://ws.audioscrobbler.com/2.0/"
NUM_REQUESTS = 100
USER = "GammelPerson"
HEADER = {"user-agent": "GammelPerson_2025_data"}

def get_listening_history(request_parameters:dict, max_retries=3) -> Dict:
    for attempt in range(max_retries):
        try:
            response = requests.get(url=BASE_URL, params=request_parameters, headers=HEADER)
            response.raise_for_status()
            data = response.json()
    
            if "error" in data:
                logging.error(f"API error: {data.get('message')}. Code: {data.get('error')}")
                raise ValueError(f"API returned error: {data.get('error')}")

            return data

        except RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logging.warning(f"Request failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"Request failed after {max_retries} attempts")
                raise

def extract_track_data(tracks:dict):
    records_out = []
    for track in tracks:
        track_info = {
            "artist_name": track["artist"]["#text"],
            "artist_mbid": track["artist"]["mbid"],
            "album_name": track["album"]["#text"],
            "album_mbid": track["album"]["mbid"],
            "track_name": track["name"],
            "track_mbid": track["mbid"],
            "date_played_unix": int(track["date"]["uts"]),
        }
        records_out.append(track_info)

    return records_out

def get_most_recent_timestamp():
    data_dir = Path("data/lastfm/listening")
    db = duckdb.read_json(data_dir/"*.jsonl")
    recent_timestamp = duckdb.query(
        """
        SELECT 
            date_played_unix 
        FROM 
            db 
        ORDER BY 
            date_played_unix 
        DESC 
        LIMIT 1;
        """
        ).fetchone()[0]
    return int(recent_timestamp)

def main(client_id:str, page=1):
    setup_logging(get_current_filename())

    data_dir = Path("data/lastfm/listening") # path relative to project root

    from_timestamp = get_most_recent_timestamp() + 1
    to_timestamp = int(pendulum.today().add(days=-1).timestamp())

    request_parameters = {
        "limit": NUM_REQUESTS,
        "user": USER,
        "from": from_timestamp,
        "to": to_timestamp,
        "extended": 0,
        "api_key": client_id,
        "format": "json",
        "method": "user.getrecenttracks",
        "page": page,  # we start indexing from page 1, 2, etc.
    }

    current_date = pendulum.now().naive().format("YYYY-MM-DD")
    base_filename = f"lastfm_listening_data {current_date}"

    # Initial response
    init_response = get_listening_history(request_parameters)
    total_pages = int(init_response["recenttracks"]["@attr"]["totalPages"])
    track_response = init_response["recenttracks"]["track"]
    tracks = extract_track_data(track_response)
    
    # dump to file
    save_to_jsonl(tracks, data_dir/f"{base_filename}_{page:04d}.jsonl")

    # Retrieval loop
    for page in range(page+1, total_pages+1):
        time.sleep(0.5)
        filepath = data_dir/f"{base_filename}_{page:04d}.jsonl"
        request_parameters["page"] = page

        try:
            page_response = get_listening_history(request_parameters)
            track_response = page_response["recenttracks"]["track"]
            tracks = extract_track_data(track_response)
            save_to_jsonl(tracks, filepath) # dump to file
        except (RequestException, ValueError) as e:
            logging.error(f"Failed to process page {page}: {e}")
            continue

        if (page % 20) == 0:
            logging.info(f"{page} requests have been processed. Sleeping...")
            time.sleep(5)



if __name__ == "__main__":
    lastfm_client_id = os.environ["CLIENT_ID"] # reads environment variable from GitHub Actions
    main(lastfm_client_id)
