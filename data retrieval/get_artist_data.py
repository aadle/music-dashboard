import sys
import requests
import logging
import time
import tomllib
import polars as pl
import json
import pendulum
from math import floor
from requests import RequestException
from pathlib import Path
from typing import Dict

sys.path.insert(0, str(Path(__file__).parent.parent))
from logging_utils.logging_utils import setup_logging, get_current_filename
 

BASE_URL = "http://ws.audioscrobbler.com/2.0/"
USER = "GammelPerson"
HEADER = {"user-agent": "GammelPerson_2025_data"}

def get_artist_table():
    lastfm_df = pl.read_parquet(
        Path("../data/lastfm/lastfm-listening-2021-2026march.parquet"),
        columns=[
            "date_played_unix",
            "track_played_utc",
            "artist_name",
            "artist_mbid",
        ],
    )
    spotify_df = pl.read_parquet(
        Path("../data/spotify/2017-2021 spotify data.parquet"),
        columns=["date_played_unix", "track_played_utc", "artist_name"],
    )
    df = pl.concat([lastfm_df, spotify_df], how="diagonal").sort(
        pl.col("date_played_unix"), descending=False
    )
    df = ( 
        df
        .filter(pl.col("track_played_utc").dt.year() >= 2021)
        .unique(subset=["artist_name"], keep="first")
        .sort("track_played_utc", descending=False)
        .drop_nulls(pl.col("artist_name"))
    )

    return df

def get_lastfm_data(request_parameters:dict, max_retries=3) -> Dict:
    for attempt in range(max_retries):
        try:
            response = requests.get(url=BASE_URL, params=request_parameters, headers=HEADER)
            response.raise_for_status()
            data = response.json()
    
            if "error" in data:
                logging.error(f"API error: {data.get('message')}. Code: {data.get('error')}")
            return data

        except RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logging.warning(f"Request failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"Request failed after {max_retries} attempts")
                raise

def extract_artist_data(response:dict):
    out_dict = {}
    artist_data = response.get("artist")

    out_dict["artist"] = artist_data.get("name")
    out_dict["artist_mbid"] = artist_data.get("mbid")
    
    out_dict["artist_image_url"] = artist_data.get("image")[2].get("#text")

    # last.fm stats
    out_dict["listeners"] = artist_data.get("stats").get("listeners")
    out_dict["playcount"] = artist_data.get("stats").get("playcount")

    # tags
    toptags = artist_data.get("tags").get("tag")
    out_dict["artist_genre_tags"] = [tag.get("name") for tag in toptags] if toptags else None

    # data retrieved
    out_dict["unix_timestamp"] = floor(pendulum.now(tz="UTC").timestamp())

    return out_dict

def main(client_id:str):
    setup_logging(get_current_filename())

    out_dir = Path("../data/lastfm/artist/2025-2026")
    out_dir.mkdir(parents=True, exist_ok=True)
    base_filename = "lastfm_artist_data_2021-2026"

    # Load in listening data
    artist_records = get_artist_table().to_dicts()

    request_parameters = {
        "api_key": client_id,
        "method": "artist.getInfo",
        "artist": None,
        "autocorrect": 1,
        "format": "json"
    }

    file_idx = 0
    for idx, artist in enumerate(artist_records, 1):
        if (idx % 2000) == 0:
            file_idx += 1

        # update request parameters
        request_parameters["artist"] = artist.get("artist_name")
        request_parameters["mbid"] = (
            artist.get("artist_mbid") if artist.get("artist_mbid") else None
        )

        response = get_lastfm_data(request_parameters)
        if "error" in response:
            logging.warning(
                f"{request_parameters.get("artist")} is not available in Last.fm's database."
            )
            continue
        
        # appending entry to file
        extracted_data = extract_artist_data(response)
        with open(out_dir/f"{base_filename}_{file_idx}.jsonl", "a") as outfile:
            outfile.write(json.dumps(extracted_data) + "\n")

        logging.info(f"{idx}. {request_parameters.get("artist")} have been processed.")

        if (idx % 500) == 0:
            logging.info(f"{idx} artists processed.")
            time.sleep(5)

        time.sleep(0.3)

if __name__ == "__main__":
    secrets = Path("../secrets")
    with open(secrets/"lastfm.toml", "rb") as f:
        lastfm_toml = tomllib.load(f)

    lastfm_client_id = lastfm_toml["lastfm-credentials"]["client_id"]
    main(lastfm_client_id)
    
