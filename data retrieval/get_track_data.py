import sys
import requests
import logging
import time
import tomllib
import polars as pl
import json
from requests import RequestException
from pathlib import Path
from typing import Dict

sys.path.insert(0, str(Path(__file__).parent.parent))
from logging_utils.logging_utils import setup_logging, get_current_filename
 

BASE_URL = "http://ws.audioscrobbler.com/2.0/"
USER = "GammelPerson"
HEADER = {"user-agent": "GammelPerson_2025_data"}

def get_track_table():
    lastfm_df = pl.read_parquet(
        Path("../data/lastfm/lastfm-listening-2021-2026march.parquet")
    )
    spotify_df = pl.read_parquet(Path("../data/spotify/2017-2021 spotify data.parquet"))
    lastfm_df = lastfm_df.with_columns(spotify_track_uri=pl.lit(""))
    spotify_df = spotify_df.with_columns(
        artist_mbid=pl.lit(""), album_mbid=pl.lit(""), track_mbid=pl.lit("")
    )
    cols = [
        "date_played_unix",
        "track_played_utc",
        "track_name",
        "artist_name",
        "track_mbid",
    ]
    lastfm_df = lastfm_df.select(cols)
    spotify_df = spotify_df.select(cols)

    df = lastfm_df.vstack(spotify_df).sort(pl.col("date_played_unix"), descending=False)
    df = ( 
        df
        .filter(pl.col("track_played_utc").dt.year() >= 2021)
        .unique(subset=["track_name"], keep="first")
        .sort("track_played_utc", descending=False)
        .drop_nulls(pl.col("track_name"))
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
                # raise ValueError(f"API returned error: {data.get('error')}")
            return data

        except RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logging.warning(f"Request failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"Request failed after {max_retries} attempts")
                raise

def extract_track_data(response:dict):
    out_dict = {}
    track_data = response.get("track")

    # track info
    out_dict["track_name"] = track_data.get("name")
    out_dict["track_mbid"] = track_data.get("mbid")
    out_dict["track_duration"] = track_data.get("duration")

    out_dict["artist"] = track_data.get("artist").get("name")

    # last.fm statistics
    out_dict["listeners"] = track_data.get("listeners")
    out_dict["playcount"] = track_data.get("playcount")

    # album info
    if "album" in track_data.keys():
        album_data = track_data.get("album")
        out_dict["album_title"] = album_data.get("title")
        out_dict["album_mbid"] = album_data.get("mbid")
        out_dict["album_image_url"] = album_data.get("image")[2].get("#text")
    else:
        out_dict["album_title"] = None
        out_dict["album_mbid"] = None
        out_dict["album_image_url"] = None

    # get user set genre tags, if available 
    toptags = track_data.get("toptags").get("tag")
    out_dict["genre_tags"] = [tag.get("name") for tag in toptags] if toptags else None
    return out_dict

def main(client_id:str):
    setup_logging(get_current_filename())

    out_dir = Path("../data/lastfm/tracks/2025-2026")
    out_dir.mkdir(parents=True, exist_ok=True)
    base_filename = "lastfm_track_data_2021-2026"

    # Load in listening data
    track_records = get_track_table().to_dicts()

    request_parameters = {
        "api_key": client_id,
        "method": "track.getInfo",
        "artist": None,
        "track": None,
        "autocorrect": 1,
        "format": "json"
    }

    file_idx = 0
    for idx, track in enumerate(track_records, 1):
        if (idx % 2000) == 0:
            file_idx += 1

        # update request parameters
        request_parameters["artist"] = track.get("artist_name")
        request_parameters["track"] = track.get("track_name")
        request_parameters["mbid"] = (
            track.get("track_mbid") if track.get("track_mbid") else None
        )

        response = get_lastfm_data(request_parameters)
        if "Track not found" in response.values():
            logging.warning(
                f"{request_parameters.get("track")} by {request_parameters.get("artist")} is not available in Last.fm's database."
            )
            continue
        
        # appending entry to file
        extracted_data = extract_track_data(response)
        with open(out_dir/f"{base_filename}_{file_idx}.jsonl", "a") as outfile:
            outfile.write(json.dumps(extracted_data) + "\n")

        logging.info(f"{idx}. {request_parameters.get("track")} by {request_parameters.get("artist")} have been processed.")

        if (idx % 500) == 0:
            logging.info(f"{idx} tracks processed.")
            time.sleep(5)

        time.sleep(0.3)

if __name__ == "__main__":
    secrets = Path("../secrets")
    with open(secrets/"lastfm.toml", "rb") as f:
        lastfm_toml = tomllib.load(f)

    lastfm_client_id = lastfm_toml["lastfm-credentials"]["client_id"]
    main(lastfm_client_id)
    

