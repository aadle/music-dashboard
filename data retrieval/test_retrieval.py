import requests
import os
import time
import logging
import json 
import pendulum
from math import floor
from requests.exceptions import RequestException
from typing import Dict
from pathlib import Path

BASE_URL = "http://ws.audioscrobbler.com/2.0/"
NUM_REQUESTS = 100
USER = "GammelPerson"
HEADER = {"user-agent": "GammelPerson_2025_data"}

def make_request(request_parameters:dict, max_retries=3) -> Dict:
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

def main():
    client_id = os.environ["CLIENT_ID"] # reads environment variable from GitHub Actions
    request_parameters = {
        "api_key": client_id,
        "method": "artist.getInfo",
        "autocorrect": 1,
        "format": "json",
        "artist": "Bladee"
    }
    response = make_request(request_parameters)
    response_out = extract_artist_data(response)
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)
    
    with open(output_dir/"foo.json", "w") as outfile:
        json.dump(response_out, outfile)

    print("Success!")

if __name__ == "__main__":
    main()
