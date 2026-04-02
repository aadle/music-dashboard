from typing import List, Dict
import json
import logging

def save_to_jsonl(data:List[Dict], full_filepath) -> None:
    """
    data: A list of records, e.g. "[{"key_1": val_1, "key_2": val_2, ...}, ...]"
    full_filepath: "directory" + "filename".jsonl
    """
    with open(full_filepath, "w") as outfile:
        for record in data:
            json.dump(record, outfile)
            outfile.write("\n")
    logging.info(f"{full_filepath} have been successfully saved.")
