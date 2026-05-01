import json
import logging
import logging.config
import pathlib
import inspect
from datetime import datetime
from pathlib import Path

def get_current_filename():
    caller_file = inspect.stack()[1].filename
    return pathlib.Path(caller_file).stem

def setup_logging(current_filename:str):
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    config_file = pathlib.Path(__file__).parent / "config.json"

    current_date = datetime.today().strftime("%Y-%m-%d")
    with open(config_file) as f_in:
        config = json.load(f_in)
        config["handlers"]["file"]["filename"] = f"logs/{current_filename}_{current_date}.log"

    logging.config.dictConfig(config)

if __name__ == "__main__":
    setup_logging("foo.py")
