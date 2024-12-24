import json
import logging
import logging.config
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

with open(Path(__file__).parent / "logger_config.json", "r") as f:
    logging.config.dictConfig(json.load(f))

if __name__ == "__main__":
    print("Hello edgar")
