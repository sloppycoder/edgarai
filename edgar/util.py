import logging
import os

import requests

EDGAR_BASE_URL = "https://www.sec.gov/Archives"
user_agent = os.environ.get("USER_AGENT", "Lee Lynn (hayashi@yahoo.com)")


def download_edgar_file(filename: str) -> bytes | None:
    url = f"{EDGAR_BASE_URL}/{filename}"
    response = requests.get(url, headers={"User-Agent": user_agent})

    if response.status_code != 200:
        logging.info(f"Failed to download file from {url}, {response.content}")
        return None

    return response.content
