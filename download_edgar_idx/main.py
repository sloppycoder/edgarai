import os
import random

import functions_framework
import requests
from flask import jsonify
from google.cloud import storage

# a crude mechanism to prevent triggering by someone unknown.
# the default random bytes will not be displayed, thus the function
# cannot be triggered
secret_word = os.environ.get("SECRET_WORD", random.randbytes(10).hex())

user_agent = os.environ.get("USER_AGENT", "Lee Lynn (hayashi@yahoo.com)")
bucket_name = os.environ.get("GCS_BUCKET_NAME", "edgar_666")
folder_name = os.environ.get("GCS_FOLDER_NAME", "edgar_master_idx")


def download_edgar_idx(url: str, blob_name: str) -> tuple[int, str]:
    response = requests.get(url, headers={"User-Agent": user_agent})

    if response.status_code != 200:
        return response.status_code, response.json()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(response.text)

    return 200, "success"


@functions_framework.http
def http_handler(request):
    """HTTP Cloud Function to download EDGAR index file and save to Cloud Storage."""
    year = request.args.get("year")
    qtr = request.args.get("qtr")
    word = request.args.get("word")

    if not word or word != secret_word:
        return jsonify({"error": "Invalid secret word"}), 403

    if not year or not qtr:
        return jsonify({"error": "Invalid year and qtr"}), 400

    destination_blob_name = f"{folder_name}/{year}_{qtr}.idx"
    status_code, message = download_edgar_idx(
        f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx",
        destination_blob_name,
    )

    if status_code != 200:
        return jsonify(
            {"error": message},
        ), status_code
    else:
        return jsonify(
            {"message": f"File downloaded and saved to {destination_blob_name}"},
        ), 200
