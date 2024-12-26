import logging
import os
import random
import sys

import functions_framework
from flask import jsonify

from edgar import chunk_filing, load_master_idx
from edgar.util import idx_filename2accession_number

# Initialize logging
app_log_level = getattr(logging, os.environ.get("LOG_LEVEL", "").upper(), logging.INFO)
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stdout
)
# adjust log level for modules in our app
# in order not to display debug messages from packages which is quite noisy
logging.getLogger("edgar").setLevel(app_log_level)
logging.getLogger("gcp_healper").setLevel(app_log_level)

# a crude mechanism to prevent triggering by someone unknown.
# the default random bytes will not be displayed, thus the function
# cannot be triggered
secret_word = os.environ.get("SECRET_WORD", random.randbytes(10).hex())


@functions_framework.http
def load_idx_handler(request):
    """HTTP Cloud Function to download EDGAR index file and save to Cloud Storage."""
    year = request.args.get("year")
    qtr = request.args.get("qtr")
    word = request.args.get("word")

    if not word or word != secret_word:
        return jsonify({"error": "Invalid secret word"}), 403

    if not year or not qtr:
        return jsonify({"error": "Invalid year and qtr"}), 400

    n_rows = load_master_idx(int(year), int(qtr))
    if n_rows is not None:
        return jsonify(
            {"message": f"{n_rows} uploaded to index table for {year} QTR{qtr}"},
        ), 200
    else:
        return jsonify(
            {"error": f"Unable to uploaded index for {year} QTR{qtr}"},
        ), 400


@functions_framework.http
def chunk_one_filing(request):
    """
    HTTP Cloud Function to download a SEC filing, find the main html file
    then split the text into chunks
    """
    filename = request.args.get("filename")
    word = request.args.get("word")

    if not word or word != secret_word:
        return jsonify({"error": "Invalid secret word"}), 403

    if not filename or len(idx_filename2accession_number(filename)) != 20:
        return jsonify({"error": "Invalid filename"}), 400

    n_chunks = chunk_filing(filename, "485BPOS")

    if n_chunks > 0:
        return jsonify(
            {"message": f"{n_chunks} chunks saved"},
        ), 200
    else:
        return jsonify(
            {"error": "Error. check the logs"},
        ), 400


if __name__ == "__main__":
    n_chunks = chunk_filing("edgar/data/1002427/0001133228-24-004879.txt", "485BPOS")
    print(f"{n_chunks} chunks saved")
