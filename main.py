import logging
import os
import random
import sys

import functions_framework
from flask import jsonify

from edgar import load_master_idx

# Initialize logging
app_log_level = getattr(logging, os.environ.get("LOG_LEVEL", "").upper(), logging.INFO)
logging.basicConfig(level=logging.WARNING, format="%(message)s", stream=sys.stdout)
# adjust log level for modules in our app
# in order not to display debug messages from packages which is quite noisy
logging.getLogger("edgar").setLevel(app_log_level)
logging.getLogger("sec").setLevel(app_log_level)

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

    rows = load_master_idx(int(year), int(qtr))
    if rows is not None:
        return jsonify(
            {"message": f"{rows} uploaded to index table for {year} QTR{qtr}"},
        ), 200
    else:
        return jsonify(
            {"error": f"Unable to uploaded index for {year} QTR{qtr}"},
        ), 400
