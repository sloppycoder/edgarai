import json
import logging

import flask
import functions_framework

import config
from edgar import chunk_filing, extractor, load_master_idx
from gcp_helper import setup_logging

# initiaze logging. use google cloud logging if running in GCP
setup_logging()

# set level of application modules
# setting root LOG_LEVEL to DEBUG will log too much noise from other packages
app_log_level = config.log_level
logging.getLogger("main").setLevel(app_log_level)
logging.getLogger("edgar").setLevel(app_log_level)
logging.getLogger("gcp_helper").setLevel(app_log_level)

logger = logging.getLogger(__name__)


def handle_calls(calls):  # noqa: C901
    replies = []
    for call in calls:
        func_name = call[0].strip()
        dataset_id = call[1].strip()
        config.setv("dataset_id", dataset_id)

        params = [p.strip() for p in call[2].split("|")]

        if func_name == "load_master_idx":
            year, qtr = params[0], params[1]
            if not year or not qtr:
                replies.append("ERROR: year and qtr are required")
                continue

            try:
                year = int(year)
                if year < 2000 or year > 2030:
                    raise ValueError
            except ValueError:
                replies.append("ERROR: year must be between 2000 and 2030")
                continue

            try:
                qtr = int(qtr)
                if qtr < 1 or qtr > 4:
                    raise ValueError
            except ValueError:
                replies.append("ERROR: qurater must be 1 and 4")
                continue

            try:
                n_rows = load_master_idx(int(year), int(qtr))
                msg = f"{n_rows} uploaded to index table for {year} QTR{qtr}"
                logger.info(msg)
                replies.append(f"SUCCESS: {msg}")
            except Exception as e:
                replies.append(f"ERROR: {str(e)}")

        elif func_name == "chunk_one_filing":
            cik, filename = params[0], params[1]
            if (
                not cik
                or not filename
                or not cik.isdigit()
                or not filename.startswith("edgar/")
                or not filename.endswith(".txt")
            ):
                replies.append("ERROR: cik and filename are required")
                continue

            try:
                n_chunks = chunk_filing(cik, filename, "485BPOS")
                msg = f"{n_chunks} chunks saved for {cik} {filename}"
                logger.info(msg)
                replies.append(f"SUCCESS: {msg}")
            except Exception as e:
                replies.append(f"ERROR: {str(e)}")

        else:
            replies.append(f"ERROR: unknown function name {func_name}")

    return replies


@functions_framework.http
def edgar_processor(request: flask.Request):  # noqa: C901
    """
    This function is intended to be invoked as a Remote Function in BigQuery,
    which groups several invocations into one batch, thus the logic below
    """
    try:
        request_json = request.get_json()
        calls = request_json["calls"]
        logger.info(
            f"trigger_processor received for {len(calls)} invocations: {request_json}"
        )
        replies = handle_calls(calls)
        return flask.jsonify({"replies": replies})

    except Exception as e:
        return flask.jsonify({"errorMessage": str(e)}), 400


@functions_framework.http
def get_most_relevant_chunks(request: flask.Request):
    try:
        replies = []
        request_json = request.get_json()
        calls = request_json["calls"]

        logger.info(
            f"get_most_relevant_chunks for {len(calls)} invocations: received {request_json}"  # noqa: E501
        )

        for call in calls:
            dataset_id = call[0].strip()
            if not dataset_id:
                replies.append("ERROR: dataset_id is required")
                continue

            config.setv("dataset_id", dataset_id)

            cik, accession_number = call[1], call[2]

            if not cik or not cik.isdigit() or not accession_number:
                replies.append("ERROR: invalid cik or accession_number")
                continue

            chunks = extractor.find_most_relevant_chunks(cik, accession_number)
            chunks_json = json.dumps(chunks)
            logger.info(
                f"get_most_relevant_chunks({cik}, {accession_number}) -> {chunks_json}"
            )
            replies.append(json.dumps(chunks))

        return flask.jsonify({"replies": replies})

    except Exception as e:
        return flask.jsonify({"errorMessage": str(e)}), 400
