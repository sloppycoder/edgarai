import base64
import datetime
import json
import logging

import flask
import functions_framework

import config
from edgar import chunk_filing, extractor, load_master_idx
from gcp_helper import create_cloudevent, publish_to_pubsub, setup_logging

# initiaze logging. use google cloud logging if running in GCP
setup_logging()

# set level of application modules
# setting root LOG_LEVEL to DEBUG will log too much noise from other packages
app_log_level = config.log_level
logging.getLogger("main").setLevel(app_log_level)
logging.getLogger("edgar").setLevel(app_log_level)
logging.getLogger("gcp_helper").setLevel(app_log_level)

logger = logging.getLogger(__name__)


def publish_response(req_id: str, status: str, message: str = "{}"):
    event = create_cloudevent(
        attributes={},
        data={
            "req_id": req_id,
            "status": status,
            "message": message,
            "timestamp": datetime.datetime.now(datetime.UTC).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        },
    )
    publish_to_pubsub(event, config.res_topic)


@functions_framework.cloud_event
def edgar_processor(cloud_event):
    logger.info(f"edgar_trigger received {cloud_event}")

    event = cloud_event.data

    message = event.get("message")
    req_id = message.get("message_id")
    data = json.loads(base64.b64decode(message["data"]))

    dataset_id = data.get("dataset_id")
    config.setv("dataset_id", dataset_id)

    func_name = data.get("function")
    if func_name == "load_master_idx":
        year = data.get("year")
        qtr = data.get("quarter")

        if not year or not qtr:
            msg = f"Invalid year/quarter {year}/{qtr}"
            logger.info(msg)
            publish_response(req_id, "ERROR", msg)
            return

        n_rows = load_master_idx(int(year), int(qtr))
        msg = f"{n_rows} uploaded to index table for {year} QTR{qtr}"
        logger.info(msg)
        publish_response(req_id, "SUCCESS", msg)

    elif func_name == "chunk_one_filing":
        cik = data.get("cik")
        filename = data.get("filename")

        if not cik or not filename:
            msg = f"Invalid cik/filename {cik}/{filename}"
            logger.info(msg)
            publish_response(req_id, "ERROR", msg)
            return

        n_chunks = chunk_filing(cik, filename, "485BPOS")
        msg = f"{n_chunks} chunks saved for {cik} {filename}"
        logger.info(msg)
        publish_response(req_id, "SUCCESS" if n_chunks > 0 else "ERROR", msg)

    else:
        publish_response(req_id, "ERROR", f"Cannot process {message}")


@functions_framework.http
def trigger_processor(request: flask.Request):  # noqa: C901
    """
    This function is intended to be invoked as a Remote Function in BigQuery
    thus the logic
    """
    try:
        replies = []
        request_json = request.get_json()

        logger.info(f"http_trigger received {request_json}")

        calls = request_json["calls"]
        for call in calls:
            func_name = call[0].strip()
            dataset_id = call[1].strip()
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

                event = create_cloudevent(
                    attributes={},
                    data={
                        "function": "load_master_idx",
                        "dataset_id": dataset_id,
                        "year": year,
                        "quarter": qtr,
                    },
                )

                msg_id = publish_to_pubsub(event, config.req_topic)
                replies.append(
                    f"SUCCESS: published {msg_id} to trigger load_master_idx {dataset_id},{year},{qtr}"  # noqa E501
                )

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

                event = create_cloudevent(
                    attributes={},
                    data={
                        "function": "chunk_one_filing",
                        "dataset_id": dataset_id,
                        "cik": cik,
                        "filename": filename,
                    },
                )

                msg_id = publish_to_pubsub(event, config.req_topic)
                replies.append(
                    f"SUCCESS: published {msg_id} to trigger chunk_one_filing {dataset_id},{cik},{filename}"  # noqa E501
                )

            else:
                replies.append(f"ERROR: unknown function name {func_name}")
                continue

        return flask.jsonify({"replies": replies})

    except Exception as e:
        return flask.jsonify({"errorMessage": str(e)}), 400


@functions_framework.http
def get_most_relevant_chunks(request: flask.Request):
    try:
        replies = []
        request_json = request.get_json()

        logger.info(f"get_most_relevant_chunks received {request_json}")

        calls = request_json["calls"]
        for call in calls:
            dataset_id = call[0].strip()
            if not dataset_id:
                replies.append("ERROR: dataset_id is required")
                continue

            config.setv("dataset_id", dataset_id)

            cik, accession_number, dimensionality = call[1], call[2], call[3]

            if not cik or not cik.isdigit() or not accession_number:
                replies.append("ERROR: invalid cik or accession_number")
                continue

            if dimensionality not in (256, 768):
                replies.append("ERROR: dimensionality must be 256 or 768")
                continue

            chunks = extractor.find_most_relevant_chunks(
                cik, accession_number, dimensionality
            )
            chunks_json = json.dumps(chunks)
            logger.info(
                f"get_most_relevant_chunks({cik}, {accession_number}) -> {chunks_json}"
            )
            replies.append(json.dumps(chunks))

        return flask.jsonify({"replies": replies})

    except Exception as e:
        return flask.jsonify({"errorMessage": str(e)}), 400
