import base64
import datetime
import json
import logging
import os
import sys

import flask
import functions_framework

from edgar import chunk_filing, load_master_idx
from gcp_helper import create_cloudevent, publish_to_pubsub

# Initialize logging
app_log_level = getattr(logging, os.environ.get("LOG_LEVEL", "").upper(), logging.INFO)
print(f"app_log_level={app_log_level}")
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stdout
)
# adjust log level for modules in our app
# in order not to display debug messages from packages which is quite noisy
logging.getLogger("edgar").setLevel(logging.DEBUG)
logging.getLogger("gcp_helper").setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)


def publish_response(req_id: str, status: str, message: str = "{}"):
    response_topic = os.environ.get("RESPONSE_TOPIC", "edgarai-response")

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
    publish_to_pubsub(event, response_topic)


@functions_framework.cloud_event
def edgar_processor(cloud_event):
    logger.info(f"edgar_trigger received {cloud_event}")

    event = cloud_event.data

    message = event.get("message")
    req_id = message.get("message_id")
    data = json.loads(base64.b64decode(message["data"]))

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
def trigger_chunk_filing(request: flask.Request):
    """
    This function is intended to be invoked as a Remote Function in BigQuery
    thus the logic
    """
    request_topic = os.environ.get("REQUEST_TOPIC", "edgarai-request")

    try:
        replies = []
        request_json = request.get_json()

        logger.info(f"http_trigger received {request_json}")

        calls = request_json["calls"]
        for params in calls:
            cik, filename = params[0], params[1]
            if not cik or not filename:
                replies.append("ERROR: cik and filename are required")
                continue

            event = create_cloudevent(
                attributes={},
                data={
                    "function": "chunk_one_filing",
                    "cik": cik,
                    "filename": filename,
                },
            )

            msg_id = publish_to_pubsub(event, request_topic)
            replies.append(
                f"SUCCESS: published {msg_id} to trigger chunk_one_filing {cik},{filename}"  # noqa E501
            )

        return flask.jsonify({"replies": replies})

    except Exception as e:
        return flask.jsonify({"errorMessage": str(e)}), 400
