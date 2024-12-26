import base64
import json
import logging
import os
import sys

import functions_framework

from edgar import chunk_filing, load_master_idx
from gcp_helper import create_cloudevent, publish_to_pubsub

# Initialize logging
app_log_level = getattr(logging, os.environ.get("LOG_LEVEL", "").upper(), logging.INFO)
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stdout
)
# adjust log level for modules in our app
# in order not to display debug messages from packages which is quite noisy
logging.getLogger("edgar").setLevel(app_log_level)
logging.getLogger("gcp_helper").setLevel(app_log_level)

logger = logging.getLogger(__name__)
logging.getLogger(__name__).setLevel(app_log_level)

response_topic = os.environ.get("RESPONSE_TOPIC", "edgarai-response")


def publish_response(req_id: str, status: str, message: str = "{}"):
    event = create_cloudevent(
        attributes={},
        data={
            "req-id": req_id,
            "status": status,
            "message": message,
        },
    )
    publish_to_pubsub(event, response_topic)


@functions_framework.cloud_event
def edgar_trigger(cloud_event):
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
        filename = data.get("filename")
        n_chunks = chunk_filing(filename, "485BPOS")
        msg = f"{n_chunks} chunks saved"
        logger.info(msg)
        publish_response(req_id, "SUCCESS" if n_chunks > 0 else "ERROR", msg)

    else:
        publish_response(req_id, "ERROR", f"Cannot process {message}")
