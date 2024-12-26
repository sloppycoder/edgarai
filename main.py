import logging
import os
import sys

import functions_framework

from edgar import chunk_filing, load_master_idx

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


@functions_framework.cloud_event
def edgar_trigger(cloud_event):
    event = cloud_event.data
    logger.info(f"edgar_trigger received {event}")

    attrs = event.get("message").get("attributes")
    func_name = attrs.get("function")

    if func_name == "load_master_idx":
        year = attrs.get("year")
        qtr = attrs.get("quarter")

        if not year or not qtr:
            logger.info(f"Invalid year/quarter {year}/{qtr}")
            return

        n_rows = load_master_idx(int(year), int(qtr))
        logger.info(f"{n_rows} uploaded to index table for {year} QTR{qtr}")

    elif func_name == "chunk_one_filing":
        filename = attrs.get("filename")

        n_chunks = chunk_filing(filename, "485BPOS")
        logger.info(f"{n_chunks} chunks saved")


if __name__ == "__main__":
    n_chunks = chunk_filing("edgar/data/1002427/0001133228-24-004879.txt", "485BPOS")
    print(f"{n_chunks} chunks saved")
