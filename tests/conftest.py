# Initialize logging
import logging
import sys

import config

logging.basicConfig(level=logging.WARNING, format="%(message)s", stream=sys.stdout)
logging.getLogger("edgar").setLevel(logging.DEBUG)
logging.getLogger("gcp_helper").setLevel(logging.DEBUG)

config.setv("dataset_id", "edgar_dev")
config.setv("bucket_id", "edgar_666")
config.setv("cache_dir", "test/cache")
