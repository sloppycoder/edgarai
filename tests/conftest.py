# Initialize logging
import logging
import sys

logging.basicConfig(level=logging.WARNING, format="%(message)s", stream=sys.stdout)
logging.getLogger("edgar").setLevel(logging.DEBUG)
logging.getLogger("gcp_helper").setLevel(logging.DEBUG)
