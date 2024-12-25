import logging
import os

import pytest
from google.cloud import bigquery

# suppress INFO logs to reduce noise in test output
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)


@pytest.fixture(scope="session", autouse=True)
def prep_bigquery():
    dataset_id = os.environ.get("BQ_DATASET_ID", "edgartest")
    if dataset_id == "edgar":
        raise ValueError("Cannot use the production dataset for testing")

    table_id = os.environ.get("BQ_IDX_TABLE_ID", "master_idx")

    bq_client = bigquery.Client()
    idx_table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    if bq_client.get_table(idx_table_ref):
        logging.info("Deleting existing test table")
        bq_client.delete_table(idx_table_ref)
