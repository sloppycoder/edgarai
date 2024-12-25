import logging
import os

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery

from edgar import load_master_idx


def test_load_index():
    _prep_bigquery()

    # first time run should get all the rows into the index table
    assert load_master_idx(2020, 1) == 327705
    # run it again should not load any rows
    assert load_master_idx(2020, 1) == 0


def _prep_bigquery():
    """cleanup data in BigQuery for testing"""
    dataset_id = os.environ.get("BQ_DATASET_ID", "edgartest")
    if dataset_id == "edgar":
        raise RuntimeError("Cannot use the production dataset for testing")

    table_id = os.environ.get("BQ_IDX_TABLE_ID", "master_idx")

    bq_client = bigquery.Client()
    idx_table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    try:
        bq_client.delete_table(idx_table_ref)
        logging.info("Deleted existing test table")
    except google_exceptions.NotFound:
        pass
