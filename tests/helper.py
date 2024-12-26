import os

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery


def prep_big_query():
    """cleanup data in BigQuery for testing"""
    dataset_id = os.environ.get("BQ_DATASET_ID", "edgartest")
    if dataset_id == "edgar":
        raise RuntimeError("Cannot use the production dataset for testing")

    bq_client = bigquery.Client()
    # delete tables used in test and start from scratch
    [
        _drop_table_if_exists(bq_client, table)
        for table in [
            f"{bq_client.project}.{dataset_id}.master_idx",
            f"{bq_client.project}.{dataset_id}.filing_text_chunks",
        ]
    ]


def _drop_table_if_exists(bq_client, table_ref):
    try:
        bq_client.delete_table(table_ref)
    except google_exceptions.NotFound:
        pass
