import os

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery


def drop_table_if_exists(table_name: str):
    dataset_id = os.environ.get("BQ_DATASET_ID", "edgar_dev")
    if dataset_id == "edgar":
        raise RuntimeError("Cannot use the production dataset for testing")

    with bigquery.Client() as bq_client:
        bq_client = bigquery.Client()
        try:
            bq_client.delete_table(f"{bq_client.project}.{dataset_id}.{table_name}")
        except google_exceptions.NotFound:
            pass
