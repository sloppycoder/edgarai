from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery

import config


def drop_table_if_exists(table_name: str):
    if config.dataset_id == "edgar":
        raise RuntimeError("Cannot use the production dataset for testing")

    with bigquery.Client() as bq_client:
        bq_client = bigquery.Client()
        try:
            bq_client.delete_table(
                f"{bq_client.project}.{config.dataset_id}.{table_name}"
            )
        except google_exceptions.NotFound:
            pass
