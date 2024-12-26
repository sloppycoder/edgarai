import logging
import re

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)


def blob_as_text(blob_uri: str) -> str:
    """read the content of a blob as text"""
    matches = re.match(r"gs://([^/]+)/(.+)", blob_uri)
    if not matches:
        return ""

    bucket_name, blob_path = matches.groups()

    storage_client = storage.Client()
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        content = blob.download_as_text(encoding="utf-8")
        return content if content else ""
    finally:
        storage_client.close()


def ensure_table_exists(bq_client, table_ref, schema):
    """
    Check if a BigQuery table exists, and create it if it doesn't.

    Args:
        bq_client (bigquery.Client): The BigQuery client.
        dataset_id (str): The dataset ID.
        table_id (str): The table ID.
        schema (list[bigquery.SchemaField]): The schema of the table.
    """
    try:
        bq_client.get_table(table_ref)
        logger.debug(f"Table {table_ref} already exists.")
    except google_exceptions.NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        logger.info(f"Table {table_ref} created.")
