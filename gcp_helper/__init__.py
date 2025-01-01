import base64
import logging
import os
import re
import uuid

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery, storage
from google.cloud import logging as cloud_logging

logger = logging.getLogger(__name__)


def setup_logging():
    if os.getenv("K_SERVICE"):  # Only initialize Google Cloud Logging in Cloud Run
        client = cloud_logging.Client()
        client.setup_logging()
        logging.info("Google Cloud Logging is set up.")
    else:
        logging.basicConfig(level=logging.INFO)
        logging.info("Running locally. Using basic logging.")


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


def short_uuid():
    uuid_bytes = uuid.uuid4().bytes
    return (
        base64.urlsafe_b64encode(uuid_bytes)
        .decode("utf-8")
        .replace("_", "")
        .replace("-", "")
        .rstrip("=")[:22]
    )
