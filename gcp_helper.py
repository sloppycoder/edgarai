import json
import logging
import re
from typing import Any

import google.auth
from cloudevents.http import CloudEvent
from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery, pubsub_v1, storage

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


def create_cloudevent(attributes: dict[str, Any], data: dict[str, Any]) -> CloudEvent:
    if "type" not in attributes:
        attributes["type"] = "edgarai.event.trigger"

    if "source" not in attributes:
        attributes["source"] = "edgarai.gcp_helper"

    return CloudEvent(attributes, data)


def publish_to_pubsub(event: CloudEvent, topic_name: str) -> str | None:
    """Publishes a CloudEvent to a Pub/Sub topic."""

    _, project_id = google.auth.default()

    if not topic_name or not project_id:
        return None

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    # Convert CloudEvent to JSON
    event_attrs = event.get_attributes()
    event_data = json.dumps(event.get_data()).encode("utf-8")

    # Publish message
    future = publisher.publish(
        topic_path,
        data=event_data,
        specversion=event_attrs["specversion"],
        type=event_attrs["type"],
        source=event_attrs["source"],
    )
    msg_id = future.result()
    return msg_id
