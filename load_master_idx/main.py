import os
import re

from google.cloud import bigquery, storage

dataset_id = os.environ.get("BQ_DATASET_ID", "edgar")
table_id = os.environ.get("BQ_TABLE_ID", "master_idx")
folder_name = os.environ.get("GCS_FOLDER_NAME", "edgar_master_idx/")


def gcs_event_handler(event, context):
    """Triggered by a file upload to a Cloud Storage bucket."""
    # Get the bucket and file details
    bucket_name = event["bucket"]
    file_name = event["name"]

    if not file_name.startswith(folder_name):
        print(f"Ignoring file outside target folder: {file_name}")
        return

    if file_name.endswith(".done"):
        return

    bq_client = bigquery.Client()
    create_table_if_not_exists(bq_client, dataset_id, table_id)
    load_csv_to_bigquery(bq_client, bucket_name, file_name, dataset_id, table_id)
    rename_file_in_gcs(bucket_name, file_name)


def create_table_if_not_exists(bq_client, dataset_id, table_id):
    """Create a BigQuery table if it does not exist."""
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"
    try:
        bq_client.get_table(table_ref)
        print(f"Table {table_ref} already exists.")
    except Exception:
        print(f"Table {table_ref} does not exist. Creating table...")

        schema = [
            bigquery.SchemaField("cik", "STRING"),
            bigquery.SchemaField("company_name", "STRING"),
            bigquery.SchemaField("form_type", "STRING"),
            bigquery.SchemaField("date_filed", "DATE"),
            bigquery.SchemaField("filename", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"Table {table_ref} created.")


def load_csv_to_bigquery(bq_client, bucket_name, file_name, dataset_id, table_id):
    """Load a CSV file into BigQuery."""
    uri = f"gs://{bucket_name}/{file_name}"
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=11,
        field_delimiter="|",
    )

    print(f"Loading data from {uri} to {table_ref}...")
    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    print(f"Data loaded successfully into {table_ref}.")


def rename_file_in_gcs(bucket_name, file_name):
    """Rename a file in Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    new_name = re.sub(r"([^/]+)$", r"\1.done", file_name)
    bucket.rename_blob(blob, new_name)
    print(f"File renamed from {file_name} to {new_name}.")
