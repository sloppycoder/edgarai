import os
import re
from datetime import datetime

import functions_framework
from google.cloud import bigquery, storage

dataset_id = os.environ.get("BQ_DATASET_ID", "edgar")
table_id = os.environ.get("BQ_TABLE_ID", "master_idx")
folder_name = os.environ.get("GCS_FOLDER_NAME", "edgar_master_idx")
timestamp = datetime.now().strftime("%H%M%S")

schema = [
    bigquery.SchemaField("cik", "STRING"),
    bigquery.SchemaField("company_name", "STRING"),
    bigquery.SchemaField("form_type", "STRING"),
    bigquery.SchemaField("date_filed", "DATE"),
    bigquery.SchemaField("filename", "STRING"),
]


@functions_framework.cloud_event
def gcs_event_handler(cloud_event):
    """Triggered by a file upload to a Cloud Storage bucket."""
    # Get the bucket and file details
    event_data = cloud_event.data
    print(event_data)

    bucket_name = event_data["bucket"]
    file_name = event_data["name"]

    if not file_name.startswith(folder_name + "/") or not file_name.endswith(".idx"):
        print(f"Ignoring file: {file_name}")
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

        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"Table {table_ref} created.")


def load_csv_to_bigquery(bq_client, bucket_name, file_name, dataset_id, table_id):
    """Load a CSV file into BigQuery."""
    uri = f"gs://{bucket_name}/{file_name}"
    temp_table_id = f"{table_id}_{timestamp}"
    temp_table_ref = f"{bq_client.project}.{dataset_id}.{temp_table_id}"
    main_table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    create_table_if_not_exists(bq_client, dataset_id, f"{table_id}_{timestamp}")
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=11,
            field_delimiter="|",
        )

        print(f"Loading data from {uri} to {temp_table_ref}...")
        load_job = bq_client.load_table_from_uri(
            uri, temp_table_ref, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        columns = [field.name for field in schema]
        print(columns)

        merge_query = f"""
        MERGE `{main_table_ref}` T
        USING `{temp_table_ref}` S
        ON {" AND ".join([f"T.{col} = S.{col}" for col in columns])}
        WHEN NOT MATCHED THEN
        INSERT ({', '.join(columns)}) VALUES
           ({', '.join([f'S.{col}' for col in columns])})
        """
        print(f"MERGE QUERY: {merge_query}")
        query_job = bq_client.query(merge_query)
        query_job.result()  # Wait for the job to complete
        print(f"Data merged successfully into {main_table_ref}.")

    finally:
        bq_client.delete_table(temp_table_ref)
        print(f"Temporary table {temp_table_ref} deleted.")


def rename_file_in_gcs(bucket_name, file_name):
    """Rename a file in Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    new_name = re.sub(r"([^/]+)$", r"\1.done", file_name)
    new_blob = bucket.blob(new_name)

    if new_blob.exists():
        new_blob.delete()
        print(f"Existing file {new_name} deleted.")

    # Rename the blob
    bucket.rename_blob(blob, new_name)
    print(f"File renamed from {file_name} to {new_name}.")
