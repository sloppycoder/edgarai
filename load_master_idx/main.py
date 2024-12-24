import os
import re
from datetime import datetime

import functions_framework
from google.cloud import bigquery, storage

dataset_id = os.environ.get("BQ_DATASET_ID", "edgar")
table_id = os.environ.get("BQ_TABLE_ID", "master_idx")
folder_name = os.environ.get("GCS_FOLDER_NAME", "edgar_master_idx")
timestamp = datetime.now().strftime("%H%M%S")
debug = os.environ.get("DEBUG", "True").lower() == "true"


@functions_framework.cloud_event
def gcs_event_handler(cloud_event):
    """Triggered by a file upload to a Cloud Storage bucket."""
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
        bq_client.create_table(
            bigquery.Table(
                table_ref,
                schema=[
                    bigquery.SchemaField("cik", "STRING", max_length=10),
                    bigquery.SchemaField("company_name", "STRING", max_length=150),
                    bigquery.SchemaField("form_type", "STRING", max_length=20),
                    bigquery.SchemaField("date_filed", "DATE"),
                    bigquery.SchemaField("filename", "STRING", max_length=100),
                    bigquery.SchemaField(
                        "accession_number",
                        "STRING",
                        max_length=20,
                        mode="NULLABLE",
                    ),
                ],
            )
        )
        print(f"Table {table_ref} created.")


def load_csv_to_bigquery(bq_client, bucket_name, file_name, dataset_id, table_id):
    """Load a CSV file into BigQuery."""
    uri = f"gs://{bucket_name}/{file_name}"
    temp_table_ref = f"{bq_client.project}.{dataset_id}.{table_id}_{timestamp}"
    main_table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=12,
        field_delimiter="|",
        autodetect=True,
    )

    print(f"Loading data from {uri} to {temp_table_ref}...")
    job = bq_client.load_table_from_uri(uri, temp_table_ref, job_config=job_config)
    job.result()

    merge_sql = rf"""
    MERGE `{main_table_ref}` T
    USING `{temp_table_ref}` S
    ON      T.cik = CAST(int64_field_0 AS STRING)
        AND T.company_name = S.string_field_1
        AND T.form_type = S.string_field_2
        AND T.date_filed = S.date_field_3
        AND T.filename = S.string_field_4
    WHEN NOT MATCHED THEN
    INSERT (
        cik,
        company_name,
        form_type,
        date_filed,
        filename,
        accession_number
    )
    VALUES
    (
        CAST(S.int64_field_0 AS STRING),
        S.string_field_1,
        S.string_field_2,
        S.date_field_3,
        S.string_field_4,
        REGEXP_EXTRACT(S.string_field_4, r'(\d{{10}}-\d{{2}}-\d{{6}})')
    )
    """
    if debug:
        print(merge_sql)
    job = bq_client.query(merge_sql)
    job.result()
    print(f"rows merged:{job.num_dml_affected_rows}")

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
