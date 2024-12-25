import logging
from datetime import datetime

from google.cloud import bigquery


def create_idx_table(bq_client: bigquery.Client, dataset_id: str, table_id: str) -> bool:
    """Create a BigQuery table if it does not exist."""
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"
    try:
        bq_client.get_table(table_ref)
        logging.debug(f"Table {table_ref} already exists.")
        return False
    except Exception:
        logging.debug(f"Table {table_ref} does not exist. Creating table...")
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
        logging.info(f"Table {table_ref} created.")
        return True


def load_idx_to_bigquery(
    bq_client: bigquery.Client,
    uri: str,
    dataset_id: str,
    table_id: str,
    keep_temp_table: bool = False,
) -> int | None:
    """
    Load an idx file into BigQuery.

    the master.idx from EDGAR are basically a CSV file with 9 header lines
    """
    timestamp = datetime.now().strftime("%H%M%S")
    temp_table_ref = f"{bq_client.project}.{dataset_id}.{table_id}_{timestamp}"
    main_table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=12,
        field_delimiter="|",
        autodetect=True,
    )

    logging.info(f"Loading data from {uri} to {temp_table_ref}...")

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
    logging.debug(merge_sql)
    job = bq_client.query(merge_sql)
    job.result()
    rows_affected = job.num_dml_affected_rows
    logging.info(f"rows merged:{rows_affected}")

    if not keep_temp_table:
        bq_client.delete_table(temp_table_ref)
        logging.debug(f"Temporary table {temp_table_ref} deleted.")

    return rows_affected
