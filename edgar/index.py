import logging

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

import config
from gcp_helper import ensure_table_exists, short_uuid

from .util import download_file

logger = logging.getLogger(__name__)

master_idx_schema = [
    bigquery.SchemaField("cik", "STRING", max_length=10, mode="REQUIRED"),
    bigquery.SchemaField("company_name", "STRING", max_length=150, mode="REQUIRED"),
    bigquery.SchemaField("form_type", "STRING", max_length=20, mode="REQUIRED"),
    bigquery.SchemaField("date_filed", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("filename", "STRING", max_length=100, mode="REQUIRED"),
    bigquery.SchemaField(
        "accession_number",
        "STRING",
        max_length=20,
        mode="NULLABLE",
    ),
]


def load_master_idx(year: int, quarter: int, refresh=False) -> int | None:
    """
    Load the master index file for a given year and quarter into BigQuery

    Args:
        year (int): The year of the master index file.
        quarter (int): The quarter of the master index file (1 to 4).
        refresh (bool): If True, download the file even if it exists in the bucket.

    Returns:
        int | None: The size of the downloaded file in bytes, or None if the
        download fails or the year/quarter is invalid.
    """
    if year < 2000 or year > 2026 or quarter < 1 or quarter > 4:
        logger.info(f"Invalid year/quarter {year}/{quarter}")
        return

    idx_filename = f"edgar/full-index/{year}/QTR{quarter}/master.idx"
    idx_uri = download_file(idx_filename, refresh=refresh)
    if idx_uri is None:
        return

    table_id = "master_idx"
    with bigquery.Client() as bq_client:
        ensure_table_exists(
            bq_client,
            f"{bq_client.project}.{config.dataset_id}.master_idx",
            schema=master_idx_schema,
        )
        rows = load_idx_to_bigquery(bq_client, idx_uri, config.dataset_id, table_id)
        return rows


def load_idx_to_bigquery(
    bq_client: bigquery.Client,
    uri: str,
    dataset_id: str,
    table_id: str,
) -> int | None:
    """
    Load an idx file into BigQuery.

    the master.idx from EDGAR are basically a CSV file with 9 header lines
    """
    temp_table_ref = f"{bq_client.project}.{dataset_id}.tmp_index_{short_uuid()}"
    main_table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=12,
        field_delimiter="|",
        autodetect=True,
    )

    logger.info(f"Loading data from {uri} to {temp_table_ref}...")

    job = bq_client.load_table_from_uri(uri, temp_table_ref, job_config=job_config)
    job.result()

    try:
        merge_query = rf"""
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
        return _run_merge_query(bq_client, merge_query)

    finally:
        bq_client.delete_table(temp_table_ref)
        logger.debug(f"Temporary table {temp_table_ref} deleted.")


@retry(
    retry=retry_if_exception_type(BadRequest),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def _run_merge_query(bq_client, merge_query):
    logger.debug(merge_query)
    job = bq_client.query(merge_query)
    job.result()
    rows_affected = job.num_dml_affected_rows
    logger.info(f"load_idx_to_bigquery: rows merged:{rows_affected}")
    return rows_affected
