import logging
import os

from google.cloud import bigquery, storage

from .index import create_idx_table, load_idx_to_bigquery
from .util import download_edgar_file

logger = logging.getLogger(__name__)

bucket_name = os.environ.get("GCS_BUCKET_NAME", "")
cache_dir = os.environ.get("EDGAR_CACHE_DIR", "cache")
dataset_id = os.environ.get("BQ_DATASET_ID", "edgar")
table_id = os.environ.get("BQ_IDX_TABLE_ID", "master_idx")

EDGAR_BASE_URL = "https://www.sec.gov/Archives"


def download_file(filename: str, refresh: bool = False) -> str | None:
    """
    Download a file to Cloud Storage if it doesn't exist or if refresh is True.

    Args:
        filename (str): The name of the file to download.
        refresh (bool): If True, download the file even if it exists in the bucket.

    Returns:
        str | None: The path to the file in the Cloud Storage bucket, or None
        if the download fails.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    cache_filename = f"{cache_dir}/{filename}"
    blob = bucket.blob(cache_filename)

    if blob.exists():
        if not refresh:
            logger.debug(f"Returning file exists in cache {cache_filename}")
            return f"gs://{bucket_name}/{cache_filename}"
        else:
            logger.debug(f"Deleting file {cache_filename}")
            blob.delete()

    content = download_edgar_file(filename)
    if content is None:
        return None

    new_blob = bucket.blob(cache_filename)
    new_blob.upload_from_string(content)
    logger.debug(f"Downloaded {filename} and saved to {cache_filename}.")

    return f"gs://{bucket_name}/{cache_filename}"


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

    bq_client = bigquery.Client()
    create_idx_table(bq_client, dataset_id, table_id)
    rows = load_idx_to_bigquery(bq_client, idx_uri, dataset_id, table_id)
    return rows
