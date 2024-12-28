import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from google.cloud import bigquery

from gcp_helper import blob_as_text, ensure_table_exists

from .util import (
    chunk_text,
    download_file,
    idx_filename2accession_number,
    idx_filename2index_headers,
    trim_html_content,
)

logger = logging.getLogger(__name__)

dataset_id = os.environ.get("BQ_DATASET_ID", "edgar")

filing_text_chunks_schema = [
    bigquery.SchemaField("cik", "STRING", max_length=20, mode="REQUIRED"),
    bigquery.SchemaField("date_filed", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("accession_number", "STRING", max_length=20, mode="REQUIRED"),
    bigquery.SchemaField("chunk_num", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("content", "STRING", mode="REQUIRED"),
]

# Document tag contents usually looks like this,
# FILENAME and DESCRIPTION are optional
#
# <DOCUMENT>
# <TYPE>stuff
# <SEQUENCE>stuff
# <FILENAME>stuff
# <TEXT>
# Document 1 - file: stuff
#
# </DOCUMENT>
# the regex below tries to parse
# doc element in index-headers.html
doc_regex = re.compile(
    r"""<DOCUMENT>\s*
<TYPE>(?P<type>.*?)\s*
<SEQUENCE>(?P<sequence>.*?)\s*
<FILENAME>(?P<filename>.*?)\s*
(?:<DESCRIPTION>(?P<description>.*?)\s*)?
<TEXT>
(?P<text>.*?)
</TEXT>""",
    re.DOTALL | re.VERBOSE | re.IGNORECASE,
)


# in SEC_HEADER
# FILED AS OF DATE:		20241017
date_filed_regex = re.compile(r"FILED AS OF DATE:\s*(\d{8})", re.IGNORECASE)


class FilingExceptin(Exception):
    pass


class SECFiling:
    def __init__(self, cik: str, idx_filename: str):
        # sometimes a same filename is used by several CIKs
        # filename as in master.idx
        # e.g. edgar/data/106830/0001683863-20-000050.txt
        self.cik = cik
        self.idx_filename = idx_filename
        self.accession_number = idx_filename2accession_number(idx_filename)

        # idx filename for the filing index-headers file
        self.index_headers_filename = idx_filename2index_headers(idx_filename)

        (self.sec_header, self.date_filed, self.documents) = _read_index_headers(
            self.index_headers_filename
        )
        logger.debug(f"initialized SECFiling({self.cik},{self.idx_filename})")

    def get_doc_by_type(self, doc_type: str) -> list[str]:
        """
        Reads the contents of documents of a specific type from the filing.

        Args:
            doc_type (str): The type of document to read (e.g., "485BPOS").

        Returns:
            list[str]: A list of filenames that matches the doc_type

        Raises:
            FilingExceptin: If the specified document type is not found in the filing
            or if the document path cannot be determined.
        """
        # Get the paths of documents of the specified type
        paths = [doc["filename"] for doc in self.documents if doc["type"] == doc_type]
        if paths is None or paths == []:
            raise FilingExceptin(
                f"{self.idx_filename} does not contain a {doc_type} document"
            )

        if len(paths) > 1:
            raise FilingExceptin(
                f"{self.idx_filename} has more than 1 document of type {doc_type}"
            )

        return [str(Path(self.index_headers_filename).parent / path) for path in paths]

    def save_chunked_texts(self, doc_type: str) -> int:
        docs = self.get_doc_by_type(doc_type)
        if len(docs) > 1:
            raise FilingExceptin(
                f"{self.idx_filename} has more than 1 document of type {doc_type}"
            )

        doc_path = download_file(docs[0])
        if not doc_path:
            raise FilingExceptin(f"Failed to download {self.idx_filename}")

        chunks = chunk_text(trim_html_content(doc_path))

        # Prepare rows to insert into the temp table
        rows_to_insert = [
            {
                "cik": self.cik,
                "date_filed": self.date_filed,
                "accession_number": self.accession_number,
                "content": content,
                "chunk_num": chunk_num,
            }
            for chunk_num, content in enumerate(chunks)
        ]

        with bigquery.Client() as bq_client:
            # Define table references
            timestamp = datetime.now().strftime("%H%M%S")
            output_table_ref = f"{bq_client.project}.{dataset_id}.filing_text_chunks"
            temp_table_ref = (
                f"{bq_client.project}.{dataset_id}.chunks_{self.cik}_{timestamp}"
            )

            # Ensure the main and temp tables exist
            ensure_table_exists(
                bq_client, output_table_ref, schema=filing_text_chunks_schema
            )
            ensure_table_exists(
                bq_client, temp_table_ref, schema=filing_text_chunks_schema
            )

            # Insert rows into the temp table
            errors = bq_client.insert_rows_json(temp_table_ref, rows_to_insert)
            if errors:
                raise FilingExceptin(
                    f"Failed to insert rows into temp table: {errors[:10]}"
                )

            # Merge temp table into the main table
            merge_query = f"""
            MERGE INTO {output_table_ref} AS main
            USING {temp_table_ref} AS temp
            ON main.cik = temp.cik
            AND main.accession_number = temp.accession_number
            AND main.chunk_num = temp.chunk_num
            WHEN MATCHED THEN UPDATE SET
                main.date_filed = temp.date_filed,
                main.content = temp.content
            WHEN NOT MATCHED THEN INSERT (
                cik, date_filed, accession_number,
                chunk_num, content
            ) VALUES (
                temp.cik, temp.date_filed, temp.accession_number,
                temp.chunk_num, temp.content
            )
            """
            job = bq_client.query(merge_query)
            job.result()

            # Clean up the temporary table
            bq_client.delete_table(temp_table_ref, not_found_ok=True)

            n_count = len(rows_to_insert)
            logging.info(f"Inserted and merged {n_count} rows into {output_table_ref}")
            return n_count


def chunk_filing(cik: str, idx_filename: str, form_type: str) -> int:
    filing = SECFiling(cik, idx_filename)
    html_filename = filing.get_doc_by_type(form_type)[0]
    if not html_filename:
        return 0

    n_chunks = filing.save_chunked_texts("485BPOS")
    return n_chunks


def _read_index_headers(index_headers_filename) -> tuple[str, str, list[dict[str, Any]]]:
    """read the index-headers.html file and extract the sec_header and documents"""

    index_headers_path = download_file(index_headers_filename)
    if index_headers_path is None:
        raise FilingExceptin(f"Index headers not found for {index_headers_filename}")

    content = blob_as_text(index_headers_path)
    if not content:
        raise FilingExceptin(f"Unable to read {index_headers_path}")

    soup = BeautifulSoup(content, "html.parser")
    # each index-headers.html file contains a single <pre> tag
    # inside there are SGML content of meta data for the filing
    pre = soup.find("pre")
    if pre is None:
        logger.debug(f"No <pre> tag found in {index_headers_filename}")
        return "", "", []

    pre_soup = BeautifulSoup(pre.get_text(), "html.parser")

    sec_header = pre_soup.find("sec-header")
    sec_header_text = ""
    date_filed = ""
    if sec_header:
        sec_header_text = str(sec_header)
        match = date_filed_regex.search(sec_header_text)
        if match:
            digits = match.group(1)
            date_filed = f"{digits[:4]}-{digits[4:6]}-{digits[6:]}"

        documents = []
        for doc in pre_soup.find_all("document"):
            match = doc_regex.search(str(doc))

            if match:
                doc_info = {
                    "type": match.group("type"),
                    "sequence": match.group("sequence"),
                    "filename": match.group("filename"),
                }
                documents.append(doc_info)

        return sec_header_text, date_filed, documents

    logger.info(f"No sec-header found in {index_headers_filename}")

    return "", "", []
