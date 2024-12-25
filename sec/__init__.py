import logging
from pathlib import Path

from .util import (
    idx_filename2accession_number,
    idx_filename2index_headers,
    read_index_headers,
)


class FilingExceptin(Exception):
    pass


class SECFiling:
    def __init__(self, idx_filename: str):
        # filename as in master.idx
        # e.g. edgar/data/106830/0001683863-20-000050.txt
        self.idx_filename = idx_filename
        self.cik = idx_filename.split("/")[2]
        self.accession_number = idx_filename2accession_number(idx_filename)

        # idx filename for the filing index-headers file
        self.index_headers_filename = idx_filename2index_headers(idx_filename)

        (self.sec_header, self.date_filed, self.documents) = read_index_headers(
            self.index_headers_filename
        )
        logging.debug(f"initialized SECFiling({self.idx_filename})")

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
