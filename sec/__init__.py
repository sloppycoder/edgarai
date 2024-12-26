import logging
from pathlib import Path

import spacy

from .util import (
    _trim_html_content,
    idx_filename2accession_number,
    idx_filename2index_headers,
    read_index_headers,
)

DEFAULT_TEXT_CHUNK_SIZE = 3500


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


def chunk_text(content: str, chunk_size: int = DEFAULT_TEXT_CHUNK_SIZE) -> list[str]:
    """
    Split a text into chunks of size chunk_size

    Args:
        text (str): The text to split into chunks
        chunk_size (int): The size of each chunk

    Returns:
        list[str]: A list of text chunks
    """
    # Load SpaCy NLP model
    nlp = spacy.load("en_core_web_sm")

    # Split content into paragraphs (based on double newline)
    paragraphs = content.split("\n\n")

    chunks = []
    current_chunk = []
    current_size = 0

    for paragraph in paragraphs:
        # Check if the paragraph contains a Markdown table
        if paragraph.strip().startswith("|") and paragraph.strip().endswith("|"):
            # Treat Markdown tables as single units
            paragraph_size = len(paragraph)
            if current_size + paragraph_size > chunk_size:
                # Save current chunk and start a new one
                chunks.append("\n\n".join(current_chunk))
                current_chunk = [paragraph]
                current_size = paragraph_size
            else:
                current_chunk.append(paragraph)
                current_size += paragraph_size
        else:
            # Process paragraph using SpaCy for sentence tokenization
            doc = nlp(paragraph)
            sentences = [sent.text for sent in doc.sents]

            for sentence in sentences:
                sentence_size = len(sentence)
                if current_size + sentence_size > chunk_size:
                    # Save current chunk and start a new one
                    chunks.append("\n\n".join(current_chunk))
                    current_chunk = [sentence]
                    current_size = sentence_size
                else:
                    current_chunk.append(sentence)
                    current_size += sentence_size

    # Add any remaining content
    if current_chunk:
        chunks.append("\n\n".join(current_chunk))

    return chunks


trim_html_content = _trim_html_content
