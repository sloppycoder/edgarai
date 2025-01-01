import logging
import os
from pathlib import Path

import html2text
import requests
import spacy
from bs4 import BeautifulSoup
from google.cloud import storage

import config
from gcp_helper import blob_as_text

DEFAULT_TEXT_CHUNK_SIZE = 3500

logger = logging.getLogger(__name__)

EDGAR_BASE_URL = "https://www.sec.gov/Archives"
user_agent = os.environ.get("USER_AGENT", "Lee Lynn (hayashi@yahoo.com)")


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
    try:
        bucket = storage_client.bucket(config.bucket_id)
        cache_filename = f"{config.cache_dir}/{filename}"
        blob = bucket.blob(cache_filename)

        if blob.exists():
            if not refresh:
                logger.debug(f"Returning file exists in cache {cache_filename}")
                return f"gs://{config.bucket_id}/{cache_filename}"
            else:
                logger.debug(f"Deleting file {cache_filename}")
                blob.delete()

        content = _download_edgar_file(filename)
        if content is None:
            return None

        new_blob = bucket.blob(cache_filename)
        new_blob.upload_from_string(content)
        logger.debug(f"Downloaded {filename} and saved to {cache_filename}.")

        return f"gs://{config.bucket_id}/{cache_filename}"
    finally:
        storage_client.close()


def _download_edgar_file(filename: str) -> bytes | None:
    url = f"{EDGAR_BASE_URL}/{filename}"
    response = requests.get(url, headers={"User-Agent": user_agent})

    if response.status_code != 200:
        logger.info(f"Failed to download file from {url}, {response.content}")
        return None

    return response.content


def idx_filename2accession_number(idx_filename: str) -> str:
    accession_number = ""
    if idx_filename.endswith(".txt"):
        accession_number = idx_filename.split("/")[-1].split(".")[0]
    elif idx_filename.endswith(".htm"):
        part = idx_filename.split("/")[-2]
        accession_number = f"{part[:10]}-{part[10:12]}-{part[12:]}"

    return accession_number


def idx_filename2index_headers(idx_filename: str) -> str:
    """
    convert a filename from master.idx to -index-headers.html
    """
    filepath = Path(idx_filename)
    basename = filepath.name.replace(".txt", "")
    return str(
        filepath.parent / basename.replace("-", "") / f"{basename}-index-headers.html"
    )


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
    # either way works
    # import en_core_web_sm
    # nlp = en_core_web_sm.load()
    nlp = spacy.load("en_core_web_sm")
    logger.debug("chunk_text: loaded model")

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


def trim_html_content(filing_html_path: str) -> str:
    """
    remove the hidden div and convert the rest of html into text
    """
    content = blob_as_text(filing_html_path)

    soup = BeautifulSoup(content, "html.parser")

    style_lambda = lambda value: value and "display:none" in value.replace(" ", "")  # noqa
    div_to_remove = soup.find("div", style=style_lambda)

    if div_to_remove:
        div_to_remove.decompose()  # type: ignore

    return _default_text_converter().handle(str(soup))


def _default_text_converter():
    converter = html2text.HTML2Text()
    converter.ignore_links = False
    converter.ignore_images = True
    converter.ignore_emphasis = True
    converter.body_width = 0
    return converter
