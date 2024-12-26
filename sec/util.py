import logging
import re
from pathlib import Path
from typing import Any

import html2text
from bs4 import BeautifulSoup
from google.cloud import storage

from edgar import download_file

logger = logging.getLogger(__name__)

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


def default_text_converter():
    converter = html2text.HTML2Text()
    converter.ignore_links = False
    converter.ignore_images = True
    converter.ignore_emphasis = True
    converter.body_width = 0
    return converter


def _trim_html_content(filing_html_path: str) -> str:
    """
    remove the hidden div and convert the rest of html into text
    """
    content = blob_as_text(filing_html_path)

    soup = BeautifulSoup(content, "html.parser")

    style_lambda = lambda value: value and "display:none" in value.replace(" ", "")  # noqa
    div_to_remove = soup.find("div", style=style_lambda)

    if div_to_remove:
        div_to_remove.decompose()  # type: ignore

    return default_text_converter().handle(str(soup))


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


def blob_as_text(blob_uri: str) -> str:
    """read the content of a blob as text"""
    bucket_name, blob_path = re.match(r"gs://([^/]+)/(.+)", blob_uri).groups()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text(encoding="utf-8")
    return content if content else ""


def read_index_headers(index_headers_filename) -> tuple[str, str, list[dict[str, Any]]]:
    """read the index-headers.html file and extract the sec_header and documents"""

    from . import FilingExceptin

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
