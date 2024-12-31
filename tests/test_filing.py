from edgar.filing import SECFiling
from edgar.util import (
    DEFAULT_TEXT_CHUNK_SIZE,
    chunk_text,
    download_file,
    trim_html_content,
)


def test_parse_485bpos_filing():
    filing = SECFiling("1002427", "edgar/data/1002427/0001133228-24-004879.txt")
    html_filename = filing.get_doc_by_type("485BPOS")[0]
    doc_path = download_file(html_filename)
    assert doc_path

    content = trim_html_content(doc_path)

    assert filing.date_filed == "2024-04-29"
    assert filing.accession_number == "0001133228-24-004879"
    assert len(filing.documents) == 26
    assert (
        html_filename == "edgar/data/1002427/000113322824004879/msif-html7854_485bpos.htm"
    )
    assert "hidden" not in content
    assert "FORM N-1A" in content


def test_chunk_large_filing():
    # this filing is very large, has 142 chunks
    filing = SECFiling("1314414", "edgar/data/1314414/0001580642-20-000131.txt")
    html_filename = filing.get_doc_by_type("485BPOS")[0]
    doc_path = download_file(html_filename)
    assert doc_path

    chunks = chunk_text(trim_html_content(doc_path))

    assert len(chunks) == 133
    assert all(len(c) <= DEFAULT_TEXT_CHUNK_SIZE + 90 for c in chunks)


def test_chunk_long_line_filing():
    # this filing has only 2 lines, one of them is 256k long (why?)
    filing = SECFiling("1658158", "edgar/data/1658158/0001528621-20-000176.txt")
    html_filename = filing.get_doc_by_type("485BPOS")[0]
    doc_path = download_file(html_filename)
    assert doc_path

    chunks = chunk_text(trim_html_content(doc_path))

    assert max([len(c) for c in chunks]) <= DEFAULT_TEXT_CHUNK_SIZE + 80
