from edgar.filing import SECFiling
from edgar.util import (
    DEFAULT_TEXT_CHUNK_SIZE,
    chunk_text,
    download_file,
    idx_filename2accession_number,
    idx_filename2index_headers,
    trim_html_content,
)

# to be tested with chunk size 400
test_text_content = """
Share Ownership

The following table provides the dollar range of Shares of the Fund beneficially owned by
the Trustees as of December 31, 2022.


| Trustee |  Aggregate Dollar Range of  Equity Securities
of the Fund |  Aggregate Dollar Range of  Equity Securities in all Funds  Overseen
Within Fund Complex |
---|---|---|
H. Bruce Bond |  None |  Over $100,000 |
Mark Berg |  None |  Over $100,000 |
Joe Stowell |  None |  Over $100,000 |
Brian J. Wildman |  None |  Over $100,000 |

As of December 31, 2022, the Independent Trustees and immediate family members did not own
beneficially or of record any class of securities of an investment adviser or principal
underwriter of the Fund or any person directly or indirectly controlling, controlled by,
or under common control with an investment adviser or principal underwriter of the Fund.
Mr. Berg does, however, have a passive limited partnership equity interest, which is

this is something else to do.


"""


def test_parse_485bpos_filing():
    filing = SECFiling("edgar/data/1002427/0001133228-24-004879.txt")
    html_filename = filing.get_doc_by_type("485BPOS")[0]
    doc_path = download_file(html_filename)
    assert doc_path

    content = trim_html_content(doc_path)

    assert filing.cik == "1002427" and filing.date_filed == "2024-04-29"
    assert filing.accession_number == "0001133228-24-004879"
    assert len(filing.documents) == 26
    assert (
        html_filename == "edgar/data/1002427/000113322824004879/msif-html7854_485bpos.htm"
    )
    assert "hidden" not in content
    assert "FORM N-1A" in content


def test_parse_and_split_chunks():
    filing = SECFiling("edgar/data/1002427/0001133228-24-004879.txt")
    html_filename = filing.get_doc_by_type("485BPOS")[0]
    doc_path = download_file(html_filename)

    assert doc_path
    assert filing.cik == "1002427" and filing.date_filed == "2024-04-29"
    assert filing.accession_number == "0001133228-24-004879"

    assert filing.save_chunked_texts("485BPOS") > 0


def test_chunk_text():
    test_chunk_size = 400
    chunks = chunk_text(test_text_content, test_chunk_size)
    assert all(len(c) < DEFAULT_TEXT_CHUNK_SIZE for c in chunks)
    assert len(chunks) == 4
    assert chunks[0].endswith("December 31, 2022.")
    assert chunks[1].endswith("Over $100,000 |")
    assert chunks[2].endswith("underwriter of the Fund.\n")
    assert chunks[3].endswith("else to do.\n\n\n")


def test_chunk_large_filing():
    # this filing is very large, has 142 chunks
    filing = SECFiling("edgar/data/1314414/0001580642-20-000131.txt")
    html_filename = filing.get_doc_by_type("485BPOS")[0]
    doc_path = download_file(html_filename)
    assert doc_path

    chunks = chunk_text(trim_html_content(doc_path))

    assert len(chunks) == 133
    assert all(len(c) <= DEFAULT_TEXT_CHUNK_SIZE + 90 for c in chunks)

    # assert filing.save_chunked_texts("485BPOS") > 0


def test_chunk_long_line_filing():
    # this filing has only 2 lines, one of them is 256k long (why?)
    filing = SECFiling("edgar/data/1658158/0001528621-20-000176.txt")
    html_filename = filing.get_doc_by_type("485BPOS")[0]
    doc_path = download_file(html_filename)
    assert doc_path

    chunks = chunk_text(trim_html_content(doc_path))

    assert max([len(c) for c in chunks]) <= DEFAULT_TEXT_CHUNK_SIZE + 80


def test_idx_filename2index_headers():
    assert (
        idx_filename2index_headers("edgar/data/1035018/0001193125-20-000327.txt")
        == "edgar/data/1035018/000119312520000327/0001193125-20-000327-index-headers.html"
    )


def test_filename2accession_number():
    assert "0001193125-20-000327" == idx_filename2accession_number(
        "edgar/data/1035018/0001193125-20-000327.txt"
    )
    assert "0001193125-20-000327" == idx_filename2accession_number(
        "edgar/data/1035018/000119312520000327/somestuff_485bpos.htm"
    )
