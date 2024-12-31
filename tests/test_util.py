from edgar.util import (
    DEFAULT_TEXT_CHUNK_SIZE,
    chunk_text,
    idx_filename2accession_number,
    idx_filename2index_headers,
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


def test_chunk_text():
    test_chunk_size = 400
    chunks = chunk_text(test_text_content, test_chunk_size)
    assert all(len(c) < DEFAULT_TEXT_CHUNK_SIZE for c in chunks)
    assert len(chunks) == 4
    assert chunks[0].endswith("December 31, 2022.")
    assert chunks[1].endswith("Over $100,000 |")
    assert chunks[2].endswith("underwriter of the Fund.\n")
    assert chunks[3].endswith("else to do.\n\n\n")


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
