from edgar.extractor import chunk_filing, find_most_relevant_chunks
from tests.helper import drop_table_if_exists


def test_chunk_filing():
    drop_table_if_exists("filing_text_chunks")

    assert (
        chunk_filing("1002427", "edgar/data/1002427/0001133228-24-004879.txt", "485BPOS")
        == 271
    )
    # run this twice to make sure we don't get errors like
    # UPDATE or DELETE statement over table some_table
    # would affect rows in the streaming buffer, which is not supported
    assert (
        chunk_filing("1002427", "edgar/data/1002427/0001133228-24-004879.txt", "485BPOS")
        == 271
    )


def test_get_relevant_chunks():
    chunks = find_most_relevant_chunks(
        cik="701804",
        access_number="0000930413-24-001430",
    )
    # this test depends on data in the database
    # as long as no excepton is thrown the logic should be correct
    assert chunks is not None
