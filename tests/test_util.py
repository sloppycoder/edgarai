from edgar.util import idx_filename2accession_number, idx_filename2index_headers


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
