from edgar.extract import find_most_relevant_chunks


def test_get_relevant_chunks():
    chunks = find_most_relevant_chunks(
        cik="1518042",
        access_number="0001580642-24-002155",
        dimensionality=256,
    )
    assert chunks == [127, 128]

    chunks = find_most_relevant_chunks(
        cik="1518042",
        access_number="0001580642-24-002155",
        dimensionality=768,
    )
    assert chunks == [66]
