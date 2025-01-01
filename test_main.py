import config
from main import handle_calls


def test_load_maseter_idx():
    calls = [
        ["load_master_idx", config.dataset_id, "2020", "1"],
    ]
    replies = handle_calls(calls)
    assert replies[0].startswith("SUCCESS")


def test_chunk_filings():
    calls = [
        [
            "chunk_one_filing",
            config.dataset_id,
            "1518042|0001580642-24-002155",
        ]
    ]

    replies = handle_calls(calls)
    assert replies[0].startswith("SUCCESS")
