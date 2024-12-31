from edgar import load_master_idx
from tests.helper import drop_table_if_exists


def test_load_master_idx():
    drop_table_if_exists("master_idx")

    # first time run should get all the rows into the index table
    assert load_master_idx(2020, 1) == 327705
    # run it again should not load any rows
    assert load_master_idx(2020, 1) == 0
