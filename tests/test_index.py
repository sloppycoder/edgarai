from edgar import load_master_idx
from tests.helper import prep_big_query


def test_load_master_idx():
    prep_big_query()

    # first time run should get all the rows into the index table
    assert load_master_idx(2020, 1) == 327705
    # run it again should not load any rows
    assert load_master_idx(2020, 1) == 0
