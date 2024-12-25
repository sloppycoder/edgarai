from edgar import load_master_idx


def test_load_index():
    # first time run should get all the rows into the index table
    assert load_master_idx(2020, 1) == 327705
    # run it again should not load any rows
    assert load_master_idx(2020, 1) == 0
