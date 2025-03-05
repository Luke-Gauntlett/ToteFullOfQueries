from src.utils import format_filepath


def test_filepath_as_expected():
    test_date_time = "2000-01-01 12:00:00.000000"
    test_table = "table"
    # check num dps
    expected = "data/by time/2000/01-January/01/12:00:00.000000/table"
    assert format_filepath(test_date_time, test_table) == expected
