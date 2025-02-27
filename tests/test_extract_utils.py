from src.utils.extract_utils import format_filepath


def test_filepath_as_expected():
    test_date_time = "2000-01-01 12:00:00.000000"
    test_table = "table"
    # check num dps
    expected = "data/by time/2000/01-January/01/12:00:00.000000/table"
    assert format_filepath(test_date_time, test_table) == expected


# def test_get_data_returns_expected():
#     expected = [{'staff_id': 1, 'first_name' : 'A',
#  'last_name': 'Name', 'department_id': 1,
#                  'email_address': 'test@gmail.com',
# 'created_at': "2025", 'last_updated': '2025'},
#                  {'staff_id': 2, 'first_name' : 'B',
#  'last_name': 'Nam', 'department_id': 3,
#                  'email_address': 'test1@gmail.com',
# 'created_at': "2021", 'last_updated': '2028'}]
