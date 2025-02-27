import pytest
import boto3
import pg8000
from unittest.mock import Mock, MagicMock
from moto import mock_aws
import os
import datetime

from src.extract_lambda import write_data, get_time

# that s3 has all files written to it
# files in correct file structure
# function db connects
# correct headings
# empty data
# errors handled
# cloudwatch...

@pytest.fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

# @pytest.fixture(scope="function", autouse=True)
# def s3_client_mock(aws_credentials):
#     with mock_aws():
#         s3 = boto3.client("s3")
#         s3.create_bucket(Bucket="testbucket-project-extract", CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
#         yield s3

# @pytest.fixture(scope="function", autouse=True)
# def current_time():
#     return "2024-02-27 10:00:00", "2024-02-27 12:00:00"

# @pytest.fixture(scope="function", autouse=True)
# def database_staff_data_sample():
#     return [10,
#   'Ozzie',
#   'B',
#   7,
#   'oz.b@terrifictotes.com',
#   datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
#   datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]

# @pytest.fixture(scope="function", autouse=True)
# def database_staff_columns_sample():
#     return ['staff_id', 'first_name', 'last_name',
#             'department_id', 'email_address',
#             'created_at', 'last_updated']


# # @pytest.fixture
# # def mock_db():
# #     db_mock = MagicMock()
# #     # Mock column retrieval
# #     db_mock.run.side_effect = lambda query, table_name=None: [
# #         [{"column_name": "staff_id"}, {"column_name": "first_name"}, {"column_name": "last_name"},
# #          {"column_name": "department_id"}, {"column_name": "email_address"}, {"column_name": "created_at"},
# #          {"column_name": "last_updated"}] if "information_schema.columns" in query else
# #         [[8, "Oswaldo", "Bergstrom", 7, "oswaldo.bergstrom@terrifictotes.com",
# #           datetime(2022, 11, 3, 14, 20, 51, 563000), datetime(2022, 11, 3, 14, 20, 51, 563000)]]
# #     return db_mock

# def test_staff_data_returned_in_expected_format(database_staff_data_sample,
#                                                 s3_client_mock, current_time):
#     last_extraction_time, this_extraction_time = current_time
    
#     mock_db = Mock()
#     # mock_db.run(side_effect = [['columns1', 'columns2'], 'datas'])
#     mock_db.run.return_value = ['columsn1', 'columns2']

#     response = write_data(last_extraction_time,this_extraction_time, s3_client_mock, mock_db)

#     column_headers = ['staff_id', 'first_name', 'last_name',
#             'department_id', 'email_address',
#             'created_at', 'last_updated']
    
    
#     assert response['staff'] == 0


def test_test():
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="testbucket-project-extract", CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        response = write_data('time', 'time', 'client')
        
        assert 1 == 0
