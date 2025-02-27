import json
import pytest
from unittest.mock import Mock
from moto import mock_aws
import boto3
from datetime import datetime
from src.extract_lambda import write_data


@pytest.fixture
def mock_client():
    """Mocks an S3 client with a test bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="testbucket123abc456def",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


@pytest.fixture
def mock_db():
    """Mocks a database connection with expected responses."""
    db_mock = Mock()

    def db_run(query, table_name=None, last_extract_time=None):
        if "information_schema.columns" in query:
            return [
                ("staff_id",),
                ("first_name",),
                ("last_name",),
                ("department_id",),
                ("email_address",),
                ("created_at",),
                ("last_updated",),
            ]
        elif "SELECT * FROM" in query:
            return [
                [
                    8,
                    "Ozzy",
                    "Osbourne",
                    7,
                    "ozzy.osbourne@terrifictotes.com",
                    datetime(2022, 11, 3, 14, 20, 51, 563000),
                    datetime(2022, 11, 3, 14, 20, 51, 563000),
                ],
                [
                    9,
                    "Lebron",
                    "James",
                    2,
                    "lebron.james@terrifictotes.com",
                    datetime(2022, 11, 3, 14, 20, 51, 563000),
                    datetime(2022, 11, 3, 14, 20, 51, 563000),
                ],
            ]
        return []

    db_mock.run = Mock(side_effect=db_run)
    return db_mock


def test_write_data_correctly_writes_to_s3(mock_client, mock_db):
    """Test that data is correctly written to S3."""
    last_extract_time = "2022-11-01 00:00:00"
    this_extract_time = "2022-11-03 14:20:51"

    write_data(last_extract_time, this_extract_time, mock_client, mock_db)

    response = mock_client.get_object(
        Bucket="testbucket123abc456def",
        Key="data/by time/2022/11-November/03/14:20:51/staff",
    )

    file_content = json.loads(response["Body"].read().decode("utf-8"))

    expected_data = [
        {
            "staff_id": 8,
            "first_name": "Ozzy",
            "last_name": "Osbourne",
            "department_id": 7,
            "email_address": "ozzy.osbourne@terrifictotes.com",
            "created_at": "2022-11-03 14:20:51.563000",
            "last_updated": "2022-11-03 14:20:51.563000",
        },
        {
            "staff_id": 9,
            "first_name": "Lebron",
            "last_name": "James",
            "department_id": 2,
            "email_address": "lebron.james@terrifictotes.com",
            "created_at": "2022-11-03 14:20:51.563000",
            "last_updated": "2022-11-03 14:20:51.563000",
        },
    ]

    assert file_content == expected_data


def test_write_data_creates_correct_file_path(mock_client, mock_db):
    """Test that the correct file paths are created in S3."""
    last_extraction_time = "2022-11-01 00:00:00"
    this_extraction_time = "2022-11-03 14:20:51"

    write_data(last_extraction_time, this_extraction_time, mock_client, mock_db)  # noqa

    objects = mock_client.list_objects(Bucket="testbucket123abc456def")["Contents"]  # noqa
    s3_keys = {obj["Key"] for obj in objects}

    expected_keys = {
        "data/by time/2022/11-November/03/14:20:51/staff",
        "data/by table/staff/staff - 2022-11-03-14:20:51",
    }

    assert expected_keys.issubset(s3_keys)


def test_write_data_handles_empty_data(mock_client, mock_db):
    """Test that write_data handles empty data properly."""
    mock_db.run.side_effect = lambda query, table_name=None, last_extract_time=None: (  # noqa
        [("staff_id",), ("first_name",), ("last_name",)]
        if "information_schema.columns" in query
        else []
    )

    last_extraction_time = "2022-11-01 00:00:00"
    this_extraction_time = "2022-11-03 14:20:51"

    write_data(last_extraction_time, this_extraction_time, mock_client, mock_db)  # noqa

    response = mock_client.list_objects(Bucket="testbucket123abc456def")

    assert "Contents" not in response or len(response["Contents"]) == 0


def test_write_data_includes_correct_columns(mock_client, mock_db):
    """Test that write_data includes all expected columns."""
    last_extraction_time = "2022-11-01 00:00:00"
    this_extraction_time = "2022-11-03 14:20:51"

    write_data(last_extraction_time, this_extraction_time, mock_client, mock_db)  # noqa

    response = mock_client.get_object(
        Bucket="testbucket123abc456def",
        Key="data/by time/2022/11-November/03/14:20:51/staff",
    )
    file_content = json.loads(response["Body"].read().decode("utf-8"))

    expected_keys = {
        "staff_id",
        "first_name",
        "last_name",
        "department_id",
        "email_address",
        "created_at",
        "last_updated",
    }

    for row in file_content:
        assert set(row.keys()) == expected_keys


def test_write_data_handles_missing_timestamps(mock_client, mock_db):
    """Test that missing timestamps are handled correctly."""
    mock_db.run.side_effect = lambda query, table_name=None, last_extract_time=None: ( # noqa
        [
            ("staff_id",),
            ("first_name",),
            ("last_name",),
            ("created_at",),
            ("last_updated",),
        ]
        if "information_schema.columns" in query
        else [[8, "Oswaldo", "Bergstrom", None, None]]
    )   # noqa

    last_extract_time = "2022-11-01 00:00:00"
    this_extract_time = "2022-11-03 14:20:51"

    write_data(last_extract_time, this_extract_time, mock_client, mock_db)

    response = mock_client.get_object(
        Bucket="testbucket123abc456def",
        Key="data/by time/2022/11-November/03/14:20:51/staff",
    )
    file_content = json.loads(response["Body"].read().decode("utf-8"))

    assert file_content[0]["created_at"] is None
    assert file_content[0]["last_updated"] is None
