from src.utils import connect_to_database
import boto3
import json
from moto import mock_aws
from unittest.mock import Mock, patch
import pytest


@pytest.fixture
def mock_secrets_manager():
    with mock_aws():
        session = boto3.Session(region_name="eu-west-2")
        client = session.client("secretsmanager")
        client.create_secret(
            Name="test-credentials",
            SecretString=json.dumps(
                {
                    "user": "test-user",
                    "password": "test-password",
                    "host": "localhost",
                    "port": "5432",
                    "database": "test-db",
                }
            ),
        )

        yield client


class TestRetrieveSecrets:

    @mock_aws
    def test_retrieve_secrets(self):

        MOCK_SECERT = {
            "user": "test-user",
            "password": "test-password",
            "host": "localhost",
            "port": "5432",
            "database": "test-db",
        }

        session = boto3.Session(region_name="eu-west-2")
        mock_client = session.client("secretsmanager")
        mock_client.create_secret(
            Name="test-credentials", SecretString=json.dumps(MOCK_SECERT)
        )

        def get_db_credentials(secret_name, region="eu-west-2"):
            response = mock_client.get_secret_value(SecretId=secret_name)
            secret = json.loads(response["SecretString"])
            return secret

        credentials = get_db_credentials("test-credentials")

        assert credentials["user"] == "test-user"
        assert credentials["database"] == "test-db"


class TestConnectToDB:
    @patch("pg8000.connect")
    def test_connection_to_db(self, mock_pg_connect, mock_secrets_manager):

        mock_connection = Mock()
        mock_pg_connect.return_value = mock_connection

        connection = connect_to_database("test-credentials", "eu-west-2")

        # Assertions
        mock_pg_connect.assert_called_once_with(
            user="test-user",
            password="test-password",
            host="localhost",
            port=5432,
            database="test-db",
        )
        assert connection == mock_connection
