from src.utils import connect_to_database,get_db_credentials
import boto3
import json
from moto import mock_aws
from unittest.mock import Mock, patch
import pytest
import logging


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



    def test_error_get_db_creds(self,caplog):  
        with caplog.at_level(logging.INFO):
                with pytest.raises(Exception):
                    get_db_credentials("notASecret")
                assert "ERROR! couldn't retrieve secret" in caplog.text

    def test_error_connect_db(self,caplog):  
        with caplog.at_level(logging.INFO):
                with pytest.raises(Exception):
                    connect_to_database("notASecret")
                assert "ERROR! couldn't retrieve secret" in caplog.text

    def test_error_connect_db_generic(self,caplog, mock_secrets_manager):
        with patch("src.utils.pg8000.connect", side_effect=Exception("generic connection error")):
            with caplog.at_level(logging.INFO):
                with pytest.raises(Exception) as exc_info:
                    connect_to_database("test-credentials", "eu-west-2")
                
                assert "ERROR! couldn't connect to database:" in caplog.text
                assert "generic connection error" in str(exc_info.value)