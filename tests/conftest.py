import os
import pytest
import psycopg2
from unittest import mock
from airflow.models import Variable, Connection, DagBag


@pytest.fixture
def api_key():
    """Provide a mock YouTube API key through an Airflow Variable."""
    # Temporarily inject a mock API key into the environment.
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")


@pytest.fixture
def channel_handle():
    """Provide a mock YouTube channel handle through an Airflow Variable."""
    # Temporarily inject a mock channel handle into the environment.
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="MRCHEESE"):
        yield Variable.get("CHANNEL_HANDLE")


@pytest.fixture
def mock_postgres_conn_vars():
    """Provide a mocked Airflow PostgreSQL connection."""
    # Define fake PostgreSQL connection parameters for testing.
    conn = Connection(
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name",  # In Airflow, schema represents the database name.
    )

    # Convert the mocked connection into an Airflow connection URI.
    conn_uri = conn.get_uri()

    # Temporarily register the mock connection as an environment variable.
    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(
            conn_id="POSTGRES_DB_YT_ELT"
        )


@pytest.fixture
def dagbag():
    """Load Airflow DAGs for structural and import validation."""
    yield DagBag()


@pytest.fixture
def airflow_variable():
    """Provide a helper function to retrieve Airflow Variables from the environment."""

    def get_airflow_variable(variable_name):
        # Convert the requested variable name to Airflow's environment format.
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)

    return get_airflow_variable


@pytest.fixture
def real_postgres_connection():
    """Open a real PostgreSQL connection using environment configuration."""
    # Read the database connection settings from environment variables.
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None

    try:
        # Establish a real connection to PostgreSQL.
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )

        # Make the open connection available to the test.
        yield conn

    except psycopg2.Error as e:
        # Fail the test if PostgreSQL rejects the connection.
        pytest.fail(f"Failed to connect to the database: {e}")

    finally:
        # Release the database connection after the test finishes.
        if conn:
            conn.close()