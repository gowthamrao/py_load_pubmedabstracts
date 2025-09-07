import psycopg
import pytest
from typer.testing import CliRunner
from testcontainers.postgres import PostgresContainer

from py_load_pubmedabstracts.cli import app

# Initialize the Typer test runner
runner = CliRunner()




def test_initialize_db(postgres_container: PostgresContainer):
    """
    Tests the initialize-db command against a live PostgreSQL container.
    """
    # Get the database connection URL from the running container
    dsn = postgres_container.get_connection_url()
    # testcontainers-python might return a DSN for psycopg2.
    # We are using psycopg (v3), which prefers the 'postgresql://' scheme.
    if dsn.startswith("postgresql+psycopg2://"):
        dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")


    # Invoke the CLI command, passing the connection string and load mode
    # as environment variables for the command's execution context.
    result = runner.invoke(
        app,
        ["initialize-db"],
        env={"PML_DB_CONNECTION_STRING": dsn, "PML_LOAD_MODE": "BOTH"},
        catch_exceptions=False  # To get a full traceback on errors
    )

    # 1. Check that the command executed successfully
    assert result.exit_code == 0, f"CLI command failed with output: {result.stdout}"
    assert "Database initialized successfully" in result.stdout

    # 2. Verify that the tables were actually created in the database
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # Check for the existence of the _pubmed_load_history table
            cur.execute("SELECT to_regclass('_pubmed_load_history');")
            table_name = cur.fetchone()[0]
            assert table_name == '_pubmed_load_history'

            # Check for the existence of the citations_json table
            cur.execute("SELECT to_regclass('citations_json');")
            table_name = cur.fetchone()[0]
            assert table_name == 'citations_json'
