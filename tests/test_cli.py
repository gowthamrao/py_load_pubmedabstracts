import psycopg
import pytest
from typer.testing import CliRunner

from py_load_pubmedabstracts.cli import app

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration

# Initialize the Typer test runner
runner = CliRunner()


import logging


def test_initialize_db(db_conn_str: str, caplog):
    """
    Tests the initialize-db command against a live PostgreSQL container.
    """
    # Invoke the CLI command, passing the connection string and load mode
    # as environment variables for the command's execution context.
    with caplog.at_level(logging.INFO):
        result = runner.invoke(
            app,
            ["initialize-db"],
            env={"PML_DB_CONNECTION_STRING": db_conn_str, "PML_LOAD_MODE": "BOTH"},
        )

        # 1. Check that the command executed successfully
        assert result.exit_code == 0, f"CLI command failed with output: {result.stdout}"
        assert "Database initialized successfully" in caplog.text

        # 2. Verify that the tables were actually created in the.
        with psycopg.connect(db_conn_str) as conn:
            with conn.cursor() as cur:
                # Check for the existence of the _pubmed_load_history table
                cur.execute("SELECT to_regclass('_pubmed_load_history');")
                assert cur.fetchone()[0] == '_pubmed_load_history'

                # Check for the existence of the citations_json table (for FULL/BOTH mode)
                cur.execute("SELECT to_regclass('citations_json');")
                assert cur.fetchone()[0] == 'citations_json'

                # Check for the existence of a normalized table (for NORMALIZED/BOTH mode)
                cur.execute("SELECT to_regclass('citations');")
                assert cur.fetchone()[0] == 'citations'
