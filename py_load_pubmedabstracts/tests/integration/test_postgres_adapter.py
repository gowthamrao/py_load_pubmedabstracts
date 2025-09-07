"""Integration tests for the PostgresAdapter and initialize-db command.

These tests require Docker to be running, as they will spin up a real
PostgreSQL container to validate the database interactions.
"""

import pytest
import psycopg
from typer.testing import CliRunner
from testcontainers.postgres import PostgresContainer

from py_load_pubmedabstracts.cli import app
from py_load_pubmedabstracts.config import Settings

# Mark all tests in this file as 'integration' tests
pytestmark = pytest.mark.integration


def test_initialize_db_creates_history_table(monkeypatch):
    """
    Tests that the 'initialize-db' command correctly creates the
    '_pubmed_load_history' table in a live PostgreSQL database.
    """
    runner = CliRunner()
    # Use the postgres container as a context manager for automatic cleanup
    with PostgresContainer("postgres:16-alpine") as postgres:
        db_url = postgres.get_connection_url()

        # Define a function that returns a Settings object with the container's URL
        def get_test_db_settings():
            return Settings(PML_DB_CONNECTION_STRING=db_url)

        # Patch 'get_settings' to return the dynamic DB URL
        monkeypatch.setattr(
            "py_load_pubmedabstracts.config.get_settings", get_test_db_settings
        )

        # Run the CLI command.
        result = runner.invoke(app, ["initialize-db"], catch_exceptions=False)

        # 1. Assert the command reported success
        assert result.exit_code == 0
        assert "✅ Database initialization complete." in result.stdout

        # 2. Verify the results directly in the database
        try:
            with psycopg.connect(db_url) as conn:
                with conn.cursor() as cur:
                    # 2a. Verify the table exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_name = '_pubmed_load_history'
                        );
                    """)
                    table_exists = cur.fetchone()[0]
                    assert table_exists, "Table '_pubmed_load_history' was not created."

                    # 2b. Verify the table has the correct columns and types
                    cur.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = '_pubmed_load_history'
                        ORDER BY ordinal_position;
                    """)
                    columns_from_db = {row[0]: row[1] for row in cur.fetchall()}

                    expected_columns = {
                        "file_name": "text",
                        "file_type": "text",
                        "md5_checksum": "text",
                        "download_timestamp": "timestamp with time zone",
                        "load_start_timestamp": "timestamp with time zone",
                        "load_end_timestamp": "timestamp with time zone",
                        "status": "text",
                        "records_processed": "integer",
                    }

                    assert columns_from_db == expected_columns, \
                        f"Schema mismatch. DB: {columns_from_db}, Expected: {expected_columns}"

        except psycopg.Error as e:
            pytest.fail(f"Database verification failed: {e}")
