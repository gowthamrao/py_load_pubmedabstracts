"""Tests for the CLI application."""

import json
import logging
import pytest
from typer.testing import CliRunner

from py_load_pubmedabstracts.cli import app
from py_load_pubmedabstracts.config import Settings
from py_load_pubmedabstracts.db.base import DatabaseAdapter

# Instantiate a runner
runner = CliRunner()


@pytest.fixture(autouse=True)
def prevent_logging_config(monkeypatch):
    """
    Fixture to prevent the app's default logging configuration from running,
    allowing pytest's `caplog` fixture to work without interference.
    This is applied automatically to all tests in this module.
    """
    monkeypatch.setattr("py_load_pubmedabstracts.cli.configure_logging", lambda: None)


def test_initialize_db_command_failure_on_dummy_db(monkeypatch, caplog):
    """
    Test the 'initialize-db' command's failure output on a bad connection.
    This uses the standard `caplog` fixture to capture logs.
    """
    monkeypatch.setattr(
        "py_load_pubmedabstracts.cli.Settings",
        lambda: Settings(db_connection_string="postgresql://dummy:user@host/db"),
    )

    caplog.set_level(logging.INFO, logger="py_load_pubmedabstracts.cli")

    result = runner.invoke(app, ["initialize-db"])

    assert result.exit_code == 1

    assert len(caplog.records) > 0

    last_record = caplog.records[-1]
    assert last_record.levelno == logging.ERROR
    assert "Error initializing database" in last_record.message
    # Check for the actual error message from psycopg
    assert "No address associated with hostname" in str(last_record.exc_info)


class MockDbAdapter(DatabaseAdapter):
    """A mock database adapter for testing the CLI without a real DB."""
    def get_completed_files(self) -> list[str]:
        return ["file1.xml.gz", "file2.xml.gz"]
    def initialize_schema(self, mode: str) -> None: pass
    def create_staging_tables(self, mode: str) -> None: pass
    def bulk_load_chunk(self, data_chunk) -> None: pass
    def process_deletions(self, pmid_list, mode) -> None: pass
    def execute_merge_strategy(self, mode, is_initial_load) -> None: pass
    def manage_load_state(self, file_name, status, **kwargs) -> None: pass
    def optimize_database(self, stage, mode) -> None: pass
    def reset_failed_files(self) -> int: return 0
    def has_completed_baseline(self) -> bool: return True


def test_check_status_command_success(monkeypatch, caplog):
    """
    Test the 'check-status' command's success output.
    """
    monkeypatch.setattr(
        "py_load_pubmedabstracts.cli.get_adapter",
        lambda *args, **kwargs: MockDbAdapter(),
    )

    caplog.set_level(logging.INFO, logger="py_load_pubmedabstracts.cli")

    result = runner.invoke(app, ["check-status"])

    assert result.exit_code == 0

    assert len(caplog.records) > 0

    last_record = caplog.records[-1]
    assert last_record.levelno == logging.INFO
    assert last_record.message == "Found completed files."

    # Extra data is available as direct attributes on the record object
    assert last_record.count == 2
    assert last_record.files == ["file1.xml.gz", "file2.xml.gz"]
