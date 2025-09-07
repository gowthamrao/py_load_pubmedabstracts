"""Tests for the CLI application."""

import pytest
from typer.testing import CliRunner

from py_load_pubmedabstracts.cli import app
from py_load_pubmedabstracts.config import Settings

# Instantiate a runner
runner = CliRunner()


def test_initialize_db_command_failure_on_dummy_db(monkeypatch):
    """
    Test the 'initialize-db' command's failure output on a bad connection.

    This unit test uses pytest's monkeypatch fixture to override the
    get_settings function, avoiding a real db connection and testing the
    application's error handling.
    """
    # Define a dummy settings object for the test
    def get_dummy_settings():
        return Settings(PML_DB_CONNECTION_STRING="postgresql://dummy:user@host/db")

    # Patch the 'get_settings' function in the 'cli' module where it is imported and used.
    monkeypatch.setattr(
        "py_load_pubmedabstracts.cli.get_settings", get_dummy_settings
    )

    result = runner.invoke(app, ["initialize-db"])

    assert result.exit_code == 1
    assert "Initializing database schema..." in result.stdout
    assert "postgresql://dummy" in result.stdout
    assert "Error initializing database" in result.stdout


def test_check_status_command():
    """
    Test the 'check-status' command.
    """
    result = runner.invoke(app, ["check-status"])

    assert result.exit_code == 0
    assert "Checking load status..." in result.stdout
