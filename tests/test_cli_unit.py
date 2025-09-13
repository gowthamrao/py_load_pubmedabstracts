import os
import logging
import pytest
from typer.testing import CliRunner
from unittest.mock import MagicMock, call

from py_load_pubmedabstracts.cli import app

# It's better to have a single runner instance
runner = CliRunner()


@pytest.fixture(autouse=True)
def mock_logging(mocker):
    """Fixture to mock the logging configuration to allow caplog to work."""
    mocker.patch("py_load_pubmedabstracts.cli.configure_logging")


@pytest.fixture
def mock_settings(mocker):
    """Fixture to mock the Settings class."""
    mock_settings_instance = MagicMock()
    mock_settings_instance.db_adapter = "postgresql"
    mock_settings_instance.db_connection_string = "dummy_string"
    mock_settings_instance.load_mode = "BOTH"
    mock_settings_instance.local_staging_dir = "/tmp"
    mocker.patch("py_load_pubmedabstracts.cli.Settings", return_value=mock_settings_instance)
    return mock_settings_instance


@pytest.fixture
def mock_adapter(mocker):
    """Fixture to mock the database adapter."""
    mock_adapter_instance = MagicMock()
    mocker.patch("py_load_pubmedabstracts.cli.get_adapter", return_value=mock_adapter_instance)
    return mock_adapter_instance


@pytest.fixture
def mock_ftp_client(mocker):
    """Fixture to mock the FTP client."""
    mock_client_instance = MagicMock()
    mocker.patch("py_load_pubmedabstracts.cli.NLMFTPClient", return_value=mock_client_instance)
    return mock_client_instance


def test_initialize_db_success(mock_settings, mock_adapter, caplog):
    """Tests happy path for initialize-db command."""
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["initialize-db"])
        assert result.exit_code == 0
        assert "Database initialized successfully" in caplog.text
        mock_adapter.initialize_schema.assert_called_once_with(mode="BOTH")


def test_initialize_db_failure(mock_settings, mock_adapter, caplog):
    """Tests exception handling for initialize-db command."""
    mock_adapter.initialize_schema.side_effect = Exception("Connection Error")
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["initialize-db"])
        assert result.exit_code == 1
        assert "Error initializing database" in caplog.text


def test_list_remote_files_success(mock_ftp_client, caplog):
    """Tests happy path for list-remote-files command."""
    mock_ftp_client.list_baseline_files.return_value = [("baseline1.xml.gz", "md5")]
    mock_ftp_client.list_update_files.return_value = [("update1.xml.gz", "md5")]
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["list-remote-files"])
        assert result.exit_code == 0
        assert "Available baseline files" in caplog.text
        assert "Available update files" in caplog.text


def test_check_status_success(mock_settings, mock_adapter, caplog):
    """Tests happy path for check-status command."""
    mock_adapter.get_completed_files.return_value = ["file1.xml.gz"]
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["check-status"])
        assert result.exit_code == 0
        assert "Found completed files" in caplog.text


def test_reset_failed_success(mock_settings, mock_adapter, caplog):
    """Tests happy path for reset-failed command."""
    mock_adapter.reset_failed_files.return_value = 1
    with caplog.at_level(logging.WARNING):
        result = runner.invoke(app, ["reset-failed"])
        assert result.exit_code == 0
        assert "Reset status for 1 failed file(s)" in caplog.text


def test_run_baseline_no_new_files(mock_settings, mock_adapter, mock_ftp_client, caplog):
    """Tests run-baseline when no new files are available."""
    mock_ftp_client.list_baseline_files.return_value = [("file1.xml.gz", "md5")]
    mock_adapter.get_completed_files.return_value = {"file1.xml.gz"}
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["run-baseline"])
        assert result.exit_code == 0
        assert "No new baseline files to process" in caplog.text


def test_run_delta_baseline_not_complete(mock_settings, mock_adapter, mock_ftp_client, caplog):
    """Tests run-delta when baseline is not complete."""
    mock_adapter.has_completed_baseline.return_value = False
    with caplog.at_level(logging.ERROR):
        result = runner.invoke(app, ["run-delta"])
        assert result.exit_code == 1
        assert "Baseline must be loaded before deltas can be processed" in caplog.text

def test_process_single_file_success(mocker, mock_settings, mock_adapter, mock_ftp_client, caplog):
    """
    Tests the core _process_single_file helper function on a successful run.
    """
    # Arrange
    mocker.patch("os.path.exists", return_value=True)
    mock_remove = mocker.patch("os.remove")
    mock_parser = mocker.patch(
        "py_load_pubmedabstracts.cli.parse_pubmed_xml",
        return_value=[("UPSERT", {"table": [1, 2]}), ("DELETE", {"pmids": [3]})],
    )
    mock_ftp_client.list_baseline_files.return_value = [("file1.xml.gz", "file1.md5")]
    mock_ftp_client.download_and_verify_file.return_value = "/tmp/file1.xml.gz"
    mock_adapter.get_completed_files.return_value = set()

    # Act
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["run-baseline"])

    # Assert
    assert result.exit_code == 0, caplog.text
    assert "Successfully processed file" in caplog.text
    mock_ftp_client.download_and_verify_file.assert_called_once()
    mock_parser.assert_called_once()
    mock_adapter.create_staging_tables.assert_called_once()
    mock_adapter.bulk_load_chunk.assert_called_once_with(data_chunk={'table': [1, 2]})
    mock_adapter.process_deletions.assert_called_once_with(pmid_list=[3], mode="BOTH")
    mock_adapter.execute_merge_strategy.assert_called_once()
    assert mock_adapter.manage_load_state.call_count == 3
    mock_remove.assert_called_once_with("/tmp/file1.xml.gz")


def test_process_single_file_failure(mocker, mock_settings, mock_adapter, mock_ftp_client, caplog):
    """Tests that _process_single_file handles exceptions and marks file as FAILED."""
    # Arrange
    mocker.patch("os.path.exists", return_value=True)
    mock_remove = mocker.patch("os.remove")
    mock_ftp_client.list_baseline_files.return_value = [("file1.xml.gz", "file1.md5")]
    mock_ftp_client.download_and_verify_file.side_effect = Exception("Download failed")
    mock_adapter.get_completed_files.return_value = set()

    # Act
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["run-baseline"])

    # Assert
    assert result.exit_code == 1
    assert "Critical error during baseline run" in caplog.text
    mock_adapter.manage_load_state.assert_any_call(file_name='file1.xml.gz', status='FAILED')
    mock_remove.assert_not_called()


def test_list_remote_files_failure(mock_ftp_client, caplog):
    """Tests exception handling for list-remote-files."""
    mock_ftp_client.list_baseline_files.side_effect = Exception("FTP Error")
    with caplog.at_level(logging.ERROR):
        result = runner.invoke(app, ["list-remote-files"])
        assert result.exit_code == 1
        assert "Error listing remote files" in caplog.text


def test_list_remote_files_no_baseline(mock_ftp_client, caplog):
    """Tests list-remote-files with --no-baseline."""
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["list-remote-files", "--no-baseline"])
        assert result.exit_code == 0
        mock_ftp_client.list_baseline_files.assert_not_called()
        mock_ftp_client.list_update_files.assert_called_once()


def test_list_remote_files_no_updates(mock_ftp_client, caplog):
    """Tests list-remote-files with --no-updates."""
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["list-remote-files", "--no-updates"])
        assert result.exit_code == 0
        mock_ftp_client.list_baseline_files.assert_called_once()
        mock_ftp_client.list_update_files.assert_not_called()


def test_check_status_failure(mock_settings, mock_adapter, caplog):
    """Tests exception handling for check-status."""
    mock_adapter.get_completed_files.side_effect = Exception("DB Error")
    with caplog.at_level(logging.ERROR):
        result = runner.invoke(app, ["check-status"])
        assert result.exit_code == 1
        assert "Error checking status" in caplog.text


def test_check_status_no_files(mock_settings, mock_adapter, caplog):
    """Tests check-status with no completed files."""
    mock_adapter.get_completed_files.return_value = []
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["check-status"])
        assert result.exit_code == 0
        assert "No files have been successfully processed yet" in caplog.text


def test_reset_failed_failure(mock_settings, mock_adapter, caplog):
    """Tests exception handling for reset-failed."""
    mock_adapter.reset_failed_files.side_effect = Exception("DB Error")
    with caplog.at_level(logging.ERROR):
        result = runner.invoke(app, ["reset-failed"])
        assert result.exit_code == 1
        assert "Error resetting failed files" in caplog.text


def test_reset_failed_no_files(mock_settings, mock_adapter, caplog):
    """Tests reset-failed with no files to reset."""
    mock_adapter.reset_failed_files.return_value = 0
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["reset-failed"])
        assert result.exit_code == 0
        assert "No failed files found to reset" in caplog.text


def test_run_baseline_limit(mock_settings, mock_adapter, mock_ftp_client, mocker, caplog):
    """Tests the --limit option for run-baseline."""
    mocker.patch("py_load_pubmedabstracts.cli._process_single_file")
    mock_ftp_client.list_baseline_files.return_value = [
        ("file1.xml.gz", "md5"),
        ("file2.xml.gz", "md5"),
    ]
    mock_adapter.get_completed_files.return_value = set()
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["run-baseline", "-l", "1"])
        assert result.exit_code == 0
        assert "Processing a maximum of 1 file(s)." in caplog.text


def test_run_baseline_initial_load(mock_settings, mock_adapter, mock_ftp_client, mocker):
    """Tests the --initial-load option for run-baseline."""
    mock_process_single = mocker.patch("py_load_pubmedabstracts.cli._process_single_file")
    mock_ftp_client.list_baseline_files.return_value = [("file1.xml.gz", "md5")]
    mock_adapter.get_completed_files.return_value = set()
    result = runner.invoke(app, ["run-baseline", "--initial-load"])
    assert result.exit_code == 0
    mock_adapter.optimize_database.assert_has_calls([
        call(stage="pre-load", mode="BOTH"),
        call(stage="post-load", mode="BOTH"),
    ])
    mock_process_single.assert_called_with(
        client=mock_ftp_client,
        adapter=mock_adapter,
        settings=mock_settings,
        file_info=('file1.xml.gz', 'md5'),
        file_type='BASELINE',
        chunk_size=20000,
        is_initial_load=True
    )


def test_run_delta_limit(mock_settings, mock_adapter, mock_ftp_client, mocker, caplog):
    """Tests the --limit option for run-delta."""
    mocker.patch("py_load_pubmedabstracts.cli._process_single_file")
    mock_adapter.has_completed_baseline.return_value = True
    mock_ftp_client.list_update_files.return_value = [
        ("file1.xml.gz", "md5"),
        ("file2.xml.gz", "md5"),
    ]
    mock_adapter.get_completed_files.return_value = set()
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["run-delta", "-l", "1"])
        assert result.exit_code == 0
        assert "Processing a maximum of 1 file(s)." in caplog.text


def test_run_delta_no_new_files(mock_settings, mock_adapter, mock_ftp_client, caplog):
    """Tests run-delta when no new files are available."""
    mock_adapter.has_completed_baseline.return_value = True
    mock_ftp_client.list_update_files.return_value = [("file1.xml.gz", "md5")]
    mock_adapter.get_completed_files.return_value = {"file1.xml.gz"}
    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["run-delta"])
        assert result.exit_code == 0
        assert "No new update files to process" in caplog.text


def test_run_delta_processing_error(mock_settings, mock_adapter, mock_ftp_client, mocker, caplog):
    """Tests when an error occurs during file processing in run-delta."""
    mock_process_single = mocker.patch("py_load_pubmedabstracts.cli._process_single_file", side_effect=Exception("Processing Error"))
    mock_adapter.has_completed_baseline.return_value = True
    mock_ftp_client.list_update_files.return_value = [("file1.xml.gz", "md5")]
    mock_adapter.get_completed_files.return_value = set()
    with caplog.at_level(logging.ERROR):
        result = runner.invoke(app, ["run-delta"])
        assert result.exit_code == 1
        assert "Error processing file1.xml.gz. Aborting delta run." in caplog.text


def test_process_single_file_cleanup(mocker, mock_settings, mock_adapter, mock_ftp_client):
    """Tests that the local file is cleaned up even if processing fails after download."""
    mocker.patch("os.path.exists", return_value=True)
    mock_remove = mocker.patch("os.remove")
    mock_ftp_client.download_and_verify_file.return_value = "/tmp/file1.xml.gz"
    mocker.patch("py_load_pubmedabstracts.cli.parse_pubmed_xml", side_effect=Exception("Parsing Error"))
    mock_ftp_client.list_baseline_files.return_value = [("file1.xml.gz", "md5")]
    mock_adapter.get_completed_files.return_value = set()

    result = runner.invoke(app, ["run-baseline"])

    assert result.exit_code == 1
    mock_remove.assert_called_once_with("/tmp/file1.xml.gz")
