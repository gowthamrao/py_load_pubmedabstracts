import ftplib
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, mock_open

import pytest

from py_load_pubmedabstracts.ftp_client import NLMFTPClient


@pytest.fixture
def mock_ftp_client(mocker):
    """Fixture to create an NLMFTPClient with a mocked FTP connection."""
    mock_ftp_instance = MagicMock(spec=ftplib.FTP)

    @contextmanager
    def mock_connect():
        yield mock_ftp_instance

    client = NLMFTPClient()
    mocker.patch.object(client, '_connect', new=mock_connect)
    return client, mock_ftp_instance

def test_list_baseline_files(mock_ftp_client):
    """Tests that file listing correctly pairs data and checksum files."""
    client, mock_ftp = mock_ftp_client

    file_list = [
        "pubmed25n0001.xml.gz",
        "pubmed25n0001.xml.gz.md5",
        "pubmed25n0002.xml.gz", # No checksum file
        "pubmed25n0003.xml.gz.md5", # No data file
        "other_file.txt"
    ]
    mock_ftp.nlst.return_value = file_list

    paired_files = client.list_baseline_files()

    mock_ftp.cwd.assert_called_once_with(client.BASELINE_DIR)
    assert paired_files == [("pubmed25n0001.xml.gz", "pubmed25n0001.xml.gz.md5")]

def test_download_and_verify_file_success(mock_ftp_client, mocker):
    """Tests the successful download and verification of a file."""
    client, mock_ftp = mock_ftp_client

    fake_checksum = "d3b4b4b5b5b5b5b5b5b5b5b5b5b5b5b5"
    fake_checksum_line = f"MD5(test.xml.gz)= {fake_checksum}".encode("utf-8")

    def retrbinary_side_effect(command, callback):
        if "test.xml.gz.md5" in command:
            callback(fake_checksum_line)
        elif "test.xml.gz" in command:
            pass  # Don't need to do anything, just simulate the call

    mock_ftp.retrbinary.side_effect = retrbinary_side_effect
    mocker.patch("builtins.open", mock_open())
    mocker.patch.object(client, '_calculate_local_checksum', return_value=fake_checksum)

    local_path = client.download_and_verify_file(
        remote_dir=client.BASELINE_DIR,
        data_filename="test.xml.gz",
        md5_filename="test.xml.gz.md5",
        local_staging_dir="/tmp/staging"
    )

    assert Path(local_path) == Path("/tmp/staging/test.xml.gz")
    assert client._calculate_local_checksum.call_count == 1
    assert mock_ftp.retrbinary.call_count == 2 # Once for checksum, once for data

def test_download_and_verify_file_checksum_mismatch_retry_and_succeed(mock_ftp_client, mocker):
    """Tests that the client retries on checksum mismatch and succeeds on the second attempt."""
    client, mock_ftp = mock_ftp_client

    correct_checksum = "iamcorrect"
    wrong_checksum = "iamwrong"
    fake_checksum_line = f"MD5(test.xml.gz)= {correct_checksum}".encode("utf-8")

    def retrbinary_side_effect(command, callback):
        if ".md5" in command:
            callback(fake_checksum_line)
        else:
            pass

    mock_ftp.retrbinary.side_effect = retrbinary_side_effect
    mocker.patch("builtins.open", mock_open())
    mocker.patch("time.sleep")

    m_calc_checksum = mocker.patch.object(client, '_calculate_local_checksum', side_effect=[wrong_checksum, correct_checksum])

    client.download_and_verify_file(
        remote_dir=client.BASELINE_DIR,
        data_filename="test.xml.gz",
        md5_filename="test.xml.gz.md5",
        local_staging_dir="/tmp/staging",
        max_retries=2
    )

    assert m_calc_checksum.call_count == 2
    assert mock_ftp.retrbinary.call_count == 4

def test_download_and_verify_file_fails_after_all_retries(mock_ftp_client, mocker):
    """Tests that the download fails permanently if checksum mismatch persists."""
    client, mock_ftp = mock_ftp_client

    correct_checksum = "iamcorrect"
    wrong_checksum = "iamwrong"
    fake_checksum_line = f"MD5(test.xml.gz)= {correct_checksum}".encode("utf-8")

    def retrbinary_side_effect(command, callback):
        if ".md5" in command:
            callback(fake_checksum_line)
        else:
            pass

    mock_ftp.retrbinary.side_effect = retrbinary_side_effect
    mocker.patch("builtins.open", mock_open())
    mocker.patch("time.sleep")
    mocker.patch.object(client, '_calculate_local_checksum', return_value=wrong_checksum)

    with pytest.raises(Exception, match="Failed to download and verify test.xml.gz after 3 attempts"):
        client.download_and_verify_file(
            remote_dir=client.BASELINE_DIR,
            data_filename="test.xml.gz",
            md5_filename="test.xml.gz.md5",
            local_staging_dir="/tmp/staging",
            max_retries=3
        )
    assert mock_ftp.retrbinary.call_count == 6


def test_connect_finalizer(mocker):
    """Tests that ftp.quit() is called even if an exception occurs."""
    mock_ftp_instance = MagicMock(spec=ftplib.FTP)
    mocker.patch("ftplib.FTP", return_value=mock_ftp_instance)
    client = NLMFTPClient()

    with pytest.raises(Exception, match="Test Exception"):
        with client._connect():
            raise Exception("Test Exception")

    mock_ftp_instance.quit.assert_called_once()


def test_download_and_verify_file_download_error(mock_ftp_client, mocker):
    """Tests that an exception during download triggers a retry."""
    client, mock_ftp = mock_ftp_client

    correct_checksum = "iamcorrect"
    fake_checksum_line = f"MD5(test.xml.gz)= {correct_checksum}".encode("utf-8")

    failed_once = False
    def retrbinary_side_effect(command, callback):
        nonlocal failed_once
        if ".md5" in command:
            callback(fake_checksum_line)
        else:
            # Simulate a download error on the first attempt
            if not failed_once:
                failed_once = True
                raise ftplib.error_perm("550 File not found")
            pass

    mock_ftp.retrbinary.side_effect = retrbinary_side_effect
    mocker.patch("builtins.open", mock_open())
    mocker.patch("time.sleep")
    m_calc_checksum = mocker.patch.object(client, '_calculate_local_checksum', return_value=correct_checksum)

    client.download_and_verify_file(
        remote_dir=client.BASELINE_DIR,
        data_filename="test.xml.gz",
        md5_filename="test.xml.gz.md5",
        local_staging_dir="/tmp/staging",
        max_retries=2
    )

    # 1 for checksum, 1 for failed download, 1 for checksum, 1 for successful download
    assert mock_ftp.retrbinary.call_count == 4
    assert m_calc_checksum.call_count == 1
