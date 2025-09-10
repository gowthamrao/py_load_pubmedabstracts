"""FTP client for interacting with the NLM FTP server."""
import ftplib
import hashlib
import os
import time
from contextlib import contextmanager
from typing import Generator, List, Tuple


class NLMFTPClient:
    """A client for interacting with the NLM FTP server."""

    FTP_HOST = "ftp.ncbi.nlm.nih.gov"
    BASELINE_DIR = "/pubmed/baseline/"
    UPDATE_DIR = "/pubmed/updatefiles/"

    @contextmanager
    def _connect(self) -> Generator[ftplib.FTP, None, None]:
        """Handle the FTP connection and anonymous login."""
        ftp = ftplib.FTP(self.FTP_HOST, timeout=60)
        ftp.login()  # Anonymous login
        try:
            yield ftp
        finally:
            ftp.quit()

    def _list_and_pair_files(
        self, ftp: ftplib.FTP, directory: str
    ) -> List[Tuple[str, str]]:
        """List and pair data and checksum files from a given directory."""
        ftp.cwd(directory)
        all_files = set(ftp.nlst())
        data_files = sorted([f for f in all_files if f.endswith(".xml.gz")])
        paired_files = []
        for data_file in data_files:
            checksum_file = f"{data_file}.md5"
            if checksum_file in all_files:
                paired_files.append((data_file, checksum_file))
        return paired_files

    def list_baseline_files(self) -> List[Tuple[str, str]]:
        """List all .xml.gz files and their .md5 files in the baseline directory."""
        with self._connect() as ftp:
            return self._list_and_pair_files(ftp, self.BASELINE_DIR)

    def list_update_files(self) -> List[Tuple[str, str]]:
        """List all .xml.gz files and their .md5 files in the update directory."""
        with self._connect() as ftp:
            return self._list_and_pair_files(ftp, self.UPDATE_DIR)

    def _get_remote_checksum(self, ftp: ftplib.FTP, remote_md5_filename: str) -> str:
        """Download the checksum file, parse it, and return the checksum hash."""
        checksum_data = []
        ftp.retrbinary(f"RETR {remote_md5_filename}", checksum_data.append)
        checksum_line = b"".join(checksum_data).decode("utf-8")
        checksum = checksum_line.split("= ")[1].strip()
        return checksum

    def get_remote_checksum(self, remote_dir: str, md5_filename: str) -> str:
        """Connect to the FTP server and retrieve the checksum for a given file."""
        with self._connect() as ftp:
            ftp.cwd(remote_dir)
            return self._get_remote_checksum(ftp, md5_filename)

    def _download_file(
        self, ftp: ftplib.FTP, remote_filename: str, local_path: str
    ) -> None:
        """Download a single file using FTP's binary transfer mode."""
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_filename}", f.write)

    def _calculate_local_checksum(self, local_path: str) -> str:
        """Calculate the MD5 checksum of a local file."""
        md5 = hashlib.md5()
        with open(local_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def download_and_verify_file(
        self,
        remote_dir: str,
        data_filename: str,
        md5_filename: str,
        local_staging_dir: str,
        max_retries: int = 3,
    ) -> str:
        """
        Download a file, verify its MD5 checksum, and retry on failure.

        Args:
            remote_dir: The remote directory on the FTP server.
            data_filename: The name of the .xml.gz file to download.
            md5_filename: The name of the corresponding .md5 file.
            local_staging_dir: The local directory to save the file to.
            max_retries: Max number of retries if checksum fails.

        Returns:
            The full path to the downloaded and verified local file.

        Raises:
            Exception: If the file cannot be downloaded after all retries.

        """
        local_filepath = os.path.join(local_staging_dir, data_filename)
        os.makedirs(local_staging_dir, exist_ok=True)

        for attempt in range(max_retries):
            try:
                with self._connect() as ftp:
                    ftp.cwd(remote_dir)

                    print(
                        f"[{attempt+1}/{max_retries}] Getting remote checksum for "
                        f"{md5_filename}..."
                    )
                    expected_checksum = self._get_remote_checksum(ftp, md5_filename)

                    print(
                        f"[{attempt+1}/{max_retries}] Downloading {data_filename} "
                        f"to {local_filepath}..."
                    )
                    self._download_file(ftp, data_filename, local_filepath)

                print(f"Verifying checksum for {local_filepath}...")
                local_checksum = self._calculate_local_checksum(local_filepath)

                if local_checksum == expected_checksum:
                    print(f"Checksum VERIFIED for {data_filename}.")
                    return local_filepath
                else:
                    print(
                        f"Checksum MISMATCH for {data_filename}. "
                        f"Expected: {expected_checksum}, Got: {local_checksum}. "
                        "Retrying..."
                    )
            except Exception as e:
                print(f"An error occurred on attempt {attempt + 1}: {e}. Retrying...")

            time.sleep(2**attempt)  # Exponential backoff

        raise Exception(
            f"Failed to download and verify {data_filename} after {max_retries} "
            "attempts."
        )
