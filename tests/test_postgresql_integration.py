import pytest
from testcontainers.postgres import PostgresContainer
import psycopg
import json
import os
import gzip
from pathlib import Path

from typer.testing import CliRunner

from py_load_pubmedabstracts.db.postgresql import PostgresAdapter
from py_load_pubmedabstracts.cli import app


# Mark all tests in this file as 'integration'
pytestmark = pytest.mark.integration

# --- Test Data ---
FAKE_XML_CONTENT = """
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <PMID Version="1">12345</PMID>
      <DateRevised>
        <Year>2023</Year>
        <Month>01</Month>
        <Day>15</Day>
      </DateRevised>
      <Article PubModel="Print">
        <Journal>
          <Title>Test Journal</Title>
        </Journal>
        <ArticleTitle>An Article Title</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <PMID Version="1">67890</PMID>
      <DateRevised>
        <Year>2023</Year>
        <Month>02</Month>
        <Day>20</Day>
      </DateRevised>
      <Article PubModel="Print">
        <Journal>
          <Title>Another Journal</Title>
        </Journal>
        <ArticleTitle>Another Title</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>
""".encode("utf-8")

FAKE_BASELINE_FILES = [("pubmed23n0001.xml.gz", "pubmed23n0001.xml.gz.md5")]
FAKE_CHECKSUM = "d41d8cd98f00b204e9800998ecf8427e" # MD5 of empty string

# --- Fixtures ---



@pytest.fixture(scope="module")
def runner():
    """Provides a Typer CliRunner."""
    return CliRunner()


@pytest.fixture
def fake_gzipped_xml_file(tmp_path: Path) -> Path:
    """Creates a fake gzipped XML file for testing."""
    p = tmp_path / FAKE_BASELINE_FILES[0][0]
    with gzip.open(p, "wb") as f:
        f.write(FAKE_XML_CONTENT)
    return p


@pytest.fixture
def mock_ftp_client(mocker, fake_gzipped_xml_file: Path):
    """Mocks the NLMFTPClient to prevent actual FTP calls."""

    mock_client_instance = mocker.MagicMock()
    mock_client_instance.list_baseline_files.return_value = FAKE_BASELINE_FILES
    mock_client_instance.get_remote_checksum.return_value = FAKE_CHECKSUM
    mock_client_instance.download_and_verify_file.return_value = str(fake_gzipped_xml_file)

    # This is how you mock the class constructor to return your instance
    mock_ftp_class = mocker.patch("py_load_pubmedabstracts.cli.NLMFTPClient")
    mock_ftp_class.return_value = mock_client_instance
    return mock_client_instance


@pytest.fixture
def test_environment(postgres_container: PostgresContainer, tmp_path: Path, monkeypatch):
    """
    Sets up the environment variables needed for the CLI runner.
    Removes the "+psycopg2" dialect from the DSN for compatibility with psycopg v3.
    """
    dsn = postgres_container.get_connection_url().replace("+psycopg2", "")
    monkeypatch.setenv("PML_DB_CONNECTION_STRING", dsn)
    monkeypatch.setenv("PML_DB_ADAPTER", "postgresql")
    monkeypatch.setenv("PML_LOAD_MODE", "FULL")
    monkeypatch.setenv("PML_LOCAL_STAGING_DIR", str(tmp_path))
    return dsn


# --- Tests ---

def test_run_baseline_end_to_end(
    runner: CliRunner,
    test_environment: str,
    mock_ftp_client,
    fake_gzipped_xml_file: Path
):
    """
    Tests the full end-to-end `run-baseline` command, including:
    - Database initialization
    - State management
    - Data loading and merging
    - Idempotency (running a second time does nothing)
    """
    dsn = test_environment

    # 1. Initialize the database
    result = runner.invoke(app, ["initialize-db"])
    assert result.exit_code == 0
    assert "Database initialized successfully" in result.stdout

    # 2. Run the baseline load for the first time
    result = runner.invoke(app, ["run-baseline"])
    print(result.stdout)
    assert result.exit_code == 0
    assert "Found 1 new baseline files to process" in result.stdout
    assert f"Processing: {FAKE_BASELINE_FILES[0][0]}" in result.stdout
    assert "Staging complete. Total records: 2" in result.stdout
    assert "Merge complete" in result.stdout
    assert f"Successfully processed {FAKE_BASELINE_FILES[0][0]}" in result.stdout
    assert "Baseline run finished" in result.stdout

    # 3. Verify the database state
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # Check history table
            cur.execute("SELECT file_name, status, records_processed FROM _pubmed_load_history;")
            history_row = cur.fetchone()
            assert history_row[0] == FAKE_BASELINE_FILES[0][0]
            assert history_row[1] == "COMPLETE"
            assert history_row[2] == 2

            # Check final data table
            cur.execute("SELECT pmid FROM citations_json ORDER BY pmid;")
            data_rows = cur.fetchall()
            assert len(data_rows) == 2
            assert data_rows[0][0] == 12345
            assert data_rows[1][0] == 67890

    # 4. Run baseline again to test idempotency
    result = runner.invoke(app, ["run-baseline"])
    assert result.exit_code == 0
    assert "No new baseline files to process" in result.stdout

    # 5. Verify that check-status works
    result = runner.invoke(app, ["check-status"])
    assert result.exit_code == 0
    assert FAKE_BASELINE_FILES[0][0] in result.stdout
