import pytest
import psycopg
import gzip
from pathlib import Path

from typer.testing import CliRunner

from py_load_pubmedabstracts.cli import app

# Mark all tests in this file as 'integration'
pytestmark = pytest.mark.integration

# --- Test Data ---
FAKE_XML_CONTENT = """
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>12345</PMID>
    </MedlineCitation>
  </PubmedArticle>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>67890</PMID>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>
""".encode("utf-8")

FAKE_BASELINE_FILES = [("pubmed23n0001.xml.gz", "pubmed23n0001.xml.gz.md5")]

@pytest.fixture
def fake_gzipped_xml_file(tmp_path: Path) -> Path:
    """Creates a fake gzipped XML file for testing."""
    p = tmp_path / FAKE_BASELINE_FILES[0][0]
    with gzip.open(p, "wb") as f:
        f.write(FAKE_XML_CONTENT)
    return p

# --- Tests ---

import logging


def test_run_baseline_end_to_end(
    db_conn_str: str,
    mocker,
    fake_gzipped_xml_file: Path,
    tmp_path: Path,
    caplog,
):
    """
    Tests the full end-to-end `run-baseline` command for the FULL load mode.
    """
    runner = CliRunner()

    # Mock the FTP client
    mocker.patch("py_load_pubmedabstracts.cli.NLMFTPClient.list_baseline_files", return_value=FAKE_BASELINE_FILES)
    mocker.patch("py_load_pubmedabstracts.cli.NLMFTPClient.get_remote_checksum", return_value="dummy")
    mocker.patch("py_load_pubmedabstracts.cli.NLMFTPClient.download_and_verify_file", return_value=str(fake_gzipped_xml_file))

    env = {
        "PML_DB_CONNECTION_STRING": db_conn_str,
        "PML_DB_ADAPTER": "postgresql",
        "PML_LOAD_MODE": "FULL",
        "PML_LOCAL_STAGING_DIR": str(tmp_path),
    }

    with caplog.at_level(logging.INFO):
        # 1. Initialize and run the baseline
        init_result = runner.invoke(app, ["initialize-db"], env=env)
        assert init_result.exit_code == 0

        run_result = runner.invoke(app, ["run-baseline"], env=env)
        assert run_result.exit_code == 0, run_result.stdout
        assert "Found 1 new baseline files to process." in caplog.text
        assert "Successfully processed file." in caplog.text

        # 2. Verify the database state
        with psycopg.connect(db_conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM citations_json;")
                assert cur.fetchone()[0] == 2
                cur.execute("SELECT status FROM _pubmed_load_history WHERE file_name = 'pubmed23n0001.xml.gz'")
                assert cur.fetchone()[0] == "COMPLETE"

        # 3. Run baseline again to test idempotency
        idempotency_result = runner.invoke(app, ["run-baseline"], env=env)
        assert idempotency_result.exit_code == 0
        assert "No new baseline files to process" in caplog.text
