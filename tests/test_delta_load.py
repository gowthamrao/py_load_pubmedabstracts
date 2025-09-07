import gzip
import os
import tempfile
from typing import Generator

import pytest
from typer.testing import CliRunner

from py_load_pubmedabstracts.cli import app
from py_load_pubmedabstracts.config import Settings
from py_load_pubmedabstracts.db.factory import get_adapter

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def runner() -> CliRunner:
    return CliRunner()


def create_test_update_xml(directory: str, filename: str, content: str) -> str:
    """Creates a gzipped XML file for testing."""
    path = os.path.join(directory, filename)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(content)
    return path


@pytest.fixture(scope="function")
def setup_test_db(postgres_container) -> Generator[Settings, None, None]:
    """
    Sets up a test database, initializes the schema, and yields settings.
    """
    # Create settings pointing to the test container
    dsn = postgres_container.get_connection_url()
    settings = Settings(db_connection_string=dsn, db_adapter="postgresql", load_mode="FULL")

    # Initialize the database schema
    adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
    adapter.initialize_schema(settings.load_mode)

    # Pre-populate with some data
    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            # This record will be updated
            cur.execute(
                "INSERT INTO citations_json (pmid, date_revised, data) VALUES (%s, %s, %s)",
                (1, "2024-01-01", '{"MedlineCitation": {"Article": {"ArticleTitle": "Old Title"}}}'),
            )
            # This record will be deleted
            cur.execute(
                "INSERT INTO citations_json (pmid, date_revised, data) VALUES (%s, %s, %s)",
                (2, "2024-01-01", '{"MedlineCitation": "..."}'),
            )
            # This record will be untouched
            cur.execute(
                "INSERT INTO citations_json (pmid, date_revised, data) VALUES (%s, %s, %s)",
                (3, "2024-01-01", '{"MedlineCitation": "..."}'),
            )
        conn.commit()

    yield settings

    # Teardown (optional, as the container will be destroyed)
    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS citations_json, _pubmed_load_history;")
        conn.commit()


def test_run_delta_end_to_end(runner: CliRunner, setup_test_db: Settings, mocker):
    """
    Tests the full delta load process: upserts, deletes, and state tracking.
    """
    # ARRANGE
    settings = setup_test_db
    test_xml_content = """
    <PubmedArticleSet>
        <!-- This should update PMID 1 -->
        <MedlineCitation Status="MEDLINE" Owner="NLM">
            <PMID Version="1">1</PMID>
            <DateRevised><Year>2024</Year><Month>02</Month><Day>02</Day></DateRevised>
            <Article PubModel="Print">
                <ArticleTitle>New Updated Title</ArticleTitle>
            </Article>
        </MedlineCitation>
        <!-- This should insert a new record for PMID 4 -->
        <MedlineCitation Status="MEDLINE" Owner="NLM">
            <PMID Version="1">4</PMID>
            <DateRevised><Year>2024</Year><Month>03</Month><Day>03</Day></DateRevised>
            <Article PubModel="Print">
                <ArticleTitle>A Brand New Article</ArticleTitle>
            </Article>
        </MedlineCitation>
        <!-- This should delete PMID 2 -->
        <DeleteCitation>
            <PMID Version="1">2</PMID>
        </DeleteCitation>
    </PubmedArticleSet>
    """

    with tempfile.TemporaryDirectory() as temp_dir:
        # Mock the FTP client to avoid actual network calls
        mocker.patch(
            "py_load_pubmedabstracts.cli.NLMFTPClient.list_update_files",
            return_value=[("update_test.xml.gz", "update_test.xml.gz.md5")],
        )
        mocker.patch(
            "py_load_pubmedabstracts.cli.NLMFTPClient.get_remote_checksum",
            return_value="mock_checksum",
        )
        # Instead of mocking download, we create the file locally where the app expects it
        mocker.patch(
            "py_load_pubmedabstracts.cli.NLMFTPClient.download_and_verify_file",
            return_value=create_test_update_xml(temp_dir, "update_test.xml.gz", test_xml_content),
        )

        # ACT
        # Run the 'run-delta' command
        result = runner.invoke(
            app,
            ["run-delta"],
            env={
                "PML_DB_CONNECTION_STRING": settings.db_connection_string,
                "PML_LOCAL_STAGING_DIR": temp_dir,
            },
        )

    # ASSERT
    assert result.exit_code == 0
    assert "Delta run finished" in result.stdout
    assert "Upserts: 2" in result.stdout
    assert "Deletions: 1" in result.stdout

    # Verify database state
    adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            # Check deletion
            cur.execute("SELECT pmid FROM citations_json WHERE pmid = 2")
            assert cur.fetchone() is None, "PMID 2 should have been deleted"

            # Check untouched record
            cur.execute("SELECT pmid FROM citations_json WHERE pmid = 3")
            assert cur.fetchone() is not None, "PMID 3 should not have been touched"

            # Check update
            cur.execute("SELECT data FROM citations_json WHERE pmid = 1")
            updated_record = cur.fetchone()
            assert updated_record is not None
            assert "New Updated Title" in str(updated_record[0]), "PMID 1 should have been updated"

            # Check insertion
            cur.execute("SELECT data FROM citations_json WHERE pmid = 4")
            new_record = cur.fetchone()
            assert new_record is not None
            assert "A Brand New Article" in str(new_record[0]), "PMID 4 should have been inserted"

            # Check history table
            completed_files = adapter.get_completed_files()
            assert "update_test.xml.gz" in completed_files
