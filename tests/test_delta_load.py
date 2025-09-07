import gzip
import psycopg
import pytest
from typer.testing import CliRunner

from py_load_pubmedabstracts.cli import app
from py_load_pubmedabstracts.db.factory import get_adapter

pytestmark = pytest.mark.integration


@pytest.fixture(scope="function")
def setup_db_for_delta(db_conn_str: str, request):
    """
    Sets up a test DB with baseline data for a given load mode.
    The load mode is passed via pytest.param.
    """
    load_mode = request.param
    adapter = get_adapter("postgresql", db_conn_str)
    adapter.initialize_schema(mode=load_mode)

    with psycopg.connect(db_conn_str) as conn, conn.cursor() as cur:
        # Add a fake baseline history record so delta can run
        cur.execute(
            "INSERT INTO _pubmed_load_history (file_name, file_type, status) "
            "VALUES ('fake_baseline.xml.gz', 'BASELINE', 'COMPLETE');"
        )
        # Pre-populate with data to be modified
        if load_mode == "FULL":
            cur.execute(
                "INSERT INTO citations_json (pmid, date_revised, data) VALUES (%s, %s, %s)",
                (1, "2024-01-01", '{"MedlineCitation": {"Article": {"ArticleTitle": {"#text": "Old Title"}}}}'),
            )
            cur.execute(
                "INSERT INTO citations_json (pmid, date_revised, data) VALUES (%s, %s, %s)",
                (2, "2024-01-01", '{"title": "To Be Deleted"}'),
            )
        elif load_mode == "NORMALIZED":
            cur.execute("INSERT INTO citations (pmid, title) VALUES (%s, %s)", (1, "Old Title"))
            cur.execute("INSERT INTO citations (pmid, title) VALUES (%s, %s)", (2, "To Be Deleted"))

    yield db_conn_str, load_mode


@pytest.mark.parametrize("setup_db_for_delta", ["FULL", "NORMALIZED"], indirect=True)
def test_run_delta_end_to_end(setup_db_for_delta, mocker, tmp_path):
    """Tests the full delta load process for both FULL and NORMALIZED modes."""
    db_conn_str, load_mode = setup_db_for_delta
    runner = CliRunner()

    # XML content for the delta file
    test_xml_content = """
    <PubmedArticleSet>
        <MedlineCitation><PMID>1</PMID><Article><ArticleTitle>New Updated Title</ArticleTitle></Article></MedlineCitation>
        <MedlineCitation><PMID>4</PMID><Article><ArticleTitle>A Brand New Article</ArticleTitle></Article></MedlineCitation>
        <DeleteCitation><PMID>2</PMID></DeleteCitation>
    </PubmedArticleSet>
    """.encode("utf-8")

    sample_gz_path = tmp_path / "delta_sample.xml.gz"
    with gzip.open(sample_gz_path, "wb") as f:
        f.write(test_xml_content)

    mocker.patch("py_load_pubmedabstracts.cli.NLMFTPClient.list_update_files", return_value=[("delta_sample.xml.gz", "delta_sample.xml.gz.md5")])
    mocker.patch("py_load_pubmedabstracts.cli.NLMFTPClient.download_and_verify_file", return_value=str(sample_gz_path))
    mocker.patch("py_load_pubmedabstracts.cli.NLMFTPClient.get_remote_checksum", return_value="dummy")

    result = runner.invoke(
        app, ["run-delta"],
        env={
            "PML_DB_CONNECTION_STRING": db_conn_str,
            "PML_LOAD_MODE": load_mode,
            "PML_LOCAL_STAGING_DIR": str(tmp_path),
        },
    )

    assert result.exit_code == 0, result.stdout
    assert "Delta run finished" in result.stdout

    with psycopg.connect(db_conn_str) as conn, conn.cursor() as cur:
        if load_mode == "FULL":
            cur.execute("SELECT COUNT(*) FROM citations_json WHERE pmid = 2")
            assert cur.fetchone()[0] == 0, "PMID 2 should have been deleted (FULL)"
            cur.execute("SELECT data->'MedlineCitation'->'Article'->'ArticleTitle'->>'#text' FROM citations_json WHERE pmid = 1")
            assert cur.fetchone()[0] == "New Updated Title", "PMID 1 should have been updated (FULL)"
            cur.execute("SELECT COUNT(*) FROM citations_json WHERE pmid = 4")
            assert cur.fetchone()[0] == 1, "PMID 4 should have been inserted (FULL)"

        elif load_mode == "NORMALIZED":
            cur.execute("SELECT COUNT(*) FROM citations WHERE pmid = 2")
            assert cur.fetchone()[0] == 0, "PMID 2 should have been deleted (NORMALIZED)"
            cur.execute("SELECT title FROM citations WHERE pmid = 1")
            assert cur.fetchone()[0] == "New Updated Title", "PMID 1 should have been updated (NORMALIZED)"
            cur.execute("SELECT COUNT(*) FROM citations WHERE pmid = 4")
            assert cur.fetchone()[0] == 1, "PMID 4 should have been inserted (NORMALIZED)"

        cur.execute("SELECT status FROM _pubmed_load_history WHERE file_name = 'delta_sample.xml.gz'")
        assert cur.fetchone()[0] == "COMPLETE"
