import os
import pathlib
import gzip
import psycopg
import pytest
from typer.testing import CliRunner

from py_load_pubmedabstracts.cli import app

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration

# Define the sample XML content within the test module for self-containment
SAMPLE_XML_CONTENT = """
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <PMID Version="1">123456</PMID>
      <DateRevised><Year>2025</Year><Month>09</Month><Day>07</Day></DateRevised>
      <Article PubModel="Print">
        <Journal>
          <ISSN IssnType="Print">1234-5678</ISSN>
          <Title>Journal of Fictional Research</Title>
          <ISOAbbreviation>J Fict Res</ISOAbbreviation>
        </Journal>
        <ArticleTitle>A study on the effects of test data on software agents.</ArticleTitle>
        <AuthorList><Author><LastName>Doe</LastName><ForeName>John</ForeName><Initials>J</Initials></Author></AuthorList>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>
""".encode("utf-8")


def test_normalized_load_end_to_end(
    db_conn_str: str, mocker, tmp_path: pathlib.Path
):
    """
    Tests the entire normalized load pipeline from CLI to database.
    This test is self-contained and creates its own test data.
    """
    # --- 1. SETUP ---
    runner = CliRunner()
    staging_dir = str(tmp_path)

    # Create the sample gzipped file inside the temporary test directory
    sample_xml_path = tmp_path / "pubmed_sample.xml"
    sample_gz_path = tmp_path / "pubmed_sample.xml.gz"
    with open(sample_xml_path, "wb") as f:
        f.write(SAMPLE_XML_CONTENT)
    with open(sample_xml_path, "rb") as f_in, gzip.open(sample_gz_path, "wb") as f_out:
        f_out.writelines(f_in)

    # Mock the FTP client to avoid network calls
    mocker.patch(
        "py_load_pubmedabstracts.cli.NLMFTPClient.list_baseline_files",
        return_value=[("pubmed_sample.xml.gz", "pubmed_sample.xml.gz.md5")],
    )
    mocker.patch(
        "py_load_pubmedabstracts.cli.NLMFTPClient.download_and_verify_file",
        return_value=str(sample_gz_path),
    )
    mocker.patch(
        "py_load_pubmedabstracts.cli.NLMFTPClient.get_remote_checksum",
        return_value="dummymd5checksum",
    )

    env = {
        "PML_DB_CONNECTION_STRING": db_conn_str,
        "PML_DB_ADAPTER": "postgresql",
        "PML_LOAD_MODE": "NORMALIZED",
        "PML_LOCAL_STAGING_DIR": staging_dir,
    }

    # --- 2. EXECUTION ---
    init_result = runner.invoke(app, ["initialize-db"], env=env)
    assert init_result.exit_code == 0, init_result.stdout

    run_result = runner.invoke(app, ["run-baseline"], env=env)
    assert run_result.exit_code == 0, run_result.stdout

    # --- 3. VERIFICATION ---
    with psycopg.connect(db_conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM journals")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM authors")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT pmid, title FROM citations")
            citation = cur.fetchone()
            assert citation[0] == 123456
            assert "software agents" in citation[1]

            cur.execute("SELECT status FROM _pubmed_load_history")
            assert cur.fetchone()[0] == "COMPLETE"
