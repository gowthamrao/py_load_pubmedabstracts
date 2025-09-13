import pytest
from py_load_pubmedabstracts.db.postgresql import PostgresAdapter

# Mark all tests in this file as 'integration'
pytestmark = pytest.mark.integration

def test_initialize_schema_full_mode(db_conn_str: str):
    """Test that the schema is initialized correctly for FULL mode."""
    adapter = PostgresAdapter(db_conn_str)
    adapter.initialize_schema(mode="FULL")

    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            tables = {row[0] for row in cur.fetchall()}
            assert "_pubmed_load_history" in tables
            assert "citations_json" in tables
            assert "citations" not in tables


def test_initialize_schema_normalized_mode(db_conn_str: str):
    """Test that the schema is initialized correctly for NORMALIZED mode."""
    adapter = PostgresAdapter(db_conn_str)
    adapter.initialize_schema(mode="NORMALIZED")

    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            tables = {row[0] for row in cur.fetchall()}
            assert "_pubmed_load_history" in tables
            assert "citations_json" not in tables
            assert "citations" in tables
            assert "journals" in tables
            assert "authors" in tables
            assert "mesh_terms" in tables
            assert "citation_authors" in tables
            assert "citation_mesh_terms" in tables


def test_create_staging_tables_full_mode(db_conn_str: str):
    """Test that staging tables are created correctly for FULL mode."""
    adapter = PostgresAdapter(db_conn_str)
    adapter.initialize_schema(mode="FULL")
    adapter.create_staging_tables(mode="FULL")

    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            tables = {row[0] for row in cur.fetchall()}
            assert "_staging_citations_json" in tables


def test_create_staging_tables_normalized_mode(db_conn_str: str):
    """Test that staging tables are created correctly for NORMALIZED mode."""
    adapter = PostgresAdapter(db_conn_str)
    adapter.initialize_schema(mode="NORMALIZED")
    adapter.create_staging_tables(mode="NORMALIZED")

    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            tables = {row[0] for row in cur.fetchall()}
            for table in adapter.normalized_tables:
                assert f"_staging_{table}" in tables


def test_manage_load_state(db_conn_str: str):
    """Test the load state management in _pubmed_load_history."""
    adapter = PostgresAdapter(db_conn_str)
    adapter.initialize_schema(mode="FULL")

    file_name = "test.xml.gz"

    # Test DOWNLOADING state
    adapter.manage_load_state(
        file_name, "DOWNLOADING", file_type="BASELINE", md5_checksum="123"
    )
    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, file_type, md5_checksum FROM _pubmed_load_history WHERE file_name = %s",
                (file_name,),
            )
            status, file_type, md5_checksum = cur.fetchone()
            assert status == "DOWNLOADING"
            assert file_type == "BASELINE"
            assert md5_checksum == "123"

    # Test LOADING state
    adapter.manage_load_state(file_name, "LOADING")
    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, load_start_timestamp IS NOT NULL FROM _pubmed_load_history WHERE file_name = %s",
                (file_name,),
            )
            status, has_start_timestamp = cur.fetchone()
            assert status == "LOADING"
            assert has_start_timestamp

    # Test COMPLETE state
    adapter.manage_load_state(file_name, "COMPLETE", records_processed=100)
    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, records_processed, load_end_timestamp IS NOT NULL FROM _pubmed_load_history WHERE file_name = %s",
                (file_name,),
            )
            status, records_processed, has_end_timestamp = cur.fetchone()
            assert status == "COMPLETE"
            assert records_processed == 100
            assert has_end_timestamp


def test_load_history_queries(db_conn_str: str):
    """Test queries against the _pubmed_load_history table."""
    adapter = PostgresAdapter(db_conn_str)
    adapter.initialize_schema(mode="FULL")

    # Populate with some data
    adapter.manage_load_state("file1.xml.gz", "COMPLETE", file_type="BASELINE")
    adapter.manage_load_state("file2.xml.gz", "FAILED", file_type="UPDATE")
    adapter.manage_load_state("file3.xml.gz", "PENDING", file_type="UPDATE")
    adapter.manage_load_state("file4.xml.gz", "COMPLETE", file_type="UPDATE")

    # Test get_completed_files
    completed_files = adapter.get_completed_files()
    assert set(completed_files) == {"file1.xml.gz", "file4.xml.gz"}

    # Test has_completed_baseline
    assert adapter.has_completed_baseline() is True

    # Test reset_failed_files
    reset_count = adapter.reset_failed_files()
    assert reset_count == 1
    with adapter._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM _pubmed_load_history WHERE file_name = 'file2.xml.gz'"
            )
            assert cur.fetchone()[0] == "PENDING"
