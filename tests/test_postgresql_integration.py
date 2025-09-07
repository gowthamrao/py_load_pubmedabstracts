import pytest
from testcontainers.postgres import PostgresContainer
import psycopg
import json

from py_load_pubmedabstracts.db.postgresql import PostgresAdapter

# Mark all tests in this file as 'integration'
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container():
    """Starts a PostgreSQL container for the test session."""
    with PostgresContainer("postgres:15-alpine") as container:
        yield container


@pytest.fixture
def db_adapter(postgres_container: PostgresContainer) -> PostgresAdapter:
    """Initializes the PostgresAdapter with the container's connection string."""
    dsn = postgres_container.get_connection_url()
    adapter = PostgresAdapter(dsn=dsn)
    # Ensure the schema is clean for each test
    adapter.initialize_schema(mode="FULL")
    return adapter


def test_initialize_schema(db_adapter: PostgresAdapter):
    """Tests that the initial schema and tables are created correctly."""
    with psycopg.connect(db_adapter.dsn) as conn:
        with conn.cursor() as cur:
            # Check for the history table
            cur.execute("SELECT to_regclass('_pubmed_load_history');")
            assert cur.fetchone()[0] == '_pubmed_load_history'
            # Check for the main citations table
            cur.execute("SELECT to_regclass('citations_json');")
            assert cur.fetchone()[0] == 'citations_json'


def test_create_staging_table(db_adapter: PostgresAdapter):
    """Tests that the staging table is created correctly."""
    db_adapter.create_staging_tables()
    with psycopg.connect(db_adapter.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('_staging_citations_json');")
            assert cur.fetchone()[0] == '_staging_citations_json'


def test_bulk_load_chunk_to_staging(db_adapter: PostgresAdapter):
    """
    Tests the full flow of creating a staging table and bulk-loading a chunk of data.
    """
    # 1. Create the staging table
    db_adapter.create_staging_tables()

    # 2. Define some test data mimicking the parser's output
    test_data = [
        {
            "pmid": 101,
            "date_revised": "2023-01-01",
            "data": json.dumps({"MedlineCitation": {"PMID": "101"}}),
        },
        {
            "pmid": 102,
            "date_revised": "2023-01-02",
            "data": json.dumps({"MedlineCitation": {"PMID": "102"}}),
        },
    ]

    # 3. Load the data
    db_adapter.bulk_load_chunk(
        data_chunk=iter(test_data), target_table="_staging_citations_json"
    )

    # 4. Verify the data was loaded correctly
    with psycopg.connect(db_adapter.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pmid, date_revised, data->'MedlineCitation'->>'PMID' FROM _staging_citations_json ORDER BY pmid;")
            rows = cur.fetchall()
            assert len(rows) == 2

            # Verify first row
            assert rows[0][0] == 101
            assert str(rows[0][1]) == "2023-01-01"
            assert rows[0][2] == "101"

            # Verify second row
            assert rows[1][0] == 102
            assert str(rows[1][1]) == "2023-01-02"
            assert rows[1][2] == "102"
