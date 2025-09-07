import psycopg
from typing import List, Generator, Optional
from contextlib import contextmanager

from .base import DatabaseAdapter


class PostgresAdapter(DatabaseAdapter):
    """PostgreSQL database adapter."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    @contextmanager
    def _get_connection(self):
        """Provides a connection from the connection pool."""
        conn = psycopg.connect(self.dsn)
        try:
            yield conn
        finally:
            conn.close()

    def initialize_schema(self, mode: str) -> None:
        """Creates the database schema."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Create the load history table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS _pubmed_load_history (
                        file_name TEXT PRIMARY KEY,
                        file_type TEXT NOT NULL,
                        md5_checksum TEXT,
                        download_timestamp TIMESTAMPTZ,
                        load_start_timestamp TIMESTAMPTZ,
                        load_end_timestamp TIMESTAMPTZ,
                        status TEXT NOT NULL,
                        records_processed INTEGER
                    );
                """)

                # Create the citations_json table for the 'FULL' representation
                if mode in ("FULL", "BOTH"):
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS citations_json (
                            pmid INTEGER PRIMARY KEY,
                            date_revised DATE NOT NULL,
                            data JSONB NOT NULL
                        );
                    """)

                # Placeholder for 'NORMALIZED' representation
                if mode in ("NORMALIZED", "BOTH"):
                    # Here we would create the normalized tables as per the FRD
                    # This is left as a placeholder for now.
                    pass
            conn.commit()

    def create_staging_tables(self) -> None:
        """Creates temporary, unlogged tables for high-speed data loading."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Staging table for the 'FULL' representation
                cur.execute("""
                    DROP TABLE IF EXISTS _staging_citations_json;
                    CREATE UNLOGGED TABLE _staging_citations_json (
                        pmid INTEGER,
                        date_revised DATE,
                        data JSONB
                    );
                """)
                # In the future, a staging table for 'NORMALIZED' would go here
            conn.commit()

    def bulk_load_chunk(
        self, data_chunk: Generator[dict, None, None], target_table: str
    ) -> None:
        """
        Loads a chunk of data into a staging table using PostgreSQL's COPY command.

        Args:
            data_chunk: A generator yielding dictionaries for each row.
            target_table: The name of the staging table to load into.
        """
        if target_table != "_staging_citations_json":
            raise ValueError(f"Unsupported staging table: {target_table}")

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                with cur.copy(f"COPY {target_table} (pmid, date_revised, data) FROM STDIN") as copy:
                    for record in data_chunk:
                        # psycopg3 can automatically adapt the dict to the COPY row format
                        copy.write_row(
                            (
                                record["pmid"],
                                record["date_revised"],
                                record["data"],
                            )
                        )
            conn.commit()

    def process_deletions(self, pmid_list: List[int]) -> None:
        raise NotImplementedError

    def execute_merge_strategy(self) -> None:
        raise NotImplementedError

    def manage_load_state(
        self, file_name: str, status: str, checksum: Optional[str] = None
    ) -> None:
        raise NotImplementedError

    def optimize_database(self, stage: str) -> None:
        raise NotImplementedError

    def get_completed_files(self) -> List[str]:
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT file_name FROM _pubmed_load_history WHERE status = 'COMPLETE'")
                # mypy check: cur.fetchall() returns a list of tuples
                return [row[0] for row in cur.fetchall()]
