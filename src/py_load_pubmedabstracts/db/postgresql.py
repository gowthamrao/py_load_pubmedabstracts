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
        """Removes specified PMIDs from the citations_json table."""
        if not pmid_list:
            return

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Use = ANY(%s) for efficient deletion of a list of IDs
                cur.execute(
                    "DELETE FROM citations_json WHERE pmid = ANY(%s)",
                    (pmid_list,),  # Pass the list as a tuple for the adapter
                )
                # Optionally, log the number of rows affected.
                # In a real app, you'd use a proper logger.
                print(f"Processed {cur.rowcount} deletions.")
            conn.commit()

    def execute_merge_strategy(self, is_initial_load: bool = False) -> None:
        """
        Moves data from staging to final tables.
        If is_initial_load is True, performs a simple INSERT.
        Otherwise, performs an UPSERT (INSERT ... ON CONFLICT).
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if is_initial_load:
                    # For the initial load, we assume no conflicts and just insert.
                    # This is used after dropping the PK for performance.
                    cur.execute("""
                        INSERT INTO citations_json (pmid, date_revised, data)
                        SELECT pmid, date_revised, data FROM _staging_citations_json;
                    """)
                else:
                    # For subsequent loads, we use ON CONFLICT to handle updates.
                    cur.execute("""
                        INSERT INTO citations_json (pmid, date_revised, data)
                        SELECT pmid, date_revised, data FROM _staging_citations_json
                        ON CONFLICT (pmid) DO UPDATE SET
                            date_revised = EXCLUDED.date_revised,
                            data = EXCLUDED.data;
                    """)
                # Clean up the staging table after merge
                cur.execute("DROP TABLE _staging_citations_json;")
            conn.commit()

    def manage_load_state(
        self,
        file_name: str,
        status: str,
        file_type: str | None = None,
        md5_checksum: str | None = None,
        records_processed: int | None = None,
    ) -> None:
        """
        Inserts or updates the state of a file in the _pubmed_load_history table.
        """
        from datetime import datetime, timezone
        from psycopg import sql

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                now = datetime.now(timezone.utc)

                if status == "DOWNLOADING":
                    # This is the initial state, so we INSERT a new record.
                    # ON CONFLICT ensures that if we re-run for a file that already
                    # exists, we just update its status without creating a duplicate.
                    if not file_type or not md5_checksum:
                        raise ValueError("file_type and md5_checksum are required for initial state.")

                    query = sql.SQL("""
                        INSERT INTO _pubmed_load_history (file_name, file_type, md5_checksum, download_timestamp, status)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (file_name) DO UPDATE SET
                            status = EXCLUDED.status,
                            download_timestamp = EXCLUDED.download_timestamp;
                    """)
                    cur.execute(query, [file_name, file_type, md5_checksum, now, status])
                else:
                    # For subsequent states, we UPDATE the existing record.
                    set_clauses = [sql.SQL("status = %s")]
                    params = [status]

                    if status == "LOADING":
                        set_clauses.append(sql.SQL("load_start_timestamp = %s"))
                        params.append(now)

                    if status in ("COMPLETE", "FAILED"):
                        set_clauses.append(sql.SQL("load_end_timestamp = %s"))
                        params.append(now)

                    if status == "COMPLETE":
                        if records_processed is None:
                            raise ValueError("records_processed is required for COMPLETE state.")
                        set_clauses.append(sql.SQL("records_processed = %s"))
                        params.append(records_processed)

                    params.append(file_name)

                    query = sql.SQL("UPDATE _pubmed_load_history SET {} WHERE file_name = %s").format(
                        sql.SQL(", ").join(set_clauses)
                    )
                    cur.execute(query, params)
            conn.commit()

    def optimize_database(self, stage: str) -> None:
        """
        Optimizes the database for bulk loading.

        'pre-load': Drops constraints and indexes that would slow down inserts.
        'post-load': Recreates the constraints and indexes.

        Warning: This is intended for an initial baseline load into an empty
        table. The `execute_merge_strategy` with `ON CONFLICT` will fail if
        the primary key constraint is missing.
        """
        if stage not in ("pre-load", "post-load"):
            raise ValueError("Stage must be either 'pre-load' or 'post-load'")

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if stage == "pre-load":
                    print("Optimizing for pre-load: Dropping primary key on citations_json...")
                    cur.execute("ALTER TABLE IF EXISTS citations_json DROP CONSTRAINT IF EXISTS citations_json_pkey;")
                elif stage == "post-load":
                    print("Optimizing for post-load: Recreating primary key on citations_json...")
                    cur.execute("ALTER TABLE IF EXISTS citations_json ADD CONSTRAINT citations_json_pkey PRIMARY KEY (pmid);")
            conn.commit()

    def get_completed_files(self) -> List[str]:
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT file_name FROM _pubmed_load_history WHERE status = 'COMPLETE'")
                # mypy check: cur.fetchall() returns a list of tuples
                return [row[0] for row in cur.fetchall()]
