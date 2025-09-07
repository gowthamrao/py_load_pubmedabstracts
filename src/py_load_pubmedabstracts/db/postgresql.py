import psycopg
from typing import List, Dict, Any
from contextlib import contextmanager

from .base import DatabaseAdapter


class PostgresAdapter(DatabaseAdapter):
    """PostgreSQL database adapter."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.normalized_tables = [
            "journals", "authors", "mesh_terms", "citations",
            "citation_authors", "citation_mesh_terms"
        ]

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
                if mode in ("FULL", "BOTH"):
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS citations_json (
                            pmid INTEGER PRIMARY KEY,
                            date_revised DATE,
                            data JSONB NOT NULL
                        );
                    """)
                if mode in ("NORMALIZED", "BOTH"):
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS journals (
                            issn TEXT PRIMARY KEY, title TEXT, iso_abbreviation TEXT
                        );
                        CREATE TABLE IF NOT EXISTS authors (
                            author_id BIGINT PRIMARY KEY, last_name TEXT, fore_name TEXT, initials TEXT
                        );
                        CREATE TABLE IF NOT EXISTS mesh_terms (
                            mesh_id BIGINT PRIMARY KEY, term TEXT, is_major_topic BOOLEAN
                        );
                        CREATE TABLE IF NOT EXISTS citations (
                            pmid INTEGER PRIMARY KEY, title TEXT, abstract TEXT, publication_date DATE,
                            journal_issn TEXT REFERENCES journals(issn)
                        );
                        CREATE TABLE IF NOT EXISTS citation_authors (
                            pmid INTEGER REFERENCES citations(pmid) ON DELETE CASCADE,
                            author_id BIGINT REFERENCES authors(author_id),
                            display_order INTEGER, PRIMARY KEY (pmid, author_id)
                        );
                        CREATE TABLE IF NOT EXISTS citation_mesh_terms (
                            pmid INTEGER REFERENCES citations(pmid) ON DELETE CASCADE,
                            mesh_id BIGINT REFERENCES mesh_terms(mesh_id),
                            PRIMARY KEY (pmid, mesh_id)
                        );
                    """)
            conn.commit()

    def create_staging_tables(self, mode: str) -> None:
        """Creates temporary, unlogged tables for high-speed data loading."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if mode in ("FULL", "BOTH"):
                    cur.execute("""
                        DROP TABLE IF EXISTS _staging_citations_json;
                        CREATE UNLOGGED TABLE _staging_citations_json (
                            pmid INTEGER, date_revised DATE, data JSONB
                        );
                    """)
                if mode in ("NORMALIZED", "BOTH"):
                    for table in self.normalized_tables:
                        cur.execute(f"DROP TABLE IF EXISTS _staging_{table};")
                        cur.execute(f"CREATE TABLE _staging_{table} (LIKE {table} INCLUDING DEFAULTS);")
                        cur.execute(f"ALTER TABLE _staging_{table} SET UNLOGGED;")
            conn.commit()

    def bulk_load_chunk(self, data_chunk: Dict[str, List[Dict[str, Any]]]) -> None:
        """Loads chunks of data into corresponding staging tables."""
        import json

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                for table_name, records in data_chunk.items():
                    if not records:
                        continue

                    staging_table_name = f"_staging_{table_name}"
                    columns = list(records[0].keys())

                    # Special handling for JSONB column
                    if table_name == "citations_json":
                        for record in records:
                            record["data"] = json.dumps(record["data"])

                    with cur.copy(f"COPY {staging_table_name} ({','.join(columns)}) FROM STDIN") as copy:
                        for record in records:
                            copy.write_row([record[col] for col in columns])
            conn.commit()

    def process_deletions(self, pmid_list: List[int], mode: str) -> None:
        """Removes specified PMIDs from the final tables."""
        if not pmid_list:
            return

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if mode in ("FULL", "BOTH"):
                    cur.execute("DELETE FROM citations_json WHERE pmid = ANY(%s)", (pmid_list,))

                if mode in ("NORMALIZED", "BOTH"):
                    # ON DELETE CASCADE on junction tables handles those.
                    # We just need to delete from the main citations table.
                    cur.execute("DELETE FROM citations WHERE pmid = ANY(%s)", (pmid_list,))

                print(f"Processed {cur.rowcount} deletions for mode '{mode}'.")
            conn.commit()

    def execute_merge_strategy(self, mode: str, is_initial_load: bool = False) -> None:
        """Moves data from staging to final tables using UPSERT logic."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if mode in ("FULL", "BOTH"):
                    self._execute_merge_full(cur, is_initial_load)
                if mode in ("NORMALIZED", "BOTH"):
                    self._execute_merge_normalized(cur, is_initial_load)
            conn.commit()

    def _execute_merge_full(self, cur, is_initial_load: bool):
        """Merge logic for the 'FULL' (JSONB) representation."""
        if is_initial_load:
            cur.execute("INSERT INTO citations_json SELECT * FROM _staging_citations_json;")
        else:
            cur.execute("""
                INSERT INTO citations_json (pmid, date_revised, data)
                SELECT pmid, date_revised, data FROM _staging_citations_json
                ON CONFLICT (pmid) DO UPDATE SET
                    date_revised = EXCLUDED.date_revised,
                    data = EXCLUDED.data;
            """)
        cur.execute("DROP TABLE _staging_citations_json;")

    def _execute_merge_normalized(self, cur, is_initial_load: bool):
        """Merge logic for the 'NORMALIZED' representation."""
        # 1. Merge dimension tables (no updates needed, just insert new ones)
        for table in ["journals", "authors", "mesh_terms"]:
            pk = "issn" if table == "journals" else "author_id" if table == "authors" else "mesh_id"
            cur.execute(f"""
                INSERT INTO {table} SELECT * FROM _staging_{table}
                ON CONFLICT ({pk}) DO NOTHING;
            """)

        # 2. Merge the main citations table (handle updates)
        if is_initial_load:
            cur.execute("INSERT INTO citations SELECT * FROM _staging_citations;")
        else:
            cur.execute("""
                INSERT INTO citations (pmid, title, abstract, publication_date, journal_issn)
                SELECT pmid, title, abstract, publication_date, journal_issn FROM _staging_citations
                ON CONFLICT (pmid) DO UPDATE SET
                    title = EXCLUDED.title,
                    abstract = EXCLUDED.abstract,
                    publication_date = EXCLUDED.publication_date,
                    journal_issn = EXCLUDED.journal_issn;
            """)

        # 3. Merge junction tables (handle updates, e.g., display_order)
        cur.execute("""
            INSERT INTO citation_authors (pmid, author_id, display_order)
            SELECT pmid, author_id, display_order FROM _staging_citation_authors
            ON CONFLICT (pmid, author_id) DO UPDATE SET
                display_order = EXCLUDED.display_order;
        """)
        cur.execute("""
            INSERT INTO citation_mesh_terms (pmid, mesh_id)
            SELECT pmid, mesh_id FROM _staging_citation_mesh_terms
            ON CONFLICT (pmid, mesh_id) DO NOTHING;
        """)

        # 4. Clean up all staging tables
        for table in self.normalized_tables:
            cur.execute(f"DROP TABLE _staging_{table};")

    def manage_load_state(self, file_name: str, status: str, file_type: str | None = None,
                          md5_checksum: str | None = None, records_processed: int | None = None) -> None:
        """Inserts or updates the state of a file in the _pubmed_load_history table."""
        from datetime import datetime, timezone
        from psycopg import sql
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                now = datetime.now(timezone.utc)
                if status == "DOWNLOADING":
                    query = sql.SQL("""
                        INSERT INTO _pubmed_load_history (file_name, file_type, md5_checksum, download_timestamp, status)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (file_name) DO UPDATE SET
                            status = EXCLUDED.status, download_timestamp = EXCLUDED.download_timestamp;
                    """)
                    cur.execute(query, [file_name, file_type, md5_checksum, now, status])
                else:
                    set_clauses = [sql.SQL("status = %s")]
                    params = [status]
                    if status == "LOADING":
                        set_clauses.append(sql.SQL("load_start_timestamp = %s"))
                        params.append(now)
                    if status in ("COMPLETE", "FAILED"):
                        set_clauses.append(sql.SQL("load_end_timestamp = %s"))
                        params.append(now)
                    if status == "COMPLETE":
                        set_clauses.append(sql.SQL("records_processed = %s"))
                        params.append(records_processed)
                    params.append(file_name)
                    query = sql.SQL("UPDATE _pubmed_load_history SET {} WHERE file_name = %s").format(
                        sql.SQL(", ").join(set_clauses)
                    )
                    cur.execute(query, params)
            conn.commit()

    def optimize_database(self, stage: str, mode: str) -> None:
        """Optimizes the database for bulk loading."""
        if stage not in ("pre-load", "post-load"):
            return
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if stage == "pre-load":
                    print(f"Optimizing for pre-load (mode: {mode})...")
                    if mode in ("FULL", "BOTH"):
                        cur.execute("ALTER TABLE IF EXISTS citations_json DROP CONSTRAINT IF EXISTS citations_json_pkey;")
                    if mode in ("NORMALIZED", "BOTH"):
                        cur.execute("ALTER TABLE IF EXISTS citation_authors DROP CONSTRAINT IF EXISTS citation_authors_pkey;")
                        cur.execute("ALTER TABLE IF EXISTS citation_mesh_terms DROP CONSTRAINT IF EXISTS citation_mesh_terms_pkey;")
                        cur.execute("ALTER TABLE IF EXISTS citations DROP CONSTRAINT IF EXISTS citations_pkey;")
                elif stage == "post-load":
                    print(f"Optimizing for post-load (mode: {mode})...")
                    if mode in ("FULL", "BOTH"):
                        cur.execute("ALTER TABLE IF EXISTS citations_json ADD CONSTRAINT citations_json_pkey PRIMARY KEY (pmid);")
                    if mode in ("NORMALIZED", "BOTH"):
                        cur.execute("ALTER TABLE IF EXISTS citations ADD CONSTRAINT citations_pkey PRIMARY KEY (pmid);")
                        cur.execute("ALTER TABLE IF EXISTS citation_authors ADD CONSTRAINT citation_authors_pkey PRIMARY KEY (pmid, author_id);")
                        cur.execute("ALTER TABLE IF EXISTS citation_mesh_terms ADD CONSTRAINT citation_mesh_terms_pkey PRIMARY KEY (pmid, mesh_id);")

            conn.commit()

    def reset_failed_files(self) -> int:
        """Resets the status of 'FAILED' files to 'PENDING'."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE _pubmed_load_history SET status = 'PENDING' WHERE status = 'FAILED'")
                return cur.rowcount

    def get_completed_files(self) -> List[str]:
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT file_name FROM _pubmed_load_history WHERE status = 'COMPLETE'")
                return [row[0] for row in cur.fetchall()]

    def has_completed_baseline(self) -> bool:
        """Checks if at least one baseline file has the 'COMPLETE' status."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM _pubmed_load_history WHERE status = 'COMPLETE' AND file_type = 'BASELINE' LIMIT 1")
                return cur.fetchone() is not None
