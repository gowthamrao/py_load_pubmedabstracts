"""PostgreSQL database adapter."""
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List

import psycopg
from psycopg import sql

from .base import DatabaseAdapter


class PostgresAdapter(DatabaseAdapter):
    """PostgreSQL database adapter."""

    def __init__(self, dsn: str):
        """
        Initialize the adapter.

        Args:
            dsn: The database connection string.

        """
        self.dsn = dsn
        self.normalized_tables = [
            "journals",
            "authors",
            "mesh_terms",
            "citations",
            "citation_authors",
            "citation_mesh_terms",
        ]

    @contextmanager
    def _get_connection(self):
        """Provide a connection from the connection pool."""
        conn = psycopg.connect(self.dsn)
        try:
            yield conn
        finally:
            conn.close()

    def initialize_schema(self, mode: str) -> None:
        """Create the database schema."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                """
                )
                if mode in ("FULL", "BOTH"):
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS citations_json (
                            pmid INTEGER PRIMARY KEY,
                            date_revised DATE,
                            data JSONB NOT NULL
                        );
                    """
                    )
                if mode in ("NORMALIZED", "BOTH"):
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS journals (
                            issn TEXT PRIMARY KEY, title TEXT, iso_abbreviation TEXT
                        );
                        CREATE TABLE IF NOT EXISTS authors (
                            author_id BIGINT PRIMARY KEY, last_name TEXT,
                            fore_name TEXT, initials TEXT
                        );
                        CREATE TABLE IF NOT EXISTS mesh_terms (
                            mesh_id BIGINT PRIMARY KEY, term TEXT,
                            is_major_topic BOOLEAN
                        );
                        CREATE TABLE IF NOT EXISTS citations (
                            pmid INTEGER PRIMARY KEY, title TEXT, abstract TEXT,
                            publication_date DATE,
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
                    """
                    )
            conn.commit()

    def create_staging_tables(self, mode: str) -> None:
        """Create temporary, unlogged tables for high-speed data loading."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if mode in ("FULL", "BOTH"):
                    cur.execute(
                        """
                        DROP TABLE IF EXISTS _staging_citations_json;
                        CREATE UNLOGGED TABLE _staging_citations_json (
                            pmid INTEGER, date_revised DATE, data JSONB
                        );
                    """
                    )
                if mode in ("NORMALIZED", "BOTH"):
                    for table in self.normalized_tables:
                        cur.execute(f"DROP TABLE IF EXISTS _staging_{table};")
                        cur.execute(
                            f"CREATE TABLE _staging_{table} "
                            f"(LIKE {table} INCLUDING DEFAULTS);"
                        )
                        cur.execute(f"ALTER TABLE _staging_{table} SET UNLOGGED;")
            conn.commit()

    def bulk_load_chunk(self, data_chunk: Dict[str, List[Any]]) -> None:
        """Load chunks of Pydantic models into corresponding staging tables."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                for table_name, records in data_chunk.items():
                    if not records:
                        continue

                    staging_table_name = f"_staging_{table_name}"
                    model_instance = records[0]
                    columns = list(model_instance.model_fields.keys())

                    copy_sql = (
                        f"COPY {staging_table_name} ({','.join(columns)}) FROM STDIN"
                    )
                    with cur.copy(copy_sql) as copy:
                        for record_model in records:
                            record_dict = record_model.model_dump()
                            if "data" in record_dict and isinstance(
                                record_dict["data"], dict
                            ):
                                record_dict["data"] = json.dumps(record_dict["data"])
                            copy.write_row([record_dict[col] for col in columns])
            conn.commit()

    def process_deletions(self, pmid_list: List[int], mode: str) -> None:
        """Remove specified PMIDs from the final tables."""
        if not pmid_list:
            return

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if mode in ("FULL", "BOTH"):
                    cur.execute(
                        "DELETE FROM citations_json WHERE pmid = ANY(%s)", (pmid_list,)
                    )

                if mode in ("NORMALIZED", "BOTH"):
                    cur.execute(
                        "DELETE FROM citations WHERE pmid = ANY(%s)", (pmid_list,)
                    )

                print(f"Processed {cur.rowcount} deletions for mode '{mode}'.")
            conn.commit()

    def execute_merge_strategy(
        self, mode: str, is_initial_load: bool = False
    ) -> None:
        """Move data from staging to final tables using UPSERT logic."""
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
            cur.execute(
                "INSERT INTO citations_json SELECT * FROM _staging_citations_json;"
            )
        else:
            cur.execute(
                """
                INSERT INTO citations_json (pmid, date_revised, data)
                SELECT pmid, date_revised, data FROM _staging_citations_json
                ON CONFLICT (pmid) DO UPDATE SET
                    date_revised = EXCLUDED.date_revised,
                    data = EXCLUDED.data;
            """
            )
        cur.execute("DROP TABLE _staging_citations_json;")

    def _execute_merge_normalized(self, cur, is_initial_load: bool):
        """Merge logic for the 'NORMALIZED' representation."""
        for table in ["journals", "authors", "mesh_terms"]:
            pk_map = {
                "journals": "issn",
                "authors": "author_id",
                "mesh_terms": "mesh_id",
            }
            pk = pk_map[table]
            cur.execute(
                f"""
                INSERT INTO {table} SELECT * FROM _staging_{table}
                ON CONFLICT ({pk}) DO NOTHING;
            """
            )

        if is_initial_load:
            cur.execute("INSERT INTO citations SELECT * FROM _staging_citations;")
        else:
            cur.execute(
                """
                INSERT INTO citations (
                    pmid, title, abstract, publication_date, journal_issn
                )
                SELECT pmid, title, abstract, publication_date, journal_issn
                FROM _staging_citations
                ON CONFLICT (pmid) DO UPDATE SET
                    title = EXCLUDED.title,
                    abstract = EXCLUDED.abstract,
                    publication_date = EXCLUDED.publication_date,
                    journal_issn = EXCLUDED.journal_issn;
            """
            )
        cur.execute(
            """
            INSERT INTO citation_authors (pmid, author_id, display_order)
            SELECT pmid, author_id, display_order FROM _staging_citation_authors
            ON CONFLICT (pmid, author_id) DO UPDATE SET
                display_order = EXCLUDED.display_order;
        """
        )
        cur.execute(
            """
            INSERT INTO citation_mesh_terms (pmid, mesh_id)
            SELECT pmid, mesh_id FROM _staging_citation_mesh_terms
            ON CONFLICT (pmid, mesh_id) DO NOTHING;
        """
        )

        for table in self.normalized_tables:
            cur.execute(f"DROP TABLE _staging_{table};")

    def manage_load_state(
        self,
        file_name: str,
        status: str,
        file_type: str | None = None,
        md5_checksum: str | None = None,
        records_processed: int | None = None,
    ) -> None:
        """Insert or update the state of a file in the load history table."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                now = datetime.now(timezone.utc)

                # Ensure a record exists. If not, create one with PENDING status.
                # This is important for subsequent updates.
                insert_query = sql.SQL(
                    """
                    INSERT INTO _pubmed_load_history (file_name, file_type, status)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (file_name) DO NOTHING;
                """
                )
                # Use a placeholder for file_type if not provided
                # to avoid issues with ON CONFLICT
                effective_file_type = file_type or "UNKNOWN"
                cur.execute(insert_query, [file_name, effective_file_type, "PENDING"])

                # Now, update the record with the new status and details
                set_clauses = [sql.SQL("status = %s")]
                params = [status]

                if file_type is not None:
                    set_clauses.append(sql.SQL("file_type = %s"))
                    params.append(file_type)
                if md5_checksum is not None:
                    set_clauses.append(sql.SQL("md5_checksum = %s"))
                    params.append(md5_checksum)
                if status == "DOWNLOADING":
                    set_clauses.append(sql.SQL("download_timestamp = %s"))
                    params.append(now)
                if status == "LOADING":
                    set_clauses.append(sql.SQL("load_start_timestamp = %s"))
                    params.append(now)
                if status in ("COMPLETE", "FAILED"):
                    set_clauses.append(sql.SQL("load_end_timestamp = %s"))
                    params.append(now)
                if records_processed is not None:
                    set_clauses.append(sql.SQL("records_processed = %s"))
                    params.append(records_processed)

                params.append(file_name)
                update_query = sql.SQL(
                    "UPDATE _pubmed_load_history SET {} WHERE file_name = %s"
                ).format(sql.SQL(", ").join(set_clauses))
                cur.execute(update_query, params)
            conn.commit()

    def optimize_database(self, stage: str, mode: str) -> None:
        """Optimize the database for bulk loading."""
        if stage not in ("pre-load", "post-load"):
            return
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                if stage == "pre-load":
                    print(f"Optimizing for pre-load (mode: {mode})...")
                    if mode in ("FULL", "BOTH"):
                        cur.execute("ALTER TABLE IF EXISTS citations_json "
                                    "DROP CONSTRAINT IF EXISTS citations_json_pkey;")
                    if mode in ("NORMALIZED", "BOTH"):
                        cur.execute("ALTER TABLE IF EXISTS citation_authors "
                                    "DROP CONSTRAINT IF EXISTS citation_authors_pkey;")
                        cur.execute("ALTER TABLE IF EXISTS citation_mesh_terms "
                                    "DROP CONSTRAINT IF EXISTS "
                                    "citation_mesh_terms_pkey;")
                        cur.execute("ALTER TABLE IF EXISTS citations "
                                    "DROP CONSTRAINT IF EXISTS citations_pkey;")
                elif stage == "post-load":
                    print(f"Optimizing for post-load (mode: {mode})...")
                    if mode in ("FULL", "BOTH"):
                        cur.execute("ALTER TABLE IF EXISTS citations_json "
                                    "ADD CONSTRAINT citations_json_pkey "
                                    "PRIMARY KEY (pmid);")
                    if mode in ("NORMALIZED", "BOTH"):
                        cur.execute("ALTER TABLE IF EXISTS citations "
                                    "ADD CONSTRAINT citations_pkey "
                                    "PRIMARY KEY (pmid);")
                        cur.execute("ALTER TABLE IF EXISTS citation_authors "
                                    "ADD CONSTRAINT citation_authors_pkey "
                                    "PRIMARY KEY (pmid, author_id);")
                        cur.execute("ALTER TABLE IF EXISTS citation_mesh_terms "
                                    "ADD CONSTRAINT citation_mesh_terms_pkey "
                                    "PRIMARY KEY (pmid, mesh_id);")

            conn.commit()

    def reset_failed_files(self) -> int:
        """Reset the status of 'FAILED' files to 'PENDING'."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE _pubmed_load_history "
                    "SET status = 'PENDING' WHERE status = 'FAILED'"
                )
                conn.commit()
                return cur.rowcount

    def get_completed_files(self) -> List[str]:
        """Get a list of successfully processed file names."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT file_name FROM _pubmed_load_history "
                    "WHERE status = 'COMPLETE'"
                )
                return [row[0] for row in cur.fetchall()]

    def has_completed_baseline(self) -> bool:
        """Check if at least one baseline file has the 'COMPLETE' status."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM _pubmed_load_history WHERE "
                    "status = 'COMPLETE' AND file_type = 'BASELINE' LIMIT 1"
                )
                return cur.fetchone() is not None
