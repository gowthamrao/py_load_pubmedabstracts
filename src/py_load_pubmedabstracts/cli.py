import logging
import os
from typing import Optional

import typer

from .config import Settings
from .db.factory import get_adapter
from .ftp_client import NLMFTPClient
from .logging_config import configure_logging
from .parser import parse_pubmed_xml

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.callback()
def main():
    """
    Configure logging for all commands.
    """
    configure_logging()


@app.command()
def initialize_db() -> None:
    """Initializes the database schema and state tracking tables."""
    settings = Settings()
    logger.info("Initializing database.", extra={"mode": settings.load_mode})
    try:
        adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
        adapter.initialize_schema(mode=settings.load_mode)
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.exception("Error initializing database.", exc_info=e)
        raise typer.Exit(code=1)


@app.command()
def list_remote_files(
    baseline: bool = typer.Option(True, "--baseline/--no-baseline", help="List baseline files."),
    updates: bool = typer.Option(True, "--updates/--no-updates", help="List update files."),
) -> None:
    """Lists available baseline and/or daily update files from the NLM FTP server."""
    client = NLMFTPClient()
    logger.info("Connecting to NLM FTP server to list files...")
    try:
        if baseline:
            baseline_files = client.list_baseline_files()
            logger.info("Available baseline files.", extra={"count": len(baseline_files), "files": baseline_files})
        if updates:
            update_files = client.list_update_files()
            logger.info("Available update files.", extra={"count": len(update_files), "files": update_files})
        logger.info("Successfully retrieved file lists.")
    except Exception as e:
        logger.exception("Error listing remote files.", exc_info=e)
        raise typer.Exit(code=1)


@app.command()
def check_status() -> None:
    """Displays the current state of the loaded files from the load history table."""
    settings = Settings()
    logger.info("Checking load status.", extra={"adapter": settings.db_adapter})
    try:
        adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
        completed_files = adapter.get_completed_files()
        if not completed_files:
            logger.info("No files have been successfully processed yet.")
        else:
            logger.info(
                "Found completed files.",
                extra={"count": len(completed_files), "files": completed_files},
            )
    except Exception as e:
        logger.exception("Error checking status.", exc_info=e)
        raise typer.Exit(code=1)


@app.command()
def reset_failed() -> None:
    """Resets the status of FAILED files in the state table for reprocessing."""
    settings = Settings()
    logger.info("Connecting to the database to reset failed files...")
    try:
        adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
        num_reset = adapter.reset_failed_files()
        if num_reset > 0:
            logger.warning(f"Reset status for {num_reset} failed file(s) to 'PENDING'.")
        else:
            logger.info("No failed files found to reset.")
    except Exception as e:
        logger.exception("Error resetting failed files.", exc_info=e)
        raise typer.Exit(code=1)


def _get_files_to_process(client: NLMFTPClient, adapter, file_type: str) -> list:
    """Helper to get the list of files that need to be processed."""
    logger.info(f"Fetching remote {file_type} file list...")
    list_func = client.list_baseline_files if file_type == "baseline" else client.list_update_files
    remote_files = list_func()

    logger.info("Fetching list of completed files from the database...")
    completed_files = set(adapter.get_completed_files())

    files_to_process = sorted(
        [f for f in remote_files if f[0] not in completed_files], key=lambda x: x[0]
    )
    return files_to_process


@app.command()
def run_baseline(
    limit: Optional[int] = typer.Option(None, "-l", help="Limit number of files."),
    initial_load: bool = typer.Option(False, help="Use optimizations for empty DB."),
    chunk_size: int = typer.Option(20000, help="Records per chunk."),
) -> None:
    """Runs the full baseline load process."""
    settings = Settings()
    logger.info("Starting baseline load.", extra={"mode": settings.load_mode, "initial_load": initial_load})

    adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
    client = NLMFTPClient()

    try:
        files_to_process = _get_files_to_process(client, adapter, "baseline")
        if not files_to_process:
            logger.info("No new baseline files to process.")
            return

        logger.info(f"Found {len(files_to_process)} new baseline files to process.")
        if limit:
            files_to_process = files_to_process[:limit]
            logger.info(f"Processing a maximum of {limit} file(s).")

        if initial_load:
            adapter.optimize_database(stage="pre-load", mode=settings.load_mode)

        for data_filename, md5_filename in files_to_process:
            _process_single_file(
                client=client,
                adapter=adapter,
                settings=settings,
                file_info=(data_filename, md5_filename),
                file_type="BASELINE",
                chunk_size=chunk_size,
                is_initial_load=initial_load,
            )

        if initial_load:
            adapter.optimize_database(stage="post-load", mode=settings.load_mode)

        logger.info("Baseline run finished.")
    except Exception as e:
        logger.exception("Critical error during baseline run.", exc_info=e)
        raise typer.Exit(code=1)


@app.command()
def run_delta(
    limit: Optional[int] = typer.Option(None, "-l", help="Limit number of files."),
    chunk_size: int = typer.Option(20000, help="Records per chunk."),
) -> None:
    """Runs the delta load process for daily update files."""
    settings = Settings()
    logger.info("Starting delta load.", extra={"mode": settings.load_mode})

    adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
    client = NLMFTPClient()

    if not adapter.has_completed_baseline():
        logger.error("Baseline must be loaded before deltas can be processed.")
        raise typer.Exit(code=1)

    try:
        files_to_process = _get_files_to_process(client, adapter, "update")
        if not files_to_process:
            logger.info("No new update files to process.")
            return

        logger.info(f"Found {len(files_to_process)} new update files to process.")
        if limit:
            files_to_process = files_to_process[:limit]
            logger.info(f"Processing a maximum of {limit} file(s).")

        for data_filename, md5_filename in files_to_process:
            try:
                _process_single_file(
                    client=client,
                    adapter=adapter,
                    settings=settings,
                    file_info=(data_filename, md5_filename),
                    file_type="DELTA",
                    chunk_size=chunk_size,
                    is_initial_load=False,
                )
            except Exception as e:
                logger.exception(
                    f"Error processing {data_filename}. Aborting delta run to ensure sequential processing.",
                    exc_info=e,
                )
                raise typer.Exit(code=1)

        logger.info("Delta run finished.")
    except Exception as e:
        logger.exception("Critical error during delta run.", exc_info=e)
        raise typer.Exit(code=1)


def _process_single_file(client, adapter, settings, file_info, file_type, chunk_size, is_initial_load):
    """Helper function to process one file (baseline or delta)."""
    data_filename, md5_filename = file_info
    local_path = ""
    total_records = 0
    log_extra = {"file_name": data_filename, "file_type": file_type}

    try:
        logger.info("Processing file.", extra=log_extra)

        remote_dir = client.BASELINE_DIR if file_type == "BASELINE" else client.UPDATE_DIR
        md5_checksum = client.get_remote_checksum(remote_dir, md5_filename)
        log_extra["md5_checksum"] = md5_checksum

        adapter.manage_load_state(
            file_name=data_filename, status="DOWNLOADING", file_type=file_type, md5_checksum=md5_checksum
        )
        logger.info("Downloading and verifying file.", extra=log_extra)
        local_path = client.download_and_verify_file(
            remote_dir, data_filename, md5_filename, settings.local_staging_dir
        )

        adapter.manage_load_state(file_name=data_filename, status="LOADING")
        adapter.create_staging_tables(mode=settings.load_mode)
        logger.info("Staging tables created.", extra=log_extra)

        parser_gen = parse_pubmed_xml(
            local_path, load_mode=settings.load_mode, chunk_size=chunk_size
        )

        for op_type, chunk_data in parser_gen:
            if op_type == "UPSERT":
                num_records = next((len(v) for v in chunk_data.values() if v), 0)
                logger.info(f"Staging {num_records} upserts...", extra=log_extra)
                adapter.bulk_load_chunk(data_chunk=chunk_data)
                total_records += num_records
            elif op_type == "DELETE":
                pmids_to_delete = chunk_data.get("pmids", [])
                logger.info(f"Processing {len(pmids_to_delete)} deletions...", extra=log_extra)
                adapter.process_deletions(pmid_list=pmids_to_delete, mode=settings.load_mode)
                total_records += len(pmids_to_delete)

        logger.info("Merging data into final tables...", extra=log_extra)
        adapter.execute_merge_strategy(mode=settings.load_mode, is_initial_load=is_initial_load)

        adapter.manage_load_state(
            file_name=data_filename, status="COMPLETE", records_processed=total_records
        )
        log_extra["records_processed"] = total_records
        logger.info("Successfully processed file.", extra=log_extra)

    except Exception as e:
        adapter.manage_load_state(file_name=data_filename, status="FAILED")
        logger.exception(f"Failed to process {data_filename}. Marked as FAILED.", exc_info=e, extra=log_extra)
        raise

    finally:
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            logger.info(f"Cleaned up local file: {local_path}", extra=log_extra)


if __name__ == "__main__":
    app()
