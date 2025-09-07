import os
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .config import Settings
from .db.factory import get_adapter
from .ftp_client import NLMFTPClient
from .parser import parse_pubmed_xml

app = typer.Typer()
console = Console()


@app.command()
def initialize_db() -> None:
    """Initializes the database schema and state tracking tables."""
    settings = Settings()
    console.print(f"Initializing database for mode: [bold]{settings.load_mode}[/bold]...")
    try:
        adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
        adapter.initialize_schema(mode=settings.load_mode)
        console.print("[bold green]Database initialized successfully.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error initializing database: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def list_remote_files(
    baseline: bool = typer.Option(True, "--baseline/--no-baseline", help="List baseline files."),
    updates: bool = typer.Option(True, "--updates/--no-updates", help="List update files."),
) -> None:
    """Lists available baseline and/or daily update files from the NLM FTP server."""
    client = NLMFTPClient()
    # ... (rest of the function is unchanged)
    console.print("Connecting to NLM FTP server to list files...")
    try:
        if baseline:
            console.print("\n[bold cyan]Baseline Files:[/bold cyan]")
            baseline_files = client.list_baseline_files()
            table = Table("Data File", "Checksum File")
            if len(baseline_files) > 10:
                display_files = baseline_files[:5] + baseline_files[-5:]
                for data_file, checksum_file in display_files:
                    table.add_row(data_file, checksum_file)
                console.print(table)
                console.print(f"... and {len(baseline_files) - 10} more files.")
            else:
                for data_file, checksum_file in baseline_files:
                    table.add_row(data_file, checksum_file)
                console.print(table)
        if updates:
            console.print("\n[bold cyan]Update Files:[/bold cyan]")
            update_files = client.list_update_files()
            table = Table("Data File", "Checksum File")
            if len(update_files) > 10:
                display_files = update_files[:5] + update_files[-5:]
                for data_file, checksum_file in display_files:
                    table.add_row(data_file, checksum_file)
                console.print(table)
                console.print(f"... and {len(update_files) - 10} more files.")
            else:
                for data_file, checksum_file in update_files:
                    table.add_row(data_file, checksum_file)
                console.print(table)
        console.print("\n[bold green]Successfully retrieved file lists.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error listing remote files: {e}[/bold red]")
        raise typer.Exit(code=1)

@app.command()
def check_status() -> None:
    """Displays the current state of the loaded files from the load history table."""
    settings = Settings()
    console.print(f"Checking status using adapter '{settings.db_adapter}'...")
    try:
        adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
        # This method is defined in the base adapter to return List[str]
        completed_files = adapter.get_completed_files()
        table = Table("Completed File Name")
        if not completed_files:
            table.add_row("[italic]No files have been successfully processed yet.[/italic]")
        else:
            for file_name in completed_files:
                table.add_row(file_name)
        console.print(table)
    except Exception as e:
        console.print(f"[bold red]Error checking status: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def reset_failed() -> None:
    """Resets the status of FAILED files in the state table for reprocessing."""
    settings = Settings()
    console.print("Connecting to the database to reset failed files...")
    try:
        adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
        num_reset = adapter.reset_failed_files()
        if num_reset > 0:
            console.print(f"[bold yellow]Reset status for {num_reset} failed file(s) to 'PENDING'.[/bold yellow]")
        else:
            console.print("[bold green]No failed files found to reset.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error resetting failed files: {e}[/bold red]")
        raise typer.Exit(code=1)


def _get_files_to_process(client: NLMFTPClient, adapter, file_type: str) -> list:
    """Helper to get the list of files that need to be processed."""
    console.print(f"Fetching remote {file_type} file list...")
    list_func = client.list_baseline_files if file_type == "baseline" else client.list_update_files
    remote_files = list_func()

    console.print("Fetching list of completed files from the database...")
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
    console.print(f"Starting baseline load for mode: [bold]{settings.load_mode}[/bold]")
    if initial_load:
        console.print("[yellow]--initial-load flag set: will optimize DB.[/yellow]")

    adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
    client = NLMFTPClient()

    try:
        files_to_process = _get_files_to_process(client, adapter, "baseline")
        if not files_to_process:
            console.print("[bold green]No new baseline files to process.[/bold green]")
            return

        console.print(f"Found {len(files_to_process)} new baseline files.")
        if limit:
            files_to_process = files_to_process[:limit]
            console.print(f"Processing a maximum of {limit} file(s).")

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

        console.rule("[bold green]Baseline run finished.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Critical error during baseline run: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def run_delta(
    limit: Optional[int] = typer.Option(None, "-l", help="Limit number of files."),
    chunk_size: int = typer.Option(20000, help="Records per chunk."),
) -> None:
    """Runs the delta load process for daily update files."""
    settings = Settings()
    console.print(f"Starting delta load for mode: [bold]{settings.load_mode}[/bold]")

    adapter = get_adapter(settings.db_adapter, settings.db_connection_string)
    client = NLMFTPClient()

    if not adapter.has_completed_baseline():
        console.print("[bold red]Error: Baseline must be loaded before deltas.[/bold red]")
        raise typer.Exit(code=1)

    try:
        files_to_process = _get_files_to_process(client, adapter, "update")
        if not files_to_process:
            console.print("[bold green]No new update files to process.[/bold green]")
            return

        console.print(f"Found {len(files_to_process)} new update files.")
        if limit:
            files_to_process = files_to_process[:limit]
            console.print(f"Processing a maximum of {limit} file(s).")

        for data_filename, md5_filename in files_to_process:
            try:
                _process_single_file(
                    client=client,
                    adapter=adapter,
                    settings=settings,
                    file_info=(data_filename, md5_filename),
                    file_type="DELTA",
                    chunk_size=chunk_size,
                    is_initial_load=False, # Deltas are never initial loads
                )
            except Exception as e:
                console.print(f"[bold red]Error on {data_filename}: {e}[/bold red]")
                console.print("Aborting delta run to ensure sequential processing.")
                raise typer.Exit(code=1)

        console.rule("[bold green]Delta run finished.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Critical error during delta run: {e}[/bold red]")
        raise typer.Exit(code=1)


def _process_single_file(client, adapter, settings, file_info, file_type, chunk_size, is_initial_load):
    """Helper function to process one file (baseline or delta)."""
    data_filename, md5_filename = file_info
    local_path = ""
    total_records = 0

    try:
        console.rule(f"[bold cyan]Processing: {data_filename}[/bold cyan]")

        remote_dir = client.BASELINE_DIR if file_type == "BASELINE" else client.UPDATE_DIR
        md5_checksum = client.get_remote_checksum(remote_dir, md5_filename)

        adapter.manage_load_state(
            file_name=data_filename, status="DOWNLOADING", file_type=file_type, md5_checksum=md5_checksum
        )

        local_path = client.download_and_verify_file(
            remote_dir, data_filename, md5_filename, settings.local_staging_dir
        )

        adapter.manage_load_state(file_name=data_filename, status="LOADING")
        adapter.create_staging_tables(mode=settings.load_mode)

        parser_gen = parse_pubmed_xml(
            local_path, load_mode=settings.load_mode, chunk_size=chunk_size
        )

        for op_type, chunk_data in parser_gen:
            if op_type == "UPSERT":
                num_records = next((len(v) for v in chunk_data.values() if v), 0)
                console.print(f"Staging {num_records} upserts...")
                adapter.bulk_load_chunk(data_chunk=chunk_data)
                total_records += num_records
            elif op_type == "DELETE":
                pmids_to_delete = chunk_data.get("pmids", [])
                console.print(f"Processing {len(pmids_to_delete)} deletions...")
                adapter.process_deletions(pmid_list=pmids_to_delete, mode=settings.load_mode)
                total_records += len(pmids_to_delete)

        console.print("Merging data into final tables...")
        adapter.execute_merge_strategy(mode=settings.load_mode, is_initial_load=is_initial_load)

        adapter.manage_load_state(
            file_name=data_filename, status="COMPLETE", records_processed=total_records
        )
        console.print(f"[bold green]Successfully processed {data_filename}.[/bold green]")

    except Exception as e:
        adapter.manage_load_state(file_name=data_filename, status="FAILED")
        console.print(f"[bold red]Failed to process {data_filename}. Marked as FAILED.[/bold red]")
        raise # Re-raise the exception to be handled by the caller

    finally:
        if local_path and os.path.exists(local_path):
            os.remove(local_path)
            console.print(f"Cleaned up local file: {local_path}")

if __name__ == "__main__":
    app()
