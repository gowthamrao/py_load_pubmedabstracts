import typer
from rich.console import Console
from rich.table import Table

from .config import Settings
from .db.factory import get_adapter
from .ftp_client import NLMFTPClient

app = typer.Typer()
console = Console()


@app.command()
def initialize_db() -> None:
    """
    Initializes the database schema and state tracking tables.
    """
    settings = Settings()
    console.print("Initializing database...")
    try:
        adapter = get_adapter(adapter_name=settings.db_adapter, dsn=settings.db_connection_string)
        adapter.initialize_schema(mode=settings.load_mode)
        console.print("[bold green]Database initialized successfully.[/bold green]")
    except (ValueError, Exception) as e:
        console.print(f"[bold red]Error initializing database: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def list_remote_files(
    baseline: bool = typer.Option(True, "--baseline/--no-baseline", help="List baseline files."),
    updates: bool = typer.Option(True, "--updates/--no-updates", help="List update files."),
) -> None:
    """
    Lists available baseline and/or daily update files from the NLM FTP server.
    """
    client = NLMFTPClient()
    console.print("Connecting to NLM FTP server to list files...")

    try:
        if baseline:
            console.print("\n[bold cyan]Baseline Files:[/bold cyan]")
            baseline_files = client.list_baseline_files()
            table = Table("Data File", "Checksum File")

            # For brevity, show a subset if the list is long
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


import json
import os
from typing import Optional

from .parser import parse_pubmed_xml


@app.command()
def check_status() -> None:
    """
    Displays the current state of the loaded files from the load history table.
    """
    settings = Settings()
    console.print(f"Checking status using adapter '{settings.db_adapter}'...")

    try:
        adapter = get_adapter(adapter_name=settings.db_adapter, dsn=settings.db_connection_string)
        completed_files = adapter.get_completed_files()

        table = Table("Completed Files")
        if not completed_files:
            table.add_row("[italic]No files have been successfully processed yet.[/italic]")
        else:
            for file_name in completed_files:
                table.add_row(file_name)
        console.print(table)
        console.print("\n[bold green]Status check complete.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error checking status: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def run_baseline(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Limit the number of files to process."),
    initial_load: bool = typer.Option(
        False,
        "--initial-load",
        help="Use optimizations for an initial, empty database. Drops and recreates the primary key."
    ),
    chunk_size: int = typer.Option(20000, help="Number of records to process in each chunk."),
) -> None:
    """
    Runs the full baseline load process: discovers, downloads, verifies, and loads files.
    """
    settings = Settings()
    console.print(f"Starting baseline load...")
    console.print(f"Using adapter: {settings.db_adapter}, Mode: {settings.load_mode}")
    if initial_load:
        console.print("[yellow]--initial-load flag set. Will optimize for an empty database.[/yellow]")

    try:
        adapter = get_adapter(adapter_name=settings.db_adapter, dsn=settings.db_connection_string)
    except ValueError as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise typer.Exit(code=1)

    client = NLMFTPClient()

    try:
        # 1. Determine which files to process
        console.print("Fetching remote file list from NLM FTP server...")
        remote_files = client.list_baseline_files()
        remote_filenames = {f[0] for f in remote_files}

        console.print("Fetching list of completed files from the database...")
        completed_files = set(adapter.get_completed_files())

        files_to_process = sorted([f for f in remote_files if f[0] not in completed_files], key=lambda x: x[0])

        if not files_to_process:
            console.print("[bold green]No new baseline files to process. Database is up-to-date.[/bold green]")
            return

        console.print(f"Found {len(files_to_process)} new baseline files to process.")
        if limit:
            console.print(f"Applying limit: processing at most {limit} file(s).")
            files_to_process = files_to_process[:limit]

        # 2. Apply pre-load optimizations if it's an initial load
        if initial_load:
            adapter.optimize_database(stage="pre-load")

        # 3. Process each file
        for data_filename, md5_filename in files_to_process:
            local_path = ""
            total_records_processed = 0
            try:
                console.rule(f"[bold cyan]Processing: {data_filename}[/bold cyan]")

                # a. Update state: DOWNLOADING
                console.print("Fetching remote checksum...")
                md5_checksum = client.get_remote_checksum(
                    remote_dir=client.BASELINE_DIR, md5_filename=md5_filename
                )
                adapter.manage_load_state(
                    file_name=data_filename, status="DOWNLOADING", file_type="BASELINE", md5_checksum=md5_checksum
                )

                # b. Download and verify
                local_path = client.download_and_verify_file(
                    remote_dir=client.BASELINE_DIR,
                    data_filename=data_filename,
                    md5_filename=md5_filename,
                    local_staging_dir=settings.local_staging_dir,
                )

                # c. Update state: LOADING
                adapter.manage_load_state(file_name=data_filename, status="LOADING")
                adapter.create_staging_tables()

                # d. Parse and load to staging
                parser_gen = parse_pubmed_xml(local_path, chunk_size=chunk_size)
                for i, chunk in enumerate(parser_gen):
                    console.print(f"Loading chunk {i + 1} with {len(chunk)} records...")
                    # Convert 'data' dict to JSON string for loading
                    for record in chunk:
                        record["data"] = json.dumps(record["data"])
                    adapter.bulk_load_chunk(data_chunk=iter(chunk), target_table="_staging_citations_json")
                    total_records_processed += len(chunk)

                console.print(f"Staging complete. Total records: {total_records_processed}")

                # e. Merge data from staging to final table
                console.print("Merging data into final table...")
                adapter.execute_merge_strategy(is_initial_load=initial_load)
                console.print("[green]Merge complete.[/green]")

                # f. Update state: COMPLETE
                adapter.manage_load_state(
                    file_name=data_filename, status="COMPLETE", records_processed=total_records_processed
                )
                console.print(f"[bold green]Successfully processed {data_filename}.[/bold green]")

            except Exception as e:
                console.print(f"[bold red]Error processing file {data_filename}: {e}[/bold red]")
                adapter.manage_load_state(file_name=data_filename, status="FAILED")
                console.print(f"Marked {data_filename} as FAILED. Continuing to next file.")

            finally:
                # g. Clean up downloaded file
                if local_path and os.path.exists(local_path):
                    os.remove(local_path)
                    console.print(f"Cleaned up local file: {local_path}")

        # 4. Apply post-load optimizations
        if initial_load:
            adapter.optimize_database(stage="post-load")

        console.rule("[bold green]Baseline run finished.[/bold green]")

    except Exception as e:
        console.print(f"[bold red]A critical error occurred during the baseline run: {e}[/bold red]")
        if initial_load:
             console.print("[bold yellow]Warning: An error occurred during an initial load. The database might be in an inconsistent state. Consider running initialize-db and retrying.[/bold yellow]")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
