import typer
from rich.console import Console
from rich.table import Table

from .db.postgresql import PostgresAdapter
from .config import Settings
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
        if settings.db_adapter == "postgresql":
            adapter = PostgresAdapter(dsn=settings.db_connection_string)
            adapter.initialize_schema(mode=settings.load_mode)
            console.print("[bold green]Database initialized successfully.[/bold green]")
        else:
            console.print(f"[bold red]Error: Unsupported database adapter '{settings.db_adapter}'.[/bold red]")
            raise typer.Exit(code=1)
    except Exception as e:
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


@app.command()
def download_file(
    filename: str = typer.Argument(..., help="The name of the baseline file to download, e.g., 'pubmed25n0001.xml.gz'"),
    staging_dir: str = typer.Option("/tmp/pubmed_staging", help="The local directory to download files to."),
) -> None:
    """
    Downloads a single baseline file from the NLM FTP server and verifies its checksum.
    """
    client = NLMFTPClient()
    console.print(f"Searching for file {filename} in baseline file list...")

    try:
        # Find the corresponding md5 file
        baseline_files = client.list_baseline_files()
        found_file = next((f for f in baseline_files if f[0] == filename), None)

        if not found_file:
            console.print(f"[bold red]Error: File '{filename}' not found in the baseline file list.[/bold red]")
            raise typer.Exit(code=1)

        data_filename, md5_filename = found_file

        console.print(f"Found file pair: {data_filename}, {md5_filename}")

        local_path = client.download_and_verify_file(
            remote_dir=client.BASELINE_DIR,
            data_filename=data_filename,
            md5_filename=md5_filename,
            local_staging_dir=staging_dir,
        )

        console.print(f"\n[bold green]Successfully downloaded and verified file to:[/bold green] {local_path}")

    except Exception as e:
        console.print(f"[bold red]An error occurred during download and verification: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def check_status() -> None:
    """
    Displays the current state of the loaded files.
    """
    settings = Settings()
    console.print(f"Checking status for adapter {settings.db_adapter}...")
    # This will be implemented later
    console.print("Status check complete.")


if __name__ == "__main__":
    app()
