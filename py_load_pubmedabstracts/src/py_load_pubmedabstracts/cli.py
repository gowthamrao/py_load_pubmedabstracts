"""Command Line Interface for the py-load-pubmedabstracts package.

This module provides the main entry point for the command-line tool,
offering commands to initialize the database, run data loads, and check
the status of the ETL process.
"""

import typer
from rich import print

from .config import get_settings
from .db.postgres_adapter import PostgresAdapter

app = typer.Typer(
    name="py-load-pubmedabstracts",
    help="A CLI tool to download, parse, and load PubMed abstracts into a database.",
    add_completion=False,
)


@app.command()
def initialize_db():
    """
    Initialize the target database schema and state-tracking tables.
    """
    print(f"[bold green]Initializing database schema...[/bold green]")
    try:
        # Get settings at runtime, allowing tests to patch get_settings
        settings = get_settings()
        print(f"Target DB: [dim]{settings.PML_DB_CONNECTION_STRING[:25]}...[/dim]")
        adapter = PostgresAdapter(settings)
        adapter.initialize_schema()
        print("[bold green]✅ Database initialization complete.[/bold green]")
    except Exception as e:
        print(f"[bold red]Error initializing database: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def run_baseline():
    """
    Execute the full download and load of the PubMed annual baseline files.
    """
    print("[bold blue]Starting baseline load...[/bold blue]")
    print("This will download all baseline files, parse them, and load into the DB.")


@app.command()
def run_delta():
    """
    Download and load the latest daily update files sequentially.
    """
    print("[bold yellow]Starting delta load...[/bold yellow]")
    print("This will process new daily update files since the last run.")


@app.command()
def check_status():
    """
    Display the current state of loaded files from the history table.
    """
    print("[bold magenta]Checking load status...[/bold magenta]")
    print("This will query the `_pubmed_load_history` table and display results.")


if __name__ == "__main__":
    app()
