# py-load-pubmedabstracts

A high-performance, extensible ETL pipeline for PubMed/MEDLINE.

This utility provides a robust command-line interface to download the complete PubMed dataset (citations, abstracts, and metadata) from the National Library of Medicine (NLM) FTP sources and load it into a relational database.

## Key Features

-   **High-Performance:** Utilizes native database bulk-loading capabilities for efficient data ingestion.
-   **Memory-Efficient:** Employs `lxml` for parsing large XML files without excessive memory consumption.
-   **Extensible Architecture:** Uses an adapter pattern to easily support multiple database backends (PostgreSQL is currently implemented).
-   **State Management:** Tracks the status of each downloaded file to ensure reliable and idempotent data loads.
-   **Command-Line Interface:** Provides a simple and powerful CLI for orchestrating the entire ETL process.

## Prerequisites

-   Python 3.10+
-   [Poetry](https://python-poetry.org/) for dependency management.
-   A running PostgreSQL instance.
-   Docker (for running integration tests).

## Installation

1.  **Clone the repository:**
    ```sh
    git clone https://github.com/your-username/py-load-pubmedabstracts.git
    cd py-load-pubmedabstracts
    ```

2.  **Install dependencies using Poetry:**
    ```sh
    poetry install
    ```

## Configuration

The application is configured via environment variables. You can create a `.env` file in the project root or set the variables directly in your shell.

The following variables are available:

| Variable                     | Description                                                                 | Default                                       |
| ---------------------------- | --------------------------------------------------------------------------- | --------------------------------------------- |
| `PML_DB_CONNECTION_STRING`   | The connection string for your database.                                    | `postgresql://user:password@localhost:5432/pubmed` |
| `PML_DB_ADAPTER`             | The database adapter to use.                                                | `postgresql`                                  |
| `PML_LOCAL_STAGING_DIR`      | A local directory for temporarily storing downloaded files.                  | `/tmp/pubmed_staging`                         |
| `PML_LOAD_MODE`              | The data model to use for loading (`FULL`, `NORMALIZED`, or `BOTH`).        | `FULL`                                        |

**Example `.env` file:**

```
PML_DB_CONNECTION_STRING="postgresql://myuser:mypassword@db.server.com:5432/pubmeddb"
PML_LOCAL_STAGING_DIR="/path/to/my/staging/dir"
PML_LOAD_MODE="NORMALIZED"
```

## Usage

All commands are run through the `py-load-pubmedabstracts` CLI, which is accessible via `poetry run`.

### 1. Initialize the Database

Before running any load process, you must initialize the database schema. This creates the necessary tables and state-tracking mechanisms.

```sh
poetry run py-load-pubmedabstracts initialize-db
```

### 2. Run the Baseline Load

The baseline load downloads and processes the entire PubMed dataset. This is a one-time operation that can take a significant amount of time.

```sh
poetry run py-load-pubmedabstracts run-baseline
```

**Options:**
-   `--limit <N>` or `-l <N>`: Limit the process to the first `N` files. Useful for testing.
-   `--initial-load`: Use optimizations for loading into an empty database (e.g., dropping indexes).
-   `--chunk-size <N>`: The number of records to process per chunk (default: `20000`).

### 3. Run a Delta Load

After a successful baseline load, you can run delta loads to fetch the daily update files and keep your database synchronized.

```sh
poetry run py-load-pubmedabstracts run-delta
```

**Options:**
-   `--limit <N>` or `-l <N>`: Limit the process to the first `N` update files.

### Utility Commands

#### Check Load Status

Display the status of all files that have been processed or are currently being processed.

```sh
poetry run py-load-pubmedabstracts check-status
```

#### List Remote Files

List the baseline and/or update files available on the NLM FTP server.

```sh
# List both baseline and update files (default)
poetry run py-load-pubmedabstracts list-remote-files

# List only baseline files
poetry run py-load-pubmedabstracts list-remote-files --no-updates

# List only update files
poetry run py-load-pubmedabstracts list-remote-files --no-baseline
```

#### Reset Failed Loads

If a file fails to process, its status is marked as `FAILED`. This command resets the status of all failed files to `PENDING`, allowing them to be reprocessed on the next run.

```sh
poetry run py-load-pubmedabstracts reset-failed
```

## Running Tests

The project includes both unit and integration tests. The integration tests require Docker to be running, as they spin up a PostgreSQL container.

To run all tests:

```sh
poetry run pytest
```

To run only unit tests:

```sh
poetry run pytest -m "not integration"
```

To run only integration tests:

```sh
poetry run pytest -m "integration"
```

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.
