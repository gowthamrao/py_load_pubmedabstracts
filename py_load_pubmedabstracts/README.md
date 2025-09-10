# py-load-pubmedabstracts

This repository contains the source code for `py-load-pubmedabstracts`, an open-source Python utility designed to provide a robust, high-performance, and extensible Extract, Transform, and Load (ETL) pipeline for the PubMed/MEDLINE database.

## Overview

The primary goal of this package is to efficiently download the complete PubMed dataset (citations, abstracts, and metadata) from the National Library of Medicine (NLM) FTP sources and load it into a relational database.

Key features include:
-   High-performance data loading using native database bulk-loading capabilities.
-   Memory-efficient XML parsing for large datasets.
-   Extensible architecture (Adapter Pattern) to support multiple database backends.
-   State management for reliable and idempotent data loads.
-   A Command Line Interface (CLI) for easy orchestration.

This project is currently under development.

## Getting Started

*(Instructions for installation and usage will be added here as the project matures.)*

### Prerequisites
- Python 3.12+
- Poetry for dependency management
- A running PostgreSQL instance

### Configuration
The application is configured via environment variables. Create a `.env` file in the project root:

```
PML_DB_CONNECTION_STRING="postgresql://user:password@host:5432/dbname"
```

### Basic Usage

1.  **Initialize the database:**
    ```sh
    poetry run py-load-pubmedabstracts initialize-db
    ```

2.  **Run the baseline load:**
    ```sh
    poetry run py-load-pubmedabstracts run-baseline
    ```

*(More commands and details to come.)*
