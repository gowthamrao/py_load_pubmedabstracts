# Functional Requirements Document (FRD): `py-load-pubmedabstracts`

**Version:** 1.0

**Date:** September 7, 2025

**Author:** Expert Python Data Architect & Bioinformatics Engineer

Package Author: Gowtham Rao <rao@ohdsi.org>

As part of the set up you will likely need postgres db in your secure virtual environment.You may need docker.

## 1. Introduction and Scope

### 1.1 Purpose

The `py-load-pubmedabstracts` package is an open-source Python utility designed to provide a robust, high-performance, and extensible Extract, Transform, and Load (ETL) pipeline for the PubMed/MEDLINE database. It facilitates the efficient download, parsing, and ingestion of the complete PubMed dataset (citations, abstracts, and metadata) from the National Library of Medicine (NLM) FTP sources into remote relational databases.

### 1.2 Key Objectives

*   **Performance:** Maximize data throughput by utilizing native database bulk loading capabilities and memory-efficient XML parsing techniques.

*   **Extensibility:** Implement a modular architecture (Adapter Pattern) that allows seamless integration of new database backends without modifying the core logic.

*   **Reliability and Idempotency:** Ensure the pipeline can recover gracefully from interruptions and prevent data duplication through rigorous state management.

*   **Cloud Agnosticism:** Design the core package to be deployable in containerized environments across any infrastructure, delegating cloud-specific interactions to extensions.

### 1.3 In-Scope

*   Data acquisition from NLM FTP servers (Annual Baseline and Daily Update files).

*   Implementation of the Baseline (Full) Load process.

*   Implementation of the Daily (Delta) Load process (additions, updates, deletions).

*   Memory-efficient, iterative parsing of gzipped XML files.

*   Default support for PostgreSQL utilizing the native `COPY` command.

*   A standardized Database Abstraction Layer (DAL) for extensibility.

*   Support for both normalized relational and semi-structured (JSONB) data representations.

*   A Command Line Interface (CLI) for orchestration.

### 1.4 Out-of-Scope

*   Real-time data retrieval using the NCBI E-utilities API (this package focuses on bulk loading).

*   Natural Language Processing (NLP) or text mining of abstract content.

*   Management or provisioning of the target database infrastructure.

## 2. Data Acquisition and Orchestration

### 2.1 Download Mechanism

The package must interact with the NLM FTP server (`ftp.ncbi.nlm.nih.gov`) to acquire data.

*   **2.1.1 Source Locations:** The package must target the specific FTP directories: `/pubmed/baseline/` and `/pubmed/updatefiles/`.

*   **2.1.2 File Handling:** Must identify and handle both the data files (`*.xml.gz`) and the corresponding checksum files (`*.xml.gz.md5`).

*   **2.1.3 MD5 Checksum Verification:** Before processing, the downloaded `*.xml.gz` file must be validated against its corresponding MD5 checksum file. If validation fails, the file must be re-downloaded.

*   **2.1.4 Network Resilience:** Implement exponential backoff and retry mechanisms using a robust FTP client library to handle transient network issues or server instability.

*   **2.1.5 Local Staging:** Downloaded files shall be stored in a configurable local directory or mounted volume.

### 2.2 Orchestration Logic

The pipeline must enforce the correct sequence of operations to maintain data integrity.

*   **2.2.1 Baseline First:** The Annual Baseline load must be completed successfully before any Daily Update files can be processed.

*   **2.2.2 Sequential Updates:** Daily Update files must be processed in the chronological order they were published (determined by filename sequence).

*   **2.2.3 Atomic Processing:** The processing of a single file (download, validation, parsing, loading) shall be treated as an atomic unit of work. The state shall only be marked as complete once the data is successfully committed to the target database.

### 2.3 State Management and Idempotency

The package must maintain a persistent state to ensure idempotency and enable reliable resumption.

*   **2.3.1 State Tracking Table:** A dedicated metadata table (e.g., `_pubmed_load_history`) must be created in the target database.

*   **2.3.2 Schema:** The state table must track:

    *   `file_name` (Primary Key)

    *   `file_type` (BASELINE or DELTA)

    *   `md5_checksum`

    *   `download_timestamp`

    *   `load_start_timestamp`

    *   `load_end_timestamp`

    *   `status` (e.g., PENDING, DOWNLOADING, LOADING, COMPLETE, FAILED)

    *   `records_processed`

*   **2.3.3 Pre-flight Check:** Before processing a file, the package must query the state table. If a file is marked COMPLETE, it shall be skipped.

## 3. XML Parsing and Transformation

### 3.1 Parsing Strategy

To handle the large size of PubMed XML files, memory efficiency is paramount.

*   **3.1.1 Iterative Parsing:** The package must utilize iterative (event-driven) parsing techniques, specifically leveraging `lxml.etree.iterparse` for performance.

*   **3.1.2 Prohibited Methods:** Loading the entire XML tree into memory (DOM parsing) is strictly prohibited.

*   **3.1.3 Memory Management:** During iteration, once a top-level element (e.g., `MedlineCitation`) has been processed and transformed, it must be explicitly cleared from the parsing tree (using `elem.clear()` and clearing preceding siblings) to release memory immediately.

*   **3.1.4 Stream Processing:** The parser should ideally read directly from the compressed `.xml.gz` stream to minimize disk I/O overhead.

*   **3.1.5 Chunking:** Parsed records must be yielded in configurable chunks (e.g., 20,000 records) to the data loading module.

### 3.2 Delta Handling (Daily Updates)

The parser must correctly identify and categorize operations within the Daily Update files.

*   **3.2.1 New and Revised Records:** `MedlineCitation` elements represent both new records and revisions. These must be processed as "UPSERT" operations (Insert or Update) based on the PMID.

*   **3.2.2 Deleted Citations:** `DeleteCitation` elements contain lists of PMIDs that have been removed. The package must extract these PMIDs and flag them for deletion from the target database.

### 3.3 Transformation

The parser must transform the complex, nested XML structure into intermediate data structures suitable for the target data model.

*   **3.3.1 Intermediate Representation:** Transformed data shall be represented using standardized Python objects (e.g., dataclasses) before serialization for loading.

*   **3.3.2 Element Mapping:** Logic must handle the extraction of core metadata (PMID, Title, Abstract, Dates), and related entities (Authors, Affiliations, MeSH Headings, Chemicals, Keywords, Journal Information).

*   **3.3.3 Robustness:** Transformation logic must gracefully handle missing elements or variations in the XML structure across different publication years.

## 4. Data Loading Architecture (Extensibility and Performance)

### 4.1 Database Abstraction Layer (DAL)

To ensure extensibility, the package will implement the Adapter pattern, defining a standardized interface (contract) that all database connectors must adhere to.

*   **4.1.1 `DatabaseAdapter` Interface:** An Abstract Base Class (ABC) defining the required contract:

    *   `initialize_schema(mode)`: Creates target tables and metadata structures.

    *   `create_staging_tables()`: Creates temporary/transient tables for the current load.

    *   `bulk_load_chunk(data_chunk, target_table)`: Efficiently loads a chunk of data using native capabilities.

    *   `process_deletions(pmid_list)`: Removes specified PMIDs.

    *   `execute_merge_strategy()`: Moves data from staging to final tables using UPSERT logic.

    *   `manage_load_state(file_name, status, checksum)`: Interface for state management interactions.

    *   `optimize_database(stage)`: Hooks for pre-load (e.g., drop indexes) and post-load (e.g., rebuild indexes) optimizations.

### 4.2 Default Implementation (PostgreSQL)

The core package will include a robust PostgreSQL implementation.

*   **4.2.1 Native Bulk Loading:** Must strictly utilize the PostgreSQL `COPY` command for all bulk data ingestion. This shall be implemented using `psycopg`'s `copy` functionality (e.g., `COPY table FROM STDIN`). Row-by-row or batched INSERTs are prohibited.

*   **4.2.2 Staging Strategy:** Data must first be `COPY`ed into temporary or unlogged staging tables.

*   **4.2.3 Delta Implementation (UPSERT/DELETE):**

    *   Deletions shall be processed first.

    *   Additions/Updates shall be merged from the staging table using `INSERT INTO target ... ON CONFLICT (pmid) DO UPDATE SET ...`.

*   **4.2.4 Optimization (Baseline Load):** During the baseline load, the implementation must temporarily drop foreign key constraints and non-primary indexes to maximize ingestion speed, reapplying them after the load completes.

### 4.3 Extension Mechanism

The architecture must support adding new database connectors without modifying the core package.

*   **4.3.1 Plugin Architecture:** Utilize Python entry points (defined in `pyproject.toml`) to allow external packages to register themselves as available `DatabaseAdapter` implementations.

*   **4.3.2 Optional Dependencies:** Extensions should be installable via optional dependencies, e.g., `pip install py-load-pubmedabstracts[redshift]`.

*   **4.3.3 Cloud-Specific Staging:** Extensions for cloud data warehouses (e.g., Redshift, BigQuery) must manage prerequisite cloud-specific staging. For example, a Redshift adapter must handle uploading data chunks to S3 before issuing the Redshift `COPY` command from S3. The core package remains agnostic to these cloud storage mechanisms.

## 5. Target Data Model and Representations

The package must support two distinct data representations, configurable by the user (Normalized, Full, or Both).

### 5.1 Standard Representation (Normalized Relational)

A normalized schema designed for complex analytical queries and data integrity.

*   **5.1.1 Entity-Relationship Description:**

    *   **Citations (Main Table):** PMID (PK), Title, Abstract, Publication Date, Language, JournalID (FK).

    *   **Journals:** JournalID (PK), ISSN, Title, ISO Abbreviation.

    *   **Authors:** AuthorID (PK), LastName, ForeName, Initials.

    *   **MeSH Terms:** MeshID (PK), TermName, Type (Descriptor/Qualifier).

    *   **Chemicals / Keywords:** Similar normalized structures.

*   **5.1.2 M:N Relationships:** Handled via junction tables:

    *   **Citation_Authors:** CitationID (FK), AuthorID (FK), DisplayOrder.

    *   **Author_Affiliations:** (Handling affiliations associated with authors).

    *   **Citation_MeSH:** CitationID (FK), MeshID (FK), IsMajorTopic.

### 5.2 Full Representation (Semi-structured)

A schema optimized for storing the complete fidelity of the citation record.

*   **5.2.1 Schema:** A single table (e.g., `citations_json`).

    *   `pmid` (Integer, PK).

    *   `date_revised` (Date).

    *   `data` (Semi-structured type).

*   **5.2.2 Data Type Utilization:** The `data` column must utilize the native semi-structured data type of the target database (e.g., `JSONB` in PostgreSQL, `VARIANT` in Snowflake) to allow for efficient indexing and querying.

## 6. Configuration and Interface

### 6.1 Configuration

Configuration must adhere to 12-factor app principles, prioritizing environment variables for containerized deployments.

*   **6.1.1 Environment Variables:** Primary configuration method (e.g., `PML_DB_CONNECTION_STRING`, `PML_DB_ADAPTER`, `PML_LOCAL_STAGING_DIR`, `PML_LOAD_MODE`).

*   **6.1.2 Configuration Files:** Optional support for `.env` files or YAML for local development convenience.

*   **6.1.3 Validation:** Configuration parameters must be validated at runtime (e.g., using `pydantic-settings`).

### 6.2 Command Line Interface (CLI)

The primary user interface will be a CLI, suitable for execution in automated ETL pipelines (e.g., Airflow, Kubernetes Jobs).

*   **6.2.1 Tooling:** Utilize a modern CLI framework (e.g., Typer or Click).

*   **6.2.2 Core Commands:**

    *   `py-load-pubmedabstracts initialize-db`: Sets up the target schema and state tracking tables.

    *   `py-load-pubmedabstracts run-baseline`: Executes the full download and load of the Annual Baseline.

    *   `py-load-pubmedabstracts run-delta`: Checks for, downloads, and loads new Daily Update files sequentially.

    *   `py-load-pubmedabstracts check-status`: Displays the current state of the loaded files.

    *   `py-load-pubmedabstracts reset-failed`: Resets the status of FAILED files in the state table for reprocessing.

## 7. Non-Functional Requirements and Package Standards

### 7.1 Performance

*   The pipeline throughput should be primarily bottlenecked by the target database's write capacity or network download speed, not the Python application logic.

*   Memory usage must remain stable and near-constant throughout the processing, regardless of the input XML file size.

### 7.2 Robustness and Observability

*   **Error Handling:** Implement comprehensive error handling. The system must distinguish between transient errors (e.g., network timeouts), which should be retried, and fatal errors (e.g., XML parsing error, data integrity failure), which should halt the processing of the current file and mark it as FAILED.

*   **Logging:** Implement structured logging (e.g., JSON format) with configurable verbosity levels to facilitate monitoring in production environments.

### 7.3 Modern Packaging

*   The package must adhere to modern Python standards.

*   Use `pyproject.toml` for metadata and build system configuration (PEP 621).

*   Utilize tools like Poetry or Hatch for dependency management and reproducible builds.

### 7.4 Testing Strategy

*   **Unit Tests:** Comprehensive unit tests using `pytest` for parsing logic, transformation rules, and configuration. External dependencies must be mocked.

*   **Integration Tests:** Mandatory for validating `DatabaseAdapter` implementations. These tests *must* use containerized databases (e.g., via the `testcontainers` library) to execute the actual `COPY`/`MERGE` commands against a real database instance.

### 7.5 Code Quality & CI/CD

*   **Linting and Formatting:** Enforce strict code style using Ruff.

*   **Type Checking:** Utilize static type checking with Mypy (strict mode) throughout the codebase.

*   **CI/CD Pipeline:** Implement automated CI/CD (e.g., GitHub Actions) to run tests, linting, and type checking. Automate the publishing of the package to PyPI upon release.

### 7.6 Deployment

*   The package must be designed to run efficiently within Docker containers.

*   A reference `Dockerfile` shall be provided to facilitate easy deployment across different environments (Kubernetes, AWS ECS, etc.).