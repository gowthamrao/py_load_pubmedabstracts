# PubMed/MEDLINE Data Source Documentation

This document provides an overview of the PubMed/MEDLINE data source and how it's used by this ETL pipeline.

## Introduction

PubMed is a free resource supporting the search and retrieval of biomedical and life sciences literature. The underlying database is maintained by the National Library of Medicine (NLM) and is comprised of more than 30 million citations and abstracts of biomedical literature. This ETL pipeline is designed to efficiently download, parse, and load this data into a local database.

## Accessing the Data

The data is made available by the NLM via an FTP server. This package is pre-configured to use this server.

- **FTP Host:** `ftp.ncbi.nlm.nih.gov`
- **Anonymous Login:** The server is accessible via anonymous FTP.

### File Locations

The data is organized into two main sets of files:

- **Annual Baseline:** A complete snapshot of all PubMed records. These are located in the `/pubmed/baseline/` directory on the FTP server. It is recommended to download and load the entire baseline annually.
- **Daily Updates:** Incremental updates containing new, revised, and deleted citations. These are located in the `/pubmed/updatefiles/` directory. These should be applied in sequential order after a baseline load.

## File Format

The data is provided in gzipped XML files (`.xml.gz`). Each file contains a set of PubMed records.

### Data Integrity

To ensure the integrity of the downloaded files, the NLM provides MD5 checksum files (`.md5`) for each data file. This ETL pipeline automatically uses these checksums to verify that the downloaded files have not been corrupted during transfer.

## Data Structure

The XML files adhere to the [PubMed DTD (Document Type Definition)](https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_250101.dtd). The pipeline is designed to parse the following key elements:

- **`<MedlineCitation>`:** The root element for a single PubMed record. It contains all the information about a citation.
- **`<PMID>`:** The unique PubMed Identifier for the citation.
- **`<Article>`:** Contains the core content of the citation, such as the title, abstract, and journal information.
- **`<Journal>`:** Contains information about the journal in which the article was published.
- **`<AuthorList>`:** A list of authors for the article.
- **`<MeshHeadingList>`:** A list of MeSH (Medical Subject Headings) terms that describe the content of the article.
- **`<DeleteCitation>`:** A tag that indicates that a previously existing PubMed record has been deleted and should be removed from local databases.

This pipeline supports two main loading modes:
- **Normalized:** The XML is parsed, and the data is loaded into a relational schema with tables for citations, authors, journals, and MeSH terms.
- **Full JSON:** The entire `<MedlineCitation>` element is stored as a single JSON object.

## Terms of Use

The use of PubMed data is governed by the terms and conditions outlined in the `README.txt` file provided with the data on the FTP server. A summary of the key points is as follows:

- **No restrictions on use:** Users are free to download and use the data for any purpose.
- **Data is provided "as is":** The NLM makes no warranties regarding the data's accuracy or completeness.
- **Acknowledgement:** While not required, it is requested that the NLM be acknowledged as the source of the data.

By downloading the data, you are agreeing to these terms. For the full and most up-to-date terms and conditions, please refer to the `README.txt` file on the FTP server or visit the [NLM's data distribution page](https://www.nlm.nih.gov/databases/download/pubmed_medline.html).
