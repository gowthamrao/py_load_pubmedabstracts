import gzip
from collections import defaultdict
from typing import Any, Dict, Generator, List, Tuple

from lxml import etree

# Type alias for the complex data structure that will be yielded
T_DataChunk = Dict[str, List[Dict[str, Any]]]


def _get_value(element: etree._Element, xpath: str, default: Any = None) -> Any:
    """Safely get a text value from an element using an XPath."""
    return element.findtext(xpath, default)


def _get_date_parts(
    element: etree._Element, base_xpath: str
) -> Tuple[str | None, str | None, str | None]:
    """Extracts Year, Month, and Day from a date element."""
    year = _get_value(element, f"{base_xpath}/Year")
    month = _get_value(element, f"{base_xpath}/Month")
    day = _get_value(element, f"{base_xpath}/Day")
    return year, month, day


def _construct_date(
    year: str | None, month: str | None, day: str | None
) -> str | None:
    """Constructs an ISO 8601 date string, handling missing parts."""
    if not year:
        return None
    month = month or "01"
    day = day or "01"
    month_map = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
        "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
    }
    month = month_map.get(month, month)
    try:
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def xml_to_dict(element: etree._Element) -> Dict[str, Any]:
    """A simple conversion of an lxml Element to a dictionary."""
    def _convert_node(node):
        result = {}
        if node.text and node.text.strip():
            result["#text"] = node.text.strip()
        for key, value in node.attrib.items():
            result[f"@{key}"] = value
        for child in node:
            child_data = _convert_node(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        return result
    return {element.tag: _convert_node(element)}


def _parse_normalized(
    elem: etree._Element, pmid: int
) -> T_DataChunk:
    """
    Parses a <MedlineCitation> element into a normalized, multi-table structure.
    """
    # This function will produce records for multiple tables from one citation
    data: T_DataChunk = defaultdict(list)

    # 1. Parse Journal
    journal_issn = _get_value(elem, "Article/Journal/ISSN")
    if journal_issn:
        data["journals"].append({
            "issn": journal_issn,
            "title": _get_value(elem, "Article/Journal/Title"),
            "iso_abbreviation": _get_value(elem, "Article/Journal/ISOAbbreviation"),
        })

    # 2. Parse Citation
    pub_date_year, pub_date_month, pub_date_day = _get_date_parts(elem, "Article/Journal/JournalIssue/PubDate")
    data["citations"].append({
        "pmid": pmid,
        "title": _get_value(elem, "Article/ArticleTitle"),
        "abstract": _get_value(elem, "Article/Abstract/AbstractText"),
        "publication_date": _construct_date(pub_date_year, pub_date_month, pub_date_day),
        "journal_issn": journal_issn,
    })

    # 3. Parse Authors
    author_list = elem.find("Article/AuthorList")
    if author_list is not None:
        for i, author_elem in enumerate(author_list.findall("Author")):
            # Some authors are collective, not individual persons
            if not _get_value(author_elem, "LastName"):
                continue

            # Simple author identifier (could be improved with a hash)
            author_id_str = (
                f"{_get_value(author_elem, 'LastName', '')}-"
                f"{_get_value(author_elem, 'ForeName', '')}"
            )

            data["authors"].append({
                "author_id": hash(author_id_str), # Using hash for a simple ID
                "last_name": _get_value(author_elem, "LastName"),
                "fore_name": _get_value(author_elem, "ForeName"),
                "initials": _get_value(author_elem, "Initials"),
            })
            data["citation_authors"].append({
                "pmid": pmid,
                "author_id": hash(author_id_str),
                "display_order": i + 1,
            })

    # 4. Parse MeSH Headings
    mesh_heading_list = elem.find("MeshHeadingList")
    if mesh_heading_list is not None:
        for mesh_elem in mesh_heading_list.findall("MeshHeading"):
            descriptor = mesh_elem.find("DescriptorName")
            if descriptor is not None and descriptor.text:
                mesh_id_str = f"{descriptor.text}-{descriptor.get('UI')}"
                data["mesh_terms"].append({
                    "mesh_id": hash(mesh_id_str),
                    "term": descriptor.text,
                    "is_major_topic": descriptor.get("MajorTopicYN", "N") == "Y",
                })
                data["citation_mesh_terms"].append({
                    "pmid": pmid,
                    "mesh_id": hash(mesh_id_str),
                })
    return data


def parse_pubmed_xml(
    file_path: str,
    load_mode: str,
    chunk_size: int = 20000,
) -> Generator[Tuple[str, T_DataChunk | List[int]], None, None]:
    """
    Parses a gzipped PubMed XML file iteratively and yields chunks of operations.

    Args:
        file_path: Path to the `.xml.gz` file.
        load_mode: One of 'FULL', 'NORMALIZED', or 'BOTH'.
        chunk_size: The number of records to include in each yielded chunk.

    Yields:
        A tuple containing the operation type ('UPSERT' or 'DELETE') and a payload.
        For 'UPSERT', the payload is a dictionary where keys are table names and
        values are lists of records.
        For 'DELETE', the payload is a list of PMIDs.
    """
    # A dictionary to hold lists of records for different tables
    upsert_chunks: T_DataChunk = defaultdict(list)
    delete_chunk: List[int] = []
    record_count = 0

    with gzip.open(file_path, "rb") as f:
        context = etree.iterparse(
            f, events=("end",), tag=("MedlineCitation", "DeleteCitation")
        )

        for _, elem in context:
            if elem.tag == "MedlineCitation":
                pmid_str = _get_value(elem, "PMID")
                if not pmid_str:
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]
                    continue

                pmid = int(pmid_str)

                # 'FULL' or 'BOTH' mode processing
                if load_mode in ("FULL", "BOTH"):
                    revised_year, revised_month, revised_day = _get_date_parts(elem, "DateRevised")
                    record = {
                        "pmid": pmid,
                        "date_revised": _construct_date(revised_year, revised_month, revised_day),
                        "data": xml_to_dict(elem),
                    }
                    upsert_chunks["citations_json"].append(record)

                # 'NORMALIZED' or 'BOTH' mode processing
                if load_mode in ("NORMALIZED", "BOTH"):
                    normalized_data = _parse_normalized(elem, pmid)
                    for table, records in normalized_data.items():
                        upsert_chunks[table].extend(records)

                record_count += 1
                if record_count >= chunk_size:
                    yield "UPSERT", upsert_chunks
                    upsert_chunks = defaultdict(list)
                    record_count = 0

            elif elem.tag == "DeleteCitation":
                pmids_to_delete = [
                    int(pmid.text) for pmid in elem.findall("PMID") if pmid.text
                ]
                delete_chunk.extend(pmids_to_delete)
                if len(delete_chunk) >= chunk_size:
                    # For deletes, the dictionary has one key
                    yield "DELETE", {"pmids": delete_chunk}
                    delete_chunk = []

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    if any(upsert_chunks.values()):
        yield "UPSERT", upsert_chunks
    if delete_chunk:
        yield "DELETE", {"pmids": delete_chunk}
