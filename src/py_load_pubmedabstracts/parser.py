"""Iterative parser for PubMed XML files."""
import datetime
import gzip
from collections import defaultdict
from typing import Any, Dict, Generator, List, Tuple, Union

from lxml import etree
from pydantic import BaseModel

from . import models

# Type alias for the complex data structure that will be yielded
T_DataChunk = Dict[str, List[BaseModel]]


def _get_value(element: etree._Element, xpath: str, default: Any = None) -> Any:
    """Safely get a text value from an element using an XPath."""
    return element.findtext(xpath, default)


def _get_date_parts(
    element: etree._Element, base_xpath: str
) -> Tuple[str | None, str | None, str | None]:
    """Extract Year, Month, and Day from a date element."""
    year = _get_value(element, f"{base_xpath}/Year")
    month = _get_value(element, f"{base_xpath}/Month")
    day = _get_value(element, f"{base_xpath}/Day")
    return year, month, day


def _construct_date(
    year: str | None, month: str | None, day: str | None
) -> str | None:
    """Construct an ISO 8601 date string, handling missing parts."""
    if not year:
        return None
    month = month or "01"
    day = day or "01"
    month_map = {
        "Jan": "01",
        "Feb": "02",
        "Mar": "03",
        "Apr": "04",
        "May": "05",
        "Jun": "06",
        "Jul": "07",
        "Aug": "08",
        "Sep": "09",
        "Oct": "10",
        "Nov": "11",
        "Dec": "12",
    }
    month = month_map.get(month, month)
    try:
        dt = datetime.date(int(year), int(month), int(day))
        return dt.isoformat()
    except (ValueError, TypeError):
        return None


def xml_to_dict(element: etree._Element) -> Dict[str, Any]:
    """Convert an lxml Element to a dictionary."""

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


def _parse_normalized(elem: etree._Element, pmid: int) -> T_DataChunk:
    """
    Parse a <MedlineCitation> element into a normalized structure.

    This uses Pydantic models.
    """
    data: T_DataChunk = defaultdict(list)
    journal_issn = _get_value(elem, "Article/Journal/ISSN")
    if journal_issn:
        data["journals"].append(
            models.Journal(
                issn=journal_issn,
                title=_get_value(elem, "Article/Journal/Title"),
                iso_abbreviation=_get_value(elem, "Article/Journal/ISOAbbreviation"),
            )
        )
    pub_date_year, pub_date_month, pub_date_day = _get_date_parts(
        elem, "Article/Journal/JournalIssue/PubDate"
    )
    data["citations"].append(
        models.Citation(
            pmid=pmid,
            title=_get_value(elem, "Article/ArticleTitle"),
            abstract=_get_value(elem, "Article/Abstract/AbstractText"),
            publication_date=_construct_date(
                pub_date_year, pub_date_month, pub_date_day
            ),
            journal_issn=journal_issn,
        )
    )
    author_list = elem.find("Article/AuthorList")
    if author_list is not None:
        for i, author_elem in enumerate(author_list.findall("Author")):
            last_name = _get_value(author_elem, "LastName")
            if not last_name:
                continue
            fore_name = _get_value(author_elem, "ForeName")
            author_id = hash(f"{last_name}-{fore_name}")
            data["authors"].append(
                models.Author(
                    author_id=author_id,
                    last_name=last_name,
                    fore_name=fore_name,
                    initials=_get_value(author_elem, "Initials"),
                )
            )
            data["citation_authors"].append(
                models.CitationAuthor(
                    pmid=pmid, author_id=author_id, display_order=i + 1
                )
            )
    mesh_heading_list = elem.find("MeshHeadingList")
    if mesh_heading_list is not None:
        for mesh_elem in mesh_heading_list.findall("MeshHeading"):
            descriptor = mesh_elem.find("DescriptorName")
            if descriptor is not None and descriptor.text:
                mesh_id = hash(f"{descriptor.text}-{descriptor.get('UI')}")
                data["mesh_terms"].append(
                    models.MeshTerm(
                        mesh_id=mesh_id,
                        term=descriptor.text,
                        is_major_topic=descriptor.get("MajorTopicYN", "N") == "Y",
                    )
                )
                data["citation_mesh_terms"].append(
                    models.CitationMeshTerm(pmid=pmid, mesh_id=mesh_id)
                )
    return data


def _process_medline_citation(elem, load_mode, upsert_chunks):
    """Process a MedlineCitation element."""
    pmid_str = _get_value(elem, "PMID")
    if not pmid_str:
        return False
    pmid = int(pmid_str)
    if load_mode in ("FULL", "BOTH"):
        revised_year, revised_month, revised_day = _get_date_parts(elem, "DateRevised")
        upsert_chunks["citations_json"].append(
            models.CitationJson(
                pmid=pmid,
                date_revised=_construct_date(revised_year, revised_month, revised_day),
                data=xml_to_dict(elem),
            )
        )
    if load_mode in ("NORMALIZED", "BOTH"):
        normalized_data = _parse_normalized(elem, pmid)
        for table, records in normalized_data.items():
            upsert_chunks[table].extend(records)
    return True


def _process_delete_citation(elem, delete_chunk):
    """Process a DeleteCitation element."""
    pmids_to_delete = [
        int(pmid.text) for pmid in elem.findall("PMID") if pmid.text
    ]
    delete_chunk.extend(pmids_to_delete)


def parse_pubmed_xml(
    file_path: str,
    load_mode: str,
    chunk_size: int = 20000,
) -> Generator[Tuple[str, Union[T_DataChunk, Dict[str, List[int]]]], None, None]:
    """
    Parse a gzipped PubMed XML file iteratively and yield chunks of operations.

    Args:
        file_path: Path to the `.xml.gz` file.
        load_mode: One of 'FULL', 'NORMALIZED', or 'BOTH'.
        chunk_size: The number of records to include in each yielded chunk.

    Yields:
        A tuple containing the operation type ('UPSERT' or 'DELETE') and payload.
        For 'UPSERT', the payload is a dict where keys are table names and
        values are lists of records. For 'DELETE', the payload is a list of PMIDs.

    """
    upsert_chunks: T_DataChunk = defaultdict(list)
    delete_chunk: List[int] = []
    record_count = 0
    with gzip.open(file_path, "rb") as f:
        context = etree.iterparse(
            f, events=("end",), tag=("MedlineCitation", "DeleteCitation")
        )
        for _, elem in context:
            if elem.tag == "MedlineCitation":
                if _process_medline_citation(elem, load_mode, upsert_chunks):
                    record_count += 1
                    if record_count >= chunk_size:
                        yield "UPSERT", upsert_chunks
                        upsert_chunks = defaultdict(list)
                        record_count = 0
            elif elem.tag == "DeleteCitation":
                _process_delete_citation(elem, delete_chunk)
                if len(delete_chunk) >= chunk_size:
                    yield "DELETE", {"pmids": delete_chunk}
                    delete_chunk = []
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
    if any(upsert_chunks.values()):
        yield "UPSERT", upsert_chunks
    if delete_chunk:
        yield "DELETE", {"pmids": delete_chunk}
