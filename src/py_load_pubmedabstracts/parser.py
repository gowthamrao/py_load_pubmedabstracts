import gzip
import xml.etree.ElementTree as ET
from typing import Generator, List, Dict, Any, Tuple

from lxml import etree

def _get_value(element: etree._Element, xpath: str) -> str | None:
    """Safely get a text value from an element using an XPath."""
    return element.findtext(xpath)

def _get_date_parts(element: etree._Element, base_xpath: str) -> Tuple[str | None, str | None, str | None]:
    """Extracts Year, Month, and Day from a date element."""
    year = _get_value(element, f"{base_xpath}/Year")
    month = _get_value(element, f"{base_xpath}/Month")
    day = _get_value(element, f"{base_xpath}/Day")
    return year, month, day

def _construct_date(year: str | None, month: str | None, day: str | None) -> str | None:
    """Constructs an ISO 8601 date string, handling missing parts."""
    if not year:
        return None
    # Month and day are sometimes not present
    month = month or "01"
    day = day or "01"
    # Convert month abbreviation to number if necessary
    month_map = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
        "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
    }
    month = month_map.get(month, month)
    try:
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None # Return None if date components are not valid integers

def xml_to_dict(element: etree._Element) -> Dict[str, Any]:
    """
    A simple conversion of an lxml Element to a dictionary.
    This is a basic implementation for the 'FULL' representation.
    """
    def _convert_node(node):
        result = {}
        if node.text and node.text.strip():
            result['#text'] = node.text.strip()

        for key, value in node.attrib.items():
            result[f'@{key}'] = value

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


def parse_pubmed_xml(
    file_path: str, chunk_size: int = 20000
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Parses a gzipped PubMed XML file iteratively and yields chunks of processed records.

    This parser is designed for memory efficiency, using `lxml.etree.iterparse`.
    It focuses on extracting data for the 'FULL' (JSONB) representation.

    Args:
        file_path: Path to the `.xml.gz` file.
        chunk_size: The number of records to include in each yielded chunk.

    Yields:
        A list of dictionaries, where each dictionary represents a citation
        ready for JSON serialization and loading.
    """
    chunk = []
    with gzip.open(file_path, "rb") as f:
        # Use iterparse to process elements iteratively
        context = etree.iterparse(f, events=("end",), tag="MedlineCitation")

        for _, elem in context:
            pmid_str = _get_value(elem, "PMID")
            if not pmid_str:
                elem.clear()
                # Also clear previous siblings from memory
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                continue

            # Extract revised date for the main table column
            revised_year, revised_month, revised_day = _get_date_parts(elem, "DateRevised")
            date_revised = _construct_date(revised_year, revised_month, revised_day)

            # For the 'FULL' representation, we convert the whole element to a dict
            citation_data = xml_to_dict(elem)

            record = {
                "pmid": int(pmid_str),
                "date_revised": date_revised,
                "data": citation_data,
            }
            chunk.append(record)

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

            # Crucial for memory management: clear the element and its predecessors
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    # Yield any remaining records in the last chunk
    if chunk:
        yield chunk
