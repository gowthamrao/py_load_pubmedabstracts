import datetime
import gzip
import os
from pathlib import Path
import pytest
from lxml import etree

from py_load_pubmedabstracts.parser import parse_pubmed_xml

# A representative sample of a PubMed XML file, containing two MedlineCitation records.
SAMPLE_XML = """
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <PMID Version="1">12345</PMID>
      <DateRevised>
        <Year>2022</Year>
        <Month>10</Month>
        <Day>15</Day>
      </DateRevised>
      <Article PubModel="Print">
        <Journal>
          <Title>Journal of Testing</Title>
        </Journal>
        <ArticleTitle>A test article.</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <PubmedArticle>
    <MedlineCitation Status="Publisher" Owner="PMC">
      <PMID Version="1">67890</PMID>
      <DateRevised>
        <Year>2023</Year>
        <Month>Jan</Month>
        <Day>01</Day>
      </DateRevised>
      <Article PubModel="Electronic">
        <ArticleTitle>Another test article.</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>
"""

@pytest.fixture
def sample_xml_gz_file(tmp_path: Path) -> str:
    """Creates a temporary gzipped XML file for testing."""
    file_path = tmp_path / "test.xml.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML)
    return str(file_path)


def test_parse_pubmed_xml_yields_correct_data(sample_xml_gz_file: str):
    """
    Tests that the parser correctly extracts and transforms data from a sample XML.
    """
    # Call the parser
    parser_gen = parse_pubmed_xml(sample_xml_gz_file, load_mode="FULL", chunk_size=10)

    # Get the first (and only) chunk
    try:
        operation_type, chunk = next(parser_gen)
    except StopIteration:
        pytest.fail("Parser did not yield any results.")

    # There should be no more chunks
    with pytest.raises(StopIteration):
        next(parser_gen)

    # Assertions
    assert operation_type == "UPSERT"
    assert "citations_json" in chunk
    assert len(chunk["citations_json"]) == 2

    # Check the first record
    record1 = chunk["citations_json"][0]
    assert record1.pmid == 12345
    assert record1.date_revised == datetime.date(2022, 10, 15)
    assert isinstance(record1.data, dict)
    assert "MedlineCitation" in record1.data
    # Check a nested value in the raw data
    assert record1.data["MedlineCitation"]["Article"]["ArticleTitle"]["#text"] == "A test article."

    # Check the second record
    record2 = chunk["citations_json"][1]
    assert record2.pmid == 67890
    # Check that month abbreviation 'Jan' was correctly converted
    assert record2.date_revised == datetime.date(2023, 1, 1)
    assert "MedlineCitation" in record2.data
    assert record2.data["MedlineCitation"]["PMID"]["#text"] == "67890"

def test_parse_pubmed_xml_handles_empty_file(tmp_path: Path):
    """Tests that the parser handles empty or malformed files gracefully."""
    file_path = tmp_path / "empty.xml.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write("<root></root>")

    parser_gen = parse_pubmed_xml(str(file_path), load_mode="FULL")
    results = list(parser_gen)

    assert len(results) == 0
