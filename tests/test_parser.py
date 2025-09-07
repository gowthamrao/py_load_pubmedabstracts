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


# A more comprehensive sample of a PubMed XML file for advanced testing.
COMPREHENSIVE_SAMPLE_XML = """
<PubmedArticleSet>
  <!-- Record 1: Full data for normalized parsing -->
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <PMID Version="1">10001</PMID>
      <DateRevised>
        <Year>2022</Year><Month>10</Month><Day>15</Day>
      </DateRevised>
      <Article PubModel="Print">
        <Journal>
          <ISSN IssnType="Print">1234-5678</ISSN>
          <Title>Journal of Normalization</Title>
          <ISOAbbreviation>J Norm</ISOAbbreviation>
        </Journal>
        <ArticleTitle>A normalized article.</ArticleTitle>
        <Abstract>
          <AbstractText>This is an abstract.</AbstractText>
        </Abstract>
        <AuthorList CompleteYN="Y">
          <Author ValidYN="Y">
            <LastName>Doe</LastName>
            <ForeName>John</ForeName>
            <Initials>J</Initials>
          </Author>
        </AuthorList>
      </Article>
      <MeshHeadingList>
        <MeshHeading>
          <DescriptorName UI="D000001" MajorTopicYN="N">Test Term 1</DescriptorName>
        </MeshHeading>
        <MeshHeading>
          <DescriptorName UI="D000002" MajorTopicYN="Y">Major Topic Term</DescriptorName>
        </MeshHeading>
      </MeshHeadingList>
    </MedlineCitation>
  </PubmedArticle>
  <!-- Record 2: Another record to test chunking -->
  <PubmedArticle>
    <MedlineCitation Status="Publisher" Owner="PMC">
      <PMID Version="1">10002</PMID>
    </MedlineCitation>
  </PubmedArticle>
  <!-- Record 3: Missing PMID, should be skipped -->
  <PubmedArticle>
    <MedlineCitation>
      <Article><ArticleTitle>Article with no PMID.</ArticleTitle></Article>
    </MedlineCitation>
  </PubmedArticle>
  <!-- Deletion Record -->
  <DeleteCitation>
    <PMID Version="1">90001</PMID>
    <PMID Version="1">90002</PMID>
  </DeleteCitation>
</PubmedArticleSet>
"""

@pytest.fixture
def comprehensive_xml_gz_file(tmp_path: Path) -> str:
    """Creates a temporary gzipped XML file with comprehensive data."""
    file_path = tmp_path / "comprehensive.xml.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write(COMPREHENSIVE_SAMPLE_XML)
    return str(file_path)


def test_parse_in_normalized_mode(comprehensive_xml_gz_file: str):
    """Tests that `load_mode='NORMALIZED'` produces correct structured data."""
    parser_gen = parse_pubmed_xml(comprehensive_xml_gz_file, load_mode="NORMALIZED")
    operation, chunk = next(parser_gen)

    assert operation == "UPSERT"
    assert "citations_json" not in chunk  # Should not exist in NORMALIZED mode

    # Check journal data
    assert len(chunk["journals"]) == 1
    assert chunk["journals"][0].issn == "1234-5678"
    assert chunk["journals"][0].title == "Journal of Normalization"

    # Check citation data
    assert len(chunk["citations"]) == 2
    assert chunk["citations"][0].pmid == 10001
    assert chunk["citations"][0].abstract == "This is an abstract."

    # Check author data
    assert len(chunk["authors"]) == 1
    assert chunk["authors"][0].last_name == "Doe"
    assert len(chunk["citation_authors"]) == 1
    assert chunk["citation_authors"][0].pmid == 10001

    # Check MeSH terms
    assert len(chunk["mesh_terms"]) == 2
    assert chunk["mesh_terms"][0].term == "Test Term 1"
    assert not chunk["mesh_terms"][0].is_major_topic
    assert chunk["mesh_terms"][1].term == "Major Topic Term"
    assert chunk["mesh_terms"][1].is_major_topic
    assert len(chunk["citation_mesh_terms"]) == 2


def test_parse_deletions(comprehensive_xml_gz_file: str):
    """Tests that `DeleteCitation` tags are correctly processed."""
    # Run the parser and collect all yielded items
    results = list(parse_pubmed_xml(comprehensive_xml_gz_file, load_mode="FULL"))

    # We expect two yields: one for UPSERTs, one for DELETEs
    assert len(results) == 2
    delete_op = next(r for r in results if r[0] == "DELETE")

    assert delete_op is not None
    operation_type, chunk = delete_op
    assert operation_type == "DELETE"
    assert "pmids" in chunk
    assert sorted(chunk["pmids"]) == [90001, 90002]


def test_parser_chunking(comprehensive_xml_gz_file: str):
    """Tests that the parser respects the chunk_size parameter."""
    parser_gen = parse_pubmed_xml(comprehensive_xml_gz_file, load_mode="FULL", chunk_size=1)

    # First chunk (UPSERT)
    op1, chunk1 = next(parser_gen)
    assert op1 == "UPSERT"
    assert len(chunk1["citations_json"]) == 1
    assert chunk1["citations_json"][0].pmid == 10001

    # Second chunk (UPSERT)
    op2, chunk2 = next(parser_gen)
    assert op2 == "UPSERT"
    assert len(chunk2["citations_json"]) == 1
    assert chunk2["citations_json"][0].pmid == 10002

    # Third chunk (DELETE)
    op3, chunk3 = next(parser_gen)
    assert op3 == "DELETE"
    assert len(chunk3["pmids"]) == 2


def test_parse_in_both_mode(comprehensive_xml_gz_file: str):
    """Tests that `load_mode='BOTH'` produces both full and structured data."""
    parser_gen = parse_pubmed_xml(comprehensive_xml_gz_file, load_mode="BOTH")
    operation, chunk = next(parser_gen)

    assert operation == "UPSERT"
    assert "citations_json" in chunk
    assert "citations" in chunk
    assert len(chunk["citations_json"]) == 2
    assert len(chunk["citations"]) == 2


from py_load_pubmedabstracts.parser import _construct_date

def test_construct_date_edge_cases():
    """Tests the _construct_date helper with various edge cases."""
    assert _construct_date(None, "10", "15") is None
    assert _construct_date("2022", None, "15") == "2022-01-15"
    assert _construct_date("2022", "10", None) == "2022-10-01"
    assert _construct_date("2022", "Feb", "28") == "2022-02-28"
    assert _construct_date("invalid", "10", "15") is None
    assert _construct_date("2022", "invalid", "15") is None
    assert _construct_date("2022", "13", "15") is None # Invalid month
