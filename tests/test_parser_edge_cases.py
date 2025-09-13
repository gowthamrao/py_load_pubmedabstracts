import datetime
import gzip
from pathlib import Path

import pytest

from py_load_pubmedabstracts.parser import parse_pubmed_xml

# XML snippet containing a collective author name
COLLECTIVE_AUTHOR_XML = """
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <PMID Version="1">20001</PMID>
      <Article PubModel="Print">
        <ArticleTitle>A study by a collective group.</ArticleTitle>
        <AuthorList CompleteYN="Y">
          <Author ValidYN="Y">
            <CollectiveName>The Research Group</CollectiveName>
          </Author>
        </AuthorList>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>
"""


@pytest.fixture
def collective_author_xml_gz_file(tmp_path: Path) -> str:
    """Creates a temporary gzipped XML file with a collective author."""
    file_path = tmp_path / "collective_author.xml.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write(COLLECTIVE_AUTHOR_XML)
    return str(file_path)


def test_parser_handles_collective_author_name(collective_author_xml_gz_file: str):
    """
    Tests that the parser correctly handles an <Author> tag with a <CollectiveName>
    instead of LastName/ForeName.
    """
    # Parse the sample file in NORMALIZED mode
    parser_gen = parse_pubmed_xml(collective_author_xml_gz_file, load_mode="NORMALIZED")
    operation, chunk = next(parser_gen)

    assert operation == "UPSERT"
    assert "authors" in chunk
    assert len(chunk["authors"]) == 1, "Should have parsed one author"

    author = chunk["authors"][0]
    assert author.last_name == "The Research Group"
    assert author.fore_name is None, "ForeName should be None for a collective author"
    assert author.initials is None, "Initials should be None for a collective author"

    assert "citation_authors" in chunk
    assert len(chunk["citation_authors"]) == 1
    assert chunk["citation_authors"][0].pmid == 20001
    assert chunk["citation_authors"][0].author_id == author.author_id


# XML snippet for testing various date formats
DATE_FORMAT_TEST_XML = """
<PubmedArticleSet>
  <!-- Case 1: Standard Date -->
  <PubmedArticle>
    <MedlineCitation>
      <PMID>30001</PMID>
      <Article>
        <Journal>
          <JournalIssue>
            <PubDate>
              <Year>2022</Year>
              <Month>08</Month>
              <Day>15</Day>
            </PubDate>
          </JournalIssue>
        </Journal>
        <ArticleTitle>Standard Date</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <!-- Case 2: Abbreviated Month and missing day -->
  <PubmedArticle>
    <MedlineCitation>
      <PMID>30002</PMID>
      <Article>
        <Journal>
          <JournalIssue>
            <PubDate>
              <Year>2021</Year>
              <Month>Jul</Month>
            </PubDate>
          </JournalIssue>
        </Journal>
        <ArticleTitle>Abbreviated Month</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <!-- Case 3: Year Only -->
  <PubmedArticle>
    <MedlineCitation>
      <PMID>30003</PMID>
      <Article>
        <Journal>
          <JournalIssue>
            <PubDate>
              <Year>2020</Year>
            </PubDate>
          </JournalIssue>
        </Journal>
        <ArticleTitle>Year Only</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <!-- Case 4: Invalid Month -->
  <PubmedArticle>
    <MedlineCitation>
      <PMID>30004</PMID>
      <Article>
        <Journal>
          <JournalIssue>
            <PubDate>
              <Year>2019</Year>
              <Month>13</Month>
              <Day>01</Day>
            </PubDate>
          </JournalIssue>
        </Journal>
        <ArticleTitle>Invalid Month</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <!-- Case 5: Invalid Day for Month -->
  <PubmedArticle>
    <MedlineCitation>
      <PMID>30005</PMID>
      <Article>
        <Journal>
          <JournalIssue>
            <PubDate>
              <Year>2018</Year>
              <Month>02</Month>
              <Day>30</Day>
            </PubDate>
          </JournalIssue>
        </Journal>
        <ArticleTitle>Invalid Day</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
  <!-- Case 6: No PubDate element -->
  <PubmedArticle>
    <MedlineCitation>
      <PMID>30006</PMID>
      <Article>
        <Journal>
          <JournalIssue>
          </JournalIssue>
        </Journal>
        <ArticleTitle>No PubDate</ArticleTitle>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>
"""


@pytest.fixture
def date_format_xml_gz_file(tmp_path: Path) -> str:
    """Creates a temporary gzipped XML file with various date formats."""
    file_path = tmp_path / "date_formats.xml.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write(DATE_FORMAT_TEST_XML)
    return str(file_path)


def test_parser_handles_various_date_formats(date_format_xml_gz_file: str):
    """
    Tests that the parser correctly handles various valid and invalid
    date formats in the <PubDate> element.
    """
    parser_gen = parse_pubmed_xml(date_format_xml_gz_file, load_mode="NORMALIZED")
    _, chunk = next(parser_gen)

    citations = {c.pmid: c for c in chunk["citations"]}
    assert len(citations) == 6

    # Case 1: Standard Date
    assert citations[30001].publication_date == datetime.date(2022, 8, 15)
    # Case 2: Abbreviated Month and missing day (defaults to day 01)
    assert citations[30002].publication_date == datetime.date(2021, 7, 1)
    # Case 3: Year Only (defaults to month 01, day 01)
    assert citations[30003].publication_date == datetime.date(2020, 1, 1)
    # Case 4: Invalid Month
    assert citations[30004].publication_date is None
    # Case 5: Invalid Day for Month
    assert citations[30005].publication_date is None
    # Case 6: No PubDate element
    assert citations[30006].publication_date is None
