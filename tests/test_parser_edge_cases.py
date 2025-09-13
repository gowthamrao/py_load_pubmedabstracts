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
