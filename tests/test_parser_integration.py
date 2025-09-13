import datetime
import gzip
from pathlib import Path

import pytest

from py_load_pubmedabstracts.parser import parse_pubmed_xml

# This XML sample is designed to test multiple edge cases in a single run.
COMPLEX_DIRTY_XML = """
<PubmedArticleSet>
  <!--
    Case 1: A complex but mostly valid record.
    - Has multiple authors, one of which is a collective.
    - Has multiple MeSH headings.
    - Has a valid PubDate and DateRevised.
  -->
  <PubmedArticle>
    <MedlineCitation Status="MEDLINE" Owner="NLM">
      <PMID Version="1">50001</PMID>
      <DateRevised>
        <Year>2022</Year><Month>10</Month><Day>15</Day>
      </DateRevised>
      <Article PubModel="Print">
        <Journal>
          <ISSN IssnType="Print">1111-2222</ISSN>
          <Title>Journal of Complex Tests</Title>
        </Journal>
        <ArticleTitle>A complex article for testing.</ArticleTitle>
        <Abstract>
          <AbstractText>This is the abstract.</AbstractText>
        </Abstract>
        <AuthorList CompleteYN="Y">
          <Author ValidYN="Y">
            <LastName>Smith</LastName>
            <ForeName>Jane</ForeName>
            <Initials>J</Initials>
          </Author>
          <Author ValidYN="Y">
            <CollectiveName>The Testing Consortium</CollectiveName>
          </Author>
          <Author ValidYN="Y">
            <LastName>Jones</LastName>
            <ForeName>Peter</ForeName>
            <Initials>P</Initials>
          </Author>
        </AuthorList>
        <ArticleDate PubStatus="pubmed">
            <Year>2021</Year>
            <Month>11</Month>
            <Day>01</Day>
        </ArticleDate>
      </Article>
      <MeshHeadingList>
        <MeshHeading>
          <DescriptorName UI="D000001" MajorTopicYN="N">Anatomy</DescriptorName>
        </MeshHeading>
        <MeshHeading>
          <DescriptorName UI="D000002" MajorTopicYN="Y">Complex Systems</DescriptorName>
        </MeshHeading>
      </MeshHeadingList>
    </MedlineCitation>
  </PubmedArticle>

  <!--
    Case 2: A "dirty" record with missing and invalid data.
    - The same PMID as a DeleteCitation below.
    - Invalid DateRevised (should fall back to None).
    - No Journal.
    - AuthorList is malformed: one author with no last name, one empty author tag.
    - MeshHeadingList has a heading with no DescriptorName text.
  -->
  <PubmedArticle>
    <MedlineCitation Status="In-Data-Review" Owner="NLM">
      <PMID>50002</PMID>
      <DateRevised>
        <Year>2023</Year><Month>Invalid</Month><Day>32</Day>
      </DateRevised>
      <Article PubModel="Electronic">
        <ArticleTitle>A dirty article with missing data.</ArticleTitle>
        <AuthorList>
          <Author>
            <ForeName>No</ForeName>
            <Initials>LN</Initials>
          </Author>
          <Author/>
          <Author>
            <LastName>Valid</LastName>
            <ForeName>Is</ForeName>
            <Initials>IV</Initials>
          </Author>
        </AuthorList>
        <Journal>
            <JournalIssue>
                <PubDate>
                    <Year>2023</Year>
                    <Month>Feb</Month>
                </PubDate>
            </JournalIssue>
        </Journal>
      </Article>
      <MeshHeadingList>
        <MeshHeading>
          <DescriptorName UI="D000003" MajorTopicYN="N"></DescriptorName>
        </MeshHeading>
        <MeshHeading>
          <DescriptorName UI="D000004" MajorTopicYN="Y">Good Term</DescriptorName>
        </MeshHeading>
      </MeshHeadingList>
    </MedlineCitation>
  </PubmedArticle>

  <!--
    Case 3: A record with no authors and no abstract.
  -->
  <PubmedArticle>
    <MedlineCitation>
        <PMID>50003</PMID>
        <Article>
            <ArticleTitle>A minimal article.</ArticleTitle>
        </Article>
    </MedlineCitation>
  </PubmedArticle>

  <!--
    Case 4: Deletion of a PMID that is also being upserted in this file.
  -->
  <DeleteCitation>
    <PMID>50002</PMID>
  </DeleteCitation>

</PubmedArticleSet>
"""


@pytest.fixture
def complex_xml_gz_file(tmp_path: Path) -> str:
    """Creates a temporary gzipped XML file with complex and dirty data."""
    file_path = tmp_path / "complex_dirty.xml.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write(COMPLEX_DIRTY_XML)
    return str(file_path)


def test_parser_handles_complex_and_dirty_data(complex_xml_gz_file: str):
    """
    Given a complex XML file with a mix of valid, invalid, and edge-case data,
    this test ensures the parser processes the file gracefully, extracting all
    valid data while correctly ignoring or handling malformed sections.
    """
    # Parse the file in 'BOTH' mode to get all data types
    results = list(parse_pubmed_xml(complex_xml_gz_file, load_mode="BOTH", chunk_size=10))

    # We expect two chunks: one for UPSERT and one for DELETE
    assert len(results) == 2
    upsert_op = next(r for r in results if r[0] == "UPSERT")
    delete_op = next(r for r in results if r[0] == "DELETE")

    assert upsert_op is not None
    assert delete_op is not None

    # --- 1. Validate the UPSERT chunk ---
    _, upsert_chunk = upsert_op

    # Check that we have data for all expected tables
    assert "citations_json" in upsert_chunk
    assert "citations" in upsert_chunk
    assert "authors" in upsert_chunk
    assert "citation_authors" in upsert_chunk
    assert "mesh_terms" in upsert_chunk
    assert "citation_mesh_terms" in upsert_chunk

    # There should be 3 citations in total
    assert len(upsert_chunk["citations"]) == 3
    citations = {c.pmid: c for c in upsert_chunk["citations"]}
    citations_json = {c.pmid: c for c in upsert_chunk["citations_json"]}

    # --- Case 1: Complex Valid Record (PMID 50001) ---
    assert 50001 in citations
    c1 = citations[50001]
    c1_json = citations_json[50001]
    assert c1.title == "A complex article for testing."
    assert c1.abstract == "This is the abstract."
    assert c1_json.date_revised == datetime.date(2022, 10, 15)

    # Check authors for PMID 50001
    c1_authors = [ca for ca in upsert_chunk["citation_authors"] if ca.pmid == 50001]
    assert len(c1_authors) == 3
    db_authors = {a.author_id: a for a in upsert_chunk["authors"]}
    author_names = {db_authors[ca.author_id].last_name for ca in c1_authors}
    assert author_names == {"Smith", "The Testing Consortium", "Jones"}

    # --- Case 2: Dirty Record (PMID 50002) ---
    assert 50002 in citations
    c2 = citations[50002]
    c2_json = citations_json[50002]
    assert c2.title == "A dirty article with missing data."
    assert c2_json.date_revised is None, "Invalid DateRevised should result in None"
    assert c2.publication_date == datetime.date(2023, 2, 1), "Should parse PubDate correctly"

    # Check authors for PMID 50002 - should only parse the valid one
    c2_authors = [ca for ca in upsert_chunk["citation_authors"] if ca.pmid == 50002]
    assert len(c2_authors) == 1
    assert db_authors[c2_authors[0].author_id].last_name == "Valid"

    # Check MeSH terms for PMID 50002 - should only parse the valid one
    c2_mesh = [cm for cm in upsert_chunk["citation_mesh_terms"] if cm.pmid == 50002]
    assert len(c2_mesh) == 1
    db_mesh = {m.mesh_id: m for m in upsert_chunk["mesh_terms"]}
    assert db_mesh[c2_mesh[0].mesh_id].term == "Good Term"

    # --- Case 3: Minimal Record (PMID 50003) ---
    assert 50003 in citations
    c3 = citations[50003]
    assert c3.title == "A minimal article."
    assert c3.abstract is None
    c3_authors = [ca for ca in upsert_chunk["citation_authors"] if ca.pmid == 50003]
    assert len(c3_authors) == 0 # No authors

    # --- 2. Validate the DELETE chunk ---
    _, delete_chunk = delete_op
    assert "pmids" in delete_chunk
    assert delete_chunk["pmids"] == [50002]
