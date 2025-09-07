import datetime
from typing import Optional
from pydantic import BaseModel, Field


class LoadHistory(BaseModel):
    """Pydantic model for the _pubmed_load_history table."""

    file_name: str
    file_type: str  # 'BASELINE' or 'DELTA'
    md5_checksum: Optional[str] = None
    download_timestamp: Optional[datetime.datetime] = None
    load_start_timestamp: Optional[datetime.datetime] = None
    load_end_timestamp: Optional[datetime.datetime] = None
    status: str  # 'PENDING', 'DOWNLOADING', 'LOADING', 'COMPLETE', 'FAILED'
    records_processed: Optional[int] = None


class CitationJson(BaseModel):
    """Pydantic model for the citations_json table."""

    pmid: int
    date_revised: Optional[datetime.date] = None
    data: dict


# --- Normalized Schema Models ---

class Journal(BaseModel):
    """Pydantic model for the journals table."""
    issn: str
    title: Optional[str] = None
    iso_abbreviation: Optional[str] = None

class Author(BaseModel):
    """Pydantic model for the authors table."""
    author_id: int
    last_name: Optional[str] = None
    fore_name: Optional[str] = None
    initials: Optional[str] = None

class MeshTerm(BaseModel):
    """Pydantic model for the mesh_terms table."""
    mesh_id: int
    term: str
    is_major_topic: bool

class Citation(BaseModel):
    """Pydantic model for the citations table."""
    pmid: int
    title: Optional[str] = None
    abstract: Optional[str] = None
    publication_date: Optional[datetime.date] = None
    journal_issn: Optional[str] = None

class CitationAuthor(BaseModel):
    """Pydantic model for the citation_authors junction table."""
    pmid: int
    author_id: int
    display_order: int

class CitationMeshTerm(BaseModel):
    """Pydantic model for the citation_mesh_terms junction table."""
    pmid: int
    mesh_id: int
