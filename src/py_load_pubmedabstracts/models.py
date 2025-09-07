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
    date_revised: datetime.date
    data: dict
