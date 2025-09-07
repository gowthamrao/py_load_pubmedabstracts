from abc import ABC, abstractmethod
from typing import Any, List, Generator

class DatabaseAdapter(ABC):
    """Abstract Base Class for database adapters."""

    @abstractmethod
    def initialize_schema(self, mode: str) -> None:
        """Creates target tables and metadata structures."""
        raise NotImplementedError

    @abstractmethod
    def create_staging_tables(self) -> None:
        """Creates temporary/transient tables for the current load."""
        raise NotImplementedError

    @abstractmethod
    def bulk_load_chunk(
        self, data_chunk: Generator[dict, None, None], target_table: str
    ) -> None:
        """Efficiently loads a chunk of data using native capabilities."""
        raise NotImplementedError

    @abstractmethod
    def process_deletions(self, pmid_list: List[int]) -> None:
        """Removes specified PMIDs."""
        raise NotImplementedError

    @abstractmethod
    def execute_merge_strategy(self) -> None:
        """Moves data from staging to final tables using UPSERT logic."""
        raise NotImplementedError

    @abstractmethod
    def manage_load_state(
        self, file_name: str, status: str, checksum: str | None = None
    ) -> None:
        """Interface for state management interactions."""
        raise NotImplementedError

    @abstractmethod
    def optimize_database(self, stage: str) -> None:
        """Hooks for pre-load and post-load optimizations."""
        raise NotImplementedError

    @abstractmethod
    def get_completed_files(self) -> List[str]:
        """Gets a list of successfully processed file names from the history table."""
        raise NotImplementedError
