"""Abstract base class for database adapters."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List


class DatabaseAdapter(ABC):
    """Abstract Base Class for database adapters."""

    @abstractmethod
    def initialize_schema(self, mode: str) -> None:
        """Create target tables and metadata structures."""
        raise NotImplementedError

    @abstractmethod
    def create_staging_tables(self, mode: str) -> None:
        """Create temporary/transient tables for the current load."""
        raise NotImplementedError

    @abstractmethod
    def bulk_load_chunk(
        self, data_chunk: Dict[str, List[Dict[str, Any]]]
    ) -> None:
        """Efficiently load a chunk of data using native capabilities."""
        raise NotImplementedError

    @abstractmethod
    def process_deletions(self, pmid_list: List[int], mode: str) -> None:
        """Remove specified PMIDs."""
        raise NotImplementedError

    @abstractmethod
    def execute_merge_strategy(self, mode: str, is_initial_load: bool = False) -> None:
        """
        Move data from staging to final tables.

        Args:
            mode: The load mode (e.g., 'FULL', 'NORMALIZED').
            is_initial_load: If True, performs a simple INSERT, assuming the
                             target table has no constraints. If False, performs
                             an UPSERT (INSERT ... ON CONFLICT).

        """
        raise NotImplementedError

    @abstractmethod
    def manage_load_state(
        self,
        file_name: str,
        status: str,
        file_type: str | None = None,
        md5_checksum: str | None = None,
        records_processed: int | None = None,
    ) -> None:
        """
        Manage the state of a file in the _pubmed_load_history table.

        This method should handle both inserting a new record for a file and
        updating the status and other metadata of an existing record.
        """
        raise NotImplementedError

    @abstractmethod
    def optimize_database(self, stage: str, mode: str) -> None:
        """Run pre-load and post-load optimizations."""
        raise NotImplementedError

    @abstractmethod
    def get_completed_files(self) -> List[str]:
        """Get a list of successfully processed file names from the history table."""
        raise NotImplementedError

    @abstractmethod
    def reset_failed_files(self) -> int:
        """
        Reset the status of any 'FAILED' files to 'PENDING' for reprocessing.

        Returns:
            The number of files that were reset.

        """
        raise NotImplementedError

    @abstractmethod
    def has_completed_baseline(self) -> bool:
        """
        Check if at least one baseline file has been successfully loaded.

        Returns:
            True if a baseline load is complete, False otherwise.

        """
        raise NotImplementedError
