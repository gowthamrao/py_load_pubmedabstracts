"""Defines the interface for all database adapters.

This module contains the Abstract Base Class (ABC) `DatabaseAdapter`, which
enforces a standard contract that all database-specific implementations must
adhere to. This follows the Adapter pattern and is key to the package's
extensibility, as outlined in the Functional Requirements Document (FRD).
"""

import abc
from typing import Any, List, Dict

class DatabaseAdapter(abc.ABC):
    """
    An abstract interface for database operations.

    This class defines the contract for all database adapters, ensuring that
    the core application logic can interact with different database backends
    in a consistent way.
    """

    @abc.abstractmethod
    def initialize_schema(self) -> None:
        """
        Create all necessary tables, indexes, and metadata structures.
        This includes the main data tables and the load history table.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def manage_load_state(self, file_name: str, status: str, checksum: str) -> None:
        """
        Update the state of a file in the load history table.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def create_staging_tables(self) -> None:
        """
        Create temporary/transient tables for the current load.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def bulk_load_chunk(self, data_chunk: List[Dict[str, Any]], target_table: str) -> None:
        """
        Efficiently load a chunk of data into a target table using native
        database capabilities (e.g., COPY command).
        """
        raise NotImplementedError

    @abc.abstractmethod
    def process_deletions(self, pmid_list: List[int]) -> None:
        """
        Remove records from the database based on a list of PMIDs.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def execute_merge_strategy(self) -> None:
        """
        Move data from staging tables to the final destination tables using
        an UPSERT (insert on conflict update) strategy.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def optimize_database(self, stage: str) -> None:
        """
        Perform database optimizations.
        For example, drop indexes before a load ('pre-load' stage) and
        rebuild them after ('post-load' stage).
        """
        raise NotImplementedError
