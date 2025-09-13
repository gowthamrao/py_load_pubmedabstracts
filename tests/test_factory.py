import pytest
from py_load_pubmedabstracts.db.factory import get_adapter
from py_load_pubmedabstracts.db.postgresql import PostgresAdapter


def test_get_adapter_success():
    """Test that the postgresql adapter is returned successfully."""
    adapter = get_adapter("postgresql", "dummy_dsn")
    assert isinstance(adapter, PostgresAdapter)


def test_get_adapter_not_found():
    """Test that a ValueError is raised when the adapter is not found."""
    with pytest.raises(ValueError, match="Database adapter 'nonexistent' not found."):
        get_adapter("nonexistent", "dummy_dsn")
