"""Factory for creating database adapters."""
import sys
from typing import TYPE_CHECKING

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

if TYPE_CHECKING:
    from .base import DatabaseAdapter

ADAPTER_GROUP = "py_load_pubmedabstracts.database_adapters"


def get_adapter(adapter_name: str, dsn: str) -> "DatabaseAdapter":
    """
    Dynamically discover and load a DatabaseAdapter plugin.

    Args:
        adapter_name: The name of the adapter to load (e.g., 'postgresql').
        dsn: The database connection string for the adapter's constructor.

    Returns:
        An initialized instance of the requested DatabaseAdapter.

    Raises:
        ValueError: If the requested adapter is not found.

    """
    discovered_plugins = entry_points(group=ADAPTER_GROUP)

    try:
        plugin = discovered_plugins[adapter_name]
    except KeyError:
        raise ValueError(
            f"Database adapter '{adapter_name}' not found. "
            f"Available adapters: {[ep.name for ep in discovered_plugins]}"
        ) from None

    adapter_class = plugin.load()
    return adapter_class(dsn=dsn)
