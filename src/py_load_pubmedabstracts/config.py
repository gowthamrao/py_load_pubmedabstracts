"""
Application settings management.

This module defines the configuration settings for the application,
leveraging Pydantic's `BaseSettings` for environment variable loading.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""

    # Using model_config to specify that settings should be loaded from a .env file
    # and have a prefix 'PML_'.
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", env_prefix="PML_"
    )

    db_connection_string: str = "postgresql://user:password@localhost:5432/pubmed"
    db_adapter: str = "postgresql"
    local_staging_dir: str = "/tmp/pubmed_staging"
    load_mode: str = "FULL"  # or "NORMALIZED" or "BOTH"
