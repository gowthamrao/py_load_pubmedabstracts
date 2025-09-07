"""Configuration management for the application.

This module uses pydantic-settings to load configuration from environment
variables. It ensures that all required configuration is present and valid.

To improve testability, the settings object is not created at import time.
Instead, a `get_settings` function is provided, which uses a cache to ensure
that the settings are loaded only once (singleton pattern).
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Defines the application's configuration parameters.

    Attributes:
        PML_DB_CONNECTION_STRING: The connection string for the target database.
    """

    PML_DB_CONNECTION_STRING: str

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


@lru_cache
def get_settings() -> Settings:
    """
    Returns the application settings.

    The settings are loaded from environment variables and/or a .env file.
    The lru_cache decorator ensures that the Settings object is created only
    once.
    """
    return Settings()
