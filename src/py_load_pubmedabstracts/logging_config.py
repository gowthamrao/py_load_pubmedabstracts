"""Logging configuration for the application."""
import json
import logging
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """Formats log records as JSON strings."""

    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record into a JSON string.

        The formatter includes a default set of attributes from the LogRecord,
        plus any extra attributes passed to the logger.
        """
        # Base attributes from the log record
        log_object: Dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
        }

        # Add any extra fields passed to the logger
        if hasattr(record, "__dict__"):
            extra_items = {
                key: value
                for key, value in record.__dict__.items()
                if key not in logging.LogRecord.__slots__ and key not in log_object
            }
            log_object.update(extra_items)

        # Add exception info if present
        if record.exc_info:
            log_object["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            log_object["exception"] = record.exc_text

        return json.dumps(log_object)


def configure_logging() -> None:
    """
    Configure the root logger for the application.

    It sets the logging level to INFO and adds a stream handler
    that uses the JSONFormatter to output logs to standard out.
    """
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove any existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a new handler and set the JSONFormatter
    handler = logging.StreamHandler()
    formatter = JSONFormatter()
    handler.setFormatter(formatter)

    # Add the new handler to the root logger
    logger.addHandler(handler)
