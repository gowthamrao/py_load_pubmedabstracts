import logging
import json
import pytest
from py_load_pubmedabstracts.logging_config import JSONFormatter, configure_logging

@pytest.fixture
def logger():
    """Fixture to create a logger for testing."""
    _logger = logging.getLogger("test_logger")
    _logger.setLevel(logging.INFO)
    return _logger

@pytest.fixture
def formatter():
    """Fixture to create a JSONFormatter for testing."""
    return JSONFormatter()

def test_json_formatter_basic_log(formatter, logger):
    """Test that a basic log message is formatted correctly."""
    record = logger.makeRecord("test_logger", logging.INFO, "test_file.py", 10, "test message", (), None)
    result = json.loads(formatter.format(record))
    assert result["level"] == "INFO"
    assert result["message"] == "test message"
    assert result["name"] == "test_logger"

def test_json_formatter_with_extra_data(formatter, logger):
    """Test that a log message with extra data is formatted correctly."""
    record = logger.makeRecord("test_logger", logging.INFO, "test_file.py", 10, "test message", (), None)
    record.extra_data = "some value"
    result = json.loads(formatter.format(record))
    assert result["extra_data"] == "some value"

def test_json_formatter_with_exception(formatter, logger):
    """Test that a log message with an exception is formatted correctly."""
    try:
        raise ValueError("test exception")
    except ValueError as e:
        record = logger.makeRecord("test_logger", logging.ERROR, "test_file.py", 10, "test message", (), (type(e), e, e.__traceback__))
    result = json.loads(formatter.format(record))
    assert result["level"] == "ERROR"
    assert "exception" in result
    assert "ValueError: test exception" in result["exception"]

def test_json_formatter_with_exc_text(formatter, logger):
    """Test that a log message with exc_text is formatted correctly."""
    record = logger.makeRecord("test_logger", logging.ERROR, "test_file.py", 10, "test message", (), None)
    record.exc_text = "test exception text"
    result = json.loads(formatter.format(record))
    assert result["level"] == "ERROR"
    assert "exception" in result
    assert result["exception"] == "test exception text"

def test_configure_logging(mocker):
    """Test that the root logger is configured correctly."""
    mock_get_logger = mocker.patch("logging.getLogger")
    mock_logger = mock_get_logger.return_value
    mock_logger.hasHandlers.return_value = False

    configure_logging()

    mock_get_logger.assert_called_with()
    mock_logger.setLevel.assert_called_with(logging.INFO)
    mock_logger.addHandler.assert_called_once()

def test_configure_logging_with_existing_handlers(mocker):
    """Test that existing handlers are cleared."""
    mock_get_logger = mocker.patch("logging.getLogger")
    mock_logger = mock_get_logger.return_value
    mock_logger.hasHandlers.return_value = True

    configure_logging()

    mock_logger.handlers.clear.assert_called_once()
