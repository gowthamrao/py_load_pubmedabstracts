import pytest
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="module")
def postgres_container():
    """
    Starts a PostgreSQL container for the test session.
    The container is shared across all tests in a module.
    """
    try:
        with PostgresContainer("postgres:15-alpine") as container:
            # The DSN (Data Source Name) needs to be compatible with psycopg3,
            # which doesn't use the "+psycopg2" dialect string.
            container.get_connection_url = (
                lambda: super(PostgresContainer, container).get_connection_url().replace("+psycopg2", "")
            )
            yield container
    except Exception as e:
        # This is a fallback for environments where Docker is not available
        # or is inaccessible (e.g., due to permissions or rate limiting).
        print(f"Could not start Docker container: {e}")
        print("Integration tests will be skipped.")
        pytest.skip("Skipping integration tests: Docker not available, permission denied, or rate-limited.")
