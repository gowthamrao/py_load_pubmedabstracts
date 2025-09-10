import pytest
from testcontainers.postgres import PostgresContainer

# This fixture is function-scoped, ensuring every test gets a fresh database.
# This is crucial for test isolation, especially with parameterized tests.
@pytest.fixture(scope="function")
def postgres_container() -> PostgresContainer:
    """
    Starts a PostgreSQL container for a single test function.
    """
    try:
        # The 'with' statement ensures the container is stopped and removed
        # even if tests fail, which is robust.
        with PostgresContainer("postgres:16-alpine") as container:
            yield container
    except Exception as e:
        pytest.skip(f"Skipping integration tests: Docker not available. Error: {e}")


@pytest.fixture(scope="function")
def db_conn_str(postgres_container: PostgresContainer) -> str:
    """
    Provides a psycopg3-compatible connection string from the container.
    """
    # The default get_connection_url() might include a dialect like "+psycopg2"
    # that psycopg3 doesn't understand. This replacement makes it compatible.
    return postgres_container.get_connection_url().replace("+psycopg2", "")
