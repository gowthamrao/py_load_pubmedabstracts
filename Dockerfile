# Stage 1: Builder
# This stage installs dependencies using Poetry
FROM python:3.10-slim as builder

# Install poetry
RUN pip install poetry==1.8.2

# Set the working directory
WORKDIR /app

# Configure Poetry to create the virtualenv in the project's root
RUN poetry config virtualenvs.in-project true

# Copy the dependency files
COPY poetry.lock pyproject.toml ./

# Install dependencies into a virtual environment
# --no-root: Don't install the project itself, just the dependencies
# --no-dev: Exclude development dependencies
RUN poetry install --no-root --no-dev

# Stage 2: Final Image
# This stage creates the lean production image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /app/.venv/ .venv/

# Copy the application source code
COPY src/py_load_pubmedabstracts/ ./py_load_pubmedabstracts/

# Add the virtual environment's bin directory to the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Set the entrypoint to the CLI application
ENTRYPOINT ["py-load-pubmedabstracts"]

# Set a default command to show help, which is useful for discoverability
CMD ["--help"]
