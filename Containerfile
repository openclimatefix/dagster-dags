FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV DAGSTER_HOME=/opt/dagster/home

# Add repository code
WORKDIR /opt/dagster/app
COPY src /opt/dagster/app
COPY pyproject.toml /opt/dagster/app

# Checkout and install dagster libraries needed to run the gRPC server by exposing
# your code location to dagster-webserver and dagster-daemon, and loading the
# DagsterInstance.
RUN uv sync

# Set the code location module to be loaded by the gRPC server
ENV MODULE_NAME=local_archives

# Using CMD rather than RUN allows the command to be overridden in
# run launchers or executors to run other commands using this image.
# This is important as runs are executed inside this container.
ENTRYPOINT ["uv", "run"]
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "local_archives"]

