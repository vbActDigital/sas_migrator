FROM python:3.12-slim

LABEL maintainer="SAS Migration Toolkit"
LABEL description="Dockerized MVP1 Discovery + MVP2 Migration demo with mock data"

WORKDIR /app

# Install OS deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pytest

# Copy project
COPY . .

# Generate mock environment at build time
RUN python tests/create_mock_environment.py

# Default: run full demo pipeline
CMD ["python", "tests/run_docker_demo.py"]
