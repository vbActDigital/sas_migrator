FROM python:3.12-slim

LABEL maintainer="SAS Migration Toolkit"
LABEL description="SAS Migration Toolkit - Discovery + Migration with PDF reports"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pytest

COPY . .

RUN python tests/create_mock_environment.py

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV TERM=xterm-256color

CMD ["python", "tests/run_mvp1_docker.py"]
