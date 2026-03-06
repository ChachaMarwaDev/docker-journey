FROM python:3.11-slim

RUN pip install --no-cache-dir uv

RUN uv pip install --system --no-cache \
    duckdb \
    jupyter \
    pandas \
    numpy

RUN curl -LsSf https://getbruin.com/install/cli | sh

WORKDIR /workspace
COPY . .