FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl

RUN pip install --no-cache-dir uv

RUN uv pip install --system --no-cache \
    duckdb \
    jupyter \
    pandas \
    numpy \ 
    psycopg2-binary

RUN curl -LsSf https://getbruin.com/install/cli | sh

WORKDIR /workspace
COPY . .