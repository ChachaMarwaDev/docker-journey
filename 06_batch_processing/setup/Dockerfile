# ============================================================
# Spark Course - Dockerfile
# Place this file at: 06_batch_processing/setup/Dockerfile
# ============================================================

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies + Java (Spark needs Java to run)
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    openjdk-17-jdk \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Make "python" work as a command (Ubuntu uses "python3" by default)
RUN ln -s /usr/bin/python3 /usr/bin/python

# Tell Java where it lives (Spark needs this)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Create a Python virtual environment at /opt/venv
RUN python3 -m venv /opt/venv

# Always use the venv's Python and pip
ENV PATH="/opt/venv/bin:${PATH}"

# Install Python packages into the venv
RUN /opt/venv/bin/pip install --upgrade pip && \
    /opt/venv/bin/pip install pyspark jupyter ipykernel pandas

# Create the workspace directory
WORKDIR /workspace/code

# Keep the container alive (VS Code / devcontainer will connect to it)
CMD ["tail", "-f", "/dev/null"]
