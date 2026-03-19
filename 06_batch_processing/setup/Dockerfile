FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    openjdk-17-jdk \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic link to make 'python' command available (optional but convenient)
RUN ln -s /usr/bin/python3 /usr/bin/python

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install uv
RUN pip3 install uv

# Create a virtual environment (this is key for IDE integration)
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Initialize uv project and install pyspark in the virtual environment
RUN uv init && \
    /venv/bin/pip install --upgrade pip && \
    /venv/bin/pip install pyspark jupyter ipykernel

# Create app directory
WORKDIR /workspace

# Copy your scripts
COPY . .

# Set the entry point
ENTRYPOINT ["/bin/bash"]