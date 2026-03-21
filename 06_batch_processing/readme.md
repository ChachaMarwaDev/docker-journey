# Batch Processing with PySpark — Homework 6

## Overview

This homework covers **batch processing** using Apache Spark and PySpark inside a Docker container.
Since the course instructor uses Linux and this setup runs on Windows, Docker is used to create
an identical Linux environment locally.

**Dataset:** NYC Yellow Taxi trip data — November 2025  
**Engine:** Apache Spark 4.1.1  
**Environment:** Ubuntu 22.04 inside Docker

---

## Project Structure

```
06_batch_processing/
├── .devcontainer/
│   └── devcontainer.json       # VS Code Dev Container config
├── code/
│   └── test_spark.py           # Verifies Spark is working correctly
├── docker-compose.yml          # Defines spark + jupyter services
├── README.md
└── setup/
    └── Dockerfile              # Builds the Ubuntu + Java + Python image
```

---

## Prerequisites

Before starting, make sure you have installed:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (and that it is running)
- [VS Code](https://code.visualstudio.com/) with the **Dev Containers** extension
- Git (with line endings configured — see Lessons Learned below)

---

## Services

This project defines two Docker services in `docker-compose.yml`:

| Service | Purpose | Port |
|---|---|---|
| `spark` | VS Code Dev Container — run scripts here | 4040 (Spark UI) |
| `jupyter` | Browser-based notebook interface | 8888 |

---

## Setup Steps

1. Clone or create the project folder
2. Create `setup/Dockerfile`
3. Create `docker-compose.yml`
4. Place your scripts in the `code/` folder
5. Start Docker Desktop

---

## Running the Environment

### Option A — VS Code Dev Container (recommended)

```powershell
# 1. Open the project in VS Code
# 2. Press Ctrl+Shift+P → "Reopen in Container"
# VS Code will build the image and connect automatically
```

### Option B — Terminal (spark service)

```powershell
# Build the image (only needed after Dockerfile changes)
docker-compose build --no-cache

# Start the spark container in the background
docker-compose up -d spark

# Verify it is running
docker-compose ps

# Open a terminal inside the container
docker-compose exec spark bash

# Run your scripts from inside the container
python test_spark.py

# Stop the container when done
docker-compose down
```

### Option C — Jupyter Notebook in browser

```powershell
# Start the Jupyter service
docker-compose up -d jupyter

# Open in browser
# http://localhost:8888

# Stop when done
docker-compose down
```

---

## Common Commands Reference

| Command | Description |
|---|---|
| `docker-compose build --no-cache` | Rebuild the image from scratch |
| `docker-compose up -d` | Start all services in the background |
| `docker-compose up -d spark` | Start only the spark service |
| `docker-compose up -d jupyter` | Start only the jupyter service |
| `docker-compose ps` | Check which containers are running |
| `docker-compose logs jupyter` | See output/errors from a service |
| `docker-compose exec spark bash` | Open a terminal inside the spark container |
| `docker-compose exec spark python test_spark.py` | Run a script directly |
| `docker-compose down` | Stop and remove all containers |

---

## Homework Questions & Answers

| # | Question | Answer |
|---|---|---|
| Q1 | Install Spark and PySpark | ✅ Spark 4.1.1 running via Docker |
| Q2 | Size of Yellow Taxi Nov 2025 in Spark | **75 MB** (68.4 MiB in memory) |
| Q3 | Count records on November 15 2025 | **162,604** |
| Q4 | Longest trip duration (hours) | **90.6** |
| Q5 | Spark UI default port | **4040** |
| Q6 | Least frequent pickup zone | **Governor's Island/Ellis Island/Liberty Island** |

---

## Errors Encountered

### 1. Obsolete `version` attribute warning

**Error:**
```
level=warning msg="docker-compose.yml: the attribute `version` is obsolete"
```
**Cause:** Docker Compose V2 no longer uses the `version` field.  
**Fix:** Remove the `version:` line from `docker-compose.yml`.

---

### 2. Dockerfile not found

**Error:**
```
failed to solve: failed to read dockerfile: open Dockerfile: no such file or directory
```
**Cause:** `build: .` looks for a `Dockerfile` in the same folder as `docker-compose.yml`.  
**Fix:** Use `build.context` and `build.dockerfile` to point explicitly to `setup/Dockerfile`.

---

### 3. Jupyter container exits immediately

**Error:**
```
bash: --ip=0.0.0.0: command not found
```
**Cause:** YAML multiline `>` block splits each flag onto a separate line, so bash receives
each flag as a standalone command instead of as arguments to jupyter.  
**Fix:** Write the entire jupyter command on a single line inside `bash -c "..."`.

---

### 4. Port conflict — two services on 8888

**Error:** One of the services silently fails to start.  
**Cause:** Two services cannot bind to the same host port at the same time.  
**Fix:** Only `jupyter` uses `8888:8888`. The `spark` service uses `4040:4040` only.

---

### 5. Service not running on exec

**Error:**
```
service "spark" is not running
```
**Cause:** `docker-compose exec` requires the container to already be running.
`docker-compose build` creates the image but does NOT start the container.  
**Fix:** Always run `docker-compose up -d` before `docker-compose exec`.

---

## Lessons Learned

> **Build ≠ Run.** `docker-compose build` creates the image. `docker-compose up` starts the container.
> You need both before you can exec into a service.

> **Consistency matters.** Make sure the working directory is the same across `Dockerfile`
> and `docker-compose.yml`. Mixing `/app` in one and `/workspace` in the other causes path issues.

> **Ports must be unique.** Each host port can only be used by one container at a time.
> If two services need the same internal port (e.g. 4040), map them to different host ports
> (e.g. `4040:4040` and `4041:4040`).

> **Windows line endings.** Git on Windows converts line endings (LF → CRLF) which can break
> scripts running inside Linux containers. Fix once with:
> ```
> git config --global core.autocrlf false
> ```

> **Spark UI (port 4040) is only live during active jobs.** If no Spark job is running,
> the page returns an empty response. This is normal behaviour.

> **Parquet files expand in memory.** A 68 MB Parquet file loaded into Spark occupies ~222 MB
> in memory. Parquet is compressed on disk — Spark decompresses it when loading.

The directory Tree

    ├── 06_batch_processing
    │   ├── .devcontainer
    │   │   └── devcontainer.json
    │   ├── code
    │   │   ├── .ipynb_checkpoints
    │   │   │   ├── batch_homework-checkpoint.ipynb
    │   │   │   └── test_spark-checkpoint.py
    │   │   ├── zones
    │   │   │   ├── .ipynb_checkpoints
    │   │   │   │   └── _SUCCESS-checkpoint
    │   │   │   ├── ._SUCCESS.crc
    │   │   │   ├── .part-00000-b0126063-7020-4f2f-832f-8b9e708b1bc2-c000.snappy.parquet.crc
    │   │   │   ├── _SUCCESS
    │   │   │   └── part-00000-b0126063-7020-4f2f-832f-8b9e708b1bc2-c000.snappy.parquet
    │   │   ├── batch_homework.ipynb
    │   │   ├── batch_homework.py
    │   │   ├── taxi_zone_lookup.csv
    │   │   ├── test_spark.py
    │   │   └── yellow_tripdata_2025-11.parquet
    │   ├── setup
    │   │   └── Dockerfile
    │   ├── .gitignore
    │   ├── docker-compose.yml
    │   └── readme.md