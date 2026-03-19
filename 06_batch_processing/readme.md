# Docker Setup Guide

## Setup Steps

1. Create the `Dockerfile`
2. Create the `docker-compose.yaml`
3. Write `test_script.py` to verify everything is working correctly

## Activating the Setup

1. Change into the code directory
2. Start the local Docker app
3. Run the following Docker commands:

| Command | Description |
|---|---|
| `docker-compose up -d` | Build and start the container |
| `docker-compose ps` | Check if the container is running |
| `docker-compose exec spark python test_spark.py` | Test Python with Spark |

---

## Errors Encountered During Docker Configuration

### 1. Obsolete `version` attribute warning

**Error:**
```
level=warning msg="docker-compose.yml: the attribute `version` is obsolete,
it will be ignored, please remove it to avoid potential confusion"
```

**Cause:** Recent versions of Docker Compose (V2) have moved to the Compose Specification format, making the `version` field obsolete.

**Fix:** Comment out the `version` field — kept for reference.

---

### 2. Dockerfile not found

**Error:**
```
failed to solve: failed to read dockerfile: open Dockerfile: no such file or directory
```

**Cause:** `build: .` in `docker-compose.yml` looks for a `Dockerfile` in the same directory as the compose file.

**Fix:** Updated the build context to point to the directory containing the `Dockerfile`.

---

## Lessons Learned

> **Consistency matters** — make sure the working directory is the same across your `Dockerfile` and `docker-compose.yml`. Mixing `/app` in one and `/workspace` in the other will cause issues.