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
| `docker-compose exec spark /bin/bash` | Overrides python entrypoint on docker to bash|
| `docker-compose run --rm spark python your_homework_script.py` | From the host machine (windows) |
| `python your_homework_script.py` | From inside bash |
| `docker-compose down` | To stop the container |
| `jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root` | Once in bash, to start jupyter |
| `docker-compose run --rm -p 8888:8888 spark` | Start juypter with port mapping |
| `docker-compose down` | Stop everything when done for the day |

---

> Since we are using juypter to run pyspark commands 

In docker-compose I have defined three services:
*Spark service*
    ```powershell
    # Start the spark service in background
    docker-compose up -d spark

    # Get into bash
    docker-compose exec spark bash

    # Run your scripts
    python test_spark.py

    # When done
    docker-compose down
    ```

*Jupyter service*
   ```powershell
    # Start Jupyter directly
    docker-compose up jupyter

    # Or run in background
    docker-compose up -d jupyter

    # Access at http://localhost:8888

    # Stop when done
    docker-compose down
   ```
*Spark shell (REPL)*
    ```powershell 
    # Start Python interactive shell
    docker-compose up spark-shell

    # You'll get a Python REPL where you can:
    # >>> from pyspark.sql import SparkSession
    # >>> spark = SparkSession.builder.master("local[*]").appName('test').getOrCreate()
    # >>> df = spark.range(10)
    # >>> df.show()
    ```
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