# Setting up
    1. Setting up docker file
    2. Setting up the docker-compose.yaml
    3. Writing a test_script.py to see if everythin is working correctly

## Activating the setup
    1. Changing into the code directory
    2. Make the local docker app active
    2. Running docker commands;
        i. `docker-compose up -d` - build and start the container
        ii. `docker-compose ps` - to check if container is running
        iii. `docker-compose exec spark python test_spark.py` - test python with spark




# Errors encountered during docker configuration
## What were the errors
    a. level=warning msg="C:\\dev\\docker-journey\\06_batch_processing\\docker-compose.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion"
    b. failed to solve: failed to read dockerfile: open Dockerfile: no such file or directory

## Why do we get the error
    a. This warning appears because you're using a recent version of Docker Compose (V2) which has moved to the Compose Specification format. The version field is no longer needed and is now considered obsolete.
    b. build . in the docker-compose is looking for dockerfile in the current directory where the docker-compose is located

## How I solved them according to their listing
    a. Keep it commented for reference
    b. I updated the build context to point to directory containing the dockerfile


### I forgot
    1. consistency in my dockerfile and docker-compose.yml where one has /app and the other /workspace