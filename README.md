# docker-journey

A structured, hands-on journey learning Docker fundamentals through notes, examples, and practice from DE zoomcamp

The pipeline directory entails all the work from week one

<details>
<summary><b>Week 1 folder: Pipeline</b></summary>

This section documents the commands used to set up the data pipeline infrastructure and ingest NYC taxi data.

### 1. **PostgreSQL Database Container**

```bash

docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18
```

**Purpose:** Initializes a PostgreSQL database container with:

- Credentials: root/root
- Database name: `ny_taxi`
- Persistent storage volume for data
- Exposed on port 5432
- Connected to custom network `pg-network`


### 2. **pgAdmin Web Interface Container**

```bash

docker run -it --rm \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```

**Purpose:** Deploys pgAdmin (database management GUI) with:

- Web interface accessible at `localhost:8085`
- Same network as PostgreSQL for internal connectivity
- Persistent storage for pgAdmin configuration

### 3. **Local Data Ingestion (Green Taxi Data)**

```bash

uv run ingest_data.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-db=ny_taxi \
  --target-table=green_taxi_2025_11 \
  --month=2025_11
```

**Purpose:** Ingests November 2025 green taxi data directly from local machine to PostgreSQL using a Python script with UV package manager.

### 4. **Taxi Zones Data Ingestion (Containerized)**

```bash

docker run -it --rm \
  --network=pg-network \
  -v "$(pwd)/data:/data" \
  --entrypoint python \
  taxi_ingest:v001 \
  zones.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=pgdatabase \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=taxi_zones \
  --data-dir=/data \
  --csv-file=taxi_zone_lookup.csv
```

**Purpose:** Uses a custom Docker image (`taxi_ingest:v001`) to load taxi zone lookup data:

- Mounts local `data` directory into container
- Connects to PostgreSQL via container network
- Executes `zones.py` ingestion script

### 5. **Green Trip Data Ingestion (Containerized)**

```bash

docker run -it --rm \
  --network=pipeline_pg-network \
  -v "$(pwd)/data:/data" \
  taxi_ingest:v001 \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=pgdatabase \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=green_trip_data_2025_11 \
  --month=2025_11 \
  --data-dir=/data
```

**Purpose:** Ingests green trip data using the same custom Docker image:

- Different network (`pipeline_pg-network`) suggests Docker Compose usage
- Default entrypoint expects trip data ingestion parameters
- Loads November 2025 trip data into specified table

### Key Notes:

- **Network Architecture:** Containers communicate via Docker networks (`pg-network`/`pipeline_pg-network`)
- **Data Persistence:** Both database and pgAdmin use Docker volumes
- **Ingestion Methods:** Shows both local development (`uv run`) and containerized execution approaches
- **Custom Image:** `taxi_ingest:v001` is a pre-built image containing data ingestion scripts

This pipeline establishes a complete ETL environment for NYC taxi data analytics, with database, management interface, and data loading capabilities.

<details>
<summary><b>Week 2 folder: 02_workflow_orchestration</b></summary>
