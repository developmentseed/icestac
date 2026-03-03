# icestac

## Overview

This project creates a Python library (`icestac`) that uses rustac to convert STAC item collections to arrow tables and writes them to Apache Iceberg tables.

The goal is a stac-geoparquet-backed system that can be used to maintain a **STAC Catalog** with many collections and support real-time ingestion. It will include an event-driven AWS pipeline for ingesting STAC items into an Iceberg catalog via SNS/SQS and Lambda.

## Development

### Tests

```bash
# Run all tests
uv run pytest
```

### Local Instance

**1. Start the local environment:**

```bash
docker compose up
```

This starts three services:
- **Iceberg REST Catalog** at `http://localhost:8181`
- **MinIO** (S3-compatible storage) at `http://localhost:9000` (API) and `http://localhost:9001` (Console)
- **MinIO Client** — initializes the `warehouse` bucket on startup

**2. Configure catalog access:**

A `.pyiceberg.yaml` is included in the repo with default credentials for the local Docker environment:

```yaml
catalog:
  default:
    type: rest
    uri: http://localhost:8181
    warehouse: s3://warehouse/
    s3.endpoint: http://localhost:9000
    s3.access-key-id: admin
    s3.secret-access-key: password
    s3.path-style-access: "true"
```

PyIceberg will pick this up automatically when running from the project directory. See the [PyIceberg configuration docs](https://py.iceberg.apache.org/configuration/) for other configuration options.

**3. Load sample items:**

`main.py` fetches 5 items from the `icesat2-boreal-v3.1-agb` collection on the MAAP STAC API and writes them to the local Iceberg catalog:

```bash
uv run python main.py
```

This creates an `icestac.icesat2_boreal_v3_1_agb` table in the catalog and upserts the items.

**4. Query with DuckDB:**

After ingesting items, query the Iceberg tables using DuckDB's `iceberg` extension. Tables live under the `icestac` namespace.

First, configure the extensions and MinIO credentials:

```sql
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial;

CREATE OR REPLACE SECRET minio (
    TYPE S3,
    KEY_ID 'admin',
    SECRET 'password',
    ENDPOINT 'localhost:9000',
    USE_SSL false,
    URL_STYLE 'path'
);
```

Query via the REST catalog:

```sql
ATTACH 'icestac' AS catalog (
    TYPE ICEBERG,
    ENDPOINT 'http://localhost:8181',
    AUTHORIZATION_TYPE 'none'
);

SELECT id, datetime, collection, geometry
FROM catalog.icestac.icesat2_boreal_v3_1_agb
LIMIT 10;

SELECT count(*)
FROM catalog.icestac.icesat2_boreal_v3_1_agb;
```

Or scan the table directly from its S3 path (no catalog required):

```sql
SET unsafe_enable_version_guessing = true;
SELECT *
FROM iceberg_scan('s3://warehouse/icestac/icesat2_boreal_v3_1_agb')
LIMIT 10;
```

