# icestac

## Overview

This project creates a Python library (`icestac`) that uses rustac to convert STAC item collections to Arrow tables and writes them to Apache Iceberg tables.

At a high level, the library is split into three pieces:

- `src/icestac/schema.py` validates STAC items with `stac-pydantic`, derives an Arrow schema from incoming items, and converts that schema to an Iceberg schema with stable field IDs.
- `src/icestac/catalog.py` wraps a PyIceberg catalog and creates per-collection item tables in the `icestac` namespace.
- `src/icestac/load.py` turns STAC items into Arrow data and writes them to Iceberg with either `append` or `upsert` semantics.

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

`main.py` is the best example of the current ingestion workflow:

```bash
uv run python main.py
```

First, load the default PyIceberg catalog from `.pyiceberg.yaml` and wrap it with `IcestacCatalog`:

```python
catalog = IcestacCatalog(catalog=load_catalog())
```

That gives `icestac` a catalog client that knows how to create and load item tables in the `icestac` namespace.

Next, fetch a collection of STAC items from a STAC API:

```python
items = await rustac.search(
    "https://stac.maap-project.org",
    collections="icesat2-boreal-v3.1-agb",
    max_items=200,
)
```

Here `rustac.search(...)` pulls pages of 200 items from the MAAP STAC API. In a real application, those items could also come from a webhook, a queue, or another ingestion step.

Then normalize the collection id into something that will work as an Iceberg table name and write that value onto each item:

```python
collection_id = "icesat2_boreal_v3_1_agb"
for item in items:
    item["collection"] = collection_id
```

The sample uses an Iceberg-safe table id with underscores. It also ensures every item carries the collection value that will be stored in the table.

Once the items are in hand, derive the schema and create the Iceberg table:

```python
schema = get_schema_from_items(items)
catalog.create_item_table(arrow_schema=schema, collection_id=collection_id)
```

`get_schema_from_items(...)` validates the items as STAC, derives an Arrow schema, and marks required STAC fields as non-nullable. `create_item_table(...)` converts that Arrow schema to an Iceberg schema and creates `icestac.icesat2_boreal_v3_1_agb`, currently partitioned by `datetime` month.

Finally, split the items into batches and upsert them into Iceberg:

```python
batches = [items[i : i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
for i, batch in enumerate(batches, start=1):
    logger.info("Loading batch %d/%d (%d items)", i, len(batches), len(batch))
    catalog.load_items(
        collection_id=collection_id,
        items=batch,
        method="upsert",
    )
```

`catalog.load_items(...)` converts each batch to Arrow and writes it to Iceberg. In `upsert` mode, the table uses STAC `id` as the join key, so rerunning the workflow updates existing items instead of blindly appending duplicates.

That is the core `icestac` usage pattern today: generate items, set the collection id, derive a schema, create the collection table, and then append or upsert batches into Iceberg.

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

