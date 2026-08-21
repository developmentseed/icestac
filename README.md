# icestac

## Overview

`icestac` uses rustac's flattened Arrow representation of STAC Items and writes it to Apache Iceberg.

The library is split into three pieces:

- `src/icestac/schema.py` validates STAC inputs, preserves Arrow metadata, and converts Arrow schemas to Iceberg schemas with field IDs.
- `src/icestac/catalog.py` wraps a PyIceberg catalog and creates one item table per collection in the `icestac` namespace.
- `src/icestac/load.py` accepts STAC dictionaries or `arro3.core.Table` data and writes it with `append` or `upsert` semantics.

This is an early foundation, not a released storage specification. Current boundaries are intentional:

- A table name matches its items' collection ID. Periods are unsupported because Iceberg uses them as namespace delimiters; `icestac` does not silently rewrite IDs.
- Tables default to partitioning by `datetime` month, but callers can replace that layout with native PyIceberg partition and sort objects. Built-in spatial layouts remain deferred until there are concrete query patterns.
- Table schemas do not evolve automatically. Loading a new shape fails until the Iceberg schema is updated separately.
- Geometry is stored as WKB, but PyIceberg does not currently emit the GeoParquet and STAC GeoParquet file metadata required to claim compliance with those specifications.

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

The script downloads four months of HLS STAC GeoParquet from public S3, reads each file directly as Arrow, and loads it through `IcestacCatalog`.

The source collection ID, `HLSS30_2.0`, contains a period and cannot be used unchanged as an Iceberg identifier. The demo explicitly migrates the dataset to `HLSS30_2_0` by replacing every item's collection value before creating the matching table. This is a dataset decision, not automatic library slugification.

The core API remains explicit:

```python
from pyiceberg.catalog import load_catalog

from icestac.catalog import IcestacCatalog
from icestac.schema import convert_schema, get_schema_from_items

catalog = IcestacCatalog(catalog=load_catalog())
arrow_schema = get_schema_from_items(items)
iceberg_schema = convert_schema(arrow_schema)
catalog.create_item_table(collection_id=collection_id, iceberg_schema=iceberg_schema)
catalog.load_items(collection_id=collection_id, items=items)
```

`get_schema_from_items(...)` accepts one STAC dictionary, a list of dictionaries, or an `arro3.core.Table`. `convert_schema(...)` validates the item schema and returns the Iceberg schema with the field IDs used to configure table layout. `load_items(...)` accepts the same item inputs, checks that every row belongs to the target collection, and upserts on STAC `id` by default.

Omitting layout configuration creates the monthly `datetime` partition and no sort order. To use a custom layout, build native PyIceberg objects from the prepared schema:

```python
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform

source_id = iceberg_schema.find_field("title").field_id
catalog.create_item_table(
    collection_id=collection_id,
    iceberg_schema=iceberg_schema,
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=source_id,
            field_id=1000,
            transform=IdentityTransform(),
            name="title",
        )
    ),
    sort_order=SortOrder(SortField(source_id=source_id)),
)
```

A supplied partition specification replaces the monthly default; pass `PartitionSpec()` for an unpartitioned table. Sorting is optional. PyIceberg validates partition and sort references, so build both against the same prepared schema passed to `create_item_table(...)`.

STAC interval items can have a null `datetime` when they include `start_datetime` and `end_datetime`. PyIceberg writes these items to the valid `datetime_month=null` partition. For a collection of interval items, override the default to partition by the start month:

```python
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import MonthTransform

start_datetime_id = iceberg_schema.find_field("start_datetime").field_id
catalog.create_item_table(
    collection_id=collection_id,
    iceberg_schema=iceberg_schema,
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=start_datetime_id,
            field_id=1000,
            transform=MonthTransform(),
            name="start_datetime_month",
        )
    ),
)
```

Iceberg partition transforms use one source field, so mixed point and interval collections need a derived timestamp column to support a per-row fallback.

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
FROM catalog.icestac.HLSS30_2_0
LIMIT 10;

SELECT count(*)
FROM catalog.icestac.HLSS30_2_0;

DESCRIBE SELECT bbox, geometry FROM catalog.icestac.HLSS30_2_0;
```

Or scan the table directly from its S3 path (no catalog required):

```sql
SET unsafe_enable_version_guessing = true;
SELECT *
FROM iceberg_scan('s3://warehouse/icestac/HLSS30_2_0')
LIMIT 10;
```

## Delete a Table

To remove a table from the local Iceberg REST catalog, call `drop_table(...)` on the underlying PyIceberg catalog:

```python
from pyiceberg.catalog import load_catalog
from icestac.catalog import IcestacCatalog

catalog = IcestacCatalog(catalog=load_catalog())
catalog.catalog.drop_table(
    ("icestac", "HLSS30_2_0"),
    purge_requested=True,
)
```

`purge_requested=True` asks the REST catalog to delete the underlying table data as well as the catalog entry.

If the metadata entry is removed but table files remain in MinIO, delete the warehouse path manually:

```bash
docker compose exec mc mc rm --recursive --force minio/warehouse/icestac/HLSS30_2_0
```

