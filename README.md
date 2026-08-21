# icestac

## Overview

`icestac` helps you store STAC Items in Apache Iceberg using rustac's flattened Arrow representation.

The library has three main pieces:

- `src/icestac/schema.py` validates STAC inputs, preserves Arrow metadata, and converts Arrow schemas to Iceberg schemas with field IDs.
- `src/icestac/catalog.py` wraps a PyIceberg catalog and creates one item table per collection in the `icestac` namespace.
- `src/icestac/load.py` accepts STAC dictionaries or `arro3.core.Table` data and writes it with `append` or `upsert` semantics.

The project is still an early foundation rather than a released storage specification. A few boundaries are worth knowing before you begin:

- `icestac` uses each collection ID as its table name. Iceberg treats periods as namespace delimiters, so collection IDs that contain periods are unsupported. Rename those IDs before loading them; `icestac` will not rewrite them for you.
- Tables use monthly `datetime` partitions by default. You can replace that layout with native PyIceberg partition and sort objects. We plan to add built-in spatial layouts as support for geospatial types across the Iceberg ecosystem improves.
- Table schemas remain strict by default. Loading a new shape fails unless you opt into compatible schema evolution.
- Geometry is stored as WKB. PyIceberg does not yet emit the GeoParquet and STAC GeoParquet file metadata needed for compliance with those specifications.

## Core API

The API adds a few STAC-focused conveniences while keeping native PyIceberg objects available for table layout and schema management.

To get started, create a table and load some items. `IcestacCatalog.create_item_table(...)` uses a monthly partition based on the `datetime` property unless you provide another layout.

```python
from pyiceberg.catalog import load_catalog

from icestac.catalog import IcestacCatalog
from icestac.schema import get_schema_from_items

catalog = IcestacCatalog(catalog=load_catalog())
iceberg_schema = get_schema_from_items(items)
catalog.create_item_table(collection_id=collection_id, iceberg_schema=iceberg_schema)
catalog.load_items(collection_id=collection_id, items=items)
```

`get_schema_from_items(...)` accepts one STAC item dictionary, a list of dictionaries, or an `arro3.core.Table`, validates it, and returns an Iceberg schema with the field IDs needed for table layout. `load_items(...)` accepts the same inputs, checks that every row belongs to the target collection, and upserts on STAC `id` by default.

Incoming fields that are absent from the table schema raise an error. If you want PyIceberg to add compatible fields by name, pass `evolve_schema=True`. The schema update and write then commit in one transaction:

```python
catalog.load_items(
    collection_id=collection_id,
    items=items,
    evolve_schema=True,
)
```

If you manage schema changes separately, update the table with PyIceberg before loading:

```python
iceberg_schema = get_schema_from_items(items)
table = catalog.catalog.load_table((catalog.namespace, collection_id))
with table.update_schema() as update:
    update.union_by_name(iceberg_schema)

catalog.load_items(collection_id=collection_id, items=items)
```

Without layout configuration, a table gets the monthly `datetime` partition and no sort order. For a custom layout, build native PyIceberg objects from the prepared schema. This example partitions items by year and sorts them by a `sortme` property:

```python
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import YearTransform

datetime_id = iceberg_schema.find_field("datetime").field_id
sort_id = iceberg_schema.find_field("sortme").field_id
catalog.create_item_table(
    collection_id=collection_id,
    iceberg_schema=iceberg_schema,
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=datetime_id,
            field_id=1000,
            transform=YearTransform(),
            name="datetime_year",
        )
    ),
    sort_order=SortOrder(SortField(source_id=sort_id)),
)
```

A partition specification replaces the monthly default. Pass `PartitionSpec()` for an unpartitioned table, and omit the sort order if you do not need one. PyIceberg validates partition and sort references, so build both from the same prepared schema that you pass to `create_item_table(...)`.

STAC interval items may have a null `datetime` when they include `start_datetime` and `end_datetime`. PyIceberg writes these items to a valid `datetime_month=null` partition. For a collection of interval items, you can partition by the start month instead:

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

Iceberg partition transforms use one source field. If your collection mixes point and interval items, add a derived timestamp column to support a per-row fallback.

## Development

### Tests

Run the test suite with:

```bash
uv run pytest
```

### Local instance

You can run a complete catalog and object storage environment on your machine.

**1. Start the local environment**

```bash
docker compose up
```

This starts three services:

- **Iceberg REST Catalog** at `http://localhost:8181`
- **MinIO** (S3-compatible storage) at `http://localhost:9000` for the API and `http://localhost:9001` for the console
- **MinIO Client**, which creates the `warehouse` bucket on startup

**2. Configure catalog access**

The repository includes `.pyiceberg.yaml` with default credentials for the local Docker environment:

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

PyIceberg reads this file when you run commands from the project directory. If you need a different setup, see the [PyIceberg configuration docs](https://py.iceberg.apache.org/configuration/).

**3. Load sample items**

Once the services are running, use `main.py` to try the current ingestion workflow:

```bash
uv run python main.py
```

The script downloads eight months of HLS STAC GeoParquet from public S3, reads each file directly as Arrow, and loads it through `IcestacCatalog`.

The source collection ID, `HLSS30_2.0`, contains a period, which Iceberg interprets as a namespace delimiter. For this demo, the script renames the collection to `HLSS30_2_0` in every item before creating the table. The script makes this dataset decision explicitly because `icestac` does not rewrite collection IDs.

The June 2026 items introduce a compatible schema change. `main.py` passes `evolve_schema=True` when it loads that batch so you can see schema evolution in use.

**4. Query with DuckDB**

After the load finishes, you can query the Iceberg tables with DuckDB's `iceberg` extension. The tables live under the `icestac` namespace.

Start by configuring the extensions and MinIO credentials:

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

Then query through the REST catalog:

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

You can also scan the table directly from its S3 path without a catalog:

```sql
SET unsafe_enable_version_guessing = true;
SELECT *
FROM iceberg_scan('s3://warehouse/icestac/HLSS30_2_0')
LIMIT 10;
```

## Delete a table

To start over with a table, call `drop_table(...)` on the underlying PyIceberg catalog:

```python
from pyiceberg.catalog import load_catalog
from icestac.catalog import IcestacCatalog

catalog = IcestacCatalog(catalog=load_catalog())
catalog.catalog.drop_table(
    ("icestac", "HLSS30_2_0"),
    purge_requested=True,
)
```

With `purge_requested=True`, the REST catalog deletes the table data along with the catalog entry.

If the catalog entry is gone but files remain in MinIO, remove the warehouse path:

```bash
docker compose exec mc mc rm --recursive --force minio/warehouse/icestac/HLSS30_2_0
```

