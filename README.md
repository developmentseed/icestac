# icestac

icestac maps STAC concepts onto Apache Iceberg.

The current implementation covers STAC Items with schema utilities for the flattened item layout defined by [STAC GeoParquet](https://radiantearth.github.io/stac-geoparquet-spec/), a small write API, and runnable PyIceberg examples.

Each collection gets one Iceberg table named with its STAC collection ID. Clients can locate an item table from a collection ID without a metadata lookup. Each row represents one item, and an item's identity is scoped to its collection. Requesting an item by collection and ID should return one current record, not multiple versions.

The `collections` table name is reserved for planned collection metadata. Collection metadata is not implemented yet.

## Storage conventions

Use PyIceberg for catalog and table administration, including namespace and table creation, partitioning, sorting, and table properties. icestac does not choose a default table layout.

The schema utilities and `put_items` handle the STAC-facing parts. They work with an Arrow table (`pyarrow.Table` or `arro3.core.Table`) using the [STAC GeoParquet](https://radiantearth.github.io/stac-geoparquet-spec/) column mapping: item properties are top-level columns, while geometry is WKB and links and assets remain nested structures. This describes the in-memory Arrow input, not a compliant STAC GeoParquet file; file compliance also requires GeoParquet and STAC metadata. `get_schema_from_items` checks the required fields and their basic Arrow types, then creates an Iceberg schema. It does not perform full STAC validation.

The inferred schema marks `id` as an Iceberg identifier field. Iceberg identifier fields are schema metadata, not uniqueness constraints. Starting with a table that has unique `(collection, id)` pairs and writing through `put_items` preserves the one-current-row convention. Native writes that bypass `put_items`, such as appends, can create duplicate IDs and violate it.

## Put items

Create a table for one collection with native PyIceberg, then pass its items to `put_items`:

```python
from pyiceberg.catalog import load_catalog

from icestac.constants import DEFAULT_NAMESPACE
from icestac.schema import get_schema_from_items
from icestac.write import put_items

# items is an Arrow table containing items from one collection.
collection_id = "my-collection"
catalog = load_catalog()
catalog.create_namespace_if_not_exists(DEFAULT_NAMESPACE)
schema = get_schema_from_items(items)
table = catalog.create_table(
    identifier=(DEFAULT_NAMESPACE, collection_id),
    schema=schema,
)
put_items(table, items)
```

`put_items` accepts items from one collection and checks that their `collection` values match the collection named by the table. It rejects duplicate IDs within an input batch. Matching items are complete replacements, so omitted optional fields are cleared, including nested link fields. New IDs are inserted.

Collection IDs can contain periods. Pass catalog identifiers as tuples such as `(namespace, collection_id)` instead of interpolated dotted strings; a catalog backend may still reject a particular ID. Do not use `collections` as an item table name.

For append semantics, use native PyIceberg:

```python
table.append(df=items)
```

### Schema evolution

New fields raise an error unless you pass `evolve_schema=True`. Schema evolution and the replacement write happen in one Iceberg transaction:

```python
put_items(table, items, evolve_schema=True)
```

To update a schema without writing items, use native PyIceberg:

```python
from pyiceberg.catalog import load_catalog

from icestac.constants import DEFAULT_NAMESPACE
from icestac.schema import get_schema_from_items

catalog = load_catalog()
table = catalog.load_table((DEFAULT_NAMESPACE, collection_id))
with table.update_schema() as update:
    update.union_by_name(get_schema_from_items(items))
```

### Concurrent writes

Each batch, including optional schema evolution, commits as one Iceberg transaction. A concurrent commit conflict raises `CommitFailedException`; refresh the table and retry from current state rather than replaying a stale replacement. A `CommitStateUnknownException` means the commit outcome is unknown, so refresh and reconcile before retrying.

## Table layout

Choose the layout when creating the table. For example, partition by month of `datetime`, sort by `datetime`, and set a Parquet row group limit:

```python
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import TableProperties
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import MonthTransform

schema = get_schema_from_items(items)
datetime_id = schema.find_field("datetime").field_id
table = catalog.create_table(
    identifier=(DEFAULT_NAMESPACE, collection_id),
    schema=schema,
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=datetime_id,
            field_id=1000,
            transform=MonthTransform(),
            name="datetime_month",
        )
    ),
    sort_order=SortOrder(SortField(source_id=datetime_id)),
    properties={TableProperties.PARQUET_ROW_GROUP_LIMIT: "50000"},
)
```

Omit `partition_spec` for an unpartitioned table. Interval items with a null `datetime` go into a null partition. To partition them by their start time, use the `start_datetime` field with `MonthTransform()`.

Geometry uses WKB in an Iceberg binary column. The output files lack the metadata required for GeoParquet and STAC GeoParquet compliance.

## Local demo

Run these commands from the repository root. You need [uv](https://docs.astral.sh/uv/), Docker Compose, and DuckDB for the query examples.

### Start the services

```bash
docker compose up -d
```

- Iceberg REST catalog: `http://localhost:8181`
- MinIO API: `http://localhost:9000`; console: `http://localhost:9001`
- The `mc` service creates the `warehouse` bucket.

The checked-in `.pyiceberg.yaml` points to these services. The credentials (`admin` / `password`) are for local development only. See [PyIceberg configuration](https://py.iceberg.apache.org/configuration/) for other environments.

### Load HLS items

```bash
uv run main.py
```

The demo downloads January–August 2026 HLS STAC GeoParquet from public S3 into `data/`, reuses cached files, and writes each month's items to the `HLSS30_2.0` collection table. Each batch must fit in memory.

It keeps the original `HLSS30_2.0` collection ID, partitions by month of `datetime`, sorts by DuckDB's Hilbert index of each bbox's lower-left coordinates, and caps Parquet row groups at 50,000 rows. Compatible schema changes use `evolve_schema=True`; rerunning the demo reuses the existing table and replaces the cached batches again.

### Query with DuckDB

The query selects items overlapping the contiguous United States bounding box (longitude -125 to -66, latitude 24 to 50) from April 1, 2026 inclusive through July 1, 2026 exclusive; the bbox predicates test bounding-box overlap, not exact geometry intersection.

```sql
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

CREATE OR REPLACE SECRET minio (
    TYPE S3,
    KEY_ID 'admin',
    SECRET 'password',
    ENDPOINT 'localhost:9000',
    USE_SSL false,
    URL_STYLE 'path'
);

ATTACH 'icestac' AS catalog (
    TYPE ICEBERG,
    ENDPOINT 'http://localhost:8181',
    AUTHORIZATION_TYPE 'none'
);

SELECT count(*) FROM catalog.icestac."HLSS30_2.0";

EXPLAIN ANALYZE SELECT count(*)
FROM catalog.icestac."HLSS30_2.0"
WHERE bbox.xmin <= -90.0
  AND bbox.xmax >= -100.0
  AND bbox.ymin <= 50.0
  AND bbox.ymax >= 40.0
  AND datetime >= TIMESTAMPTZ '2026-04-01 00:00:00+00'
  AND datetime < TIMESTAMPTZ '2026-07-01 00:00:00+00'
;

```

### Delete the demo table

**This deletes the table and its data.**

```python
from pyiceberg.catalog import load_catalog

from icestac.constants import DEFAULT_NAMESPACE

load_catalog().purge_table((DEFAULT_NAMESPACE, "HLSS30_2.0"))
```

To remove only the catalog entry and keep the files, use `drop_table(...)` instead.

## Tests

```bash
uv run pytest
```
