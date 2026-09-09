# icestac

Store STAC Items in Apache Iceberg, with one table per collection.

## Load items

The input is an Arrow table (`pyarrow.Table` or `arro3.core.Table`) in rustac's flattened STAC format.

```python
from pyiceberg.catalog import load_catalog

from icestac.catalog import IcestacCatalog
from icestac.schema import get_schema_from_items

# items is an Arrow table containing items from one collection.
collection_id = "my-collection"
catalog = IcestacCatalog(catalog=load_catalog())
schema = get_schema_from_items(items)
catalog.create_item_table(collection_id=collection_id, iceberg_schema=schema)
catalog.load_items(collection_id=collection_id, items=items)
```

Use the collection ID from your items. IDs containing periods are unsupported because icestac uses them in dotted table identifiers. Tables live in the `icestac` namespace by default.

Loading checks the collection ID and upserts on STAC `id`. To append without replacing existing items, pass `method="append"`.

### Schema evolution

New fields raise an error unless you pass `evolve_schema=True`:

```python
catalog.load_items(collection_id=collection_id, items=items, evolve_schema=True)
```

Compatible schema changes and the write commit in one transaction. To update a schema without loading items, use PyIceberg:

```python
table = catalog.catalog.load_table((catalog.namespace, collection_id))
with table.update_schema() as update:
    update.union_by_name(get_schema_from_items(items))
```

### Table layout

Tables use monthly `datetime` partitions and no sort order by default. Pass native PyIceberg objects to choose another layout. For example, partition by year and sort by `datetime`:

```python
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import YearTransform

datetime_id = schema.find_field("datetime").field_id
catalog.create_item_table(
    collection_id=collection_id,
    iceberg_schema=schema,
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=datetime_id,
            field_id=1000,
            transform=YearTransform(),
            name="datetime_year",
        )
    ),
    sort_order=SortOrder(SortField(source_id=datetime_id)),
)
```

Use this instead of the earlier `create_item_table` call. Build field references from the schema you pass to it. Pass `PartitionSpec()` for an unpartitioned table.

Interval items with a null `datetime` go into a null partition. To partition them by their start time, use the `start_datetime` field with `MonthTransform()`.

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

The demo downloads January–August 2026 HLS STAC GeoParquet from public S3 into `data/`, reuses cached files, and upserts each month. Each batch must fit in memory.

It renames collection `HLSS30_2.0` to `HLSS30_2_0`, sorts items by DuckDB's Hilbert index of their bbox lower-left coordinates, and caps Parquet row groups at 50,000 rows. New tables record the Hilbert sort order. Compatible schema changes use `evolve_schema=True`.

### Query with DuckDB

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

SELECT id, datetime, collection, geometry
FROM catalog.icestac.HLSS30_2_0
LIMIT 10;

SELECT count(*) FROM catalog.icestac.HLSS30_2_0;
```

### Delete the demo table

**This deletes the table and its data.**

```python
from pyiceberg.catalog import load_catalog

load_catalog().purge_table(("icestac", "HLSS30_2_0"))
```

To remove only the catalog entry and keep the files, use `drop_table(...)` instead.

## Tests

```bash
uv run pytest
```
