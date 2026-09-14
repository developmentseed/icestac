# icestac

icestac maps STAC Items onto Apache Iceberg. It provides schema utilities for
flattened STAC GeoParquet-shaped Arrow tables and a small replacement-oriented
write API.

## Install

```bash
uv add icestac
```

## Write items

Create the Iceberg table with PyIceberg, then pass a `pyarrow.Table` or
`arro3.core.Table` to `put_items`:

```python
from pyiceberg.catalog import load_catalog

from icestac.constants import DEFAULT_NAMESPACE
from icestac.schema import get_schema_from_items
from icestac.write import put_items

catalog = load_catalog()
catalog.create_namespace_if_not_exists(DEFAULT_NAMESPACE)
schema = get_schema_from_items(items)
table = catalog.create_table(
    identifier=(DEFAULT_NAMESPACE, "my-collection"),
    schema=schema,
)
put_items(table, items)
```

`items` must contain the required STAC columns (`type`, `id`, `geometry`,
`datetime`, `links`, `collection`, and `assets`). A batch belongs to one
collection and duplicate IDs are rejected. Existing IDs are complete
replacements, so omitted optional fields are cleared.

Pass `evolve_schema=True` when new top-level or nested fields should be added
to the Iceberg schema in the same transaction as the write:

```python
put_items(table, items, evolve_schema=True)
```

For append semantics, use PyIceberg directly with `table.append(df=items)`.

## Local development

The repository contains a Docker Compose Iceberg/MinIO demo. See the
[project README](https://github.com/developmentseed/icestac#local-demo) for
its storage conventions and query examples.

[uv]: https://docs.astral.sh/uv/
