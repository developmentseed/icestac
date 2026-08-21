from typing import Any, Literal, cast

import pyarrow
import rustac
from arro3.core import Table as ArrowTable
from pyiceberg.table import Table

from icestac.schema import IcestacItem, ItemsInput

Method = Literal["append", "upsert"]


def load_items(
    items: ItemsInput,
    table: Table,
    method: Method = "upsert",
    evolve_schema: bool = False,
) -> None:
    """Load STAC items, optionally evolving the Iceberg schema by name."""
    if method not in ("append", "upsert"):
        raise ValueError(f"Unsupported load method: {method}")

    if isinstance(items, dict):
        item = cast(dict[str, Any], items)
        items = [item]

    if not isinstance(items, ArrowTable):
        items = rustac.to_arrow(items)

    enforced_schema = IcestacItem.enforce_required_fields(items.schema)
    arrow_table = pyarrow.table(items).cast(pyarrow.schema(enforced_schema))
    collection_ids = set(arrow_table.column("collection").unique().to_pylist())
    expected_collection_id = table.name()[-1]
    if collection_ids != {expected_collection_id}:
        raise ValueError(
            f"Items for {expected_collection_id!r} contain collection ids "
            f"{sorted(map(str, collection_ids))}"
        )

    with table.transaction() as transaction:
        if evolve_schema:
            with transaction.update_schema() as update:
                update.union_by_name(arrow_table.schema)

        if method == "upsert":
            transaction.upsert(
                df=arrow_table,
                join_cols=["id"],
            )
        else:
            transaction.append(df=arrow_table)
