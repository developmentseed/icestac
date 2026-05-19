from typing import Literal

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
) -> None:
    if isinstance(items, dict):
        items = [items]

    if not isinstance(items, ArrowTable):
        items = rustac.to_arrow(items)

    enforced_schema = IcestacItem.enforce_required_fields(items.schema)
    arrow_table = pyarrow.table(items).cast(pyarrow.schema(enforced_schema))

    if method == "upsert":
        table.upsert(
            df=arrow_table,
            join_cols=["id"],
        )
    elif method == "append":
        table.append(df=arrow_table)
