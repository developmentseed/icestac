from typing import Any, Literal

import pyarrow
from pyiceberg.table import Table
from rustac import to_arrow

from icestac.schema import enforce_required_fields

Method = Literal["append", "upsert"]


def load_items(
    items: list[dict[str, Any]],
    table: Table,
    method: Method = "upsert",
) -> None:
    arrow_data = to_arrow(items)
    enforced_schema = enforce_required_fields(arrow_data.schema)
    arrow_table = pyarrow.table(arrow_data).cast(pyarrow.schema(enforced_schema))

    if method == "upsert":
        table.upsert(
            df=arrow_table,
            join_cols=["id"],
        )
    elif method == "append":
        table.append(df=arrow_table)
