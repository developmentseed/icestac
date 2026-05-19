from typing import Any, cast

import pyarrow as pa
import rustac
from arro3.core import Table as ArrowTable

from icestac.schema import ItemsInput


def items_to_list(items: ItemsInput) -> list[dict[str, Any]]:
    if isinstance(items, dict):
        item = cast(dict[str, Any], items)
        return [item]
    if isinstance(items, ArrowTable):
        return rustac.from_arrow(pa.table(items))["features"]
    return items
