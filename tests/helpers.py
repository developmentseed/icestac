from typing import Any

import pyarrow as pa
import rustac
from arro3.core import Table as ArrowTable

from icestac.schema import ItemsInput


def items_to_list(items: ItemsInput) -> list[dict[str, Any]]:
    if isinstance(items, dict):
        return [items]
    if isinstance(items, ArrowTable):
        return rustac.from_arrow(pa.table(items))["features"]
    return items
