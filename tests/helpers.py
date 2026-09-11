from typing import Any

import pyarrow as pa
import rustac


def items_to_arrow(items: list[dict[str, Any]]) -> pa.Table:
    """Convert test STAC fixtures to the public Arrow input format."""
    return pa.table(rustac.to_arrow(items))


def items_to_list(items: pa.Table) -> list[dict[str, Any]]:
    """Reconstruct Arrow rows for test assertions."""
    return rustac.from_arrow(items)["features"]
