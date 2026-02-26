import asyncio

import rustac

from icestac.config import IcebergCatalogConfig
from icestac.constants import DEFAULT_NAMESPACE
from icestac.item_table import create_item_table
from icestac.load import load_items
from icestac.schema import get_schema_from_item


async def run():
    config = IcebergCatalogConfig()
    catalog = config.load_catalog()

    items = await rustac.search(
        "https://stac.maap-project.org",
        collections="icesat2-boreal-v3.1-agb",
        max_items=5,
    )

    for item in items:
        item["collection"] = "icesat2_boreal_v3_1_agb"

    schema = get_schema_from_item(items[0])
    table = create_item_table(
        arrow_schema=schema,
        collection_id=items[0]["collection"],
        catalog=catalog,
        namespace=DEFAULT_NAMESPACE,
    )

    load_items(
        items=items,
        table=table,
        method="upsert",
    )


if __name__ == "__main__":
    asyncio.run(run())
