import asyncio

import rustac
from pyiceberg.catalog import load_catalog

from icestac.catalog import IcestacCatalog
from icestac.schema import get_schema_from_item


async def run():
    catalog = IcestacCatalog(catalog=load_catalog())

    items = await rustac.search(
        "https://stac.maap-project.org",
        collections="icesat2-boreal-v3.1-agb",
        max_items=5,
    )
    collection_id = "icesat2_boreal_v3_1_agb"
    for item in items:
        item["collection"] = collection_id

    schema = get_schema_from_item(items[0])
    catalog.create_item_table(arrow_schema=schema, collection_id=collection_id)

    catalog.load_items(
        collection_id=collection_id,
        items=items,
        method="upsert",
    )


if __name__ == "__main__":
    asyncio.run(run())
