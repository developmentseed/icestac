import asyncio

import rustac

from icestac.catalog import IcestacCatalog
from icestac.config import IcestacCatalogConfig
from icestac.schema import get_schema_from_item


async def run():
    config = IcestacCatalogConfig.model_validate({})
    catalog = IcestacCatalog.from_config(config)

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
