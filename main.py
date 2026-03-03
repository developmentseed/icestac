import asyncio
import logging

import rustac
from pyiceberg.catalog import load_catalog

from icestac.catalog import IcestacCatalog
from icestac.schema import get_schema_from_item

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000


async def run():
    logging.basicConfig(level=logging.INFO)
    catalog = IcestacCatalog(catalog=load_catalog())

    items = await rustac.search(
        "https://stac.maap-project.org",
        collections="icesat2-boreal-v3.1-agb",
        limit=200,
    )

    collection_id = "icesat2_boreal_v3_1_agb"
    for item in items:
        item["collection"] = collection_id

    schema = get_schema_from_item(items[0])
    catalog.create_item_table(arrow_schema=schema, collection_id=collection_id)

    batches = [items[i : i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    for i, batch in enumerate(batches, start=1):
        logger.info("Loading batch %d/%d (%d items)", i, len(batches), len(batch))
        catalog.load_items(
            collection_id=collection_id,
            items=batch,
            method="upsert",
        )


if __name__ == "__main__":
    asyncio.run(run())
