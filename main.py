import asyncio
import logging
from pathlib import Path

import pyarrow as pa
from obstore.store import LocalStore, S3Store
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.table import TableProperties
from pyiceberg.table.sorting import SortField, SortOrder
from rustac import DuckdbClient

from icestac.catalog import IcestacCatalog
from icestac.schema import get_schema_from_items

logger = logging.getLogger("icestac-demo")

HLS_STAC_GEOPARQUET_BUCKET = "nasa-maap-data-store"
HLS_STAC_GEOPARQUET_PREFIX = "file-staging/nasa-map/hls-stac-geoparquet-archive/v2"
MAX_ROW_GROUP_SIZE = 50_000


def read_hls_items(client: DuckdbClient, path: Path, collection_id: str) -> pa.Table:
    """Read an HLS batch, rename its collection, and sort by bbox Hilbert index."""
    # Keep GeoParquet geometry as WKB for Iceberg's binary column.
    client.execute("SET enable_geoparquet_conversion = false")
    client.execute("SET TimeZone = 'UTC'")
    items = pa.table(
        client.query_to_table(
            """
            SELECT * REPLACE (? AS collection),
                CASE WHEN isfinite(bbox.xmin) AND isfinite(bbox.ymin)
                    THEN ST_Hilbert(
                        bbox.xmin, bbox.ymin,
                        {min_x: -180.0, min_y: -90.0,
                         max_x: 180.0, max_y: 90.0}::BOX_2D
                    )::BIGINT
                END AS hilbert_idx
            FROM read_parquet(?, hive_partitioning = false)
            ORDER BY hilbert_idx
            """,
            [collection_id, str(path)],
        )
    )
    if not len(items):
        raise ValueError(f"No items found in {path}")
    if items["hilbert_idx"].null_count:
        raise ValueError(f"Missing or non-finite bbox coordinates in {path}")
    return items


async def run() -> None:
    """Load January–August 2026 HLS items into the local Iceberg catalog."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    catalog = IcestacCatalog(catalog=load_catalog())
    client = DuckdbClient()
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    local_store = LocalStore(data_dir)
    source_store = S3Store(
        bucket=HLS_STAC_GEOPARQUET_BUCKET,
        prefix=HLS_STAC_GEOPARQUET_PREFIX,
        region="us-west-2",
        skip_signature=True,
    )
    source_collection_id = "HLSS30_2.0"
    collection_id = "HLSS30_2_0"

    for month in range(1, 9):
        path = (
            f"{source_collection_id}/year=2026/month={month}/"
            f"{source_collection_id}-2026-{month}.parquet"
        )
        try:
            local_store.head(path)
        except FileNotFoundError:
            logger.info("Downloading %s", path)
            response = await source_store.get_async(path)
            await local_store.put_async(path, response)

        logger.info("Reading and sorting %s", path)
        items = read_hls_items(client, data_dir / path, collection_id)

        if month == 1:
            schema = get_schema_from_items(items)
            try:
                table = catalog.create_item_table(
                    iceberg_schema=schema,
                    collection_id=collection_id,
                    sort_order=SortOrder(
                        SortField(source_id=schema.find_field("hilbert_idx").field_id)
                    ),
                )
            except TableAlreadyExistsError:
                table = catalog.catalog.load_table((catalog.namespace, collection_id))
                logger.info("Using existing table %s", collection_id)
            with table.transaction() as transaction:
                transaction.set_properties(
                    {TableProperties.PARQUET_ROW_GROUP_LIMIT: str(MAX_ROW_GROUP_SIZE)}
                )

        logger.info("Loading %s items for 2026-%02d", len(items), month)
        catalog.load_items(collection_id, items, evolve_schema=True)


if __name__ == "__main__":
    asyncio.run(run())
