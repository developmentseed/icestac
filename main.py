import asyncio
import logging
from pathlib import Path

import pyarrow as pa
from obstore.store import LocalStore, S3Store
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import TableProperties
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import MonthTransform
from rustac import DuckdbClient

from icestac.constants import DEFAULT_NAMESPACE
from icestac.write import put_items
from icestac.schema import get_schema_from_items

logger = logging.getLogger("icestac-demo")

HLS_STAC_GEOPARQUET_BUCKET = "nasa-maap-data-store"
HLS_STAC_GEOPARQUET_PREFIX = "file-staging/nasa-map/hls-stac-geoparquet-archive/v2"
MAX_ROW_GROUP_SIZE = 50_000


def read_hls_items(client: DuckdbClient, path: Path) -> pa.Table:
    """Read an HLS batch and sort it by bbox Hilbert index."""
    # Keep GeoParquet geometry as WKB for Iceberg's binary column.
    client.execute("SET enable_geoparquet_conversion = false")
    client.execute("SET TimeZone = 'UTC'")
    items = pa.table(
        client.query_to_table(
            """
            SELECT *,
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
            [str(path)],
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
    catalog = load_catalog()
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
    collection_id = "HLSS30_2.0"
    table = None

    for month in range(1, 9):
        path = (
            f"{collection_id}/year=2026/month={month}/"
            f"{collection_id}-2026-{month}.parquet"
        )
        try:
            local_store.head(path)
        except FileNotFoundError:
            logger.info("Downloading %s", path)
            response = await source_store.get_async(path)
            await local_store.put_async(path, response)

        logger.info("Reading and sorting %s", path)
        items = read_hls_items(client, data_dir / path)

        if month == 1:
            schema = get_schema_from_items(items)
            datetime_id = schema.find_field("datetime").field_id
            catalog.create_namespace_if_not_exists(DEFAULT_NAMESPACE)
            try:
                table = catalog.create_table(
                    identifier=(DEFAULT_NAMESPACE, collection_id),
                    schema=schema,
                    partition_spec=PartitionSpec(
                        PartitionField(
                            source_id=datetime_id,
                            field_id=1000,
                            transform=MonthTransform(),
                            name="datetime_month",
                        )
                    ),
                    sort_order=SortOrder(
                        SortField(source_id=schema.find_field("hilbert_idx").field_id)
                    ),
                    properties={
                        TableProperties.PARQUET_ROW_GROUP_LIMIT: str(MAX_ROW_GROUP_SIZE)
                    },
                )
            except TableAlreadyExistsError:
                table = catalog.load_table((DEFAULT_NAMESPACE, collection_id))
                logger.info("Using existing table %s", collection_id)

        logger.info("Loading %s items for 2026-%02d", len(items), month)
        assert table is not None
        put_items(table, items, evolve_schema=True)


if __name__ == "__main__":
    asyncio.run(run())
