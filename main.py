import asyncio
import logging

import pyarrow
from arro3.core import Table as ArrowTable
from obstore.store import LocalStore, S3Store
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import TableAlreadyExistsError
from rustac import DuckdbClient

from icestac.catalog import IcestacCatalog
from icestac.schema import get_schema_from_items

logger = logging.getLogger("icestac-demo")

HLS_STAC_GEOPARQUET_BUCKET = "nasa-maap-data-store"
HLS_STAC_GEOPARQUET_PREFIX = "file-staging/nasa-map/hls-stac-geoparquet-archive/v2"
HLS_STAC_GEOPARQUET_PATH_FMT = (
    "{collection}/year={year}/month={month}/{collection}-{year}-{month}.parquet"
)


async def copy_hls_stac_geoparquet(path: str, store: LocalStore) -> None:
    """Copy one public HLS STAC GeoParquet file into a local store."""
    hls_stac_store = S3Store(
        bucket=HLS_STAC_GEOPARQUET_BUCKET,
        prefix=HLS_STAC_GEOPARQUET_PREFIX,
        region="us-west-2",
        skip_signature=True,
    )

    resp = await hls_stac_store.get_async(path)
    await store.put_async(path, resp)


async def run() -> None:
    """Load several months of HLS STAC GeoParquet into local Iceberg."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    catalog = IcestacCatalog(catalog=load_catalog())

    duckdb_client = DuckdbClient()
    duckdb_client.execute("SET TimeZone = 'UTC';")
    local_store = LocalStore("data")

    source_collection_id = "HLSS30_2.0"
    collection_id = "HLSS30_2_0"
    table_exists = False

    for month in range(1, 9, 1):
        month_logger = logger.getChild(f"2026-{month}")
        stac_geoparquet_path = HLS_STAC_GEOPARQUET_PATH_FMT.format(
            collection=source_collection_id,
            year="2026",
            month=str(month),
        )

        try:
            _ = local_store.head(stac_geoparquet_path)
        except FileNotFoundError:
            month_logger.info("downloading %s", stac_geoparquet_path)
            await copy_hls_stac_geoparquet(
                path=stac_geoparquet_path,
                store=local_store,
            )

        month_logger.info("loading items as arrow table")
        items = duckdb_client.search_to_arrow(href=f"data/{stac_geoparquet_path}")

        if not items:
            raise ValueError("No items found")

        items_table = pyarrow.table(items)
        collection_index = items_table.schema.get_field_index("collection")
        collection_field = items_table.schema.field(collection_index)
        items = ArrowTable.from_arrow(
            items_table.set_column(
                collection_index,
                collection_field,
                pyarrow.array(
                    [collection_id] * len(items_table), type=collection_field.type
                ),
            )
        )
        iceberg_schema = get_schema_from_items(items)

        if not table_exists:
            try:
                catalog.create_item_table(
                    iceberg_schema=iceberg_schema,
                    collection_id=collection_id,
                )
            except TableAlreadyExistsError:
                month_logger.warning("%s table already exists; using it", collection_id)
            table_exists = True

        month_logger.info("loading items into icestac catalog")
        try:
            catalog.load_items(
                collection_id=collection_id,
                items=items,
                method="upsert",
                evolve_schema=False,
            )
        except ValueError as e:
            if "Update the schema first (hint, use union_by_name)" not in str(e):
                raise

            month_logger.warning(str(e))
            month_logger.info("retrying load with evolve_schema=True")

            catalog.load_items(
                collection_id=collection_id,
                items=items,
                method="upsert",
                evolve_schema=True,
            )


if __name__ == "__main__":
    asyncio.run(run())
