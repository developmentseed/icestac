import warnings
from pyiceberg.table import Table, TableProperties
from pyiceberg.table.upsert_util import create_match_filter, has_duplicate_rows

from icestac.constants import COLLECTIONS_TABLE_NAME
from icestac.schema import ItemsInput, _align_empty_links, prepare_arrow_table


def put_items(
    table: Table,
    items: ItemsInput,
    *,
    evolve_schema: bool = False,
) -> None:
    """Replace or insert a batch of STAC items in an Iceberg table.

    Matching IDs are complete replacements: omitted optional fields are
    cleared. Commit retries are disabled because PyIceberg cannot safely replay
    the match against refreshed table state. Commit failures are propagated;
    an unknown commit outcome requires caller reconciliation before retrying.
    """

    arrow_table = prepare_arrow_table(items)
    expected_collection_id = table.name()[-1]
    if expected_collection_id == COLLECTIONS_TABLE_NAME:
        raise ValueError(
            f"Cannot load items into reserved table {COLLECTIONS_TABLE_NAME!r}"
        )

    collection_ids = set(arrow_table.column("collection").unique().to_pylist())
    if collection_ids != {expected_collection_id}:
        raise ValueError(
            f"Items for {expected_collection_id!r} contain collection ids "
            f"{sorted(map(str, collection_ids))}"
        )

    # PyIceberg retries a staged merge against refreshed metadata without
    # rerunning the original lookup, which can duplicate an ID.
    retry_metadata = table.metadata
    previous_retries = retry_metadata.properties.get(TableProperties.COMMIT_NUM_RETRIES)
    retry_metadata.properties[TableProperties.COMMIT_NUM_RETRIES] = "0"

    try:
        with table.transaction() as transaction:
            if evolve_schema:
                with transaction.update_schema() as update:
                    update.union_by_name(arrow_table.schema)

            arrow_table = _align_empty_links(
                arrow_table, transaction.table_metadata.schema()
            )
            # Reject duplicate incoming IDs before staging any write.
            if has_duplicate_rows(arrow_table, ["id"]):
                raise ValueError(
                    "Duplicate rows found in source dataset based on the key "
                    "columns. No write executed"
                )
            with warnings.catch_warnings():
                # New IDs legitimately have no existing records to delete.
                warnings.filterwarnings(
                    "ignore",
                    message="^Delete operation did not match any records$",
                    category=UserWarning,
                    module=r"^pyiceberg\.table$",
                )
                transaction.overwrite(
                    df=arrow_table,
                    overwrite_filter=create_match_filter(arrow_table, ["id"]),
                )
    finally:
        # A refresh during commit means another metadata object owns the
        # policy; never overwrite it with this call's temporary value.
        if table.metadata is retry_metadata:
            properties = retry_metadata.properties
            if properties.get(TableProperties.COMMIT_NUM_RETRIES) == "0":
                if previous_retries is None:
                    properties.pop(TableProperties.COMMIT_NUM_RETRIES, None)
                else:
                    properties[TableProperties.COMMIT_NUM_RETRIES] = previous_retries
