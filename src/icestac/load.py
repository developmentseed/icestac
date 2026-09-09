import warnings
from typing import Literal

from pyiceberg.table import Table, TableProperties
from pyiceberg.table.upsert_util import create_match_filter, has_duplicate_rows

from icestac.schema import ItemsInput, prepare_arrow_table

Method = Literal["append", "upsert"]


def load_items(
    items: ItemsInput,
    table: Table,
    method: Method = "upsert",
    evolve_schema: bool = False,
) -> None:
    """Load STAC items, optionally evolving the Iceberg schema by name.

    Upserts are complete replacements: omitted optional fields are written as
    null rather than retained from the previous item. Upsert commit retries
    are disabled because PyIceberg cannot safely replay the match against
    refreshed table state. A known ``CommitFailedException``
    is propagated for a fresh retry; a ``CommitStateUnknownException`` is also
    propagated and requires refreshing/reconciling before retrying the load.
    """
    if method not in ("append", "upsert"):
        raise ValueError(f"Unsupported load method: {method}")

    arrow_table = prepare_arrow_table(items)
    collection_ids = set(arrow_table.column("collection").unique().to_pylist())
    expected_collection_id = table.name()[-1]
    if collection_ids != {expected_collection_id}:
        raise ValueError(
            f"Items for {expected_collection_id!r} contain collection ids "
            f"{sorted(map(str, collection_ids))}"
        )

    if method == "upsert":
        # PyIceberg retries a staged merge against refreshed metadata without
        # rerunning the original upsert lookup, which can duplicate an ID.
        table.metadata.properties[TableProperties.COMMIT_NUM_RETRIES] = "0"

    with table.transaction() as transaction:
        if evolve_schema:
            with transaction.update_schema() as update:
                update.union_by_name(arrow_table.schema)

        if method == "upsert":
            # Overwrite matching IDs so omitted optional fields are cleared.
            # PyIceberg handles schema alignment, including staged evolution.
            if has_duplicate_rows(arrow_table, ["id"]):
                raise ValueError(
                    "Duplicate rows found in source dataset based on the key "
                    "columns. No upsert executed"
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
        else:
            transaction.append(df=arrow_table)
