from typing import Any

import pyarrow
import pytest
from pyiceberg.catalog import Catalog
from rustac import to_arrow

from icestac.errors import InvalidCollectionIdError
from icestac.item_table import create_item_table
from icestac.schema import enforce_required_fields, get_schema_from_item


def test_create_item_table(
    test_catalog: Catalog,
    test_namespace: str,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    table = create_item_table(
        arrow_schema=arrow_schema,
        collection_id=sample_stac_items[0]["collection"],
        catalog=test_catalog,
        namespace=test_namespace,
    )

    assert table.schema().find_field("datetime")

    # Ensure data has required fields marked as non-nullable to match table schema
    arrow_data = to_arrow(sample_stac_items)
    enforced_schema = enforce_required_fields(arrow_data.schema)
    arrow_table = pyarrow.table(arrow_data).cast(pyarrow.schema(enforced_schema))

    table.upsert(
        df=arrow_table,
        join_cols=["id"],
    )

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(sample_stac_items)
    assert result.column("id").to_pylist() == [item["id"] for item in sample_stac_items]


def test_create_item_table_bad_collection_id(
    test_catalog: Catalog,
    test_namespace: str,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    with pytest.raises(InvalidCollectionIdError):
        create_item_table(
            arrow_schema=arrow_schema,
            collection_id="bad.collection",
            catalog=test_catalog,
            namespace=test_namespace,
        )
