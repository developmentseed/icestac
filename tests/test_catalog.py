from typing import Any

import pyarrow
import pytest
from pyiceberg.exceptions import TableAlreadyExistsError
from rustac import to_arrow

from icestac.catalog import IcestacCatalog
from icestac.errors import InvalidCollectionIdError
from icestac.schema import IcestacItem, get_schema_from_item


def test_create_item_table(
    test_catalog: IcestacCatalog,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=sample_stac_items[0]["collection"],
    )

    assert table.schema().find_field("datetime")

    # Ensure data has required fields marked as non-nullable to match table schema
    arrow_data = to_arrow(sample_stac_items)
    enforced_schema = IcestacItem.enforce_required_fields(arrow_data.schema)
    arrow_table = pyarrow.table(arrow_data).cast(pyarrow.schema(enforced_schema))

    table.upsert(
        df=arrow_table,
        join_cols=["id"],
    )

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(sample_stac_items)
    assert result.column("id").to_pylist() == [item["id"] for item in sample_stac_items]

    with pytest.raises(TableAlreadyExistsError):
        test_catalog.create_item_table(
            arrow_schema=arrow_schema,
            collection_id=sample_stac_items[0]["collection"],
        )


def test_create_item_table_bad_collection_id(
    test_catalog: IcestacCatalog,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    with pytest.raises(InvalidCollectionIdError):
        test_catalog.create_item_table(
            arrow_schema=arrow_schema,
            collection_id="bad.collection",
        )


def test_load_items(
    test_catalog: IcestacCatalog,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    collection_id = sample_stac_items[0]["collection"]
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=collection_id,
    )

    test_catalog.load_items(
        collection_id=sample_stac_items[0]["collection"],
        items=sample_stac_items,
        method="upsert",
    )

    table.refresh()

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(sample_stac_items)
    assert result.column("id").to_pylist() == [item["id"] for item in sample_stac_items]
