import pyarrow
import pytest
from arro3.core import Table as ArrowTable
from pyiceberg.exceptions import TableAlreadyExistsError
from rustac import to_arrow

from icestac.catalog import IcestacCatalog
from icestac.errors import InvalidCollectionIdError
from icestac.schema import IcestacItem, ItemsInput, get_schema_from_items
from tests.helpers import items_to_list


def test_create_item_table(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    items: ItemsInput,
) -> None:
    expected_items = items_to_list(items)
    arrow_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=test_collection_id,
    )

    assert table.schema().find_field("datetime")

    # Ensure data has required fields marked as non-nullable to match table schema
    arrow_data = (
        to_arrow(expected_items) if not isinstance(items, ArrowTable) else items
    )
    enforced_schema = IcestacItem.enforce_required_fields(arrow_data.schema)
    arrow_table = pyarrow.table(arrow_data).cast(pyarrow.schema(enforced_schema))

    table.upsert(
        df=arrow_table,
        join_cols=["id"],
    )

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)
    assert result.column("id").to_pylist() == [item["id"] for item in expected_items]

    with pytest.raises(TableAlreadyExistsError):
        test_catalog.create_item_table(
            arrow_schema=arrow_schema,
            collection_id=test_collection_id,
        )


def test_create_item_table_bad_collection_id(
    test_catalog: IcestacCatalog,
    items: ItemsInput,
) -> None:
    arrow_schema = get_schema_from_items(items)
    with pytest.raises(InvalidCollectionIdError):
        test_catalog.create_item_table(
            arrow_schema=arrow_schema,
            collection_id="bad.collection",
        )


def test_load_items(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    items: ItemsInput,
) -> None:
    expected_items = items_to_list(items)
    arrow_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=test_collection_id,
    )

    test_catalog.load_items(
        collection_id=test_collection_id,
        items=items,
        method="upsert",
    )

    table.refresh()

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)
    assert result.column("id").to_pylist() == [item["id"] for item in expected_items]
