from typing import Any

import pytest

from icestac.catalog import IcestacCatalog
from icestac.load import load_items
from icestac.schema import ItemsInput, get_schema_from_items
from tests.helpers import items_to_list


def test_load_items_upsert_default(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    items: ItemsInput,
) -> None:
    """Test loading items with default upsert method."""
    expected_items = items_to_list(items)

    # Create the table
    arrow_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=test_collection_id,
    )

    # Load items (default method is upsert)
    load_items(items, table)

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)
    assert sorted(result.column("id").to_pylist()) == sorted(
        item["id"] for item in expected_items
    )


def test_load_items_upsert_explicit(
    test_catalog: IcestacCatalog,
    items: ItemsInput,
) -> None:
    """Test loading items with explicit upsert method."""
    expected_items = items_to_list(items)

    # Create the table
    arrow_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=expected_items[0]["collection"],
    )

    # Load items with explicit upsert method
    load_items(items, table, method="upsert")

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)


def test_load_items_upsert_updates_existing(
    test_catalog: IcestacCatalog,
    items: ItemsInput,
) -> None:
    """Test that upsert updates existing records with same ID."""
    expected_items = items_to_list(items)

    # Create the table
    arrow_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=expected_items[0]["collection"],
    )

    # Load initial items
    load_items(items, table, method="upsert")

    # Modify items (same IDs but different data)
    modified_items = []
    for item in expected_items:
        modified_item = item.copy()
        modified_item["properties"] = item["properties"].copy()
        modified_item["properties"]["title"] = f"Updated {item['properties']['title']}"
        modified_items.append(modified_item)

    # Load modified items with upsert
    load_items(modified_items, table, method="upsert")

    # Verify only the original record count exists and they have updated titles
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)

    # Check that titles were updated
    titles = result.column("title").to_pylist()
    assert all(title.startswith("Updated") for title in titles)


def test_load_items_append(
    test_catalog: IcestacCatalog,
    items: ItemsInput,
) -> None:
    """Test loading items with append method."""
    expected_items = items_to_list(items)

    # Create the table
    arrow_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=expected_items[0]["collection"],
    )

    # Load items with append method
    load_items(items, table, method="append")

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)


def test_load_items_append_creates_duplicates(
    test_catalog: IcestacCatalog,
    items: ItemsInput,
) -> None:
    """Test that append creates duplicate records when IDs overlap."""
    expected_items = items_to_list(items)

    # Create the table
    arrow_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=expected_items[0]["collection"],
    )

    # Load items twice with append
    load_items(items, table, method="append")
    load_items(items, table, method="append")

    # Verify we have double the records (append doesn't deduplicate)
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items) * 2


def test_load_items_multiple_batches(
    test_catalog: IcestacCatalog,
    items: ItemsInput,
) -> None:
    """Test loading items in multiple batches with different methods."""
    expected_items = items_to_list(items)

    # Create the table
    arrow_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=expected_items[0]["collection"],
    )

    # Load first batch
    load_items(expected_items[:2], table, method="upsert")

    result = table.scan().to_arrow()
    assert len(result) == min(2, len(expected_items))

    # Load second batch
    if len(expected_items) > 2:
        load_items(expected_items[2:], table, method="upsert")

    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)


def test_load_items_different_schema(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    # Create the table
    arrow_schema = get_schema_from_items(sample_stac_item)
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=sample_stac_item["collection"],
    )

    # load an item
    load_items([sample_stac_item], table)

    # change the schema
    item_new_schema = sample_stac_item.copy()
    item_new_schema["properties"]["new_field"] = True
    with pytest.raises(ValueError, match="Update the schema first"):
        load_items([item_new_schema], table)
