from typing import Any

import pytest

from icestac.catalog import IcestacCatalog
from icestac.load import load_items
from icestac.schema import get_schema_from_item


def test_load_items_upsert_default(
    test_catalog: IcestacCatalog,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    """Test loading items with default upsert method."""
    # Create the table
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=sample_stac_items[0]["collection"],
    )

    # Load items (default method is upsert)
    load_items(sample_stac_items, table)

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(sample_stac_items)
    assert sorted(result.column("id").to_pylist()) == sorted(
        [item["id"] for item in sample_stac_items]
    )


def test_load_items_upsert_explicit(
    test_catalog: IcestacCatalog,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    """Test loading items with explicit upsert method."""
    # Create the table
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=sample_stac_items[0]["collection"],
    )

    # Load items with explicit upsert method
    load_items(sample_stac_items, table, method="upsert")

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(sample_stac_items)


def test_load_items_upsert_updates_existing(
    test_catalog: IcestacCatalog,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    """Test that upsert updates existing records with same ID."""
    # Create the table
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=sample_stac_items[0]["collection"],
    )

    # Load initial items
    load_items(sample_stac_items, table, method="upsert")

    # Modify items (same IDs but different data)
    modified_items = []
    for item in sample_stac_items:
        modified_item = item.copy()
        modified_item["properties"] = item["properties"].copy()
        modified_item["properties"]["title"] = f"Updated {item['properties']['title']}"
        modified_items.append(modified_item)

    # Load modified items with upsert
    load_items(modified_items, table, method="upsert")

    # Verify only 3 records exist (not 6) and they have updated titles
    result = table.scan().to_arrow()
    assert len(result) == len(sample_stac_items)

    # Check that titles were updated
    titles = result.column("title").to_pylist()
    assert all(title.startswith("Updated") for title in titles)


def test_load_items_append(
    test_catalog: IcestacCatalog,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    """Test loading items with append method."""
    # Create the table
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=sample_stac_items[0]["collection"],
    )

    # Load items with append method
    load_items(sample_stac_items, table, method="append")

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(sample_stac_items)


def test_load_items_append_creates_duplicates(
    test_catalog: IcestacCatalog,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    """Test that append creates duplicate records when IDs overlap."""
    # Create the table
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=sample_stac_items[0]["collection"],
    )

    # Load items twice with append
    load_items(sample_stac_items, table, method="append")
    load_items(sample_stac_items, table, method="append")

    # Verify we have double the records (append doesn't deduplicate)
    result = table.scan().to_arrow()
    assert len(result) == len(sample_stac_items) * 2


def test_load_items_multiple_batches(
    test_catalog: IcestacCatalog,
    sample_stac_items: list[dict[str, Any]],
) -> None:
    """Test loading items in multiple batches with different methods."""
    # Create the table
    arrow_schema = get_schema_from_item(sample_stac_items[0])
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=sample_stac_items[0]["collection"],
    )

    # Load first batch
    load_items(sample_stac_items[:2], table, method="upsert")

    result = table.scan().to_arrow()
    assert len(result) == 2

    # Load second batch
    load_items(sample_stac_items[2:], table, method="upsert")

    result = table.scan().to_arrow()
    assert len(result) == 3


def test_load_items_single_item(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    """Test loading a single item."""
    # Create the table
    arrow_schema = get_schema_from_item(sample_stac_item)
    table = test_catalog.create_item_table(
        arrow_schema=arrow_schema,
        collection_id=sample_stac_item["collection"],
    )

    # Load single item as a list
    load_items([sample_stac_item], table)

    # Verify record was inserted
    result = table.scan().to_arrow()
    assert len(result) == 1
    assert result.column("id").to_pylist()[0] == sample_stac_item["id"]


def test_load_items_different_schema(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:

    # Create the table
    arrow_schema = get_schema_from_item(sample_stac_item)
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
