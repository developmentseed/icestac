from copy import deepcopy
from typing import Any

import pytest

from icestac.catalog import IcestacCatalog
from icestac.load import Method, load_items
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
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
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
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
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
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
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
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
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
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
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
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
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


def test_load_items_rejects_invalid_method(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    iceberg_schema = get_schema_from_items(sample_stac_item)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
        collection_id=sample_stac_item["collection"],
    )

    with pytest.raises(ValueError, match="Unsupported load method"):
        load_items(sample_stac_item, table, method="insert")  # type: ignore[arg-type]


def test_load_items_supports_interval_datetime(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    interval_item = deepcopy(sample_stac_item)
    interval_item["properties"] = {
        "datetime": None,
        "start_datetime": "2024-01-01T00:00:00Z",
        "end_datetime": "2024-01-02T00:00:00Z",
    }
    iceberg_schema = get_schema_from_items(interval_item)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
        collection_id=interval_item["collection"],
    )

    load_items(interval_item, table)

    assert table.scan().to_arrow().column("id").to_pylist() == [interval_item["id"]]


def test_load_items_different_schema(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    iceberg_schema = get_schema_from_items(sample_stac_item)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
        collection_id=sample_stac_item["collection"],
    )
    load_items([sample_stac_item], table)

    item_new_schema = deepcopy(sample_stac_item)
    item_new_schema["properties"]["new_field"] = True
    with pytest.raises(ValueError, match="Update the schema first"):
        load_items([item_new_schema], table)


@pytest.mark.parametrize("method", ["append", "upsert"])
def test_load_items_evolves_schema(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
    method: Method,
) -> None:
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(sample_stac_item),
        collection_id=sample_stac_item["collection"],
    )
    load_items(sample_stac_item, table)

    evolved_item = deepcopy(sample_stac_item)
    evolved_item["id"] = "evolved-item"
    evolved_item["properties"]["processing:software"] = {
        "Atmospheric Correction": "6.0"
    }
    load_items(evolved_item, table, method=method, evolve_schema=True)

    table.refresh()
    assert table.schema().find_field("processing:software.Atmospheric Correction")
    assert len(table.scan().to_arrow()) == 2


def test_load_items_does_not_evolve_schema_when_write_fails(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(sample_stac_item),
        collection_id=sample_stac_item["collection"],
    )
    evolved_item = deepcopy(sample_stac_item)
    evolved_item["properties"]["new_field"] = True

    with pytest.raises(ValueError, match="Duplicate rows"):
        load_items([evolved_item, evolved_item], table, evolve_schema=True)

    table.refresh()
    with pytest.raises(ValueError, match="Could not find field"):
        table.schema().find_field("new_field")
