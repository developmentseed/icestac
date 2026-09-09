from copy import deepcopy
from typing import Any

import pyarrow as pa
import pytest
import rustac
from pyiceberg.exceptions import CommitFailedException

from icestac.catalog import IcestacCatalog
from icestac.load import Method, load_items
from icestac.schema import get_schema_from_items, prepare_arrow_table
from tests.helpers import items_to_arrow, items_to_list


@pytest.mark.filterwarnings(
    "error:Delete operation did not match any records:UserWarning"
)
def test_load_items_upsert_default(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    items: pa.Table,
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
    items: pa.Table,
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
    items: pa.Table,
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
    load_items(items_to_arrow(modified_items), table, method="upsert")

    # Verify only the original record count exists and they have updated titles
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)

    # Check that titles were updated
    titles = result.column("title").to_pylist()
    assert all(title.startswith("Updated") for title in titles)


def test_upsert_reconstructs_evolved_existing_item(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    """An existing item can gain a top-level property during replacement."""
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(initial),
        collection_id=sample_stac_item["collection"],
    )
    load_items(initial, table)

    replacement = deepcopy(sample_stac_item)
    replacement["properties"]["processing:software"] = {"version": "1.0"}
    load_items(items_to_arrow([replacement]), table, evolve_schema=True)

    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"]
    assert len(reconstructed) == 1
    assert reconstructed[0]["id"] == replacement["id"]
    assert reconstructed[0]["properties"] == replacement["properties"]


def test_upsert_reconstructs_nested_asset_and_property_evolution(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    """Nested asset fields and flattened properties evolve in one replacement."""
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(initial),
        collection_id=sample_stac_item["collection"],
    )
    load_items(initial, table)

    replacement = deepcopy(sample_stac_item)
    replacement["properties"]["processing:software"] = {"version": "1.0"}
    replacement["assets"]["data"]["roles"] = ["data"]
    replacement["assets"]["thumbnail"] = {
        "href": "https://example.com/thumbnail.jpg",
        "type": "image/jpeg",
    }
    load_items(items_to_arrow([replacement]), table, evolve_schema=True)

    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"][0]
    assert reconstructed["properties"]["processing:software"] == {"version": "1.0"}
    assert reconstructed["assets"] == replacement["assets"]


def test_upsert_clears_omitted_optional_fields(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    """Omitted optional values are cleared instead of being retained."""
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(initial),
        collection_id=sample_stac_item["collection"],
    )
    load_items(initial, table)

    replacement = deepcopy(sample_stac_item)
    replacement["properties"] = {"datetime": sample_stac_item["properties"]["datetime"]}
    replacement["assets"]["data"] = {"href": "https://example.com/replacement.tif"}
    load_items(items_to_arrow([replacement]), table)

    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"][0]
    assert "title" not in reconstructed["properties"]
    assert reconstructed["assets"] == {
        "data": {"href": replacement["assets"]["data"]["href"]}
    }


def test_competing_upserts_fail_without_duplicate_ids(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    """A stale competing upsert fails instead of committing a duplicate."""
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(initial),
        collection_id=sample_stac_item["collection"],
    )
    first = test_catalog.catalog.load_table(table.name())
    second = test_catalog.catalog.load_table(table.name())
    arrow_table = prepare_arrow_table(items_to_arrow([sample_stac_item]))

    first_transaction = first.transaction()
    second_transaction = second.transaction()
    first_transaction.upsert(df=arrow_table, join_cols=["id"])
    second_transaction.upsert(df=arrow_table, join_cols=["id"])

    first_transaction.commit_transaction()
    with pytest.raises(CommitFailedException):
        second_transaction.commit_transaction()

    second.refresh()
    result = second.scan().to_arrow()
    assert result.column("id").to_pylist() == [sample_stac_item["id"]]


def test_load_items_append(
    test_catalog: IcestacCatalog,
    items: pa.Table,
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
    items: pa.Table,
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
    items: pa.Table,
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
    load_items(items_to_arrow(expected_items[:2]), table, method="upsert")

    result = table.scan().to_arrow()
    assert len(result) == min(2, len(expected_items))

    # Load second batch
    if len(expected_items) > 2:
        load_items(items_to_arrow(expected_items[2:]), table, method="upsert")

    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)


def test_load_items_rejects_invalid_method(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    initial = items_to_arrow([sample_stac_item])
    iceberg_schema = get_schema_from_items(initial)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
        collection_id=sample_stac_item["collection"],
    )

    with pytest.raises(ValueError, match="Unsupported load method"):
        load_items(initial, table, method="insert")  # type: ignore[arg-type]


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
    interval_items = items_to_arrow([interval_item])
    iceberg_schema = get_schema_from_items(interval_items)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
        collection_id=interval_item["collection"],
    )

    load_items(interval_items, table)

    assert table.scan().to_arrow().column("id").to_pylist() == [interval_item["id"]]


def test_load_items_different_schema(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    initial = items_to_arrow([sample_stac_item])
    iceberg_schema = get_schema_from_items(initial)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
        collection_id=sample_stac_item["collection"],
    )
    load_items(initial, table)

    item_new_schema = deepcopy(sample_stac_item)
    item_new_schema["properties"]["new_field"] = True
    with pytest.raises(ValueError, match="Update the schema first"):
        load_items(items_to_arrow([item_new_schema]), table)


@pytest.mark.parametrize("method", ["append", "upsert"])
def test_load_items_evolves_schema(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
    method: Method,
) -> None:
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(initial),
        collection_id=sample_stac_item["collection"],
    )
    load_items(initial, table)

    evolved_item = deepcopy(sample_stac_item)
    evolved_item["id"] = "evolved-item"
    evolved_item["properties"]["processing:software"] = {
        "Atmospheric Correction": "6.0"
    }
    load_items(items_to_arrow([evolved_item]), table, method=method, evolve_schema=True)

    table.refresh()
    assert table.schema().find_field("processing:software.Atmospheric Correction")
    assert len(table.scan().to_arrow()) == 2


def test_populated_link_fields_survive_reconstruction(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    item = deepcopy(sample_stac_item)
    item["links"] = [
        {
            "href": "https://example.com/query",
            "rel": "data",
            "type": "application/json",
            "title": "Query",
            "method": "POST",
            "headers": {"content-type": "application/json"},
            "body": {"limit": 1},
            "merge": True,
        }
    ]
    input_items = items_to_arrow([item])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(input_items),
        collection_id=item["collection"],
    )

    load_items(input_items, table)

    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"][0]
    assert reconstructed["links"] == item["links"]


def test_populated_links_require_evolution_after_empty_schema(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    empty_links = deepcopy(sample_stac_item)
    empty_links["links"] = []
    empty_links_table = items_to_arrow([empty_links])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(empty_links_table),
        collection_id=empty_links["collection"],
    )
    load_items(empty_links_table, table)

    populated = deepcopy(sample_stac_item)
    populated["id"] = "populated-links"
    populated["links"] = [
        {
            "href": "https://example.com/query",
            "rel": "data",
            "method": "POST",
            "headers": {"content-type": "application/json"},
            "body": {"limit": 1},
            "merge": True,
        }
    ]
    populated_table = items_to_arrow([populated])
    with pytest.raises(ValueError, match="Update the schema first"):
        load_items(populated_table, table)

    load_items(populated_table, table, evolve_schema=True)
    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"]
    assert (
        next(item for item in reconstructed if item["id"] == populated["id"])["links"]
        == populated["links"]
    )


def test_arrow_temporal_semantics_are_callers_responsibility(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    invalid = deepcopy(sample_stac_item)
    invalid["properties"] = {
        "datetime": None,
        "start_datetime": "2024-01-02T00:00:00Z",
        "end_datetime": "2024-01-01T00:00:00Z",
    }
    arrow_items = items_to_arrow([invalid])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(arrow_items),
        collection_id=sample_stac_item["collection"],
    )

    load_items(arrow_items, table)

    assert table.scan().to_arrow().column("id").to_pylist() == [sample_stac_item["id"]]


def test_arrow_structural_edge_cases_follow_arrow_schema(
    sample_stac_item: dict[str, Any],
) -> None:
    arrow = items_to_arrow([sample_stac_item])
    geometry_index = arrow.schema.get_field_index("geometry")
    null_geometry = arrow.set_column(
        geometry_index,
        "geometry",
        pa.array([None], type=pa.binary()),
    )

    assert get_schema_from_items(null_geometry).find_field("geometry")


def test_load_items_accepts_arro3_table(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    items = rustac.to_arrow([sample_stac_item])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(items),
        collection_id=sample_stac_item["collection"],
    )

    load_items(items, table)

    assert table.scan().to_arrow().column("id").to_pylist() == [sample_stac_item["id"]]


@pytest.mark.parametrize("method", ["append", "upsert"])
def test_load_items_accepts_dictionary_columns(
    test_catalog: IcestacCatalog,
    items: pa.Table,
    method: Method,
) -> None:
    """Dictionary-encoded columns can be written without manual re-encoding."""
    for name in ("id", "collection", "title"):
        items = items.set_column(
            items.schema.get_field_index(name), name, items[name].dictionary_encode()
        )
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(items),
        collection_id=items["collection"][0].as_py(),
    )

    load_items(items, table, method=method)

    result = table.scan().to_arrow()
    assert sorted(zip(result["id"].to_pylist(), result["title"].to_pylist())) == sorted(
        zip(items["id"].to_pylist(), items["title"].to_pylist())
    )


def test_load_items_rejects_non_arrow_inputs(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(initial),
        collection_id=sample_stac_item["collection"],
    )

    with pytest.raises(TypeError, match="pyarrow.Table"):
        load_items(sample_stac_item, table)


def test_load_items_rejects_invalid_arrow_schema(
    test_catalog: IcestacCatalog,
    items: pa.Table,
) -> None:
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(items),
        collection_id="test-collection",
    )
    id_index = items.schema.get_field_index("id")
    invalid = items.set_column(
        id_index,
        "id",
        pa.array([1, 2, 3], type=pa.int64()),
    )

    with pytest.raises(ValueError, match="Unsupported types for STAC fields: id"):
        load_items(invalid, table)
    assert len(table.scan().to_arrow()) == 0


def test_load_items_does_not_evolve_schema_when_write_fails(
    test_catalog: IcestacCatalog,
    sample_stac_item: dict[str, Any],
) -> None:
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(initial),
        collection_id=sample_stac_item["collection"],
    )
    load_items(initial, table)
    before = rustac.from_arrow(table.scan().to_arrow())["features"]

    evolved_item = deepcopy(sample_stac_item)
    evolved_item["properties"]["new_field"] = True

    evolved = items_to_arrow([evolved_item, evolved_item])
    with pytest.raises(ValueError, match="Duplicate rows"):
        load_items(evolved, table, evolve_schema=True)

    table.refresh()
    assert rustac.from_arrow(table.scan().to_arrow())["features"] == before
    with pytest.raises(ValueError, match="Could not find field"):
        table.schema().find_field("new_field")
