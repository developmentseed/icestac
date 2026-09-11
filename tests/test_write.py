from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Barrier
from typing import Any

import pyarrow as pa
import pytest
import rustac
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table, TableProperties

from icestac.constants import COLLECTIONS_TABLE_NAME, DEFAULT_NAMESPACE
from icestac.schema import get_schema_from_items
from icestac.write import put_items
from tests.helpers import items_to_arrow, items_to_list


@pytest.mark.filterwarnings(
    "error:Delete operation did not match any records:UserWarning"
)
def test_put_items_inserts_items(
    test_catalog: Catalog,
    test_collection_id: str,
    items: pa.Table,
) -> None:
    """Insert a batch into a native Iceberg table."""
    expected_items = items_to_list(items)

    # Create the table
    iceberg_schema = get_schema_from_items(items)
    assert iceberg_schema.identifier_field_ids == [
        iceberg_schema.find_field("id").field_id
    ]
    table = test_catalog.create_table(
        schema=iceberg_schema,
        identifier=(DEFAULT_NAMESPACE, test_collection_id),
    )

    # Put items through the standalone writer.
    put_items(table, items)

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)
    assert sorted(result.column("id").to_pylist()) == sorted(
        item["id"] for item in expected_items
    )


def test_put_items_accepts_dotted_collection_ids(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    item = deepcopy(sample_stac_item)
    item["collection"] = "foo.bar"
    items = items_to_arrow([item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(items),
        identifier=(DEFAULT_NAMESPACE, item["collection"]),
    )

    put_items(table, items)

    assert table.name() == (DEFAULT_NAMESPACE, "foo.bar")
    assert table.scan().to_arrow().column("collection").to_pylist() == ["foo.bar"]


def test_put_items_rejects_different_collection(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    items = items_to_arrow([sample_stac_item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(items),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )
    different = deepcopy(sample_stac_item)
    different["collection"] = "other-collection"

    with pytest.raises(ValueError, match="other-collection"):
        put_items(table, items_to_arrow([different]))

    assert len(table.scan().to_arrow()) == 0


def test_put_items_rejects_reserved_collections_table(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(initial),
        identifier=(DEFAULT_NAMESPACE, COLLECTIONS_TABLE_NAME),
    )
    evolved = deepcopy(sample_stac_item)
    evolved["properties"]["new_field"] = True

    with pytest.raises(ValueError, match="reserved table 'collections'"):
        put_items(table, items_to_arrow([evolved]), evolve_schema=True)

    table.refresh()
    assert len(table.scan().to_arrow()) == 0
    with pytest.raises(ValueError, match="Could not find field"):
        table.schema().find_field("new_field")


def test_put_items_inserts_items_explicitly(
    test_catalog: Catalog,
    items: pa.Table,
) -> None:
    """Insert a batch through the standalone writer."""
    expected_items = items_to_list(items)

    # Create the table
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_table(
        schema=iceberg_schema,
        identifier=(DEFAULT_NAMESPACE, expected_items[0]["collection"]),
    )

    # Put items through the standalone writer.
    put_items(table, items)

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)


def test_put_items_replaces_existing_items(
    test_catalog: Catalog,
    items: pa.Table,
) -> None:
    """Replace existing records with the same ID."""
    expected_items = items_to_list(items)

    # Create the table
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_table(
        schema=iceberg_schema,
        identifier=(DEFAULT_NAMESPACE, expected_items[0]["collection"]),
    )

    # Put initial items
    put_items(table, items)

    # Modify items (same IDs but different data)
    modified_items = []
    for item in expected_items:
        modified_item = item.copy()
        modified_item["properties"] = item["properties"].copy()
        modified_item["properties"]["title"] = f"Updated {item['properties']['title']}"
        modified_items.append(modified_item)

    # Replace the existing items.
    put_items(table, items_to_arrow(modified_items))

    # Verify only the original record count exists and they have updated titles
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)

    # Check that titles were updated
    titles = result.column("title").to_pylist()
    assert all(title.startswith("Updated") for title in titles)


def test_put_items_reconstructs_evolved_existing_item(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    """An existing item can gain a top-level property during replacement."""
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(initial),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )
    put_items(table, initial)

    replacement = deepcopy(sample_stac_item)
    replacement["properties"]["processing:software"] = {"version": "1.0"}
    put_items(table, items_to_arrow([replacement]), evolve_schema=True)

    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"]
    assert len(reconstructed) == 1
    assert reconstructed[0]["id"] == replacement["id"]
    assert reconstructed[0]["properties"] == replacement["properties"]


def test_put_items_reconstructs_nested_asset_and_property_evolution(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    """Nested asset fields and flattened properties evolve in one replacement."""
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(initial),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )
    put_items(table, initial)

    replacement = deepcopy(sample_stac_item)
    replacement["properties"]["processing:software"] = {"version": "1.0"}
    replacement["assets"]["data"]["roles"] = ["data"]
    replacement["assets"]["thumbnail"] = {
        "href": "https://example.com/thumbnail.jpg",
        "type": "image/jpeg",
    }
    put_items(table, items_to_arrow([replacement]), evolve_schema=True)

    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"][0]
    assert reconstructed["properties"]["processing:software"] == {"version": "1.0"}
    assert reconstructed["assets"] == replacement["assets"]


def test_put_items_clears_omitted_optional_fields(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    """Omitted optional values are cleared instead of being retained."""
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(initial),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )
    put_items(table, initial)

    replacement = deepcopy(sample_stac_item)
    replacement["properties"] = {"datetime": sample_stac_item["properties"]["datetime"]}
    replacement["assets"]["data"] = {"href": "https://example.com/replacement.tif"}
    put_items(table, items_to_arrow([replacement]))

    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"][0]
    assert "title" not in reconstructed["properties"]
    assert reconstructed["assets"] == {
        "data": {"href": replacement["assets"]["data"]["href"]}
    }


@pytest.mark.parametrize("populate_first", [False, True])
def test_competing_replacement_writes_do_not_duplicate_ids(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    populate_first: bool,
) -> None:
    """Concurrent replacement writes fail rather than replay stale matches."""
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(initial),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )
    if populate_first:
        put_items(table, initial)
    first = test_catalog.load_table(table.name())
    second = test_catalog.load_table(table.name())
    assert TableProperties.COMMIT_NUM_RETRIES not in first.metadata.properties

    first_item = deepcopy(sample_stac_item)
    first_item["properties"]["title"] = "First"
    second_item = deepcopy(sample_stac_item)
    second_item["properties"]["title"] = "Second"
    barrier = Barrier(2)
    original_commit = Table._do_commit

    def synchronized_commit(self, updates, requirements):
        barrier.wait(timeout=10)
        return original_commit(self, updates, requirements)

    monkeypatch.setattr(Table, "_do_commit", synchronized_commit)

    def write(item: dict[str, Any], target: Table) -> Exception | None:
        try:
            put_items(target, items_to_arrow([item]))
        except Exception as error:  # noqa: BLE001
            return error
        return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        errors = list(
            executor.map(
                lambda args: write(*args),
                ((first_item, first), (second_item, second)),
            )
        )

    assert sum(error is not None for error in errors) == 1
    assert any(isinstance(error, CommitFailedException) for error in errors)
    table.refresh()
    result = table.scan().to_arrow()
    assert len(result) == 1
    assert result.column("id").to_pylist() == [sample_stac_item["id"]]


def test_failed_put_restores_retry_policy(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    items = items_to_arrow([sample_stac_item])
    retry_property = TableProperties.COMMIT_NUM_RETRIES
    table = test_catalog.create_table(
        schema=get_schema_from_items(items),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
        properties={retry_property: "7"},
    )

    def fail_commit(self, updates, requirements):
        raise CommitFailedException("controlled commit failure")

    monkeypatch.setattr(Table, "_do_commit", fail_commit)
    with pytest.raises(CommitFailedException, match="controlled commit failure"):
        put_items(table, items)

    assert table.metadata.properties[retry_property] == "7"


def test_successful_put_restores_retry_policy(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    items = items_to_arrow([sample_stac_item])
    retry_property = TableProperties.COMMIT_NUM_RETRIES
    table = test_catalog.create_table(
        schema=get_schema_from_items(items),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
        properties={retry_property: "7"},
    )

    put_items(table, items)

    assert table.metadata.properties[retry_property] == "7"


def test_failed_put_does_not_overwrite_refreshed_retry_policy(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    items = items_to_arrow([sample_stac_item])
    retry_property = TableProperties.COMMIT_NUM_RETRIES
    table = test_catalog.create_table(
        schema=get_schema_from_items(items),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
        properties={retry_property: "7"},
    )

    def refresh_then_fail(self, updates, requirements):
        self.refresh()
        raise CommitFailedException("controlled commit failure")

    monkeypatch.setattr(Table, "_do_commit", refresh_then_fail)
    with pytest.raises(CommitFailedException, match="controlled commit failure"):
        put_items(table, items)

    assert table.metadata.properties[retry_property] == "7"


def test_put_items_multiple_batches(
    test_catalog: Catalog,
    items: pa.Table,
) -> None:
    """Put multiple batches into one table."""
    expected_items = items_to_list(items)

    # Create the table
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_table(
        schema=iceberg_schema,
        identifier=(DEFAULT_NAMESPACE, expected_items[0]["collection"]),
    )

    # Put the first batch
    put_items(table, items_to_arrow(expected_items[:2]))

    result = table.scan().to_arrow()
    assert len(result) == min(2, len(expected_items))

    # Put the second batch
    if len(expected_items) > 2:
        put_items(table, items_to_arrow(expected_items[2:]))

    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)


def test_put_items_supports_interval_datetime(
    test_catalog: Catalog,
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
    table = test_catalog.create_table(
        schema=iceberg_schema,
        identifier=(DEFAULT_NAMESPACE, interval_item["collection"]),
    )

    put_items(table, interval_items)

    assert table.scan().to_arrow().column("id").to_pylist() == [interval_item["id"]]


def test_put_items_different_schema(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    initial = items_to_arrow([sample_stac_item])
    iceberg_schema = get_schema_from_items(initial)
    table = test_catalog.create_table(
        schema=iceberg_schema,
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )
    put_items(table, initial)

    item_new_schema = deepcopy(sample_stac_item)
    item_new_schema["properties"]["new_field"] = True
    with pytest.raises(ValueError, match="Update the schema first"):
        put_items(table, items_to_arrow([item_new_schema]))


def test_put_items_evolves_schema(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(initial),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )
    put_items(table, initial)

    evolved_item = deepcopy(sample_stac_item)
    evolved_item["id"] = "evolved-item"
    evolved_item["properties"]["processing:software"] = {
        "Atmospheric Correction": "6.0"
    }
    put_items(table, items_to_arrow([evolved_item]), evolve_schema=True)

    table.refresh()
    assert table.schema().find_field("processing:software.Atmospheric Correction")
    assert len(table.scan().to_arrow()) == 2


def test_populated_link_fields_survive_reconstruction(
    test_catalog: Catalog,
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
    table = test_catalog.create_table(
        schema=get_schema_from_items(input_items),
        identifier=(DEFAULT_NAMESPACE, item["collection"]),
    )

    put_items(table, input_items)

    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"][0]
    assert reconstructed["links"] == item["links"]


def test_put_items_clears_extended_links_without_schema_evolution(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    populated = deepcopy(sample_stac_item)
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
    table = test_catalog.create_table(
        schema=get_schema_from_items(populated_table),
        identifier=(DEFAULT_NAMESPACE, populated["collection"]),
    )
    put_items(table, populated_table)

    replacement = deepcopy(populated)
    replacement["links"] = []
    put_items(table, items_to_arrow([replacement]))

    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"][0]
    assert reconstructed["links"] == []


def test_populated_links_require_evolution_after_empty_schema(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    empty_links = deepcopy(sample_stac_item)
    empty_links["links"] = []
    empty_links_table = items_to_arrow([empty_links])
    table = test_catalog.create_table(
        schema=get_schema_from_items(empty_links_table),
        identifier=(DEFAULT_NAMESPACE, empty_links["collection"]),
    )
    put_items(table, empty_links_table)

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
        put_items(table, populated_table)

    put_items(table, populated_table, evolve_schema=True)
    reconstructed = rustac.from_arrow(table.scan().to_arrow())["features"]
    assert (
        next(item for item in reconstructed if item["id"] == populated["id"])["links"]
        == populated["links"]
    )


def test_arrow_temporal_semantics_are_callers_responsibility(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    invalid = deepcopy(sample_stac_item)
    invalid["properties"] = {
        "datetime": None,
        "start_datetime": "2024-01-02T00:00:00Z",
        "end_datetime": "2024-01-01T00:00:00Z",
    }
    arrow_items = items_to_arrow([invalid])
    table = test_catalog.create_table(
        schema=get_schema_from_items(arrow_items),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )

    put_items(table, arrow_items)

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


def test_put_items_accepts_arro3_table(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    items = rustac.to_arrow([sample_stac_item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(items),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )

    put_items(table, items)

    assert table.scan().to_arrow().column("id").to_pylist() == [sample_stac_item["id"]]


def test_put_items_accepts_dictionary_columns(
    test_catalog: Catalog,
    items: pa.Table,
) -> None:
    """Dictionary-encoded columns can be written without manual re-encoding."""
    for name in ("id", "collection", "title"):
        items = items.set_column(
            items.schema.get_field_index(name), name, items[name].dictionary_encode()
        )
    table = test_catalog.create_table(
        schema=get_schema_from_items(items),
        identifier=(DEFAULT_NAMESPACE, items["collection"][0].as_py()),
    )

    put_items(table, items)

    result = table.scan().to_arrow()
    assert sorted(zip(result["id"].to_pylist(), result["title"].to_pylist())) == sorted(
        zip(items["id"].to_pylist(), items["title"].to_pylist())
    )


def test_put_items_rejects_non_arrow_inputs(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(initial),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )

    with pytest.raises(TypeError, match="pyarrow.Table"):
        put_items(table, sample_stac_item)


def test_put_items_rejects_invalid_arrow_schema(
    test_catalog: Catalog,
    items: pa.Table,
) -> None:
    table = test_catalog.create_table(
        schema=get_schema_from_items(items),
        identifier=(DEFAULT_NAMESPACE, "test-collection"),
    )
    id_index = items.schema.get_field_index("id")
    invalid = items.set_column(
        id_index,
        "id",
        pa.array([1, 2, 3], type=pa.int64()),
    )

    with pytest.raises(ValueError, match="Unsupported types for STAC fields: id"):
        put_items(table, invalid)
    assert len(table.scan().to_arrow()) == 0


def test_put_items_does_not_evolve_schema_when_write_fails(
    test_catalog: Catalog,
    sample_stac_item: dict[str, Any],
) -> None:
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_table(
        schema=get_schema_from_items(initial),
        identifier=(DEFAULT_NAMESPACE, sample_stac_item["collection"]),
    )
    put_items(table, initial)
    before = rustac.from_arrow(table.scan().to_arrow())["features"]

    evolved_item = deepcopy(sample_stac_item)
    evolved_item["properties"]["new_field"] = True

    evolved = items_to_arrow([evolved_item, evolved_item])
    with pytest.raises(ValueError, match="Duplicate rows"):
        put_items(table, evolved, evolve_schema=True)

    table.refresh()
    assert rustac.from_arrow(table.scan().to_arrow())["features"] == before
    with pytest.raises(ValueError, match="Could not find field"):
        table.schema().find_field("new_field")
