from typing import Any

import pyarrow
from pyiceberg.catalog import Catalog
from rustac import to_arrow

from icestac.item_table import create_item_table, sanitize_collection_id
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


def test_sanitize_collection_id_basic():
    """Test basic sanitization of collection IDs."""
    result = sanitize_collection_id("sentinel-2-l2a")
    # Should be lowercase with underscores and have 8-char hash suffix
    assert result.startswith("sentinel_2_l2a_")
    assert len(result.split("_")[-1]) == 8
    assert result.islower() or "_" in result


def test_sanitize_collection_id_dots():
    """Test that dots are replaced with underscores."""
    result = sanitize_collection_id("my.collection.id")
    assert result.startswith("my_collection_id_")
    assert ".." not in result


def test_sanitize_collection_id_uniqueness():
    """Test that different collection IDs produce different sanitized names."""
    # These would collide with simple character replacement
    id1 = sanitize_collection_id("my.collection")
    id2 = sanitize_collection_id("my_collection")
    id3 = sanitize_collection_id("my-collection")

    # All should be different due to hash suffix
    assert id1 != id2
    assert id2 != id3
    assert id1 != id3


def test_sanitize_collection_id_deterministic():
    """Test that sanitization is deterministic."""
    collection_id = "test-collection-123"
    result1 = sanitize_collection_id(collection_id)
    result2 = sanitize_collection_id(collection_id)

    assert result1 == result2


def test_sanitize_collection_id_special_chars():
    """Test handling of various special characters."""
    result = sanitize_collection_id("my@collection#with$special%chars!")
    # Should only contain lowercase alphanumeric and underscores
    assert all(c.islower() or c.isdigit() or c == "_" for c in result)


def test_sanitize_collection_id_consecutive_underscores():
    """Test that consecutive underscores are collapsed."""
    result = sanitize_collection_id("my___collection___id")
    # Should not have triple underscores in the sanitized portion
    base_name = "_".join(result.split("_")[:-1])  # exclude hash suffix
    assert "___" not in base_name


def test_sanitize_collection_id_starts_with_digit():
    """Test handling of collection IDs that start with a digit."""
    result = sanitize_collection_id("3dep-lidar")
    # Should be prepended with 'c_' to make it valid
    assert result.startswith("c_3")


def test_sanitize_collection_id_uppercase():
    """Test that uppercase letters are converted to lowercase."""
    result = sanitize_collection_id("MyCollection-ID")
    assert result == result.lower()
