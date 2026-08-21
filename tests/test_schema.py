from typing import Any

import pyarrow as pa
import pytest
from arro3.core import Schema
from pydantic import ValidationError

from icestac.schema import IcestacItem, convert_schema, get_schema_from_items


def test_get_schema_from_items(sample_stac_item: dict[str, Any]) -> None:
    """Test that we can extract an Arrow schema from a STAC item."""
    schema = get_schema_from_items(sample_stac_item)

    assert isinstance(schema, Schema)
    assert "id" in schema.names
    assert "datetime" in schema.names
    assert "collection" in schema.names
    assert schema.field("geometry").metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"


def test_get_schema_from_items_validates(sample_stac_item: dict[str, Any]) -> None:
    """Test that get_schema_from_items validates the STAC item."""
    invalid_item = {"not": "a stac item"}

    with pytest.raises(ValidationError):
        get_schema_from_items(invalid_item)


def test_get_schema_from_items_no_collection(sample_stac_item: dict[str, Any]) -> None:
    """Test that missing collection field raises ValueError."""
    _ = sample_stac_item.pop("collection")

    with pytest.raises(ValidationError):
        _ = get_schema_from_items(sample_stac_item)


def test_validate_schema_valid(sample_stac_item: dict[str, Any]) -> None:
    """Test that a valid STAC schema passes validation."""
    schema = get_schema_from_items(sample_stac_item)

    # Should not raise
    IcestacItem.validate_schema(schema)


def test_convert_schema_prepares_item_schema(
    sample_stac_item: dict[str, Any],
) -> None:
    """Test that conversion preserves item fields and assigns source IDs."""
    iceberg_schema = convert_schema(get_schema_from_items(sample_stac_item))

    assert iceberg_schema.find_field("title").field_id > 0
    assert iceberg_schema.find_field("id").required
    assert str(iceberg_schema.find_field("geometry").field_type) == "binary"


def test_convert_schema_validates_required_fields() -> None:
    """Test that conversion rejects an Arrow schema missing STAC fields."""
    with pytest.raises(ValueError, match="missing required STAC fields.*'id'"):
        convert_schema(pa.schema([("type", pa.string())]))


def test_validate_prepared_schema_missing_required_field(
    sample_stac_item: dict[str, Any],
) -> None:
    """Test that required-field validation accepts prepared Iceberg schemas."""
    iceberg_schema = convert_schema(get_schema_from_items(sample_stac_item))
    missing_id = type(iceberg_schema)(
        *(field for field in iceberg_schema.fields if field.name != "id")
    )

    with pytest.raises(ValueError, match="missing required STAC fields.*'id'"):
        IcestacItem.validate_schema(missing_id)


def test_validate_schema_missing_required_field() -> None:
    """Test that a schema missing required fields raises ValueError."""
    # Create a schema missing the required 'id' field
    schema = pa.schema(
        [
            ("type", pa.string()),
            ("geometry", pa.string()),
            ("collection", pa.string()),
            ("datetime", pa.string()),
        ]
    )

    with pytest.raises(ValueError, match="missing required STAC fields.*'id'"):
        IcestacItem.validate_schema(schema)


def test_validate_schema_missing_datetime() -> None:
    """Test that a schema missing datetime field raises ValueError."""
    # Create a schema with all required fields except datetime
    schema = pa.schema(
        [
            ("type", pa.string()),
            ("id", pa.string()),
            ("geometry", pa.string()),
            ("collection", pa.string()),
            ("stac_version", pa.string()),
            ("links", pa.string()),
            ("assets", pa.string()),
            ("bbox", pa.list_(pa.float64())),
        ]
    )

    with pytest.raises(ValueError, match="missing required STAC fields.*'datetime'"):
        IcestacItem.validate_schema(schema)
