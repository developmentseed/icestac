from typing import Any

import pyarrow as pa
import pytest
from arro3.core import Schema
from pydantic import ValidationError

from icestac.schema import get_schema_from_item, validate_schema


def test_get_schema_from_item(sample_stac_item: dict[str, Any]) -> None:
    """Test that we can extract an Arrow schema from a STAC item."""
    schema = get_schema_from_item(sample_stac_item)

    assert isinstance(schema, Schema)
    assert "id" in schema.names
    assert "datetime" in schema.names
    assert "collection" in schema.names


def test_get_schema_from_item_validates(sample_stac_item: dict[str, Any]) -> None:
    """Test that get_schema_from_item validates the STAC item."""
    invalid_item = {"not": "a stac item"}

    with pytest.raises(ValidationError):
        get_schema_from_item(invalid_item)


def test_get_schema_from_item_no_collection(sample_stac_item: dict[str, Any]) -> None:
    """Test that missing collection field raises ValueError."""
    _ = sample_stac_item.pop("collection")

    with pytest.raises(ValidationError):
        _ = get_schema_from_item(sample_stac_item)


def test_validate_schema_valid(sample_stac_item: dict[str, Any]) -> None:
    """Test that a valid STAC schema passes validation."""
    schema = get_schema_from_item(sample_stac_item)

    # Should not raise
    validate_schema(schema)


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
        validate_schema(schema)


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
        validate_schema(schema)
