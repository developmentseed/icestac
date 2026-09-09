import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import ListType
import rustac

from icestac.schema import IcestacItem, get_schema_from_items


def test_get_schema_from_items(items: pa.Table) -> None:
    """Derive an Iceberg schema from a flattened Arrow table."""
    schema = get_schema_from_items(items)

    assert isinstance(schema, Schema)
    assert schema.find_field("title").field_id > 0
    assert schema.find_field("id").required
    assert str(schema.find_field("geometry").field_type) == "binary"

    nested_paths = (
        "bbox.xmin",
        "bbox.ymax",
        "links.element.href",
        "assets.data.href",
    )
    nested_ids = [schema.find_field(path).field_id for path in nested_paths]
    assert all(field_id > 0 for field_id in nested_ids)
    assert len(nested_ids) == len(set(nested_ids))
    links_type = schema.find_field("links").field_type
    assert isinstance(links_type, ListType)
    assert links_type.element_id > 0


def test_get_schema_from_items_rejects_empty_table(items: pa.Table) -> None:
    with pytest.raises(
        ValueError, match="Cannot infer or load a schema from an empty Arrow table"
    ):
        get_schema_from_items(items.slice(0, 0))


def test_public_ingestion_rejects_dictionary_inputs(sample_stac_item) -> None:
    for items in (sample_stac_item, [sample_stac_item]):
        with pytest.raises(TypeError, match="pyarrow.Table or arro3.core.Table"):
            get_schema_from_items(items)


def test_public_ingestion_accepts_arro3_table(sample_stac_item) -> None:
    items = rustac.to_arrow([sample_stac_item])

    assert get_schema_from_items(items).find_field("id")


def test_get_schema_from_items_accepts_semantically_unvalidated_arrow(
    sample_stac_item,
) -> None:
    invalid_temporal_item = {
        **sample_stac_item,
        "properties": {"datetime": None},
    }
    items = pa.table(rustac.to_arrow([invalid_temporal_item]))

    assert get_schema_from_items(items).find_field("datetime")


def test_get_schema_from_items_rejects_missing_required_field(items: pa.Table) -> None:
    with pytest.raises(ValueError, match="missing required STAC fields.*'id'"):
        get_schema_from_items(items.drop(["id"]))


def test_get_schema_from_items_rejects_invalid_field_type(items: pa.Table) -> None:
    index = items.schema.get_field_index("id")
    invalid = items.set_column(index, "id", pa.array([1, 2, 3], type=pa.int64()))

    with pytest.raises(ValueError, match="Unsupported types for STAC fields: id"):
        get_schema_from_items(invalid)


def test_get_schema_from_items_rejects_null_required_field(items: pa.Table) -> None:
    index = items.schema.get_field_index("id")
    invalid = items.set_column(
        index, "id", pa.array([None, None, None], type=pa.string())
    )

    with pytest.raises(ValueError, match="Casting field 'id' with null values"):
        get_schema_from_items(invalid)


def test_get_schema_from_items_rejects_null_links(items: pa.Table) -> None:
    """Empty-link normalization must not replace null links with empty lists."""
    invalid = items.set_column(
        items.schema.get_field_index("links"),
        "links",
        pa.array([[], None, []], type=pa.list_(pa.null())),
    )

    with pytest.raises(ValueError, match="Casting field 'links' with null values"):
        get_schema_from_items(invalid)


def test_validate_schema_valid(items: pa.Table) -> None:
    """A valid inferred schema passes structural validation."""
    IcestacItem.validate_schema(get_schema_from_items(items))


def test_validate_schema_missing_required_field() -> None:
    schema = pa.schema(
        [
            ("type", pa.string()),
            ("geometry", pa.binary()),
            ("collection", pa.string()),
            ("datetime", pa.timestamp("ms")),
        ]
    )

    with pytest.raises(ValueError, match="missing required STAC fields.*'id'"):
        IcestacItem.validate_schema(schema)


def test_validate_schema_rejects_invalid_field_type() -> None:
    schema = pa.schema(
        [
            ("type", pa.string()),
            ("id", pa.int64()),
            ("geometry", pa.binary()),
            ("collection", pa.string()),
            ("datetime", pa.timestamp("ms")),
            ("links", pa.list_(pa.string())),
            ("assets", pa.struct([])),
        ]
    )

    with pytest.raises(ValueError, match="Unsupported types for STAC fields: id"):
        IcestacItem.validate_schema(schema)
