from types import NoneType
from typing import Any, cast, get_args

import pyarrow as pa
import rustac
from arro3.core import Schema as ArrowSchema
from arro3.core import Table as ArrowTable
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import NestedField
from stac_pydantic.item import Item

ItemsInput = ArrowTable | list[dict[str, Any]] | dict[str, Any]


class IcestacItem(Item):
    collection: str

    @classmethod
    def get_required_fields(cls) -> set[str]:
        """
        Get the set of required field names from IcestacItem.

        Returns:
            Set of required field names, with special handling for flattened properties
        """
        required_fields = set()

        for field_name, field_info in cls.model_fields.items():
            if field_info.is_required():
                required_fields.add(field_name)

        # Special handling: rustac flattens properties.datetime to just "datetime"
        if "properties" in required_fields:
            required_fields.remove("properties")
            required_fields.add("datetime")

        return required_fields

    @classmethod
    def get_non_nullable_fields(cls) -> set[str]:
        """Get STAC fields whose values cannot be null."""
        fields = {
            name
            for name, info in cls.model_fields.items()
            if info.is_required() and NoneType not in get_args(info.annotation)
        }
        fields.discard("properties")
        return fields

    @classmethod
    def enforce_required_fields(cls, schema: ArrowSchema) -> ArrowSchema:
        """Mark non-null STAC fields as non-nullable without losing metadata."""
        non_nullable_fields = cls.get_non_nullable_fields()
        fields = [
            field.with_nullable(False) if field.name in non_nullable_fields else field
            for field in schema
        ]
        return ArrowSchema(fields=fields, metadata=schema.metadata)

    @classmethod
    def validate_schema(cls, schema: ArrowSchema | IcebergSchema) -> None:
        """Validate that a schema contains the required STAC item fields."""
        schema_fields = set(
            schema.column_names if isinstance(schema, IcebergSchema) else schema.names
        )
        missing_fields = cls.get_required_fields() - schema_fields

        if missing_fields:
            raise ValueError(
                f"Schema is missing required STAC fields: {sorted(missing_fields)}"
            )


def _first_item_from_arrow(items: ArrowTable) -> dict[str, Any]:
    table = pa.table(items)

    if len(table) == 0:
        raise ValueError("Cannot validate an empty Arrow table")

    feature_collection = rustac.from_arrow(table.slice(0, 1))

    return feature_collection["features"][0]


def get_schema_from_items(items: ItemsInput) -> ArrowSchema:
    """Derive an enforced Arrow schema from STAC dictionaries or Arrow data."""
    if isinstance(items, dict):
        item = cast(dict[str, Any], items)
        items = [item]
    elif isinstance(items, list):
        item = items[0]
    elif isinstance(items, ArrowTable):
        item = _first_item_from_arrow(items)

    IcestacItem.model_validate(item)

    if not isinstance(items, ArrowTable):
        items = rustac.to_arrow(items)

    return IcestacItem.enforce_required_fields(items.schema)


def convert_schema(schema: ArrowSchema) -> IcebergSchema:
    """Validate and convert an Arrow item schema to Iceberg with field IDs."""
    IcestacItem.validate_schema(schema)
    _schema = _pyarrow_to_schema_without_ids(
        pa.schema(IcestacItem.enforce_required_fields(schema))
    )

    fields = []
    for i, _field in enumerate(_schema.fields, start=1):
        field_dict = _field.model_dump()
        field_dict["id"] = i
        fields.append(NestedField(**field_dict))

    return IcebergSchema(*fields)
