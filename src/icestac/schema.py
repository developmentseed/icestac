from typing import Any

import pyarrow as pa
from arro3.core import Field
from arro3.core import Schema as ArrowSchema
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import NestedField
from rustac import to_arrow
from stac_pydantic.item import Item


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
    def enforce_required_fields(cls, schema: ArrowSchema) -> ArrowSchema:
        """
        Ensure required STAC fields are marked as non-nullable in the Arrow schema.

        Returns a schema with required fields marked as nullable=False. This ensures
        the Iceberg table will enforce these fields as required.

        Args:
            schema: arro3.core.Schema from rustac

        Returns:
            arro3.core.Schema with required fields marked as non-nullable
        """
        required_fields = cls.get_required_fields()

        new_fields = []

        for field in schema:
            if field.name in required_fields:
                # Mark as non-nullable (required)
                new_fields.append(
                    Field(name=field.name, type=field.type, nullable=False)
                )
            else:
                # Keep original nullable setting
                new_fields.append(field)

        return ArrowSchema(fields=new_fields)

    @classmethod
    def validate_schema(cls, schema: ArrowSchema) -> None:
        """
        Validate that an Arrow schema contains required STAC item fields.

        Checks for top-level required fields from IcestacItem.
        Note: rustac flattens nested properties, so 'properties.datetime'
        becomes 'datetime' in the Arrow schema.

        Args:
            schema: arro3.core.Schema to validate

        Raises:
            ValueError: If required STAC fields are missing from the schema
        """
        schema_fields = set(schema.names)
        required_fields = cls.get_required_fields()
        missing_fields = required_fields - schema_fields

        if missing_fields:
            raise ValueError(
                f"Arrow schema is missing required STAC fields: {sorted(missing_fields)}"
            )


def get_schema_from_item(item: dict[str, Any]) -> ArrowSchema:

    return IcestacItem(**item).enforce_required_fields(to_arrow([item]).schema)


def convert_schema(schema: ArrowSchema) -> IcebergSchema:
    """Convert the arrow schema to an iceberg schema with field ids

    Necessary because built-in converter functions do not assign field ids.
    """
    _schema = _pyarrow_to_schema_without_ids(
        pa.schema(IcestacItem.enforce_required_fields(schema))
    )

    fields = []
    for i, _field in enumerate(_schema.fields, start=1):
        field_dict = _field.model_dump()
        field_dict["id"] = i
        fields.append(NestedField(**field_dict))

    return IcebergSchema(*fields)
