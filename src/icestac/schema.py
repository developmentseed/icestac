import pyarrow as pa
from arro3.core import Schema as ArrowSchema
from arro3.core import Table as ArrowTable
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.types import (
    BinaryType,
    ListType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
)

ItemsInput = pa.Table | ArrowTable
LINK_TYPE = pa.list_(
    pa.struct(
        [
            pa.field("href", pa.string(), nullable=False),
            pa.field("rel", pa.string(), nullable=False),
            pa.field("type", pa.string()),
            pa.field("title", pa.string()),
        ]
    )
)
EMPTY_ITEMS_ERROR = "Cannot infer or load a schema from an empty Arrow table"


class IcestacItem:
    """Structural fields required by the flattened STAC representation."""

    @classmethod
    def get_required_fields(cls) -> set[str]:
        """Return required top-level fields, including flattened datetime."""
        return {"geometry", "type", "id", "datetime", "links", "collection", "assets"}

    @classmethod
    def get_non_nullable_fields(cls) -> set[str]:
        """Return required fields that cannot contain null values."""
        return {"type", "id", "links", "collection", "assets"}

    @classmethod
    def enforce_required_fields(cls, schema: ArrowSchema) -> ArrowSchema:
        """Make required fields non-null and give empty links a concrete type."""
        non_nullable_fields = cls.get_non_nullable_fields()
        arrow_schema = pa.schema(schema)
        fields = []
        for field in arrow_schema:
            if field.name in non_nullable_fields:
                field = field.with_nullable(False)
            if (
                field.name == "links"
                and pa.types.is_list(field.type)
                and pa.types.is_null(field.type.value_type)
            ):
                field = field.with_type(LINK_TYPE)
            fields.append(field)
        return ArrowSchema.from_arrow(pa.schema(fields, metadata=arrow_schema.metadata))

    @classmethod
    def validate_schema(cls, schema: pa.Schema | ArrowSchema | IcebergSchema) -> None:
        """Validate required fields and the types used by the STAC representation."""
        if isinstance(schema, IcebergSchema):
            schema_fields = set(schema.column_names)
        else:
            schema_fields = set(schema.names)
        missing_fields = cls.get_required_fields() - schema_fields

        if missing_fields:
            raise ValueError(
                f"Schema is missing required STAC fields: {sorted(missing_fields)}"
            )

        if isinstance(schema, IcebergSchema):
            fields = {field.name: field.field_type for field in schema.fields}
            expected = {
                "id": StringType,
                "collection": StringType,
                "geometry": BinaryType,
                "datetime": (TimestampType, TimestamptzType),
                "links": ListType,
                "assets": StructType,
            }
            invalid = [
                name
                for name, field_type in expected.items()
                if not isinstance(fields[name], field_type)
            ]
        else:
            arrow_schema = pa.schema(schema)
            invalid = []
            for name, predicate in (
                ("id", pa.types.is_string),
                ("collection", pa.types.is_string),
                ("geometry", pa.types.is_binary),
                ("datetime", pa.types.is_timestamp),
                ("links", pa.types.is_list),
                ("assets", pa.types.is_struct),
            ):
                field_type = arrow_schema.field(name).type
                if name in ("id", "collection") and pa.types.is_dictionary(field_type):
                    field_type = field_type.value_type
                if not predicate(field_type):
                    invalid.append(name)

        if invalid:
            raise ValueError(
                "Unsupported types for STAC fields: " + ", ".join(sorted(invalid))
            )


def prepare_arrow_table(items: ItemsInput) -> pa.Table:
    """Check an Arrow table's structural schema and normalize required fields."""
    if isinstance(items, ArrowTable):
        table = pa.table(items)
    elif isinstance(items, pa.Table):
        table = items
    else:
        raise TypeError("items must be a pyarrow.Table or arro3.core.Table")
    if len(table) == 0:
        raise ValueError(EMPTY_ITEMS_ERROR)

    IcestacItem.validate_schema(table.schema)
    schema = IcestacItem.enforce_required_fields(ArrowSchema.from_arrow(table.schema))
    return table.cast(pa.schema(schema))


def get_schema_from_items(items: ItemsInput) -> IcebergSchema:
    """Derive an Iceberg schema from a structurally valid Arrow table."""
    arrow_table = prepare_arrow_table(items)
    schema_without_ids = _pyarrow_to_schema_without_ids(arrow_table.schema)
    return assign_fresh_schema_ids(schema_without_ids)
