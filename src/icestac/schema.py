import pyarrow as pa
from arro3.core import Schema as ArrowSchema
from arro3.core import Table as ArrowTable
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids, schema_to_pyarrow
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
REQUIRED_FIELDS = frozenset(
    {"geometry", "type", "id", "datetime", "links", "collection", "assets"}
)
NON_NULLABLE_FIELDS = frozenset({"type", "id", "links", "collection", "assets"})
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


def enforce_required_fields(schema: ArrowSchema) -> ArrowSchema:
    """Make required fields non-null and give empty links a concrete type."""
    arrow_schema = pa.schema(schema)
    fields = []
    for field in arrow_schema:
        if field.name in NON_NULLABLE_FIELDS:
            field = field.with_nullable(False)
        if (
            field.name == "links"
            and pa.types.is_list(field.type)
            and pa.types.is_null(field.type.value_type)
        ):
            field = field.with_type(LINK_TYPE)
        fields.append(field)
    return ArrowSchema.from_arrow(pa.schema(fields, metadata=arrow_schema.metadata))


def validate_schema(schema: pa.Schema | ArrowSchema | IcebergSchema) -> None:
    """Validate the structural fields and types of a flattened STAC schema."""
    if isinstance(schema, IcebergSchema):
        schema_fields = set(schema.column_names)
    else:
        schema_fields = set(schema.names)
    missing_fields = REQUIRED_FIELDS - schema_fields

    if missing_fields:
        raise ValueError(
            f"Schema is missing required STAC fields: {sorted(missing_fields)}"
        )

    if isinstance(schema, IcebergSchema):
        fields = {field.name: field.field_type for field in schema.fields}
        expected = {
            "type": StringType,
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
            ("type", pa.types.is_string),
            ("id", pa.types.is_string),
            ("collection", pa.types.is_string),
            ("geometry", pa.types.is_binary),
            ("datetime", pa.types.is_timestamp),
            ("links", pa.types.is_list),
            ("assets", pa.types.is_struct),
        ):
            field_type = arrow_schema.field(name).type
            if name in ("type", "id", "collection") and pa.types.is_dictionary(
                field_type
            ):
                field_type = field_type.value_type
            if not predicate(field_type):
                invalid.append(name)

    if invalid:
        raise ValueError(
            "Unsupported types for STAC fields: " + ", ".join(sorted(invalid))
        )


def _align_empty_links(items: pa.Table, schema: IcebergSchema) -> pa.Table:
    """Align empty links with the table's nested schema before writing."""
    if not all(not links for links in items["links"].to_pylist()):
        return items

    links_type = schema_to_pyarrow(schema, include_field_ids=False).field("links").type
    links = items["links"].cast(links_type)
    links_index = items.schema.get_field_index("links")
    links_field = (
        items.schema.field(links_index).with_type(links_type).with_nullable(False)
    )
    return items.set_column(links_index, links_field, links)


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

    validate_schema(table.schema)
    schema = enforce_required_fields(ArrowSchema.from_arrow(table.schema))
    return table.cast(pa.schema(schema))


def get_schema_from_items(items: ItemsInput) -> IcebergSchema:
    """Derive an Iceberg schema from a structurally valid Arrow table."""
    arrow_table = prepare_arrow_table(items)
    schema_without_ids = _pyarrow_to_schema_without_ids(arrow_table.schema)
    schema = assign_fresh_schema_ids(schema_without_ids)
    return IcebergSchema(
        *schema.fields,
        schema_id=schema.schema_id,
        identifier_field_ids=[schema.find_field("id").field_id],
    )
