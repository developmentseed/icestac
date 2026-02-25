import pyarrow
from arro3.core import Schema as ArrowSchema
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchNamespaceError
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import NestedField

from icestac.constants import DEFAULT_NAMESPACE
from icestac.errors import InvalidCollectionIdError
from icestac.schema import enforce_required_fields, validate_schema


def create_item_table(
    arrow_schema: ArrowSchema,
    collection_id: str,
    catalog: Catalog,
    namespace: str = DEFAULT_NAMESPACE,
) -> Table:
    """
    Create an Iceberg table from a stac-geoparquet Arrow schema

    Converts the Arrow schema to an Iceberg schema with manually assigned field IDs,
    then creates or loads the Iceberg table partitioned by datetime month.

    Args:
        schema: arro3.core.Schema for the items in this collection
        collection_id: the collection id for the items in this table
        catalog: PyIceberg catalog instance
        namespace: Namespace for the Iceberg table

    Returns:
        PyIceberg Table instance

    """
    validate_schema(arrow_schema)

    # Ensure required STAC fields are marked as non-nullable
    pa_schema = pyarrow.schema(enforce_required_fields(arrow_schema))
    _schema = _pyarrow_to_schema_without_ids(pa_schema)

    # assign iceberg field ids manually
    fields = []
    for i, _field in enumerate(_schema.fields, start=1):
        field_dict = _field.model_dump()
        field_dict["id"] = i
        fields.append(NestedField(**field_dict))

    table_id = f"{namespace}.{collection_id}"

    iceberg_schema = IcebergSchema(*fields)

    catalog.create_namespace_if_not_exists(namespace)

    try:
        return catalog.create_table_if_not_exists(
            identifier=table_id,
            schema=iceberg_schema,
            partition_spec=PartitionSpec(
                # TODO: make temporal partitioning configurable
                PartitionField(
                    source_id=iceberg_schema.find_field("datetime").field_id,
                    field_id=1000,
                    transform=MonthTransform(),
                    name="datetime_month",
                )
            ),
        )
    except NoSuchNamespaceError as e:
        if "." in collection_id:
            raise InvalidCollectionIdError(
                f"{collection_id} contains a '.' character which is not allowed"
            )
        else:
            raise e
