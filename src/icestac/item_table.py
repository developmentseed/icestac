from arro3.core import Schema as ArrowSchema
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform

from icestac.constants import DEFAULT_NAMESPACE
from icestac.errors import InvalidCollectionIdError
from icestac.schema import convert_schema, validate_schema


def validate_collection_id(collection_id: str) -> None:
    """Ensure collection id is valid for icestac schema"""

    if "." in collection_id:
        raise InvalidCollectionIdError


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
    validate_collection_id(collection_id)
    validate_schema(arrow_schema)

    catalog.create_namespace_if_not_exists(namespace)

    # TODO: check if collection record is present in collections table

    iceberg_schema = convert_schema(arrow_schema)

    return catalog.create_table(
        identifier=f"{namespace}.{collection_id}",
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
