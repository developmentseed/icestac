import hashlib
import re

import pyarrow
from arro3.core import Schema as ArrowSchema
from pyiceberg.catalog import Catalog
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import NestedField

from icestac.constants import DEFAULT_NAMESPACE
from icestac.schema import enforce_required_fields, validate_schema


def sanitize_collection_id(collection_id: str) -> str:
    """
    Sanitize a STAC collection ID to a valid Iceberg table name.

    Creates a deterministic, unique table identifier by:
    1. Converting to lowercase
    2. Replacing non-alphanumeric characters with underscores
    3. Collapsing consecutive underscores
    4. Ensuring it starts with a letter or underscore
    5. Appending an 8-character hash suffix to guarantee uniqueness

    This prevents collisions where different collection IDs might otherwise
    map to the same table name (e.g., "my.collection" vs "my_collection").

    Args:
        collection_id: STAC collection identifier

    Returns:
        Sanitized table name that is valid for Iceberg and guaranteed unique

    Examples:
        >>> sanitize_collection_id("sentinel-2-l2a")
        'sentinel_2_l2a_a1b2c3d4'
        >>> sanitize_collection_id("my.collection")
        'my_collection_e5f6g7h8'
        >>> sanitize_collection_id("my_collection")
        'my_collection_i9j0k1l2'
    """
    # Convert to lowercase and replace non-alphanumeric chars with underscores
    sanitized = re.sub(r"[^a-z0-9_]", "_", collection_id.lower())
    sanitized = re.sub(r"_+", "_", sanitized)
    sanitized = sanitized.strip("_")

    if sanitized and sanitized[0].isdigit():
        sanitized = f"c_{sanitized}"

    # Generate a short hash of the original collection_id for uniqueness
    hash_suffix = hashlib.sha256(collection_id.encode()).hexdigest()[:8]

    # Combine sanitized name with hash suffix
    return f"{sanitized}_{hash_suffix}"


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

    table_id = f"{namespace}.{sanitize_collection_id(collection_id)}"

    iceberg_schema = IcebergSchema(*fields)

    catalog.create_namespace_if_not_exists(namespace)

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
