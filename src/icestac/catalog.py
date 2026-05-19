from __future__ import annotations

from dataclasses import dataclass

from arro3.core import Schema as ArrowSchema
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform

from icestac.constants import DEFAULT_NAMESPACE
from icestac.errors import InvalidCollectionIdError
from icestac.load import Method, load_items
from icestac.schema import IcestacItem, ItemsInput, convert_schema


def validate_collection_id(collection_id: str) -> None:
    """Ensure collection id is valid for icestac schema"""

    if "." in collection_id:
        raise InvalidCollectionIdError(collection_id)


@dataclass
class IcestacCatalog:
    """Icestac client class for pyiceberg Catalog"""

    catalog: Catalog
    namespace: str = DEFAULT_NAMESPACE

    def __post_init__(self) -> None:
        self.catalog.create_namespace_if_not_exists(self.namespace)

    def create_item_table(
        self,
        collection_id: str,
        arrow_schema: ArrowSchema,
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
        IcestacItem.validate_schema(arrow_schema)

        # TODO: check if collection record is present in collections table

        iceberg_schema = convert_schema(arrow_schema)

        return self.catalog.create_table(
            identifier=f"{self.namespace}.{collection_id}",
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

    def load_items(
        self, collection_id: str, items: ItemsInput, method: Method = "upsert"
    ) -> None:
        load_items(
            items,
            table=self.catalog.load_table(
                identifier=f"{self.namespace}.{collection_id}"
            ),
            method=method,
        )
