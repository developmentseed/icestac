from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.transforms import MonthTransform

from icestac.constants import DEFAULT_NAMESPACE
from icestac.errors import InvalidCollectionIdError
from icestac.load import Method, load_items
from icestac.schema import IcestacItem, ItemsInput


def validate_collection_id(collection_id: str) -> None:
    """Ensure a collection ID can be used as an Iceberg table name."""

    if "." in collection_id:
        raise InvalidCollectionIdError(collection_id)


@dataclass
class IcestacCatalog:
    """Manage collection item tables through a PyIceberg catalog."""

    catalog: Catalog
    namespace: str = DEFAULT_NAMESPACE

    def __post_init__(self) -> None:
        self.catalog.create_namespace_if_not_exists(self.namespace)

    def create_item_table(
        self,
        collection_id: str,
        iceberg_schema: IcebergSchema,
        partition_spec: PartitionSpec | None = None,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
    ) -> Table:
        """Create an Iceberg item table for a collection."""
        validate_collection_id(collection_id)
        IcestacItem.validate_schema(iceberg_schema)

        # TODO: check if collection record is present in collections table

        if partition_spec is None:
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=iceberg_schema.find_field("datetime").field_id,
                    field_id=1000,
                    transform=MonthTransform(),
                    name="datetime_month",
                )
            )

        return self.catalog.create_table(
            identifier=f"{self.namespace}.{collection_id}",
            schema=iceberg_schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
        )

    def load_items(
        self, collection_id: str, items: ItemsInput, method: Method = "upsert"
    ) -> None:
        """Load items into the table matching their collection ID."""
        load_items(
            items,
            table=self.catalog.load_table(
                identifier=f"{self.namespace}.{collection_id}"
            ),
            method=method,
        )
