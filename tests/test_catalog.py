from typing import Any

import pyarrow
import pytest
from arro3.core import Schema as ArrowSchema
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform, MonthTransform
from icestac.catalog import IcestacCatalog
from icestac.errors import InvalidCollectionIdError
from icestac.schema import IcestacItem, get_schema_from_items
from tests.helpers import items_to_arrow, items_to_list


def test_catalog_constructor_does_not_create_namespace(
    test_catalog: IcestacCatalog,
) -> None:
    """Constructing a catalog wrapper does not mutate a read-only backend."""
    assert not test_catalog.catalog.namespace_exists(test_catalog.namespace)


def test_create_item_table(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    items: pyarrow.Table,
) -> None:
    expected_items = items_to_list(items)
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
        collection_id=test_collection_id,
    )

    assert table.schema().find_field("datetime")
    assert len(table.spec().fields) == 1
    assert isinstance(table.spec().fields[0].transform, MonthTransform)
    assert not table.sort_order().fields

    # Ensure data has required fields marked as non-nullable to match table schema
    enforced_schema = IcestacItem.enforce_required_fields(
        ArrowSchema.from_arrow(items.schema)
    )
    arrow_table = items.cast(pyarrow.schema(enforced_schema))

    table.upsert(
        df=arrow_table,
        join_cols=["id"],
    )

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)
    assert result.column("id").to_pylist() == [item["id"] for item in expected_items]

    with pytest.raises(TableAlreadyExistsError):
        test_catalog.create_item_table(
            iceberg_schema=iceberg_schema,
            collection_id=test_collection_id,
        )


def test_create_item_table_bad_collection_id(
    test_catalog: IcestacCatalog,
    items: pyarrow.Table,
) -> None:
    iceberg_schema = get_schema_from_items(items)
    with pytest.raises(InvalidCollectionIdError):
        test_catalog.create_item_table(
            iceberg_schema=iceberg_schema,
            collection_id="bad.collection",
        )


def test_create_item_table_custom_layout(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    sample_stac_item: dict[str, Any],
) -> None:
    items = items_to_arrow([sample_stac_item])
    iceberg_schema = get_schema_from_items(items)
    title_id = iceberg_schema.find_field("title").field_id

    table = test_catalog.create_item_table(
        collection_id=test_collection_id,
        iceberg_schema=iceberg_schema,
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=title_id,
                field_id=1000,
                transform=IdentityTransform(),
                name="title",
            )
        ),
        sort_order=SortOrder(SortField(source_id=title_id)),
    )

    assert [field.name for field in table.spec().fields] == ["title"]
    assert isinstance(table.spec().fields[0].transform, IdentityTransform)
    assert (
        table.spec().fields[0].source_id == table.schema().find_field("title").field_id
    )
    assert (
        table.sort_order().fields[0].source_id
        == table.schema().find_field("title").field_id
    )


def test_create_item_table_nested_layout(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    sample_stac_item: dict[str, Any],
) -> None:
    items = items_to_arrow([sample_stac_item])
    iceberg_schema = get_schema_from_items(items)
    xmin_id = iceberg_schema.find_field("bbox.xmin").field_id

    table = test_catalog.create_item_table(
        collection_id=test_collection_id,
        iceberg_schema=iceberg_schema,
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=xmin_id,
                field_id=1000,
                transform=IdentityTransform(),
                name="bbox_xmin",
            )
        ),
        sort_order=SortOrder(SortField(source_id=xmin_id)),
    )

    table_xmin_id = table.schema().find_field("bbox.xmin").field_id
    table_ymax_id = table.schema().find_field("bbox.ymax").field_id
    assert table_xmin_id != table_ymax_id
    assert table.spec().fields[0].source_id == table_xmin_id
    assert table.sort_order().fields[0].source_id == table_xmin_id


def test_create_item_table_unpartitioned(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    sample_stac_item: dict[str, Any],
) -> None:
    items = items_to_arrow([sample_stac_item])
    table = test_catalog.create_item_table(
        collection_id=test_collection_id,
        iceberg_schema=get_schema_from_items(items),
        partition_spec=PartitionSpec(),
    )

    assert not table.spec().fields


def test_create_item_table_rejects_invalid_schema(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    sample_stac_item: dict[str, Any],
) -> None:
    items = items_to_arrow([sample_stac_item])
    iceberg_schema = get_schema_from_items(items)
    missing_id = IcebergSchema(
        *(field for field in iceberg_schema.fields if field.name != "id")
    )

    with pytest.raises(ValueError, match="missing required STAC fields.*'id'"):
        test_catalog.create_item_table(
            collection_id=test_collection_id,
            iceberg_schema=missing_id,
        )


def test_create_item_table_rejects_unknown_partition_source(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    sample_stac_item: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        test_catalog.create_item_table(
            collection_id=test_collection_id,
            iceberg_schema=get_schema_from_items(items_to_arrow([sample_stac_item])),
            partition_spec=PartitionSpec(
                PartitionField(
                    source_id=9999,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="missing",
                )
            ),
        )


def test_create_item_table_rejects_unknown_sort_source(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    sample_stac_item: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        test_catalog.create_item_table(
            collection_id=test_collection_id,
            iceberg_schema=get_schema_from_items(items_to_arrow([sample_stac_item])),
            sort_order=SortOrder(SortField(source_id=9999)),
        )


def test_load_items_rejects_a_different_collection(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    sample_stac_item: dict[str, Any],
) -> None:
    items = items_to_arrow([sample_stac_item])
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
        collection_id=test_collection_id,
    )
    different_collection = items.set_column(
        items.schema.get_field_index("collection"),
        "collection",
        pyarrow.array(["different-collection"] * len(items)),
    )

    with pytest.raises(ValueError, match="different-collection"):
        test_catalog.load_items(test_collection_id, different_collection)

    assert len(table.scan().to_arrow()) == 0


def test_load_items(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    items: pyarrow.Table,
) -> None:
    expected_items = items_to_list(items)
    iceberg_schema = get_schema_from_items(items)
    table = test_catalog.create_item_table(
        iceberg_schema=iceberg_schema,
        collection_id=test_collection_id,
    )

    test_catalog.load_items(
        collection_id=test_collection_id,
        items=items,
        method="upsert",
    )

    table.refresh()

    # Verify records were inserted
    result = table.scan().to_arrow()
    assert len(result) == len(expected_items)
    assert result.column("id").to_pylist() == [item["id"] for item in expected_items]


def test_catalog_load_items_evolves_schema(
    test_catalog: IcestacCatalog,
    test_collection_id: str,
    sample_stac_item: dict[str, Any],
) -> None:
    initial = items_to_arrow([sample_stac_item])
    table = test_catalog.create_item_table(
        iceberg_schema=get_schema_from_items(initial),
        collection_id=test_collection_id,
    )
    evolved_item = {
        **sample_stac_item,
        "properties": {**sample_stac_item["properties"], "new_field": True},
    }

    test_catalog.load_items(
        collection_id=test_collection_id,
        items=items_to_arrow([evolved_item]),
        evolve_schema=True,
    )

    table.refresh()
    assert table.schema().find_field("new_field")
