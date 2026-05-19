import gc
import tempfile
from pathlib import Path
from typing import Any, Generator

import pytest
import rustac
from arro3.core import Table as ArrowTable
from pyiceberg.catalog import load_catalog

from icestac.catalog import IcestacCatalog
from icestac.schema import ItemsInput


@pytest.fixture
def temp_warehouse():
    """Create a temporary warehouse directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_catalog(temp_warehouse: Path) -> Generator[IcestacCatalog, None, None]:
    """Create a temporary SQL catalog for testing."""
    catalog = load_catalog(
        "test_catalog",
        **{
            "type": "sql",
            "uri": f"sqlite:///{temp_warehouse}/catalog.db",
            "warehouse": str(temp_warehouse),
        },
    )
    icestac_catalog = IcestacCatalog(catalog=catalog)

    yield icestac_catalog

    gc.collect()
    icestac_catalog.catalog.close()


@pytest.fixture
def test_collection_id():
    """Provide a test collection ID."""
    return "test-collection"


@pytest.fixture
def sample_stac_item():
    """Provide a sample STAC item for testing."""
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "test-item-001",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-180.0, -90.0],
                    [180.0, -90.0],
                    [180.0, 90.0],
                    [-180.0, 90.0],
                    [-180.0, -90.0],
                ]
            ],
        },
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "properties": {
            "datetime": "2024-01-01T00:00:00Z",
            "title": "Test Item",
        },
        "collection": "test-collection",
        "links": [],
        "assets": {
            "data": {"href": "https://example.com/data.tif", "type": "image/tiff"}
        },
    }


@pytest.fixture
def sample_stac_items(sample_stac_item) -> list[dict[str, Any]]:
    """Provide a list of sample STAC items for testing."""
    items = []
    for i in range(3):
        item = sample_stac_item.copy()
        item["id"] = f"test-item-{i:03d}"
        item["properties"] = sample_stac_item["properties"].copy()
        item["properties"]["datetime"] = f"2024-01-{i + 1:02d}T00:00:00Z"
        items.append(item)

    return items


@pytest.fixture
def sample_stac_item_arrow_table(sample_stac_items) -> ArrowTable:
    return rustac.to_arrow(sample_stac_items)


@pytest.fixture
def sample_item_arrow_table(sample_stac_item_arrow_table: ArrowTable) -> ArrowTable:
    """Backward-compatible alias for the Arrow-backed STAC items fixture."""
    return sample_stac_item_arrow_table


@pytest.fixture(
    params=[
        pytest.param("sample_stac_item", id="single-item"),
        pytest.param("sample_stac_items", id="item-list"),
        pytest.param("sample_stac_item_arrow_table", id="arrow-table"),
    ]
)
def items(request) -> ItemsInput:
    return request.getfixturevalue(request.param)
