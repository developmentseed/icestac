import gc
import tempfile
from pathlib import Path
from typing import Any

import pyarrow
import pytest
from pyarrow import Table
from pyiceberg.catalog.sql import SqlCatalog
from rustac import to_arrow


@pytest.fixture
def temp_warehouse():
    """Create a temporary warehouse directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_catalog(temp_warehouse):
    """Create an in-memory SQL catalog for testing."""
    catalog = SqlCatalog(
        "test_catalog",
        **{
            "uri": f"sqlite:///{temp_warehouse}/catalog.db",
            "warehouse": f"file://{temp_warehouse}",
        },
    )

    # Create test namespace
    catalog.create_namespace("test_namespace")

    yield catalog

    gc.collect()
    catalog.close()


@pytest.fixture
def test_namespace():
    """Provide a test namespace name."""
    return "test_namespace"


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
def sample_item_arrow_table(sample_stac_items) -> Table:
    return pyarrow.table(to_arrow(sample_stac_items))
