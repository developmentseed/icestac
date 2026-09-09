import asyncio
from copy import deepcopy
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.table import TableProperties
from rustac import DuckdbClient, RustacError

import main
from tests.helpers import items_to_arrow


@pytest.fixture
def duckdb_client():
    """Use the installed spatial extension without downloading during tests."""
    try:
        return DuckdbClient(install_extensions=False)
    except RustacError as exc:
        pytest.skip(f"DuckDB extensions are not installed: {exc}")


def test_demo_loads_cached_batches_and_can_rerun(
    tmp_path, monkeypatch, test_catalog, sample_stac_item, duckdb_client
) -> None:
    """Load real Parquet through DuckDB and Iceberg, including schema evolution."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(main, "load_catalog", lambda: test_catalog.catalog)
    monkeypatch.setattr(main, "DuckdbClient", lambda: duckdb_client)
    for month in range(1, 9):
        batch = []
        for index, xmin in enumerate((120.0, -120.0, 0.0)):
            item = deepcopy(sample_stac_item)
            item["id"] = f"{month}-{index}"
            item["collection"] = "HLSS30_2.0"
            item["bbox"] = [xmin, 0.0, xmin + 1, 1.0]
            item["properties"]["datetime"] = f"2026-{month:02d}-01T00:00:00Z"
            if month >= 6:
                item["properties"]["new_field"] = True
            batch.append(item)
        path = Path(
            f"data/HLSS30_2.0/year=2026/month={month}/HLSS30_2.0-2026-{month}.parquet"
        )
        path.parent.mkdir(parents=True)
        pq.write_table(items_to_arrow(batch), path)
        prepared = main.read_hls_items(duckdb_client, path, "HLSS30_2_0")
        keys = prepared["hilbert_idx"].to_pylist()
        assert keys == sorted(keys)
        assert len(set(keys)) == 3
        assert pa.types.is_binary(prepared.schema.field("geometry").type)
        assert "year" not in prepared.column_names
        assert "month" not in prepared.column_names

    asyncio.run(main.run())
    asyncio.run(main.run())

    table = test_catalog.catalog.load_table(("icestac", "HLSS30_2_0"))
    result = table.scan().to_arrow()
    assert len(result) == 24
    assert len(result["id"].unique()) == 24
    assert result["collection"].unique().to_pylist() == ["HLSS30_2_0"]
    assert result["new_field"].null_count == 15
    assert table.properties[TableProperties.PARQUET_ROW_GROUP_LIMIT] == "50000"
    assert (
        table.sort_order().fields[0].source_id
        == table.schema().find_field("hilbert_idx").field_id
    )


@pytest.mark.parametrize("case", ["empty", "null_bbox", "nonfinite_bbox"])
def test_read_hls_items_rejects_invalid_batches(
    tmp_path, items, duckdb_client, case
) -> None:
    """Reject unusable batches before creating or writing an Iceberg table."""
    if case == "empty":
        items = items.slice(0, 0)
    else:
        bbox_type = items.schema.field("bbox").type
        value = (
            None
            if case == "null_bbox"
            else {"xmin": float("nan"), "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
        )
        items = items.set_column(
            items.schema.get_field_index("bbox"),
            "bbox",
            pa.array([value] * len(items), type=bbox_type),
        )
    path = tmp_path / "items.parquet"
    pq.write_table(items, path)

    with pytest.raises(ValueError, match="No items found|bbox coordinates"):
        main.read_hls_items(duckdb_client, path, "test-collection")
