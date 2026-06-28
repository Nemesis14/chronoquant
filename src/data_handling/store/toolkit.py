"""Dataset inspection helpers for notebook and script use.

Thin wrappers over store.duckdb_query and utils — returns row counts,
column lists, and time ranges for any DuckDB dataset.
"""

from typing import cast

import pandas as pd

import utils
from data_handling.store.duckdb_query import dataset_columns, dataset_exists, query_range, row_count

# =============================================================================
# CONFIG HELPERS
# =============================================================================

def resolve_db_path(asset_id: str | None = None) -> str:
    """Return the resolved .duckdb file path for an asset."""
    return utils.load_asset_config(asset_id)["database"]["db_path"]


# =============================================================================
# DATASET INSPECTION
# =============================================================================

def list_datasets(asset_id: str | None = None) -> list[str]:
    """Return dataset names that exist and have at least one row."""
    db_path = resolve_db_path(asset_id)
    return [
        ds for ds in ("ohlcv", "target", "feat_ohlcv_quant", "predictions")
        if dataset_exists(db_path, ds)
    ]


def get_dataset_columns(dataset: str, asset_id: str | None = None) -> list[str]:
    """Return column names for a dataset."""
    return dataset_columns(resolve_db_path(asset_id), dataset)


def get_row_count(dataset: str, asset_id: str | None = None) -> int:
    """Return total row count for a dataset."""
    return row_count(resolve_db_path(asset_id), dataset)


def get_time_range(dataset: str, asset_id: str | None = None) -> tuple[str | None, str | None]:
    """Return (min_open_time, max_open_time) for a dataset."""
    db_path = resolve_db_path(asset_id)
    df = cast(pd.DataFrame, query_range(db_path, dataset, columns=["open_time"]))
    if df.empty:
        return None, None
    return str(df["open_time"].min()), str(df["open_time"].max())


def print_summary(asset_id: str | None = None) -> None:
    """Print a summary of all datasets for an asset."""
    db_path = resolve_db_path(asset_id)
    print(f"DB path: {db_path}")
    print("-" * 60)
    for ds in ("ohlcv", "target", "feat_ohlcv_quant", "predictions"):
        if not dataset_exists(db_path, ds):
            print(f"{ds:20s}  (no data)")
            continue
        n      = row_count(db_path, ds)
        mn, mx = get_time_range(ds, asset_id)
        n_cols = len(dataset_columns(db_path, ds))
        print(f"{ds:20s}  rows={n:>8,}  cols={n_cols:>4}  {mn} -> {mx}")
