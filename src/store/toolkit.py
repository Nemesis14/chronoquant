"""Dataset inspection helpers for notebook and script use.

Thin wrappers over store.duckdb_query and utils — returns row counts,
column lists, and time ranges for any Parquet dataset.
"""

from store.duckdb_query import dataset_columns, dataset_exists, query_range, row_count

import utils

# =============================================================================
# CONFIG HELPERS
# =============================================================================

def resolve_data_dir(asset_id: str | None = None) -> str:
    """Return the resolved data directory for an asset."""
    return utils.load_asset_config(asset_id)["database"]["data_dir"]


# =============================================================================
# DATASET INSPECTION
# =============================================================================

def list_datasets(asset_id: str | None = None) -> list[str]:
    """Return dataset names that have at least one Parquet partition."""
    data_dir = resolve_data_dir(asset_id)
    return [ds for ds in ("ohlcv", "features", "predictions") if dataset_exists(data_dir, ds)]


def get_dataset_columns(dataset: str, asset_id: str | None = None) -> list[str]:
    """Return column names for a dataset."""
    return dataset_columns(resolve_data_dir(asset_id), dataset)


def get_row_count(dataset: str, asset_id: str | None = None) -> int:
    """Return total row count across all partitions of a dataset."""
    return row_count(resolve_data_dir(asset_id), dataset)


def get_time_range(dataset: str, asset_id: str | None = None) -> tuple[str | None, str | None]:
    """Return (min_open_time, max_open_time) for a dataset."""
    data_dir = resolve_data_dir(asset_id)
    df = query_range(data_dir, dataset, columns=["open_time"])
    if df.empty:
        return None, None
    return str(df["open_time"].min()), str(df["open_time"].max())


def print_summary(asset_id: str | None = None) -> None:
    """Print a summary of all datasets for an asset."""
    data_dir = resolve_data_dir(asset_id)
    print(f"Data dir: {data_dir}")
    print("-" * 60)
    for ds in ("ohlcv", "features", "predictions"):
        if not dataset_exists(data_dir, ds):
            print(f"{ds:15s}  (no partitions)")
            continue
        n        = row_count(data_dir, ds)
        mn, mx   = get_time_range(ds, asset_id)
        n_cols   = len(dataset_columns(data_dir, ds))
        print(f"{ds:15s}  rows={n:>8,}  cols={n_cols:>4}  {mn} -> {mx}")
