"""Write layer for daily Parquet partitions.

Provides atomic single-writer locking and idempotent upsert by open_time.
Each dataset (ohlcv, features, predictions) is stored as daily .parquet files
under a per-asset data directory.
"""

import logging
import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# %% Lock


@contextmanager
def partition_write_lock(parquet_path: Path) -> Generator[None, None, None]:
    """Acquire an exclusive write lock for a single partition file.

    Uses atomic O_CREAT|O_EXCL file creation — fails immediately if another
    writer holds the lock (no waiting, no retry).

    Args:
        parquet_path : Path to the .parquet file being written.

    Raises:
        RuntimeError: If the lock file already exists (another writer active).
    """
    lock_path = parquet_path.with_suffix(".lock")
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
    except FileExistsError:
        raise RuntimeError(f"Partition locked, another writer is active: {parquet_path.name}")
    logger.debug("Lock acquired: %s", parquet_path.name)
    try:
        yield
    finally:
        lock_path.unlink(missing_ok=True)
        logger.debug("Lock released: %s", parquet_path.name)


# %% Partition helpers


def _partition_path(data_dir: str, dataset: str, date_str: str) -> Path:
    """Return the .parquet path for a given dataset and date string (YYYY-MM-DD).

    Args:
        data_dir : Root data directory for the asset (e.g. data/solusdt_fw60).
        dataset  : Sub-directory name: 'ohlcv', 'features', or 'predictions'.
        date_str : Date string in YYYY-MM-DD format.

    Returns:
        Absolute Path to the partition file.
    """
    return Path(data_dir) / dataset / f"{date_str}.parquet"


def _date_str(open_time: pd.Timestamp) -> str:
    """Return YYYY-MM-DD string from a Timestamp."""
    return open_time.strftime("%Y-%m-%d")


# %% Write


def upsert_partition(data_dir: str, dataset: str, df: pd.DataFrame) -> int:
    """Write df rows into daily Parquet partitions, deduplicating by open_time.

    Groups df by calendar date (UTC), then for each date:
      1. Acquires an exclusive lock.
      2. Reads the existing partition (if any).
      3. Concatenates, deduplicates on open_time (keep last), sorts.
      4. Writes the merged result back.

    open_time column must be present and parseable as datetime.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory: 'ohlcv', 'features', or 'predictions'.
        df       : DataFrame with an open_time column.

    Returns:
        Total number of rows written across all partitions.
    """
    if df.empty:
        return 0

    df = df.copy()
    df["open_time"] = pd.to_datetime(df["open_time"])

    dataset_dir = Path(data_dir) / dataset
    dataset_dir.mkdir(parents=True, exist_ok=True)

    df["_date"] = df["open_time"].dt.strftime("%Y-%m-%d")
    total_written = 0

    for date_str, day_df in df.groupby("_date"):
        day_df = day_df.drop(columns=["_date"]).copy()
        path   = _partition_path(data_dir, dataset, str(date_str))

        with partition_write_lock(path):
            if path.exists():
                try:
                    existing = pd.read_parquet(path)
                except Exception:
                    logger.exception("Partition olvasas sikertelen: %s", path.name)
                    raise
                existing["open_time"] = pd.to_datetime(existing["open_time"])
                before   = len(existing)
                merged   = pd.concat([existing, day_df], ignore_index=True)
                merged   = (
                    merged
                    .drop_duplicates(subset=["open_time"], keep="last")
                    .sort_values("open_time")
                    .reset_index(drop=True)
                )
                dupes = before + len(day_df) - len(merged)
                if dupes:
                    logger.debug("Duplikaciok eltavolitva: %d sor, particio=%s", dupes, date_str)
            else:
                merged = (
                    day_df
                    .drop_duplicates(subset=["open_time"], keep="last")
                    .sort_values("open_time")
                    .reset_index(drop=True)
                )

            merged["open_time"] = merged["open_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            try:
                merged.to_parquet(path, index=False)
            except Exception:
                logger.exception("Partition iras sikertelen: %s", path.name)
                raise
            total_written += len(merged)
            logger.debug("Partition irva: %s, sorok=%d", date_str, len(merged))

    return total_written


# %% Read


def read_partition(data_dir: str, dataset: str, date_str: str) -> pd.DataFrame:
    """Read a single daily partition. Returns empty DataFrame if file missing.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory: 'ohlcv', 'features', or 'predictions'.
        date_str : Date in YYYY-MM-DD format.

    Returns:
        DataFrame with partition contents, or empty DataFrame.
    """
    path = _partition_path(data_dir, dataset, date_str)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def list_partitions(data_dir: str, dataset: str) -> list[str]:
    """Return sorted list of date strings for all partitions in a dataset.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory: 'ohlcv', 'features', or 'predictions'.

    Returns:
        Sorted list of YYYY-MM-DD strings.
    """
    dataset_dir = Path(data_dir) / dataset
    if not dataset_dir.exists():
        return []
    return sorted(
        p.stem for p in dataset_dir.glob("*.parquet")
    )


def drop_partitions(data_dir: str, dataset: str) -> int:
    """Delete all .parquet files for a dataset.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory to clear.

    Returns:
        Number of files deleted.
    """
    dataset_dir = Path(data_dir) / dataset
    if not dataset_dir.exists():
        return 0
    files = list(dataset_dir.glob("*.parquet"))
    for f in files:
        f.unlink()
    return len(files)


def drop_partitions_from(data_dir: str, dataset: str, from_date: str) -> int:
    """Delete partitions on or after from_date (YYYY-MM-DD).

    Args:
        data_dir  : Root data directory for the asset.
        dataset   : Sub-directory to partially clear.
        from_date : Inclusive lower bound in YYYY-MM-DD format.

    Returns:
        Number of files deleted.
    """
    dataset_dir = Path(data_dir) / dataset
    if not dataset_dir.exists():
        return 0
    deleted = 0
    for f in dataset_dir.glob("*.parquet"):
        if f.stem >= from_date:
            f.unlink()
            deleted += 1
    return deleted
