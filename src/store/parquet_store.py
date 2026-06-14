"""Write layer for Parquet partitions.

All datasets now use hourly Hive-style partitions (year/month/day/hour/part.parquet):
  - open_time rows are immutable events; no business upsert is performed.
  - Only rows past the stored maximum open_time per bucket are appended.

ohlcv: raw immutable; write_ohlcv_hive / write_hive(dataset="ohlcv").
features: derived, append-only live sync; rebuild on schema/logic change.
predictions: derived, append-only live sync; rebuild on model change.
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


# %% OHLCV Hive write (KAN-35)
# Layout: <data_dir>/ohlcv/year=YYYY/month=MM/day=DD/hour=HH/part.parquet
# OHLCV rows are immutable: open_time is a unique event key from Binance.
# No upsert is performed — only rows strictly past the stored max are appended.


def _ohlcv_hive_path(data_dir: str, hour_ts: pd.Timestamp) -> Path:
    """Return the part.parquet path for a given hour bucket."""
    return (
        Path(data_dir) / "ohlcv"
        / f"year={hour_ts.year}"
        / f"month={hour_ts.month:02d}"
        / f"day={hour_ts.day:02d}"
        / f"hour={hour_ts.hour:02d}"
        / "part.parquet"
    )


def write_ohlcv_hive(data_dir: str, df: pd.DataFrame) -> int:
    """Append new OHLCV rows into hourly Hive-style partitions.

    Rows with open_time <= the stored maximum for an hour bucket are silently
    skipped; the caller should already request only new rows from Binance.

    Args:
        data_dir : Root data directory for the asset.
        df       : DataFrame with open_time column (1m OHLCV rows).

    Returns:
        Total number of new rows appended (not total rows in files).
    """
    if df.empty:
        return 0

    df = df.copy()
    df["open_time"] = pd.to_datetime(df["open_time"])
    df = (
        df.drop_duplicates(subset=["open_time"], keep="last")
        .sort_values("open_time")
        .reset_index(drop=True)
    )

    df["_hour"] = df["open_time"].dt.floor("h")
    total_appended = 0

    for _hour_key, hour_df in df.groupby("_hour"):
        hour_ts = pd.Timestamp(str(_hour_key))
        if hour_ts is pd.NaT:
            continue
        hour_df = hour_df.drop(columns=["_hour"]).copy()
        path = _ohlcv_hive_path(data_dir, hour_ts)  # type: ignore[arg-type]
        path.parent.mkdir(parents=True, exist_ok=True)

        with partition_write_lock(path):
            if path.exists():
                try:
                    existing = pd.read_parquet(path)
                except Exception:
                    logger.exception("OHLCV Hive particio olvasas sikertelen: %s", path)
                    raise
                existing["open_time"] = pd.to_datetime(existing["open_time"])
                stored_max = existing["open_time"].max()
                new_rows: pd.DataFrame = hour_df.loc[hour_df["open_time"] > stored_max].copy()
                if new_rows.empty:
                    logger.debug(
                        "OHLCV Hive: nincs uj sor, hora=%s stored_max=%s", hour_ts, stored_max
                    )
                    continue
                merged = pd.concat([existing, new_rows], ignore_index=True)
                assert isinstance(merged, pd.DataFrame)
                merged = merged.sort_values("open_time").reset_index(drop=True)
                appended = len(new_rows)
            else:
                merged = hour_df.sort_values("open_time").reset_index(drop=True)
                appended = len(merged)

            if len(merged) > 60:
                logger.warning(
                    "OHLCV Hive: 60-nal tobb sor az oras bucketben: %d, hora=%s",
                    len(merged), hour_ts,
                )

            merged["open_time"] = merged["open_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            try:
                merged.to_parquet(path, index=False)
            except Exception:
                logger.exception("OHLCV Hive particio iras sikertelen: %s", path)
                raise
            total_appended += appended
            logger.debug(
                "OHLCV Hive irva: hora=%s, uj_sorok=%d, ossz=%d", hour_ts, appended, len(merged)
            )

    return total_appended


# %% Generic Hive write (KAN-54)
# Layout: <data_dir>/<dataset>/year=YYYY/month=MM/day=DD/hour=HH/part.parquet
# Applies to features and predictions (same append-only contract as OHLCV).


def _hive_path(data_dir: str, dataset: str, hour_ts: pd.Timestamp) -> Path:
    """Return the part.parquet path for a given dataset and hour bucket."""
    return (
        Path(data_dir) / dataset
        / f"year={hour_ts.year}"
        / f"month={hour_ts.month:02d}"
        / f"day={hour_ts.day:02d}"
        / f"hour={hour_ts.hour:02d}"
        / "part.parquet"
    )


def write_hive(data_dir: str, dataset: str, df: pd.DataFrame) -> int:
    """Append new rows into hourly Hive-style partitions for any dataset.

    Only rows with open_time strictly greater than the stored maximum for the
    hour bucket are written. Older or duplicate open_time rows are skipped
    and logged; they never overwrite stored data.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory: 'features' or 'predictions'.
        df       : DataFrame with open_time column.

    Returns:
        Total number of new rows appended.
    """
    if df.empty:
        return 0

    df = df.copy()
    df["open_time"] = pd.to_datetime(df["open_time"])

    duped = df["open_time"].duplicated(keep=False)
    if duped.any():
        dup_times = df.loc[duped, "open_time"].unique().tolist()
        raise ValueError(
            f"write_hive: duplicate open_time in input batch dataset={dataset} "
            f"times={dup_times[:5]}"
        )

    df = df.sort_values("open_time").reset_index(drop=True)

    df["_hour"] = df["open_time"].dt.floor("h")
    total_appended = 0

    for _hour_key, hour_df in df.groupby("_hour"):
        hour_ts = pd.Timestamp(str(_hour_key))
        if hour_ts is pd.NaT:
            continue
        hour_df = hour_df.drop(columns=["_hour"]).copy()
        path = _hive_path(data_dir, dataset, hour_ts)  # type: ignore[arg-type]
        path.parent.mkdir(parents=True, exist_ok=True)

        with partition_write_lock(path):
            if path.exists():
                try:
                    existing = pd.read_parquet(path)
                except Exception:
                    logger.exception("Hive particio olvasas sikertelen: %s", path)
                    raise
                existing["open_time"] = pd.to_datetime(existing["open_time"])
                stored_max = existing["open_time"].max()
                new_rows: pd.DataFrame = hour_df.loc[hour_df["open_time"] > stored_max].copy()
                skipped = len(hour_df) - len(new_rows)
                if skipped:
                    logger.debug(
                        "Hive append: %d sor kihagyva (open_time <= stored_max) dataset=%s ora=%s",
                        skipped, dataset, hour_ts,
                    )
                if new_rows.empty:
                    continue
                merged = pd.concat([existing, new_rows], ignore_index=True)
                assert isinstance(merged, pd.DataFrame)
                merged = merged.sort_values("open_time").reset_index(drop=True)
                appended = len(new_rows)
            else:
                merged = hour_df.sort_values("open_time").reset_index(drop=True)
                appended = len(merged)

            merged["open_time"] = merged["open_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            try:
                merged.to_parquet(path, index=False)
            except Exception:
                logger.exception("Hive particio iras sikertelen: %s", path)
                raise
            total_appended += appended
            logger.debug(
                "Hive irva: dataset=%s ora=%s uj_sorok=%d ossz=%d",
                dataset, hour_ts, appended, len(merged),
            )

    return total_appended


def hive_latest_open_time(data_dir: str, dataset: str) -> pd.Timestamp | None:
    """Return the latest open_time stored in the Hive layout for a dataset.

    Scans Hive part.parquet files sorted lexicographically to find the most
    recent hour bucket. Returns None if no Hive files exist yet.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory: 'features' or 'predictions'.

    Returns:
        Latest open_time as Timestamp, or None.
    """
    dataset_dir = Path(data_dir) / dataset
    parts = sorted(dataset_dir.rglob("part.parquet"))
    if not parts:
        return None

    last = parts[-1]
    try:
        df = pd.read_parquet(last, columns=["open_time"])
    except Exception:
        logger.exception("Hive latest open_time olvasas sikertelen: %s", last)
        return None

    if df.empty:
        return None

    df["open_time"] = pd.to_datetime(df["open_time"])
    max_val = df["open_time"].max()  # type: ignore[assignment]
    if max_val is None or max_val is pd.NaT:
        return None
    return pd.Timestamp(str(max_val))  # type: ignore[return-value]


def drop_hive_partitions(data_dir: str, dataset: str) -> int:
    """Delete all Hive part.parquet files (and empty parent dirs) for a dataset.

    Used before a full rebuild to remove the existing Hive tree.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory to clear.

    Returns:
        Number of part.parquet files deleted.
    """
    dataset_dir = Path(data_dir) / dataset
    if not dataset_dir.exists():
        return 0
    parts = list(dataset_dir.rglob("part.parquet"))
    for p in parts:
        p.unlink()
        try:
            p.parent.rmdir()
            p.parent.parent.rmdir()
            p.parent.parent.parent.rmdir()
            p.parent.parent.parent.parent.rmdir()
        except OSError:
            pass
    return len(parts)


def ohlcv_hive_latest_open_time(data_dir: str) -> pd.Timestamp | None:
    """Return the latest open_time stored in the Hive OHLCV layout.

    Scans Hive part.parquet files sorted lexicographically to find the most
    recent hour bucket. Returns None if no Hive files exist yet.

    Args:
        data_dir : Root data directory for the asset.

    Returns:
        Latest open_time as Timestamp, or None.
    """
    ohlcv_dir = Path(data_dir) / "ohlcv"
    parts = sorted(ohlcv_dir.rglob("part.parquet"))
    if not parts:
        return None

    last = parts[-1]
    try:
        df = pd.read_parquet(last, columns=["open_time"])
    except Exception:
        logger.exception("OHLCV Hive latest open_time olvasas sikertelen: %s", last)
        return None

    if df.empty:
        return None

    df["open_time"] = pd.to_datetime(df["open_time"])
    max_val = df["open_time"].max()  # type: ignore[assignment]
    if max_val is None or max_val is pd.NaT:
        return None
    return pd.Timestamp(str(max_val))  # type: ignore[return-value]


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
