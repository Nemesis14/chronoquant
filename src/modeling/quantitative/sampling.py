# =============================================================================
# Shared modeling sample and CV split definitions
# =============================================================================
# Purpose:
#  - Generate deterministic time-based train/validation/test splits
#  - Persist split definitions so multiple model families use identical samples
# =============================================================================

import json
from pathlib import Path

import duckdb
import pandas as pd

import utils


# =============================================================================
# create_expanding_window_splits(...) -> dict
# =============================================================================
# Purpose:
#  - Create expanding train windows with fixed validation windows and final test
# =============================================================================
def create_expanding_window_splits(
    sample_id: str,
    data_start: str,
    data_end: str,
    target_horizon_minutes: int = 240,
    min_train_days: int = 730,
    valid_days: int = 180,
    step_days: int = 180,
    test_days: int = 365,
    embargo_minutes: int | None = None,
) -> dict:
    data_start_ts = pd.to_datetime(data_start)
    data_end_ts = pd.to_datetime(data_end)
    embargo_minutes = target_horizon_minutes if embargo_minutes is None else embargo_minutes

    if data_end_ts <= data_start_ts:
        raise ValueError("data_end must be after data_start")

    embargo = pd.Timedelta(minutes=embargo_minutes)
    one_minute = pd.Timedelta(minutes=1)
    test_start = data_end_ts - pd.Timedelta(days=test_days)
    cv_end = test_start - embargo
    valid_start = data_start_ts + pd.Timedelta(days=min_train_days)

    folds = []
    fold_no = 1
    while valid_start + pd.Timedelta(days=valid_days) <= cv_end:
        valid_end = valid_start + pd.Timedelta(days=valid_days) - one_minute
        train_end = valid_start - embargo - one_minute

        if train_end > data_start_ts:
            folds.append(
                {
                    "fold": fold_no,
                    "train_start": _format_time(data_start_ts),
                    "train_end": _format_time(train_end),
                    "valid_start": _format_time(valid_start),
                    "valid_end": _format_time(valid_end),
                }
            )
            fold_no += 1

        valid_start = valid_start + pd.Timedelta(days=step_days)

    if not folds:
        raise ValueError("No folds generated. Reduce min_train_days, valid_days, or test_days.")

    return {
        "sample_id": sample_id,
        "split_type": "expanding_window",
        "target_horizon_minutes": target_horizon_minutes,
        "embargo_minutes": embargo_minutes,
        "data": {
            "start": _format_time(data_start_ts),
            "end": _format_time(data_end_ts),
        },
        "parameters": {
            "min_train_days": min_train_days,
            "valid_days": valid_days,
            "step_days": step_days,
            "test_days": test_days,
        },
        "folds": folds,
        "test": {
            "start": _format_time(test_start),
            "end": _format_time(data_end_ts),
        },
    }


# =============================================================================
# create_sample_definition_from_db(...) -> dict
# =============================================================================
# Purpose:
#  - Query the features table time range and create a sample definition
# =============================================================================
def create_sample_definition_from_db(
    sample_id: str,
    asset_id: str | None = None,
    target_horizon_minutes: int = 240,
    min_train_days: int = 730,
    valid_days: int = 180,
    step_days: int = 180,
    test_days: int = 365,
) -> dict:
    db_cfg = utils.load_asset_config(asset_id)["database"]
    data_dir = db_cfg["data_dir"]
    data_start, data_end = features_time_range(data_dir)

    sample = create_expanding_window_splits(
        sample_id=sample_id,
        data_start=data_start,
        data_end=data_end,
        target_horizon_minutes=target_horizon_minutes,
        min_train_days=min_train_days,
        valid_days=valid_days,
        step_days=step_days,
        test_days=test_days,
    )
    sample["source"] = {
        "asset_id": db_cfg.get("asset_id"),
        "data_dir": data_dir,
        "dataset": "features",
    }
    return sample


# =============================================================================
# features_time_range(data_dir: str) -> tuple[str, str]
# =============================================================================
# Purpose:
#  - Return min/max open_time for the features Parquet dataset
# =============================================================================
def features_time_range(data_dir: str) -> tuple[str, str]:
    glob_path = str(Path(data_dir) / "features" / "*.parquet")
    parts = list(Path(data_dir, "features").glob("*.parquet"))
    if not parts:
        raise ValueError(f"No features partitions found: {data_dir}")

    conn = duckdb.connect()
    try:
        row = conn.execute(
            f"SELECT MIN(open_time), MAX(open_time) FROM read_parquet('{glob_path}', union_by_name=true)"
        ).fetchone()
    finally:
        conn.close()

    if row is None or row[0] is None or row[1] is None:
        raise ValueError(f"Features dataset has no open_time range: {data_dir}")

    return str(row[0]), str(row[1])


# =============================================================================
# save_sample_definition(sample: dict, output_dir: str | Path) -> None
# =============================================================================
# Purpose:
#  - Save fold definitions and metadata in a reusable sample directory
# =============================================================================
def save_sample_definition(sample: dict, output_dir: str | Path) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    folds_payload = {
        "sample_id": sample["sample_id"],
        "split_type": sample["split_type"],
        "target_horizon_minutes": sample["target_horizon_minutes"],
        "embargo_minutes": sample["embargo_minutes"],
        "folds": sample["folds"],
        "test": sample["test"],
    }
    metadata_payload = {
        key: value
        for key, value in sample.items()
        if key not in {"folds", "test"}
    }
    metadata_payload["n_folds"] = len(sample["folds"])

    _write_json(output_dir / "folds.json", folds_payload)
    _write_json(output_dir / "metadata.json", metadata_payload)


# =============================================================================
# load_sample_definition(sample_dir: str | Path) -> dict
# =============================================================================
# Purpose:
#  - Load persisted folds and metadata into one sample definition dict
# =============================================================================
def load_sample_definition(sample_dir: str | Path) -> dict:
    sample_dir = Path(sample_dir)
    metadata = json.loads((sample_dir / "metadata.json").read_text(encoding="utf-8"))
    folds = json.loads((sample_dir / "folds.json").read_text(encoding="utf-8"))
    return {
        **metadata,
        "folds": folds["folds"],
        "test": folds["test"],
    }


# =============================================================================
# validate_sample_definition(sample: dict) -> None
# =============================================================================
# Purpose:
#  - Validate chronological order and non-overlap of folds/test ranges
# =============================================================================
def validate_sample_definition(sample: dict) -> None:
    test_start = pd.to_datetime(sample["test"]["start"])
    test_end = pd.to_datetime(sample["test"]["end"])

    if test_end <= test_start:
        raise ValueError("Test end must be after test start")

    for fold in sample["folds"]:
        train_start = pd.to_datetime(fold["train_start"])
        train_end = pd.to_datetime(fold["train_end"])
        valid_start = pd.to_datetime(fold["valid_start"])
        valid_end = pd.to_datetime(fold["valid_end"])

        if not train_start < train_end < valid_start < valid_end < test_start:
            raise ValueError(f"Invalid or overlapping fold: {fold}")


# =============================================================================
# _write_json(path: Path, payload: dict) -> None
# =============================================================================
# Purpose:
#  - Write stable, readable JSON
# =============================================================================
def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=4), encoding="utf-8")


# =============================================================================
# _format_time(ts: pd.Timestamp) -> str
# =============================================================================
# Purpose:
#  - Format timestamps in project DB format
# =============================================================================
def _format_time(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")
