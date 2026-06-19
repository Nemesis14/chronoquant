"""Sample artifact IO — write, load, and validate sample definition files.

Yearly format: metadata.json, audit.json, sample_train_valid.parquet (Polars).
  sample_train_valid.parquet : hourly-sampled rows, segments train/valid/purge
  sample_oos.parquet         : full-resolution OOS year predictions (written by 03_fit_model)
Legacy format (load/validate only): metadata.json + folds.json — kept for
backward compatibility with lightgbm_model.py and lgbm_search.py.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

# %% Yearly format — write


def write_yearly_artifacts(
    sample_dir : Path,
    metadata   : dict,
    segment_df : pl.DataFrame,
    audit      : dict,
) -> None:
    """Write metadata.json, audit.json, and sample_train_valid.parquet to sample_dir.

    Injects generated_at (UTC ISO string) into the metadata before writing.
    Creates sample_dir if it does not exist.

    Args:
        sample_dir : Target directory (created if absent).
        metadata   : Sample metadata dict (sample_id, year, seed, …).
        segment_df : Polars DataFrame with open_time, segment, and target columns.
        audit      : Source data quality metrics dict.
    """
    sample_dir = Path(sample_dir)
    sample_dir.mkdir(parents=True, exist_ok=True)

    meta_out = {**metadata, "generated_at": datetime.now(UTC).isoformat()}
    _write_json(sample_dir / "metadata.json", meta_out)
    _write_json(sample_dir / "audit.json", audit)

    segment_df.write_parquet(sample_dir / "sample_train_valid.parquet", compression="zstd")


# %% Yearly format — load


def load_yearly_sample(sample_dir: str | Path) -> dict:
    """Load yearly sample metadata and return path to sample_train_valid.parquet.

    Args:
        sample_dir : Path to the directory produced by write_yearly_artifacts.

    Returns:
        Metadata dict augmented with 'sample_parquet_path' key.

    Raises:
        FileNotFoundError: If metadata.json or sample_train_valid.parquet are missing.
    """
    sample_dir = Path(sample_dir)
    metadata = json.loads((sample_dir / "metadata.json").read_text(encoding="utf-8"))
    parquet_path = sample_dir / "sample_train_valid.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"sample_train_valid.parquet not found in {sample_dir}")
    return {**metadata, "sample_parquet_path": str(parquet_path)}


# %% Legacy format — write (kept for existing tests)


def write_sample_artifacts(
    sample_dir : Path,
    metadata   : dict,
    folds      : dict,
    audit      : dict,
) -> None:
    """Write metadata.json, folds.json, and audit.json (expanding-window format).

    Args:
        sample_dir : Target directory (created if absent).
        metadata   : Sample metadata dict.
        folds      : Output of build_expanding_window_splits — {'folds': […], 'test': {…}}.
        audit      : Output of audit_feature_table.
    """
    sample_dir = Path(sample_dir)
    sample_dir.mkdir(parents=True, exist_ok=True)

    meta_out = {**metadata, "generated_at": datetime.now(UTC).isoformat()}
    _write_json(sample_dir / "metadata.json", meta_out)
    _write_json(sample_dir / "folds.json",    folds)
    _write_json(sample_dir / "audit.json",    audit)


# %% Legacy format — load / validate (referenced by lightgbm_model.py, lgbm_search.py)


def load_sample_definition(sample_dir: str | Path) -> dict:
    """Load an expanding-window sample definition from its artifact directory.

    Args:
        sample_dir : Path to the directory produced by write_sample_artifacts.

    Returns:
        Merged dict with sample["folds"], sample["test"], sample["data"]["start"/"end"].

    Raises:
        FileNotFoundError: If metadata.json or folds.json are missing.
    """
    sample_dir = Path(sample_dir)
    metadata   = json.loads((sample_dir / "metadata.json").read_text(encoding="utf-8"))
    folds      = json.loads((sample_dir / "folds.json").read_text(encoding="utf-8"))
    return {
        **metadata,
        "folds": folds["folds"],
        "test" : folds["test"],
    }


def validate_sample_definition(sample: dict) -> None:
    """Validate chronological order and non-overlap of folds and test range.

    Args:
        sample : Dict as returned by load_sample_definition.

    Raises:
        ValueError: If test range is inverted or any fold has an invalid/overlapping window.
    """
    def _dt(s: str) -> datetime:
        return datetime.fromisoformat(s)

    test_start = _dt(sample["test"]["start"])
    test_end   = _dt(sample["test"]["end"])

    if test_end <= test_start:
        raise ValueError("Test end must be after test start")

    for fold in sample["folds"]:
        ts = _dt(fold["train_start"])
        te = _dt(fold["train_end"])
        vs = _dt(fold["valid_start"])
        ve = _dt(fold["valid_end"])

        if not ts < te < vs < ve < test_start:
            raise ValueError(f"Invalid or overlapping fold: {fold}")


# %% Internal


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=4), encoding="utf-8")
