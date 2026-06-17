"""Sampling orchestrator — runs audit → splits → write in one call.

Only this module imports utils; audit, splits, and artifacts are project-agnostic.
"""

from datetime import datetime, timedelta
from pathlib import Path

import duckdb

import utils
from modeling.quantitative.sampling.artifacts import write_sample_artifacts
from modeling.quantitative.sampling.audit import audit_feature_table
from modeling.quantitative.sampling.config import SamplingConfig
from modeling.quantitative.sampling.splits import build_expanding_window_splits


def _month_start(ts: str) -> str:
    """Round ts UP to the first minute of the next calendar month."""
    dt = datetime.fromisoformat(ts)
    first = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if dt == first:
        return ts
    if dt.month == 12:
        return datetime(dt.year + 1, 1, 1).strftime("%Y-%m-%d %H:%M:%S")
    return datetime(dt.year, dt.month + 1, 1).strftime("%Y-%m-%d %H:%M:%S")


def _month_end(ts: str) -> str:
    """Round ts DOWN to the last minute of the previous calendar month."""
    dt = datetime.fromisoformat(ts)
    last = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(minutes=1)
    return last.strftime("%Y-%m-%d %H:%M:%S")

# %% Public API


def create_sample(config: SamplingConfig) -> None:
    """Generate and persist a time-based CV sample definition for a given config.

    Steps:
        1. Resolve db_path and sample_dir from config.
        2. Audit feat_ohlcv_quant to determine safe data boundaries.
        3. Build expanding-window splits from the safe boundaries.
        4. Assemble metadata and write all three artifact files.

    Args:
        config : Frozen SamplingConfig with all required parameters.

    Raises:
        ValueError: If the database is missing, the feature table is empty,
                    or no folds can be generated with the given parameters.
    """
    # --- resolve paths ---
    db_path        = utils.load_asset_config(config.asset_id)["database"]["db_path"]
    assets_raw     = utils.load_assets_config()
    db_path_raw    = assets_raw["assets"][config.asset_id]["db_path"]
    sample_dir     = Path(f"database/{config.asset_id}/samples/{config.sample_id}")
    embargo_minutes = config.embargo_minutes or config.target_horizon_minutes

    # --- audit ---
    audit = audit_feature_table(db_path, config.target_col)

    if audit["data_start_safe"] is None or audit["data_end_safe"] is None:
        raise ValueError(
            f"Cannot determine safe data boundaries for {config.asset_id}. "
            "Check that feat_ohlcv_quant and target tables are populated."
        )

    # --- optionally round to complete calendar months ---
    if config.round_to_months:
        data_start = _month_start(audit["data_start_safe"])
        data_end   = _month_end(audit["data_end_safe"])
    else:
        data_start = audit["data_start_safe"]
        data_end   = audit["data_end_safe"]

    # --- splits ---
    splits = build_expanding_window_splits(
        data_start      = data_start,
        data_end        = data_end,
        min_train_days  = config.min_train_days,
        valid_days      = config.valid_days,
        step_days       = config.step_days,
        test_days       = config.test_days,
        embargo_minutes = embargo_minutes,
    )

    # --- metadata ---
    metadata = {
        "sample_id"             : config.sample_id,
        "asset_id"              : config.asset_id,
        "target_col"            : config.target_col,
        "target_horizon_minutes": config.target_horizon_minutes,
        "split_type"            : "expanding_window",
        "embargo_minutes"       : embargo_minutes,
        "data"                  : {
            "start": data_start,
            "end"  : data_end,
        },
        "parameters": {
            "min_train_days": config.min_train_days,
            "valid_days"    : config.valid_days,
            "step_days"     : config.step_days,
            "test_days"     : config.test_days,
        },
        "n_folds": len(splits["folds"]),
        "source" : {
            "db_relative_path": db_path_raw,
            "feature_table"   : "feat_ohlcv_quant",
            "target_table"    : "target",
        },
    }

    # --- write ---
    write_sample_artifacts(sample_dir, metadata, splits, audit)
    _write_sample_parquet(db_path, sample_dir, splits)


# %% Internal


def _write_sample_parquet(db_path: str, sample_dir: Path, splits: dict) -> None:
    """Stream all sample segments to sample.parquet via DuckDB COPY TO.

    Builds a UNION ALL query over every (fold_N_train, fold_N_valid, test)
    segment and writes directly to parquet without materialising the full
    dataset in Python memory.

    Column layout: all feat_ohlcv_quant columns, then target columns (excluding
    the shared open_time), then a 'segment' string column.
    """
    parts: list[str] = []

    for fold in splits["folds"]:
        n = fold["fold"]
        for label, start, end in [
            (f"fold_{n}_train", fold["train_start"], fold["train_end"]),
            (f"fold_{n}_valid", fold["valid_start"], fold["valid_end"]),
        ]:
            parts.append(
                f"SELECT f.*, t.* EXCLUDE (open_time), '{label}' AS segment"
                f" FROM feat_ohlcv_quant f JOIN target t USING (open_time)"
                f" WHERE f.open_time BETWEEN '{start}' AND '{end}'"
            )

    test = splits["test"]
    parts.append(
        f"SELECT f.*, t.* EXCLUDE (open_time), 'test' AS segment"
        f" FROM feat_ohlcv_quant f JOIN target t USING (open_time)"
        f" WHERE f.open_time BETWEEN '{test['start']}' AND '{test['end']}'"
    )

    union_sql  = "\nUNION ALL\n".join(parts)
    out_path   = str(sample_dir.resolve() / "sample.parquet")
    con        = duckdb.connect(db_path, read_only=True)
    try:
        con.execute(
            f"COPY ({union_sql}) TO '{out_path}' (FORMAT PARQUET, CODEC 'ZSTD')"
        )
    finally:
        con.close()
