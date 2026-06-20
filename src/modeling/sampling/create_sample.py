"""Yearly sampling orchestrator — runs DB load → hourly select → fold-assign → write.

Source table: quant_train (feat_* + target columns, NULL targets excluded).
Only this module imports utils and duckdb; yearly_sampler and artifacts are project-agnostic.
"""

import calendar
from pathlib import Path

import duckdb
import polars as pl

import utils
from modeling.sampling.artifacts import write_yearly_artifacts
from modeling.sampling.config import WalkForwardSamplingConfig, YearlySamplingConfig
from modeling.sampling.yearly_sampler import (
    assign_fold_ids,
    assign_walk_forward_fold_ids,
    generate_walk_forward_folds,
    select_hourly_observations,
)


def create_model_sample(model_id: str) -> None:
    """Generate and persist a yearly sample for a specific model into its artifact directory.

    Reads model config (asset_id, target_name, sample_id, artifact_dir).
    Includes only the model's own target column and all feat_* columns.
    Writes sample_train_valid.parquet, metadata.json, audit.json to artifact_dir.

    Args:
        model_id : Model key from config/models.json.

    Raises:
        ValueError: If model_id is not found or no data for the sample year.
        RuntimeError: If quant_train table does not exist.
    """
    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta         = models_cfg["models"][model_id]
    asset_id     = meta["asset_id"]
    target_name  = meta["target_name"]
    sample_id    = meta["sampling"]["sample_id"]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))

    year = int(sample_id.split("_yearly_")[-1])
    seed = 42 + year

    config = YearlySamplingConfig(
        sample_id   = sample_id,
        asset_id    = asset_id,
        year        = year,
        seed        = seed,
        target_cols = (target_name,),
        feature_cols= (),
    )

    artifact_dir.mkdir(parents=True, exist_ok=True)
    create_yearly_sample(config, output_dir=artifact_dir)


def create_yearly_sample(
    config    : YearlySamplingConfig,
    output_dir: Path | None = None,
) -> None:
    """Generate and persist a yearly random-hour sample for the given config.

    Source: quant_train table (feat_* columns + target columns).

    Steps:
        1. Load quant_train rows for config.year from DuckDB (NULL targets excluded).
        2. Resolve feature columns: use config.feature_cols if non-empty, else all feat_*.
        3. Select one random minute per hour (select_hourly_observations).
        4. Assign fold_id 1..n_folds to every row (assign_fold_ids).
        5. Write metadata.json, audit.json, sample_train_valid.parquet.

    Args:
        config     : Frozen YearlySamplingConfig with all required parameters.
        output_dir : Directory to write artifacts. Defaults to
                     database/{asset_id}/samples/{sample_id}/.

    Raises:
        ValueError: If quant_train has no rows with valid targets for config.year.
        RuntimeError: If quant_train table does not exist in the database.
    """
    db_path    = utils.load_asset_config(config.asset_id)["database"]["db_path"]
    sample_dir = output_dir if output_dir is not None else Path(
        f"database/{config.asset_id}/samples/{config.sample_id}"
    )

    null_checks = " AND ".join(f"{col} IS NOT NULL" for col in config.target_cols)

    conn = duckdb.connect(db_path, read_only=True)
    try:
        feat_cols = _resolve_feature_cols(conn, config.feature_cols)
        target_select = ", ".join(config.target_cols)
        feat_select   = ", ".join(feat_cols) if feat_cols else ""
        col_select    = f"open_time, {feat_select + ', ' if feat_select else ''}{target_select}"

        df: pl.DataFrame = conn.execute(
            f"""
            SELECT {col_select}
            FROM quant_train
            WHERE YEAR(open_time) = {config.year}
              AND {null_checks}
            ORDER BY open_time
            """
        ).pl()

        total_rows_row = conn.execute(
            f"SELECT COUNT(*) FROM quant_train WHERE YEAR(open_time) = {config.year}"
        ).fetchone()
        total_rows = int(total_rows_row[0]) if total_rows_row else 0
    finally:
        conn.close()

    if len(df) == 0:
        raise ValueError(
            f"No data with valid targets found for year {config.year} "
            f"in asset '{config.asset_id}'."
        )

    hourly_df                    = select_hourly_observations(df, config.year, config.seed)
    fold_df, fold_week_assignments = assign_fold_ids(hourly_df, config.year, config.seed, config.n_folds)

    # Enforce column order: open_time, target(s), fold_id, feat_*
    _target_cols = list(config.target_cols)
    _feat_cols   = sorted(c for c in fold_df.columns if c.startswith("feat_"))
    _col_order   = ["open_time"] + _target_cols + ["fold_id"] + _feat_cols
    fold_df      = fold_df.select(_col_order)

    fold_row_counts = _fold_counts(fold_df)

    expected_hours = 8784 if calendar.isleap(config.year) else 8760
    audit = {
        "total_quant_train_rows_in_year": total_rows,
        "source_rows_with_valid_targets": len(df),
        "expected_hours"                : expected_hours,
        "actual_hourly_rows"            : len(hourly_df),
        "missing_hours"                 : expected_hours - len(hourly_df),
    }

    metadata = {
        "sample_id"            : config.sample_id,
        "asset_id"             : config.asset_id,
        "year"                 : config.year,
        "seed"                 : config.seed,
        "purge_minutes"        : config.purge_minutes,
        "n_folds"              : config.n_folds,
        "target_cols"          : list(config.target_cols),
        "feature_cols"         : feat_cols,
        "fold_week_assignments": {str(k): v for k, v in fold_week_assignments.items()},
        "fold_row_counts"      : fold_row_counts,
    }

    write_yearly_artifacts(sample_dir, metadata, fold_df, audit)


def create_walk_forward_sample(
    config    : WalkForwardSamplingConfig,
    output_dir: Path | None = None,
) -> None:
    """Generate and persist a walk-forward CV sample for the given config.

    Source: quant_train table (feat_* columns + target columns).

    The sample covers from the anchor year's first month through the last fold's
    validation window end.  Exactly one random minute per hour is selected
    (``select_hourly_observations``), applied to the full date range.

    fold_id assignment:
      - Rows inside a fold's validation window → that fold's fold_id (1..n)
      - All other rows → fold_id = 0 (train-only)

    Steps:
        1. Generate fold time windows (``generate_walk_forward_folds``).
        2. Load quant_train rows for anchor_year_start … last_valid_end from DuckDB.
        3. Select one random minute per hour (``select_hourly_observations``),
           filtering for the anchor year slice, then extended to full range.
        4. Assign fold_id via validation window membership.
        5. Write metadata.json, audit.json, sample_train_valid.parquet.

    Args:
        config     : Frozen WalkForwardSamplingConfig.
        output_dir : Directory to write artifacts.  Defaults to
                     database/{asset_id}/samples/{sample_id}/.

    Raises:
        ValueError : If quant_train has no rows for the required date range.
        RuntimeError: If quant_train table does not exist.
    """
    db_path    = utils.load_asset_config(config.asset_id)["database"]["db_path"]
    sample_dir = output_dir if output_dir is not None else Path(
        f"database/{config.asset_id}/samples/{config.sample_id}"
    )

    # 1. Generate fold windows
    fold_time_windows = generate_walk_forward_folds(
        year          = config.year,
        train_months  = config.train_months,
        valid_months  = config.valid_months,
        shift_months  = config.shift_months,
        purge_minutes = config.purge_minutes,
        n_folds       = config.n_folds,
    )

    range_start = f"{config.year}-01-01"
    range_end   = fold_time_windows[-1]["valid_end"]

    null_checks = " AND ".join(f"{col} IS NOT NULL" for col in config.target_cols)

    conn = duckdb.connect(db_path, read_only=True)
    try:
        feat_cols     = _resolve_feature_cols(conn, config.feature_cols)
        target_select = ", ".join(config.target_cols)
        feat_select   = ", ".join(feat_cols) if feat_cols else ""
        col_select    = f"open_time, {feat_select + ', ' if feat_select else ''}{target_select}"

        df: pl.DataFrame = conn.execute(
            f"""
            SELECT {col_select}
            FROM quant_train
            WHERE open_time >= '{range_start}'
              AND open_time <= '{range_end} 23:59:59'
              AND {null_checks}
            ORDER BY open_time
            """
        ).pl()

        total_rows_row = conn.execute(
            f"""
            SELECT COUNT(*) FROM quant_train
            WHERE open_time >= '{range_start}'
              AND open_time <= '{range_end} 23:59:59'
            """
        ).fetchone()
        total_rows = int(total_rows_row[0]) if total_rows_row else 0
    finally:
        conn.close()

    if len(df) == 0:
        raise ValueError(
            f"No data with valid targets found for range {range_start}…{range_end} "
            f"in asset '{config.asset_id}'."
        )

    # 3. Select one random minute per hour — use anchor year seed but apply to full range
    #    select_hourly_observations filters by year; we need to extend it to cover
    #    multi-year ranges by calling it per year and concatenating.
    years_in_range: list[int] = []
    start_y = config.year
    end_y   = int(range_end[:4])
    for y in range(start_y, end_y + 1):
        years_in_range.append(y)

    hourly_parts: list[pl.DataFrame] = []
    for y in years_in_range:
        yearly_seed = config.seed + (y - config.year)
        part = (
            df.filter(pl.col("open_time").dt.year() == y)
            .with_columns(
                pl.col("open_time").dt.truncate("1h").alias("_hour"),
                pl.col("open_time").cast(pl.Int64).hash(seed=yearly_seed, seed_1=yearly_seed + 1).alias("_rand"),
            )
            .sort(["_hour", "_rand"])
            .unique(subset=["_hour"], keep="first", maintain_order=False)
            .drop(["_hour", "_rand"])
            .sort("open_time")
        )
        hourly_parts.append(part)

    hourly_df = pl.concat(hourly_parts).sort("open_time")

    # 4. Assign fold_ids via validation window membership
    fold_df = assign_walk_forward_fold_ids(hourly_df, fold_time_windows)

    # Enforce column order: open_time, target(s), fold_id, feat_*
    _target_cols = list(config.target_cols)
    _feat_cols   = sorted(c for c in fold_df.columns if c.startswith("feat_"))
    _col_order   = ["open_time"] + _target_cols + ["fold_id"] + _feat_cols
    fold_df      = fold_df.select(_col_order)

    fold_row_counts = _fold_counts(fold_df)

    # For multi-year ranges, sum up expected hours per year
    expected_hours = sum(
        8784 if calendar.isleap(y) else 8760 for y in years_in_range
    )

    audit = {
        "total_quant_train_rows_in_range": total_rows,
        "source_rows_with_valid_targets" : len(df),
        "expected_hours"                 : expected_hours,
        "actual_hourly_rows"             : len(hourly_df),
        "missing_hours"                  : expected_hours - len(hourly_df),
        "date_range_start"               : range_start,
        "date_range_end"                 : range_end,
        "years_covered"                  : years_in_range,
    }

    metadata = {
        "sample_id"        : config.sample_id,
        "asset_id"         : config.asset_id,
        "year"             : config.year,
        "seed"             : config.seed,
        "purge_minutes"    : config.purge_minutes,
        "train_months"     : config.train_months,
        "valid_months"     : config.valid_months,
        "shift_months"     : config.shift_months,
        "target_cols"      : list(config.target_cols),
        "feature_cols"     : feat_cols,
        "fold_time_windows": fold_time_windows,
        "fold_row_counts"  : fold_row_counts,
        "sampling_mode"    : "walk_forward",
    }

    sample_dir.mkdir(parents=True, exist_ok=True)
    write_yearly_artifacts(sample_dir, metadata, fold_df, audit)


def create_model_walk_forward_sample(model_id: str) -> None:
    """Generate and persist a walk-forward sample for a specific model.

    Reads model config (asset_id, target_name, sample_id, artifact_dir).
    Uses WalkForwardSamplingConfig with default walk-forward parameters.
    Writes sample_train_valid.parquet, metadata.json, audit.json to artifact_dir.

    Args:
        model_id : Model key from config/models.json.

    Raises:
        ValueError: If model_id is not found or no data for the sample range.
        RuntimeError: If quant_train table does not exist in the database.
    """
    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta          = models_cfg["models"][model_id]
    asset_id      = meta["asset_id"]
    target_name   = meta["target_name"]
    sampling_meta = meta["sampling"]
    sample_id     = sampling_meta["sample_id"]
    artifact_dir  = Path(utils._resolve_path(meta["artifact_dir"]))

    if "year" in sampling_meta:
        year = int(sampling_meta["year"])
    elif "_yearly_" in sample_id:
        year = int(sample_id.split("_yearly_")[-1])
    else:
        year = int(sample_id.split("_")[-1])
    seed = 42 + year

    config = WalkForwardSamplingConfig(
        sample_id    = sample_id,
        asset_id     = asset_id,
        year         = year,
        seed         = seed,
        train_months = int(sampling_meta.get("train_months", 9)),
        valid_months = int(sampling_meta.get("valid_months", 3)),
        shift_months = int(sampling_meta.get("shift_months", 3)),
        n_folds      = int(sampling_meta.get("n_folds", 4)),
        target_cols  = (target_name,),
        feature_cols = (),
    )

    artifact_dir.mkdir(parents=True, exist_ok=True)
    create_walk_forward_sample(config, output_dir=artifact_dir)


# %% Internal


def _resolve_feature_cols(
    conn        : duckdb.DuckDBPyConnection,
    feature_cols: tuple[str, ...],
) -> list[str]:
    """Return the feature column list to select from quant_train.

    If ``feature_cols`` is non-empty, validate that each column exists and
    starts with 'feat_', then return it as a list.  If empty, discover all
    feat_* columns from the quant_train schema.

    Raises:
        RuntimeError: If quant_train does not exist.
        ValueError: If a requested column is missing from quant_train.
    """
    try:
        schema_rows = conn.execute("DESCRIBE quant_train").fetchall()
    except Exception as exc:
        raise RuntimeError(
            "quant_train table not found — run src/data_handling/03_build_quant_train.py first."
        ) from exc

    all_cols = {row[0] for row in schema_rows}

    if feature_cols:
        missing = [c for c in feature_cols if c not in all_cols]
        if missing:
            raise ValueError(f"Requested feature columns not in quant_train: {missing}")
        return list(feature_cols)

    # Auto-discover: all feat_* columns, sorted for reproducibility.
    return sorted(c for c in all_cols if c.startswith("feat_"))


def _fold_counts(fold_df: pl.DataFrame) -> dict[str, int]:
    rows = (
        fold_df.group_by("fold_id")
        .agg(pl.len().alias("count"))
        .to_dict(as_series=False)
    )
    return {str(k): v for k, v in zip(rows["fold_id"], rows["count"], strict=False)}
