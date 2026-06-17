"""Yearly sampling orchestrator — runs DB load → hourly select → segment → write.

Only this module imports utils and duckdb; yearly_sampler and artifacts are project-agnostic.
"""

import calendar
from pathlib import Path

import duckdb
import polars as pl

import utils
from modeling.quantitative.sampling.artifacts import write_yearly_artifacts
from modeling.quantitative.sampling.config import YearlySamplingConfig
from modeling.quantitative.sampling.yearly_sampler import (
    assign_segments,
    select_hourly_observations,
    select_monthly_validation_weeks,
)


def create_yearly_sample(config: YearlySamplingConfig) -> None:
    """Generate and persist a yearly random-hour sample for the given config.

    Steps:
        1. Load ohlcv ⋈ target rows for config.year from DuckDB (only rows with
           non-null target values are included).
        2. Select one random minute per hour (select_hourly_observations).
        3. Select one validation week per calendar month (select_monthly_validation_weeks).
        4. Assign train / valid / purge segments (assign_segments).
        5. Write metadata.json, audit.json, sample.parquet.

    Args:
        config : Frozen YearlySamplingConfig with all required parameters.

    Raises:
        ValueError: If no rows with valid targets exist for config.year.
    """
    db_path    = utils.load_asset_config(config.asset_id)["database"]["db_path"]
    sample_dir = Path(f"database/{config.asset_id}/samples/{config.sample_id}")

    target_select = ", ".join(f"t.{col}" for col in config.target_cols)
    null_checks   = " AND ".join(f"t.{col} IS NOT NULL" for col in config.target_cols)

    conn = duckdb.connect(db_path, read_only=True)
    try:
        df: pl.DataFrame = conn.execute(
            f"""
            SELECT o.open_time, {target_select}
            FROM ohlcv o
            JOIN target t USING (open_time)
            WHERE YEAR(o.open_time) = {config.year}
              AND {null_checks}
            ORDER BY o.open_time
            """
        ).pl()

        total_ohlcv_row = conn.execute(
            f"SELECT COUNT(*) FROM ohlcv WHERE YEAR(open_time) = {config.year}"
        ).fetchone()
        total_ohlcv = int(total_ohlcv_row[0]) if total_ohlcv_row else 0
    finally:
        conn.close()

    if len(df) == 0:
        raise ValueError(
            f"No data with valid targets found for year {config.year} "
            f"in asset '{config.asset_id}'."
        )

    hourly_df  = select_hourly_observations(df, config.year, config.seed)
    valid_weeks = select_monthly_validation_weeks(hourly_df, config.year, config.seed)
    segment_df  = assign_segments(hourly_df, valid_weeks, config.purge_minutes)

    row_counts = _segment_counts(segment_df)

    expected_hours = 8784 if calendar.isleap(config.year) else 8760
    audit = {
        "source_rows_with_valid_targets": len(df),
        "total_ohlcv_rows_in_year"       : total_ohlcv,
        "expected_hours"                 : expected_hours,
        "actual_hourly_rows"             : len(hourly_df),
        "missing_hours"                  : expected_hours - len(hourly_df),
    }

    metadata = {
        "sample_id"          : config.sample_id,
        "asset_id"           : config.asset_id,
        "year"               : config.year,
        "seed"               : config.seed,
        "purge_minutes"      : config.purge_minutes,
        "target_cols"        : list(config.target_cols),
        "selected_valid_weeks": [
            {"start": str(ws), "end": str(we)} for ws, we in valid_weeks
        ],
        "row_counts"         : row_counts,
    }

    write_yearly_artifacts(sample_dir, metadata, segment_df, audit)


# %% Internal


def _segment_counts(segment_df: pl.DataFrame) -> dict[str, int]:
    rows = (
        segment_df.group_by("segment")
        .agg(pl.len().alias("count"))
        .to_dict(as_series=False)
    )
    return dict(zip(rows["segment"], rows["count"], strict=False))
