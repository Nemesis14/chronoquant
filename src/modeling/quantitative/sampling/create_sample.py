"""Yearly sampling orchestrator — runs DB load → hourly select → segment → write.

Source table: quant_train (feat_* + target columns, NULL targets excluded).
Only this module imports utils and duckdb; yearly_sampler and artifacts are project-agnostic.
"""

import calendar
from datetime import date
from pathlib import Path

import duckdb
import polars as pl

import utils
from database.store.duckdb_store import materialize_sample_table
from modeling.quantitative.sampling.artifacts import write_yearly_artifacts
from modeling.quantitative.sampling.config import YearlySamplingConfig
from modeling.quantitative.sampling.yearly_sampler import (
    assign_segments,
    select_hourly_observations,
    select_monthly_validation_weeks,
)


def create_yearly_sample(config: YearlySamplingConfig) -> None:
    """Generate and persist a yearly random-hour sample for the given config.

    Source: quant_train table (feat_* columns + target columns).

    Steps:
        1. Load quant_train rows for config.year from DuckDB (NULL targets excluded).
        2. Resolve feature columns: use config.feature_cols if non-empty, else all feat_*.
        3. Select one random minute per hour (select_hourly_observations).
        4. Select one validation week per non-test calendar month.
        5. Assign train / valid / purge / test segments (assign_segments).
        6. Write metadata.json, audit.json, sample.parquet.

    Args:
        config : Frozen YearlySamplingConfig with all required parameters.

    Raises:
        ValueError: If quant_train has no rows with valid targets for config.year.
        RuntimeError: If quant_train table does not exist in the database.
    """
    db_path    = utils.load_asset_config(config.asset_id)["database"]["db_path"]
    sample_dir = Path(f"database/{config.asset_id}/samples/{config.sample_id}")

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

    test_start = _test_start(config.year, config.test_months)

    hourly_df   = select_hourly_observations(df, config.year, config.seed)
    valid_weeks = select_monthly_validation_weeks(
        hourly_df, config.year, config.seed, test_months=config.test_months
    )
    segment_df  = assign_segments(
        hourly_df, valid_weeks, config.purge_minutes, test_start=test_start
    )

    row_counts = _segment_counts(segment_df)

    expected_hours = 8784 if calendar.isleap(config.year) else 8760
    audit = {
        "total_quant_train_rows_in_year": total_rows,
        "source_rows_with_valid_targets": len(df),
        "expected_hours"                : expected_hours,
        "actual_hourly_rows"            : len(hourly_df),
        "missing_hours"                 : expected_hours - len(hourly_df),
    }

    metadata = {
        "sample_id"           : config.sample_id,
        "asset_id"            : config.asset_id,
        "year"                : config.year,
        "seed"                : config.seed,
        "purge_minutes"       : config.purge_minutes,
        "target_cols"         : list(config.target_cols),
        "feature_cols"        : feat_cols,
        "test_months"         : config.test_months,
        "sample_table_name"   : f"sample_{config.sample_id}",
        "selected_valid_weeks": [
            {"start": str(ws), "end": str(we)} for ws, we in valid_weeks
        ],
        "row_counts"          : row_counts,
    }

    write_yearly_artifacts(sample_dir, metadata, segment_df, audit)

    conn_rw = duckdb.connect(db_path)
    try:
        materialize_sample_table(conn_rw, config.sample_id, segment_df)
    finally:
        conn_rw.close()


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
            "quant_train table not found — run src/database/03_build_quant_train.py first."
        ) from exc

    all_cols = {row[0] for row in schema_rows}

    if feature_cols:
        missing = [c for c in feature_cols if c not in all_cols]
        if missing:
            raise ValueError(f"Requested feature columns not in quant_train: {missing}")
        return list(feature_cols)

    # Auto-discover: all feat_* columns, sorted for reproducibility.
    return sorted(c for c in all_cols if c.startswith("feat_"))


def _test_start(year: int, test_months: int) -> date | None:
    """Return the first day of the test holdout period, or None if disabled."""
    if test_months <= 0:
        return None
    test_month = 13 - test_months
    return date(year, test_month, 1)


def _segment_counts(segment_df: pl.DataFrame) -> dict[str, int]:
    rows = (
        segment_df.group_by("segment")
        .agg(pl.len().alias("count"))
        .to_dict(as_series=False)
    )
    return dict(zip(rows["segment"], rows["count"], strict=False))
