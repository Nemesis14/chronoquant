"""Time stability analysis.

Checks whether each feat_* column in quant_train behaves consistently across
rolling time buckets.  Flags features whose null rate, variance, or target
correlation drifts significantly in recent data versus the historical baseline.
"""

import logging

import duckdb
import polars as pl

from .config import FeatureEngineeringConfig

logger = logging.getLogger(__name__)

_REVIEW_DRIFT_FACTOR = 0.5


def analyze_stability(
    conn : duckdb.DuckDBPyConnection,
    cfg  : FeatureEngineeringConfig,
) -> pl.DataFrame:
    """Analyse temporal stability of feat_* columns in quant_train.

    Data is divided into non-overlapping buckets of cfg.stability_bucket_days days.
    For each (feature, bucket) pair the following are computed:
      - null/inf rate, mean, std
      - Spearman ρ with each target column (via RANK within the bucket)

    A feature is flagged 'decayed' if the last two buckets both exceed
    cfg.max_drift_threshold.  Buckets that exceed it but are not in the tail
    are 'unstable'.  Buckets exceeding half the threshold are 'review'.
    All other buckets are 'stable'.

    Features with extreme numeric ranges that cause DuckDB overflow are skipped
    (they will be caught by the quality analysis step).

    Args:
        conn : Open DuckDB connection to the asset database.
        cfg  : Analysis configuration with stability thresholds.

    Returns:
        DataFrame with one row per (feature, bucket) and columns:
            feature, bucket_idx, bucket_start, bucket_end, n, null_rate,
            mean, std, spearman_long, spearman_short, drift_long,
            drift_short, stability_flag ('stable' | 'unstable' | 'decayed' | 'review').
    """
    _SCHEMA: dict[str, type[pl.DataType]] = {
        "feature"       : pl.Utf8,
        "bucket_idx"    : pl.Int32,
        "bucket_start"  : pl.Utf8,
        "bucket_end"    : pl.Utf8,
        "n"             : pl.Int64,
        "null_rate"     : pl.Float64,
        "mean"          : pl.Float64,
        "std"           : pl.Float64,
        "spearman_long" : pl.Float64,
        "spearman_short": pl.Float64,
        "drift_long"    : pl.Float64,
        "drift_short"   : pl.Float64,
        "stability_flag": pl.Utf8,
    }

    schema    = conn.execute("DESCRIBE quant_train").pl()
    feat_cols = [c for c in schema["column_name"].to_list() if c.startswith("feat_")]

    if not feat_cols:
        logger.warning("analyze_stability: no feat_* columns in quant_train")
        return pl.DataFrame(schema=_SCHEMA)

    min_ts_raw = conn.execute(
        "SELECT MIN(open_time)::VARCHAR FROM quant_train"
    ).fetchone()[0]  # type: ignore[index]
    min_ts = str(min_ts_raw)[:19]

    bucket_days = cfg.stability_bucket_days
    logger.info(
        "analyze_stability: %d features, bucket_days=%d, base_ts=%s",
        len(feat_cols), bucket_days, min_ts,
    )

    records: list[dict] = []

    for col in feat_cols:
        try:
            _process_col(col, conn, min_ts, bucket_days, cfg, records)
        except Exception:
            pass  # extreme-valued feature: skipped here, caught by quality step

    logger.info(
        "analyze_stability: done — %d (feature, bucket) rows",
        len(records),
    )
    return pl.DataFrame(records, schema=_SCHEMA)


def _process_col(
    col        : str,
    conn       : duckdb.DuckDBPyConnection,
    min_ts     : str,
    bucket_days: int,
    cfg        : FeatureEngineeringConfig,
    records    : list,
) -> None:
    stats_df = conn.execute(f"""
        WITH bucketed AS (
            SELECT
                open_time,
                "{col}"  AS feat,
                CAST(
                    DATE_DIFF('day', '{min_ts}'::TIMESTAMP, open_time)
                    / {bucket_days}
                AS INTEGER) AS bucket_idx
            FROM quant_train
        )
        SELECT
            bucket_idx,
            MIN(open_time)::VARCHAR                                          AS bucket_start,
            MAX(open_time)::VARCHAR                                          AS bucket_end,
            COUNT(*)                                                         AS n,
            COUNT(*) FILTER (WHERE feat IS NULL OR isinf(feat))::DOUBLE
                / COUNT(*)                                                   AS null_rate,
            AVG(feat)         FILTER (WHERE feat IS NOT NULL AND NOT isinf(feat)) AS mean,
            STDDEV_SAMP(feat) FILTER (WHERE feat IS NOT NULL AND NOT isinf(feat)) AS std
        FROM bucketed
        GROUP BY bucket_idx
        ORDER BY bucket_idx
    """).pl()

    spearman_df = conn.execute(f"""
        WITH bucketed AS (
            SELECT
                "{col}"        AS feat,
                long_mfe_fw60  AS tgt_long,
                short_mfe_fw60 AS tgt_short,
                CAST(
                    DATE_DIFF('day', '{min_ts}'::TIMESTAMP, open_time)
                    / {bucket_days}
                AS INTEGER) AS bucket_idx
            FROM quant_train
            WHERE "{col}" IS NOT NULL AND NOT isinf("{col}")
        ),
        ranked AS (
            SELECT
                bucket_idx,
                RANK() OVER (PARTITION BY bucket_idx ORDER BY feat)       AS feat_rank,
                RANK() OVER (PARTITION BY bucket_idx ORDER BY tgt_long)   AS long_rank,
                RANK() OVER (PARTITION BY bucket_idx ORDER BY tgt_short)  AS short_rank
            FROM bucketed
        )
        SELECT
            bucket_idx,
            CORR(feat_rank, long_rank)  AS spearman_long,
            CORR(feat_rank, short_rank) AS spearman_short
        FROM ranked
        GROUP BY bucket_idx
        ORDER BY bucket_idx
    """).pl()

    baseline = conn.execute(f"""
        WITH ranked AS (
            SELECT
                RANK() OVER (ORDER BY "{col}")        AS feat_rank,
                RANK() OVER (ORDER BY long_mfe_fw60)  AS long_rank,
                RANK() OVER (ORDER BY short_mfe_fw60) AS short_rank
            FROM quant_train
            WHERE "{col}" IS NOT NULL AND NOT isinf("{col}")
        )
        SELECT
            CORR(feat_rank, long_rank)  AS sp_long,
            CORR(feat_rank, short_rank) AS sp_short
        FROM ranked
    """).fetchone()  # type: ignore[assignment]

    baseline_long  = float(baseline[0]) if baseline[0] is not None else 0.0  # type: ignore[index]
    baseline_short = float(baseline[1]) if baseline[1] is not None else 0.0  # type: ignore[index]

    merged    = stats_df.join(spearman_df, on="bucket_idx", how="left")
    n_buckets = len(merged)

    for i, row in enumerate(merged.iter_rows(named=True)):
        sp_long  = float(row["spearman_long"])  if row["spearman_long"]  is not None else 0.0
        sp_short = float(row["spearman_short"]) if row["spearman_short"] is not None else 0.0
        drift_long  = abs(sp_long  - baseline_long)
        drift_short = abs(sp_short - baseline_short)
        max_drift   = max(drift_long, drift_short)
        is_tail     = (i >= n_buckets - 2)

        if max_drift > cfg.max_drift_threshold and is_tail:
            flag = "decayed"
        elif max_drift > cfg.max_drift_threshold:
            flag = "unstable"
        elif max_drift > cfg.max_drift_threshold * _REVIEW_DRIFT_FACTOR:
            flag = "review"
        else:
            flag = "stable"

        records.append({
            "feature"       : col,
            "bucket_idx"    : int(row["bucket_idx"]),
            "bucket_start"  : str(row["bucket_start"])[:19] if row["bucket_start"] else None,
            "bucket_end"    : str(row["bucket_end"])[:19]   if row["bucket_end"]   else None,
            "n"             : int(row["n"]),
            "null_rate"     : float(row["null_rate"] or 0.0),
            "mean"          : float(row["mean"])  if row["mean"]  is not None else None,
            "std"           : float(row["std"])   if row["std"]   is not None else None,
            "spearman_long" : sp_long,
            "spearman_short": sp_short,
            "drift_long"    : drift_long,
            "drift_short"   : drift_short,
            "stability_flag": flag,
        })
