"""Univariate feature quality analysis.

Reads feat_* columns from a DuckDB connection that exposes a 'quant_train' table
(typically an in-memory DB loaded from the model's sample parquet) and produces
per-feature metrics: null rate, infinite-value rate, variance, outlier ratio, and a
keep / drop / review decision for each variable.
"""

import logging

import duckdb
import polars as pl

from .config import FeatureEngineeringConfig

logger = logging.getLogger(__name__)


def analyze_quality(
    conn : duckdb.DuckDBPyConnection,
    cfg  : FeatureEngineeringConfig,
) -> pl.DataFrame:
    """Compute univariate quality metrics for every feat_* column in quant_train.

    Evaluates each feature independently against the thresholds in cfg.
    Rows with NULL targets are still included (quality is target-agnostic).

    Args:
        conn : Open DuckDB connection to the asset database.
        cfg  : Analysis configuration with quality thresholds.

    Returns:
        DataFrame with one row per feature and columns:
            feature, null_rate, inf_rate, variance, outlier_ratio,
            decision ('keep' | 'drop' | 'review'), drop_reason (str | None).
    """
    _SCHEMA: dict[str, type[pl.DataType]] = {
        "feature"      : pl.Utf8,
        "null_rate"    : pl.Float64,
        "inf_rate"     : pl.Float64,
        "variance"     : pl.Float64,
        "outlier_ratio": pl.Float64,
        "decision"     : pl.Utf8,
        "drop_reason"  : pl.Utf8,
    }

    # --- discover feat_* columns ---
    schema  = conn.execute("DESCRIBE quant_train").pl()
    feat_cols = [c for c in schema["column_name"].to_list() if c.startswith("feat_")]

    if not feat_cols:
        logger.warning("analyze_quality: no feat_* columns in quant_train")
        return pl.DataFrame(schema=_SCHEMA)

    n_rows: int = conn.execute("SELECT COUNT(*) FROM quant_train").fetchone()[0]  # type: ignore[index]
    logger.info("analyze_quality: %d features, %d rows", len(feat_cols), n_rows)

    records: list[dict] = []

    for col in feat_cols:
        # --- aggregate stats in DuckDB (one query per feature) ---
        null_rate: float = conn.execute(
            f'SELECT COUNT(*) FILTER (WHERE "{col}" IS NULL)::DOUBLE / {n_rows} FROM quant_train'
        ).fetchone()[0] or 0.0  # type: ignore[index]

        inf_rate: float = conn.execute(
            f'SELECT COUNT(*) FILTER (WHERE isinf("{col}"))::DOUBLE / {n_rows} FROM quant_train'
        ).fetchone()[0] or 0.0  # type: ignore[index]

        try:
            stats_row = conn.execute(f"""
                SELECT
                    VAR_SAMP("{col}") FILTER (WHERE "{col}" IS NOT NULL
                                                  AND NOT isinf("{col}"))            AS variance,
                    AVG("{col}")      FILTER (WHERE "{col}" IS NOT NULL
                                                  AND NOT isinf("{col}"))            AS col_mean,
                    STDDEV_SAMP("{col}") FILTER (WHERE "{col}" IS NOT NULL
                                                     AND NOT isinf("{col}"))         AS col_std
                FROM quant_train
            """).fetchone()
            variance, col_mean, col_std = stats_row  # type: ignore[misc]
            variance = variance or 0.0
        except Exception:
            # VAR_SAMP overflow: feature has extreme numeric range — treat as review
            variance, col_mean, col_std = float("inf"), None, None

        # --- outlier ratio: |z-score| > 3 over valid values ---
        if col_std is not None and col_std > 0 and col_mean is not None:
            outlier_ratio: float = conn.execute(f"""
                SELECT COUNT(*)::DOUBLE / {n_rows}
                FROM quant_train
                WHERE "{col}" IS NOT NULL
                  AND NOT isinf("{col}")
                  AND ABS(("{col}" - {col_mean}) / {col_std}) > 3.0
            """).fetchone()[0] or 0.0  # type: ignore[index]
        else:
            outlier_ratio = 0.0

        # --- decision ---
        drop_reason: str | None = None
        if null_rate > cfg.max_null_rate:
            decision    = "drop"
            drop_reason = f"null_rate={null_rate:.4f} > max={cfg.max_null_rate}"
        elif inf_rate > cfg.max_inf_rate:
            decision    = "drop"
            drop_reason = f"inf_rate={inf_rate:.6f} > max={cfg.max_inf_rate}"
        elif variance < cfg.min_variance:
            decision    = "drop"
            drop_reason = f"variance={variance:.2e} < min={cfg.min_variance}"
        elif outlier_ratio > cfg.max_outlier_ratio:
            decision    = "review"
        else:
            decision    = "keep"

        records.append({
            "feature"      : col,
            "null_rate"    : null_rate,
            "inf_rate"     : float(inf_rate),
            "variance"     : float(variance),
            "outlier_ratio": float(outlier_ratio),
            "decision"     : decision,
            "drop_reason"  : drop_reason,
        })

    logger.info(
        "analyze_quality: done — keep=%d drop=%d review=%d",
        sum(1 for r in records if r["decision"] == "keep"),
        sum(1 for r in records if r["decision"] == "drop"),
        sum(1 for r in records if r["decision"] == "review"),
    )
    return pl.DataFrame(records, schema=_SCHEMA)
