"""Feature–target relationship analysis.

Evaluates how each feat_* column in quant_train correlates with
long_mfe_fw60 and short_mfe_fw60.  Flags weak signals and leakage suspects.
"""

import logging

import duckdb
import polars as pl

from .config import FeatureEngineeringConfig

logger = logging.getLogger(__name__)


def analyze_target_relation(
    conn : duckdb.DuckDBPyConnection,
    cfg  : FeatureEngineeringConfig,
) -> pl.DataFrame:
    """Compute feature–target relationship metrics for every feat_* column in quant_train.

    Metrics are computed separately for each target column in cfg.target_cols.
    Rows where the target is NULL are excluded (quant_train already guarantees
    non-NULL targets, so no filtering is needed beyond the feature NULL check).

    Args:
        conn : Open DuckDB connection to the asset database.
        cfg  : Analysis configuration with target-relation thresholds.

    Returns:
        DataFrame with one row per (feature, target) pair and columns:
            feature, target, pearson_r, spearman_rho, signal_proxy,
            leakage_flag (bool), decision ('keep' | 'weak' | 'leakage').
    """
    _SCHEMA: dict[str, type[pl.DataType]] = {
        "feature"     : pl.Utf8,
        "target"      : pl.Utf8,
        "pearson_r"   : pl.Float64,
        "spearman_rho": pl.Float64,
        "signal_proxy": pl.Float64,
        "leakage_flag": pl.Boolean,
        "decision"    : pl.Utf8,
    }

    # --- discover feat_* columns ---
    schema    = conn.execute("DESCRIBE quant_train").pl()
    feat_cols = [c for c in schema["column_name"].to_list() if c.startswith("feat_")]

    if not feat_cols:
        logger.warning("analyze_target_relation: no feat_* columns in quant_train")
        return pl.DataFrame(schema=_SCHEMA)

    logger.info(
        "analyze_target_relation: %d features × %d targets",
        len(feat_cols), len(cfg.target_cols),
    )

    records: list[dict] = []

    for target in cfg.target_cols:
        for col in feat_cols:
            # --- Pearson via DuckDB CORR() ---
            pearson_r: float | None = conn.execute(f"""
                SELECT CORR("{col}", "{target}")
                FROM quant_train
                WHERE "{col}" IS NOT NULL AND NOT isinf("{col}")
            """).fetchone()[0]  # type: ignore[index]

            # --- Spearman via RANK() within DuckDB ---
            spearman_rho: float | None = conn.execute(f"""
                WITH ranked AS (
                    SELECT
                        RANK() OVER (ORDER BY "{col}")    AS feat_rank,
                        RANK() OVER (ORDER BY "{target}") AS tgt_rank
                    FROM quant_train
                    WHERE "{col}" IS NOT NULL AND NOT isinf("{col}")
                )
                SELECT CORR(feat_rank, tgt_rank)
                FROM ranked
            """).fetchone()[0]  # type: ignore[index]

            spearman_abs = abs(spearman_rho) if spearman_rho is not None else 0.0
            signal_proxy = spearman_abs
            leakage_flag = spearman_abs > cfg.max_spearman_leakage

            if leakage_flag:
                decision = "leakage"
            elif spearman_abs < cfg.min_spearman_abs:
                decision = "weak"
            else:
                decision = "keep"

            records.append({
                "feature"     : col,
                "target"      : target,
                "pearson_r"   : float(pearson_r)    if pearson_r    is not None else None,
                "spearman_rho": float(spearman_rho) if spearman_rho is not None else None,
                "signal_proxy": float(signal_proxy),
                "leakage_flag": leakage_flag,
                "decision"    : decision,
            })

    logger.info(
        "analyze_target_relation: done — keep=%d weak=%d leakage=%d",
        sum(1 for r in records if r["decision"] == "keep"),
        sum(1 for r in records if r["decision"] == "weak"),
        sum(1 for r in records if r["decision"] == "leakage"),
    )
    return pl.DataFrame(records, schema=_SCHEMA)
