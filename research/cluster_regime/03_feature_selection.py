"""Feature redundancy analysis and selection for cluster regime research (t872).

Selects ~14 low-redundancy indicator features (one representative per
dimension: trend, momentum, volatility, volume/liquidity, price action,
market structure, candle shape) from the champion model's 135-feature pool,
using pairwise Pearson correlation to avoid picking near-duplicate signals.

TRAIN-ONLY CONSTRAINT (t872 fix vs. the original t863 version): correlation
is computed using ONLY the 2021-01-01 -- 2022-12-31 window. This is Fold 1's
training period in the walk-forward CV scheme used downstream by
04_cv_cluster_optimization.py, and critically it is the ONLY date range that
is a training period in every fold and never a validation period in any
fold. Selecting features on this window guarantees the feature set itself
carries zero information from any CV validation window, at the cost of a
smaller (but still ample, ~35K-row) sample.

Pipeline (t872 rebuild)
------------------------
03_feature_selection (THIS SCRIPT, train-only)
    -> 02_window_representation (consumes this script's selected feature list)
    -> 01_target_computation (independent, fw360 targets)
    -> 04_cv_cluster_optimization (walk-forward ANOVA F-score k-search)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import duckdb
import numpy as np

import utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# TRAIN-ONLY window: Fold 1's training period (never a validation period in
# any of the 4 walk-forward folds -- see 04_cv_cluster_optimization.py).
FEATURE_SELECTION_TRAIN_START = "2021-01-01"
FEATURE_SELECTION_TRAIN_END = "2022-12-31"

MODEL_FEATURES_PATH = Path("artifacts") / "lgbm_solusdt_l_fw60_2101_2607" / "features.json"
OUTPUT_REPORT = Path("artifacts") / "research_cluster_regime" / "feature_selection_report.json"

CORR_THRESHOLD = 0.6  # redundancy cutoff: don't add a feature correlated > this with an already-selected one
MAX_PER_DIMENSION = 2
SAMPLE_LIMIT = 50_000  # cap on rows pulled for the correlation matrix (train window is ~35K rows anyway)

# ============================================================================
# Feature Dimension Mapping
# ============================================================================

FEATURE_DIMENSIONS = {
    # Trend/Slope
    "feat_ema_slope_10": "trend",
    "feat_ema_slope_30": "trend",
    "feat_lr_slope_10": "trend",
    "feat_lr_slope_30": "trend",
    "feat_lr_slope_60": "trend",
    "feat_macd_12_26": "trend",
    "feat_macd_diff": "trend",
    "feat_macd_signal_12_26_9": "trend",
    "feat_ema_ratio_14": "trend",
    "feat_ema_ratio_140": "trend",
    "feat_kama_ratio_10_2_30": "trend",
    "feat_kijun_ratio": "trend",
    "feat_senkou_b_ratio": "trend",
    "feat_directional_agreement_10_30": "trend",
    # Momentum
    "feat_roc_14": "momentum",
    "feat_roc_140": "momentum",
    "feat_roc_delta_5": "momentum",
    "feat_roc_delta_10": "momentum",
    "feat_roc_delta_30": "momentum",
    "feat_rsi_delta_10": "momentum",
    "feat_rsi_delta_30": "momentum",
    "feat_stoch_d_14": "momentum",
    "feat_cci_20": "momentum",
    "feat_return_momentum_delta_10": "momentum",
    "feat_return_momentum_delta_30": "momentum",
    "feat_adx_14": "momentum",
    "feat_adx_pos_14": "momentum",
    "feat_adx_neg_14": "momentum",
    # Volatility/Volatility Rank
    "feat_atr_14": "volatility",
    "feat_atr_dist_high_30": "volatility",
    "feat_atr_dist_low_30": "volatility",
    "feat_atr_dist_low_60": "volatility",
    "feat_natr_14": "volatility",
    "feat_natr_rank_60": "volatility",
    "feat_hist_vol_20": "volatility",
    "feat_hist_vol_rank_60": "volatility",
    "feat_gk_vol_10": "volatility",
    "feat_gk_vol_30": "volatility",
    "feat_gk_vol_60": "volatility",
    "feat_bb_width_14": "volatility",
    "feat_bb_width_140": "volatility",
    "feat_bb_width_rank_20": "volatility",
    "feat_bb_width_rank_60": "volatility",
    "feat_bb_position_14": "volatility",
    "feat_bb_position_140": "volatility",
    # Volume / Liquidity
    "feat_volume_ratio_14": "volume_liquidity",
    "feat_volume_sma_14": "volume_liquidity",
    "feat_obv": "volume_liquidity",
    "feat_obv_roc_14": "volume_liquidity",
    "feat_ad_line": "volume_liquidity",
    "feat_quote_volume_ratio_30": "volume_liquidity",
    "feat_taker_buy_base_ratio_10": "volume_liquidity",
    "feat_taker_buy_quote_ratio_30": "volume_liquidity",
    "feat_avg_trade_quote_30": "volume_liquidity",
    "feat_trade_count_rank_30": "volume_liquidity",
    "feat_trade_count_accel_10_30": "volume_liquidity",
    # Price Action / Returns
    "feat_returns_log": "price_action",
    "feat_returns_sma_14": "price_action",
    "feat_returns_std_14": "price_action",
    "feat_returns_skew_14": "price_action",
    "feat_return_10": "price_action",
    "feat_return_30": "price_action",
    "feat_return_60": "price_action",
    "feat_return_z_10": "price_action",
    "feat_return_asymmetry_30": "price_action",
    "feat_return_asymmetry_60": "price_action",
    "feat_return_autocorr_lag1_30": "price_action",
    "feat_pos_return_mean_10": "price_action",
    "feat_pos_return_mean_30": "price_action",
    "feat_pos_return_mean_60": "price_action",
    "feat_neg_return_mean_10": "price_action",
    "feat_neg_return_mean_30": "price_action",
    "feat_neg_return_mean_60": "price_action",
    "feat_dist_rolling_high_10": "price_action",
    "feat_dist_rolling_high_30": "price_action",
    "feat_dist_rolling_high_60": "price_action",
    "feat_dist_rolling_low_10": "price_action",
    "feat_dist_rolling_low_30": "price_action",
    "feat_dist_rolling_low_60": "price_action",
    "feat_vol_adj_return_30": "price_action",
    "feat_volume_confirmed_return_10": "price_action",
    "feat_volume_confirmed_return_30": "price_action",
    "feat_taker_flow_confirmed_return_10": "price_action",
    "feat_recovery_ratio_60": "price_action",
    # Candle Shape / Body Ratio
    "feat_body_ratio": "candle_shape",
    "feat_body_ratio_sma_10": "candle_shape",
    "feat_body_ratio_sma_30": "candle_shape",
    "feat_signed_body_ratio": "candle_shape",
    "feat_signed_body_sma_30": "candle_shape",
    "feat_wick_imbalance_sma_10": "candle_shape",
    "feat_lower_wick_ratio": "candle_shape",
    "feat_upper_wick_ratio": "candle_shape",
    "feat_bull_bars_ratio_30": "candle_shape",
    "feat_bull_bars_ratio_60": "candle_shape",
    "feat_doji": "candle_shape",
    # Market Structure / Range
    "feat_donchian_position_30": "market_structure",
    "feat_donchian_position_60": "market_structure",
    "feat_donchian_width_10": "market_structure",
    "feat_donchian_width_30": "market_structure",
    "feat_donchian_width_60": "market_structure",
    "feat_swing_high_5": "market_structure",
    "feat_higher_high_count_5": "market_structure",
    "feat_efficiency_ratio_10": "market_structure",
    "feat_efficiency_ratio_30": "market_structure",
    "feat_efficiency_ratio_60": "market_structure",
    "feat_lr_r2_30": "market_structure",
    "feat_lr_residual_10": "market_structure",
    "feat_lr_residual_30": "market_structure",
    "feat_lr_residual_60": "market_structure",
    "feat_tenkan_kijun_delta": "market_structure",
    "feat_price_vs_tenkan": "market_structure",
    "feat_ichimoku_cloud_thickness": "market_structure",
    "feat_hml_range": "market_structure",
    "feat_close_position": "market_structure",
    # Time/Session (temporal, excluded from clustering)
    "feat_bars_into_session_norm": "time_session",
    "feat_hour_sin": "time_session",
    "feat_hour_cos": "time_session",
    "feat_dayofweek_sin": "time_session",
    "feat_dayofweek_cos": "time_session",
    "feat_weekend": "time_session",
    "feat_session_asia": "time_session",
    "feat_session_us": "time_session",
    "feat_day_open_return": "time_session",
    "feat_weekly_open_return": "time_session",
    "feat_gap_open": "time_session",
    # Drawdown / Risk
    "feat_max_drawdown_10": "risk",
    "feat_max_drawdown_30": "risk",
    "feat_max_drawdown_60": "risk",
    "feat_rolling_drawdown_10": "risk",
    "feat_prev_session_high_dist": "risk",
    "feat_prev_session_low_dist": "risk",
    "feat_time_since_high_60": "risk",
}

# Core dimensions to extract (excluding time_session and risk, as before)
CORE_DIMENSIONS = {
    "trend",
    "momentum",
    "volatility",
    "volume_liquidity",
    "price_action",
    "market_structure",
    "candle_shape",
}


def load_model_features() -> list[str]:
    """Load candidate feature list from the champion model artifact."""
    logger.info("Loading candidate feature list from %s", MODEL_FEATURES_PATH)
    with open(MODEL_FEATURES_PATH) as f:
        data = json.load(f)
    return data["features"]


def compute_correlation_matrix_sql(
    conn: duckdb.DuckDBPyConnection,
    features: list[str],
) -> np.ndarray:
    """Compute a Pearson correlation matrix over the TRAIN-ONLY window.

    Args:
        conn     : Open read-only DuckDB connection.
        features : Candidate feature column names.

    Returns:
        (len(features), len(features)) Pearson correlation matrix.
    """
    logger.info(
        "Computing correlation matrix for %d features on TRAIN-ONLY window [%s, %s]...",
        len(features), FEATURE_SELECTION_TRAIN_START, FEATURE_SELECTION_TRAIN_END,
    )

    not_null_clause = " AND ".join(f"{f} IS NOT NULL" for f in features[:5])
    sample_query = f"""
    SELECT {", ".join(features)}
    FROM main.quant_train
    WHERE open_time >= '{FEATURE_SELECTION_TRAIN_START}'
      AND open_time <= '{FEATURE_SELECTION_TRAIN_END}'
      AND {not_null_clause}
    LIMIT {SAMPLE_LIMIT}
    """

    result = conn.execute(sample_query).fetchall()
    X = np.array(result, dtype=np.float64)
    logger.info("Sampled %d rows (train-only window)", X.shape[0])

    return np.corrcoef(X.T)


def select_features_per_dimension(
    features: list[str],
    corr_matrix: np.ndarray,
) -> dict[str, dict]:
    """Select up to MAX_PER_DIMENSION representative features per dimension.

    Selection prefers features with low mean absolute correlation to all
    other candidates (less redundant / noisier signal), then greedily adds
    features to each dimension's selection only if they are not highly
    correlated (> CORR_THRESHOLD) with an already-selected feature in that
    dimension.

    Args:
        features    : Candidate feature names (order matches corr_matrix rows/cols).
        corr_matrix : Pearson correlation matrix for `features`.

    Returns:
        Dict of {feature_name: {"dimension": ..., "mean_abs_corr": ...}}.
    """
    logger.info("Selecting representatives per dimension...")

    dimension_features: dict[str, list[str]] = {}
    for feat in features:
        dim = FEATURE_DIMENSIONS.get(feat, "other")
        if dim in CORE_DIMENSIONS:
            dimension_features.setdefault(dim, []).append(feat)

    selected: dict[str, dict] = {}
    feature_to_idx = {feat: i for i, feat in enumerate(features)}

    for dim in sorted(CORE_DIMENSIONS):
        feats_in_dim = dimension_features.get(dim, [])
        if not feats_in_dim:
            logger.warning("  No candidate features found for dimension '%s'", dim)
            continue

        logger.info("  Dimension '%s': %d candidates", dim, len(feats_in_dim))

        scores = {}
        for feat in feats_in_dim:
            if feat not in feature_to_idx:
                continue
            idx = feature_to_idx[feat]
            row = np.abs(corr_matrix[idx, :])
            row_valid = row[~np.isnan(row)]
            if len(row_valid) == 0:
                logger.warning("    Feature '%s' has an all-NaN correlation row (constant/degenerate on train window) -- skipping", feat)
                continue
            scores[feat] = float(np.mean(row_valid))

        sorted_feats = sorted(scores.items(), key=lambda x: x[1])
        selected_for_dim: list[str] = []

        for feat, score in sorted_feats:
            idx = feature_to_idx[feat]
            can_add = all(
                abs(corr_matrix[idx, feature_to_idx[prev]]) <= CORR_THRESHOLD
                for prev in selected_for_dim
            )
            if can_add:
                selected_for_dim.append(feat)
                selected[feat] = {"dimension": dim, "mean_abs_corr": float(score)}
                if len(selected_for_dim) >= MAX_PER_DIMENSION:
                    break

        logger.info("    Selected: %s", selected_for_dim)

    return selected


def generate_report(
    selected_features: dict,
    corr_matrix_selected: np.ndarray,
    all_features: list[str],
) -> dict:
    """Assemble the final feature-selection report dict."""
    selected_cols = list(selected_features.keys())

    report = {
        "metadata": {
            "description": "Feature redundancy analysis (t872) for CV-optimized cluster regime research",
            "train_only_window": [FEATURE_SELECTION_TRAIN_START, FEATURE_SELECTION_TRAIN_END],
            "leakage_note": (
                "Correlation matrix computed ONLY on the Fold-1 training window, which is "
                "never a validation period in the downstream 4-fold walk-forward CV -- "
                "guarantees zero information leakage from any CV validation window into "
                "feature selection."
            ),
            "dimensions_targeted": sorted(CORE_DIMENSIONS),
            "method": "Per-dimension greedy selection on Pearson |correlation|, redundancy cutoff > 0.6",
            "corr_threshold": CORR_THRESHOLD,
            "max_per_dimension": MAX_PER_DIMENSION,
            "data_source": "quant_train (train-only window sample)",
        },
        "selected_features": selected_features,
        "n_selected": len(selected_features),
        "n_total_candidates": len(all_features),
        "selected_correlation_matrix": {
            "features": selected_cols,
            "matrix": corr_matrix_selected.tolist(),
        },
        "dimension_summary": {},
    }

    for feat, meta in selected_features.items():
        report["dimension_summary"].setdefault(meta["dimension"], []).append(feat)

    return report


def main() -> dict:
    """Run the train-only feature selection pipeline end to end."""
    logger.info("=== Feature Redundancy Analysis (t872, train-only) ===")

    OUTPUT_REPORT.parent.mkdir(parents=True, exist_ok=True)

    features = load_model_features()
    logger.info("Loaded %d candidate features from model artifact", len(features))

    db_cfg = utils.load_asset_config()
    db_path = db_cfg["database"]["db_path"]
    conn = duckdb.connect(db_path, read_only=True)

    try:
        corr_matrix = compute_correlation_matrix_sql(conn, features)
        selected_features = select_features_per_dimension(features, corr_matrix)

        selected_indices = [features.index(f) for f in selected_features]
        corr_selected = corr_matrix[np.ix_(selected_indices, selected_indices)]

        report = generate_report(selected_features, corr_selected, features)

        logger.info("Saving report to %s", OUTPUT_REPORT)
        with open(OUTPUT_REPORT, "w") as f:
            json.dump(report, f, indent=2)

        logger.info("Selected %d features across dimensions:", len(selected_features))
        for dim in sorted(CORE_DIMENSIONS):
            dims_selected = [f for f, m in selected_features.items() if m["dimension"] == dim]
            if dims_selected:
                logger.info("  %s: %s", dim, dims_selected)

        logger.info("=== Feature Redundancy Analysis Complete ===")
        return report

    finally:
        conn.close()


if __name__ == "__main__":
    main()
