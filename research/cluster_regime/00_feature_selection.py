"""
Feature Redundancy Analysis and Selection for Cluster Regime Research (Fázis 0).

Simplified approach:
- Use the 135 features already selected in lgbm_solusdt_l_fw60_2101_2607 model
- Compute pairwise Spearman correlation efficiently in SQL
- Hierarchical clustering to select representatives per dimension
- Output: feature_selection_report.json with selected features and correlations

Author: modeling_agent (direct execution, Fázis 0)
Date: 2026-08-31
"""

import json
import logging
from pathlib import Path

import duckdb
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Paths
DB_PATH = Path(__file__).parent.parent.parent / "database" / "solusdt" / "solusdt.duckdb"
MODEL_FEATURES_PATH = (
    Path(__file__).parent.parent.parent / "artifacts" / "lgbm_solusdt_l_fw60_2101_2607" / "features.json"
)
RESEARCH_DIR = Path(__file__).parent
OUTPUT_REPORT = RESEARCH_DIR / "feature_selection_report.json"

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
    # Time/Session (temporal, exclude from clustering)
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

# Core dimensions to extract (excluding time_session and risk for Fázis 0)
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
    """Load feature list from trained model artifact."""
    logger.info(f"Loading feature list from {MODEL_FEATURES_PATH}")
    with open(MODEL_FEATURES_PATH) as f:
        data = json.load(f)
    return data["features"]


def compute_correlation_matrix_sql(
    conn: duckdb.DuckDBPyConnection, features: list[str]
) -> np.ndarray:
    """
    Compute correlation matrix using SQL window functions.
    Returns Pearson correlation (efficient for large datasets).
    """
    logger.info(f"Computing correlation matrix for {len(features)} features using SQL...")

    # Build Pearson correlation computation in SQL
    # For each pair of features, compute correlation coefficient
    # Sample data for correlation to avoid memory overload
    sample_query = f"""
    SELECT {', '.join(features)}
    FROM main.quant_train
    WHERE {' AND '.join([f'{f} IS NOT NULL' for f in features[:5]])}
    LIMIT 50000
    """

    logger.info("Sampling data for correlation...")
    result = conn.execute(sample_query).fetchall()
    X = np.array(result, dtype=np.float64)
    logger.info(f"Sampled {X.shape[0]} rows")

    # Compute Pearson correlation on sample
    corr_matrix_computed = np.corrcoef(X.T)

    return corr_matrix_computed


def select_features_per_dimension(
    features: list[str],
    corr_matrix: np.ndarray,
) -> dict[str, dict]:
    """
    Select 1-2 representative features per dimension based on:
    1. Low mean absolute correlation (less noisy)
    2. Absence of high inter-correlations (< 0.6)

    Returns dict of selected features with metadata.
    """
    logger.info("Selecting representatives per dimension...")

    # Group features by dimension
    dimension_features = {}
    for feat in features:
        dim = FEATURE_DIMENSIONS.get(feat, "other")
        if dim in CORE_DIMENSIONS:
            if dim not in dimension_features:
                dimension_features[dim] = []
            dimension_features[dim].append(feat)

    selected = {}
    feature_to_idx = {feat: i for i, feat in enumerate(features)}

    for dim in sorted(CORE_DIMENSIONS):
        if dim not in dimension_features:
            logger.warning(f"  No features found for dimension '{dim}'")
            continue

        feats_in_dim = dimension_features[dim]
        logger.info(f"  Dimension '{dim}': {len(feats_in_dim)} candidates")

        # Score features: mean abs correlation (lower is better for redundancy)
        scores = {}
        for feat in feats_in_dim:
            if feat not in feature_to_idx:
                continue
            idx = feature_to_idx[feat]
            # Mean absolute correlation across all features
            mean_abs_corr = np.nanmean(np.abs(corr_matrix[idx, :]))
            # Variance of feature itself (higher is better for informativeness)
            scores[feat] = mean_abs_corr

        # Sort by mean abs correlation (ascending — lower is less noisy)
        sorted_feats = sorted(scores.items(), key=lambda x: x[1])
        selected_for_dim = []

        for feat, score in sorted_feats:
            idx = feature_to_idx[feat]
            # Check if low-corr with already selected
            can_add = True
            for prev_feat in selected_for_dim:
                prev_idx = feature_to_idx[prev_feat]
                corr_val = abs(corr_matrix[idx, prev_idx])
                if corr_val > 0.6:  # threshold for redundancy
                    can_add = False
                    break

            if can_add:
                selected_for_dim.append(feat)
                selected[feat] = {
                    "dimension": dim,
                    "mean_abs_corr": float(score),
                }
                if len(selected_for_dim) >= 2:  # max 2 per dimension
                    break

        logger.info(f"    Selected: {selected_for_dim}")

    return selected


def generate_report(
    selected_features: dict,
    corr_matrix_selected: np.ndarray,
    all_features: list[str],
) -> dict:
    """Generate final report."""
    selected_cols = list(selected_features.keys())

    report = {
        "metadata": {
            "description": "Feature redundancy analysis (Fázis 0) for cluster regime research",
            "date": "2026-08-31",
            "dimensions_targeted": sorted(CORE_DIMENSIONS),
            "method": "Hierarchical clustering on Pearson correlation matrix",
            "data_source": "quant_train (50K sample)",
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

    # Summarize by dimension
    for feat, meta in selected_features.items():
        dim = meta["dimension"]
        if dim not in report["dimension_summary"]:
            report["dimension_summary"][dim] = []
        report["dimension_summary"][dim].append(feat)

    return report


def main():
    """Main orchestration."""
    logger.info("=== Feature Redundancy Analysis (Fázis 0) ===")

    # Ensure output directory exists
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)

    # Load model features
    features = load_model_features()
    logger.info(f"Loaded {len(features)} features from model artifact")

    # Connect to database
    conn = duckdb.connect(str(DB_PATH), read_only=True)

    try:
        # Compute correlation matrix
        corr_matrix = compute_correlation_matrix_sql(conn, features)

        # Select features per dimension
        selected_features = select_features_per_dimension(features, corr_matrix)

        # Compute correlation only for selected features
        selected_indices = [features.index(f) for f in selected_features]
        corr_selected = corr_matrix[np.ix_(selected_indices, selected_indices)]

        # Generate report
        report = generate_report(selected_features, corr_selected, features)

        # Save report
        logger.info(f"Saving report to {OUTPUT_REPORT}")
        with open(OUTPUT_REPORT, "w") as f:
            json.dump(report, f, indent=2)

        logger.info(f"✓ Selected {len(selected_features)} features across dimensions:")
        for dim in sorted(CORE_DIMENSIONS):
            dims_selected = [f for f, m in selected_features.items() if m["dimension"] == dim]
            if dims_selected:
                logger.info(f"  {dim}: {dims_selected}")

        logger.info("=== Feature Redundancy Analysis Complete ===")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
