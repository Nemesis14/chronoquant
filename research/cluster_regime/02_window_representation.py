"""Compute windowed feature representations for cluster regime analysis.

For every (downsampled) observation, computes a 360×10 lookback window
of selected features and reduces it to summary statistics (mean, slope, std, min, max).
Then normalizes features via z-score within each window (shape-only, not price-level).

Pipeline
--------
01_target_computation (fw360 targets)  →  02_window_representation  →  03_clustering

Data flow
---------
live.duckdb (quant_train + 14 selected features)  →  DuckDB window functions / Polars
    →  360-bar rolling windows (ritkított mintavétel: 15-30 perces lépcső)
    →  summary statistics (70 features: 5 stats × 14 features)
    →  z-score normalizálás az ablak belül
    →  Polars DataFrame  →  Parquet output
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import polars as pl
from scipy import stats

import utils
from data_handling.store.duckdb_store import get_connection, ensure_tables

logger = logging.getLogger(__name__)

# Downsampling interval (minutes)
DEFAULT_SAMPLING_INTERVAL = 30

# Window size (bars = minutes for 1-minute OHLCV)
WINDOW_SIZE = 360

# Selected features (from t863 feature_selection_report.json)
SELECTED_FEATURES = [
    "feat_body_ratio",
    "feat_body_ratio_sma_10",
    "feat_lr_r2_30",
    "feat_efficiency_ratio_10",
    "feat_adx_14",
    "feat_return_momentum_delta_10",
    "feat_return_autocorr_lag1_30",
    "feat_returns_skew_14",
    "feat_directional_agreement_10_30",
    "feat_lr_slope_60",
    "feat_bb_width_rank_20",
    "feat_hist_vol_rank_60",
    "feat_ad_line",
    "feat_volume_ratio_14",
]


def _fetch_quant_train_with_targets(
    conn: duckdb.DuckDBPyConnection,
    asset_id: str | None = None,
) -> pl.DataFrame:
    """Fetch quant_train with fw360 targets.

    Reads:
    - quant_train table (features + long_mfe_fw60, short_mfe_fw60)
    - fw360 targets (long_mfe_fw360, short_mfe_fw360) if available

    Returns:
        Polars DataFrame with [open_time, selected_features..., mfe_targets...]
    """
    # Build feature selector list
    feat_cols = ", ".join(f'"{f}"' for f in SELECTED_FEATURES)
    query_train = f"""
        SELECT
            open_time,
            {feat_cols},
            long_mfe_fw60,
            short_mfe_fw60
        FROM quant_train
        ORDER BY open_time
    """
    df_train = conn.execute(query_train).pl()
    logger.info("Quant_train betoltve: %d rows", len(df_train))

    # Try to attach fw360 targets if available
    try:
        query_fw360 = """
            SELECT open_time, long_mfe_fw360, short_mfe_fw360
            FROM target_fw360
            WHERE long_mfe_fw360 IS NOT NULL AND short_mfe_fw360 IS NOT NULL
        """
        df_fw360 = conn.execute(query_fw360).pl()
        logger.info("fw360 targetok betoltve: %d rows", len(df_fw360))

        # Join on open_time
        df = df_train.join(df_fw360, on="open_time", how="left")
    except Exception as e:
        logger.warning("fw360 targetok nem elerheto (%s), csak fw60 hasznalva", e)
        df = df_train

    return df


def _downsample_by_interval(
    df: pl.DataFrame,
    interval_minutes: int = DEFAULT_SAMPLING_INTERVAL,
) -> pl.DataFrame:
    """Downsample observations to every Nth minute.

    Selects rows where minute % interval_minutes == 0.
    This reduces autocorrelation in windowed computations.

    Args:
        df              : Input DataFrame with 'open_time' column (parsed timestamp).
        interval_minutes: Downsampling interval in minutes (default 30).

    Returns:
        Downsampled DataFrame.
    """
    # Extract minute from open_time and filter
    df_sampled = df.with_columns(
        minute_mod=pl.col("open_time").dt.minute() % interval_minutes
    ).filter(pl.col("minute_mod") == 0).drop("minute_mod")

    logger.info(
        "Ritkitett mintavetelez: %d -> %d rows (interval=%d min)",
        len(df), len(df_sampled), interval_minutes
    )
    return df_sampled


def _compute_summary_stats(
    values: list[float],
    feature_name: str | None = None,
) -> dict[str, float]:
    """Compute summary statistics for a 1D array.

    Args:
        values       : List of floats (may contain NaN).
        feature_name : Optional name for logging.

    Returns:
        Dict with keys: mean, slope, std, min, max
    """
    arr = np.array(values)
    valid = arr[~np.isnan(arr)]

    if len(valid) < 2:
        # Not enough data; return NaNs
        return {
            "mean": np.nan,
            "slope": np.nan,
            "std": np.nan,
            "min": np.nan,
            "max": np.nan,
        }

    stats_dict = {
        "mean": float(np.mean(valid)),
        "std": float(np.std(valid, ddof=1) if len(valid) > 1 else np.nan),
        "min": float(np.min(valid)),
        "max": float(np.max(valid)),
    }

    # Compute slope via linear regression (indices vs values)
    try:
        x = np.arange(len(valid))
        slope, _, _, _, _ = stats.linregress(x, valid)
        stats_dict["slope"] = float(slope)
    except Exception as e:
        logger.debug("Slope szamitas kudarc (%s): %s", feature_name, e)
        stats_dict["slope"] = np.nan

    return stats_dict


def _normalize_within_window(
    values: np.ndarray,
) -> np.ndarray:
    """Normalize values via z-score within the window (shape-only normalization).

    Args:
        values : 1D array (may contain NaN).

    Returns:
        Z-score normalized array (NaN for NaN inputs).
    """
    valid = values[~np.isnan(values)]
    if len(valid) < 2:
        return values  # Cannot normalize; return as-is

    mean_val = np.mean(valid)
    std_val = np.std(valid, ddof=1)

    if std_val == 0:
        return values  # Constant values; return as-is

    # Apply z-score only to non-NaN values
    normalized = values.copy()
    non_nan_mask = ~np.isnan(values)
    normalized[non_nan_mask] = (values[non_nan_mask] - mean_val) / std_val

    return normalized


def _build_window_features(
    df: pl.DataFrame,
    window_size: int = WINDOW_SIZE,
    feature_cols: list[str] | None = None,
) -> pl.DataFrame:
    """Build windowed summary statistics for each (downsampled) observation.

    For each row i in df, computes a lookback window [i-window_size+1, i]
    and extracts summary stats (mean, slope, std, min, max) for each feature.

    Args:
        df           : Input DataFrame (must have 'open_time' + feature columns).
        window_size  : Lookback window size in bars (rows).
        feature_cols : List of feature column names. If None, uses SELECTED_FEATURES.

    Returns:
        DataFrame with [open_time, summary_stats_columns...]
            - N_features = len(feature_cols)
            - N_stats = 5 (mean, slope, std, min, max)
            - Total new columns = N_features * 5
    """
    if feature_cols is None:
        feature_cols = SELECTED_FEATURES

    # Convert to numpy for efficient window operations
    n_rows = len(df)
    feature_arrays = {col: df[col].to_numpy() for col in feature_cols}
    open_times = df["open_time"].to_list()

    # Result storage
    result_rows = []

    for i in range(n_rows):
        if (i + 1) % 10000 == 0:
            logger.info("Processing row %d / %d", i + 1, n_rows)

        # Define window [i - window_size + 1, i] (inclusive)
        start_idx = max(0, i - window_size + 1)
        end_idx = i + 1

        row_dict = {"open_time": open_times[i]}

        # Process each feature
        for feat_col in feature_cols:
            feat_arr = feature_arrays[feat_col]
            window_values = feat_arr[start_idx:end_idx]

            # Normalize within window (z-score)
            normalized = _normalize_within_window(window_values)

            # Compute summary stats on normalized values
            stats_dict = _compute_summary_stats(normalized, feat_col)

            # Store with feature-specific prefix
            for stat_name, stat_value in stats_dict.items():
                col_name = f"{feat_col}_{stat_name}"
                row_dict[col_name] = stat_value

        result_rows.append(row_dict)

    logger.info("Ablak-reprezentacio szamitva: %d rows", len(result_rows))

    # Convert to Polars DataFrame
    return pl.DataFrame(result_rows)


def compute_window_features(
    asset_id: str | None = None,
    sampling_interval: int = DEFAULT_SAMPLING_INTERVAL,
    output_format: str = "parquet",
) -> str | pl.DataFrame:
    """Compute windowed feature representations for cluster regime research.

    Pipeline:
    1. Load quant_train with fw360 targets (if available).
    2. Downsample to every Nth minute (reduce autocorrelation).
    3. For each observation, compute 360-bar lookback window.
    4. Within each window: z-score normalize features (shape-only), then
       extract summary stats (mean, slope, std, min, max).
    5. Output as Parquet (or return DataFrame).

    Args:
        asset_id         : Asset key from config; default if None.
        sampling_interval: Downsampling interval in minutes (default 30).
        output_format    : "parquet" or "dataframe".

    Returns:
        Path to output Parquet file or DataFrame if output_format="dataframe".
    """
    db_cfg = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]

    conn = get_connection(db_path)
    ensure_tables(conn)

    try:
        # Step 1: Load data
        logger.info("Adatok betoltese...")
        df = _fetch_quant_train_with_targets(conn)

        if df.is_empty():
            logger.warning("Ures quant_train — ablak-reprezentacio szamitas kihagyva")
            return None

        # Step 2: Downsample
        logger.info("Ritkitett mintavetelez (%d perc / megfigyelés)...", sampling_interval)
        df = _downsample_by_interval(df, sampling_interval)

        if df.is_empty():
            logger.warning("Ritkites utan ures adat — szamitas kihagyva")
            return None

        # Step 3: Build window features
        logger.info("Ablak-reprezentacio szamitasa (%d bar lookback)...", WINDOW_SIZE)
        df_windows = _build_window_features(df, WINDOW_SIZE, SELECTED_FEATURES)

        # Step 4: Attach targets (if available)
        # Select only open_time and fw360 targets from original df
        target_cols = ["open_time"]
        if "long_mfe_fw360" in df.columns:
            target_cols.append("long_mfe_fw360")
        if "short_mfe_fw360" in df.columns:
            target_cols.append("short_mfe_fw360")
        if "long_mfe_fw60" in df.columns:
            target_cols.append("long_mfe_fw60")
        if "short_mfe_fw60" in df.columns:
            target_cols.append("short_mfe_fw60")

        df_targets = df.select(target_cols)
        df_result = df_windows.join(df_targets, on="open_time", how="left")

        logger.info("Ablak-reprezentacio keszult: %d rows, %d cols",
                    len(df_result), len(df_result.columns))

        # Step 5: Output
        if output_format == "dataframe":
            return df_result

        elif output_format == "parquet":
            artifacts_dir = Path("artifacts") / "research_cluster_regime"
            artifacts_dir.mkdir(parents=True, exist_ok=True)

            asset_name = asset_id or "solusdt"
            output_path = artifacts_dir / f"{asset_name}_window_repr.parquet"
            df_result.write_parquet(str(output_path))
            logger.info("Ablak-reprezentacio Parquetbe irva: %s", output_path)
            return str(output_path)

        else:
            raise ValueError(f"Unknown output_format: {output_format}")

    finally:
        conn.close()


def validate_window_representation(
    asset_id: str | None = None,
) -> dict[str, Any]:
    """Validate window representation quality.

    Checks:
    - NaN/NULL distribution in summary stats
    - Distribution of normalized features (should be ~N(0,1))
    - Kolmogorov-Smirnov test for normality on pooled normalized feature samples

    Args:
        asset_id : Asset key; default if None.

    Returns:
        Dictionary with validation metrics.
    """
    db_cfg = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]

    conn = get_connection(db_path)
    ensure_tables(conn)

    try:
        logger.info("Ablak-reprezentacio validacioja...")
        df = _fetch_quant_train_with_targets(conn)
        df = _downsample_by_interval(df, DEFAULT_SAMPLING_INTERVAL)
        df_windows = _build_window_features(df, WINDOW_SIZE, SELECTED_FEATURES)

        validation = {}

        # Check NaN ratio
        nan_counts = {}
        for col in df_windows.columns:
            if col != "open_time":
                nan_ratio = df_windows[col].null_count() / len(df_windows)
                nan_counts[col] = nan_ratio

        validation["nan_counts"] = {
            col: float(ratio) for col, ratio in nan_counts.items()
        }

        # Kolmogorov-Smirnov test on pooled normalized features
        # (we already normalized within windows, so should be ~N(0,1))
        ks_stats = []
        for col in df_windows.columns:
            if col != "open_time" and "_mean" in col:
                # Use mean as proxy for normalized feature distribution
                vals = df_windows[col].drop_nulls().to_numpy()
                if len(vals) > 100:
                    ks_stat, ks_pval = stats.kstest(vals, "norm")
                    ks_stats.append((col, ks_stat, ks_pval))

        if ks_stats:
            validation["ks_test_mean_pval"] = float(
                np.mean([pval for _, _, pval in ks_stats])
            )
            logger.info(
                "KS test (normalized features): mean p-value = %.4f",
                validation["ks_test_mean_pval"]
            )

        validation["n_rows"] = len(df_windows)
        validation["n_cols"] = len(df_windows.columns)
        logger.info("Validacio eredmenye: %s", validation)

        return validation

    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Ablak-reprezentacio szamitasa...")
    result = compute_window_features(output_format="parquet")
    logger.info("Eredmeny: %s", result)

    logger.info("Validacio...")
    validation = validate_window_representation()
    logger.info("Validacio eredmenye: %s", validation)
