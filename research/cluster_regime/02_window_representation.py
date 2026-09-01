"""Compute windowed feature representations for cluster regime analysis (t872).

For every (downsampled) observation, computes a 360-bar lookback window of
selected features and reduces it to summary statistics (mean, slope, std,
min, max). Features are z-score normalized WITHIN each window (shape-only,
not price-level) before the summary stats are computed.

Computed ONCE over the full available history (2021-01-01 .. latest OHLCV);
downstream CV folds (04_cv_cluster_optimization.py) slice this single
parquet by date range per fold rather than recomputing it four times --
window construction is a purely row-local transform (each row's summary
stats depend only on that row's own 360-bar lookback), so there is no
leakage risk in sharing one precomputed table across folds.

Pipeline (t872 rebuild) -- EXECUTION ORDER
--------------------------------------------
03_feature_selection.py   (RUN FIRST: produces feature_selection_report.json,
                            train-only on Fold-1's 2021-2022 window)
01_target_computation.py  (RUN ANYTIME: fw360 targets, independent)
02_window_representation.py (THIS SCRIPT: needs 03's output; run after 01+03)
04_cv_cluster_optimization.py (RUN LAST: walk-forward ANOVA F-score k-search)

The Epic 065 ticket lists deliverables in file-number order (01, 02, 03, 04)
but 02 has a hard runtime dependency on 03's output file, so the numbering
reflects the ticket's requested filenames, not the execution order.

Data flow
---------
live.duckdb (quant_train + N selected features)  ->  DuckDB query / Polars
    ->  360-bar rolling windows (downsampled: 30-min step)
    ->  z-score normalization within each window
    ->  summary statistics (5 stats x N features)
    ->  Polars DataFrame  ->  Parquet output
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import polars as pl
from scipy import stats

import utils
from data_handling.store.duckdb_store import ensure_tables, get_connection

logger = logging.getLogger(__name__)

# Downsampling interval (minutes)
DEFAULT_SAMPLING_INTERVAL = 30

# Window size (bars = minutes for 1-minute OHLCV)
WINDOW_SIZE = 360

FEATURE_SELECTION_REPORT_PATH = Path("artifacts") / "research_cluster_regime" / "feature_selection_report.json"


def _load_selected_features(report_path: Path | None = None) -> list[str]:
    """Load the selected feature list produced by 03_feature_selection.py.

    Args:
        report_path : Path to feature_selection_report.json. Uses default if None.

    Returns:
        List of selected feature column names.

    Raises:
        FileNotFoundError: If the report does not exist (03_feature_selection.py
            must be run first).
    """
    path = report_path or FEATURE_SELECTION_REPORT_PATH
    if not path.exists():
        raise FileNotFoundError(
            f"Feature selection report not found: {path}. "
            "Run 03_feature_selection.py first (it must run before this script)."
        )
    with open(path) as f:
        report = json.load(f)
    features = sorted(report["selected_features"].keys())
    logger.info("Loaded %d selected features from %s", len(features), path)
    return features


def _fetch_quant_train_with_targets(
    conn: duckdb.DuckDBPyConnection,
    selected_features: list[str],
) -> pl.DataFrame:
    """Fetch quant_train (selected features only) with fw360 targets attached.

    Args:
        conn              : Open DuckDB connection.
        selected_features : Feature column names to pull.

    Returns:
        Polars DataFrame with [open_time, selected_features..., mfe_targets...].
    """
    feat_cols = ", ".join(f'"{f}"' for f in selected_features)
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

    try:
        query_fw360 = """
            SELECT open_time, long_mfe_fw360, short_mfe_fw360
            FROM target_fw360
            WHERE long_mfe_fw360 IS NOT NULL AND short_mfe_fw360 IS NOT NULL
        """
        df_fw360 = conn.execute(query_fw360).pl()
        logger.info("fw360 targetok betoltve: %d rows", len(df_fw360))
        df = df_train.join(df_fw360, on="open_time", how="left")
    except Exception as e:
        logger.warning("fw360 targetok nem elerheto (%s), csak fw60 hasznalva", e)
        df = df_train

    return df


def _downsample_by_interval(
    df: pl.DataFrame,
    interval_minutes: int = DEFAULT_SAMPLING_INTERVAL,
) -> pl.DataFrame:
    """Downsample observations to every Nth minute (reduces autocorrelation).

    Args:
        df               : Input DataFrame with 'open_time' column (parsed timestamp).
        interval_minutes : Downsampling interval in minutes (default 30).

    Returns:
        Downsampled DataFrame.
    """
    df_sampled = df.with_columns(
        minute_mod=pl.col("open_time").dt.minute() % interval_minutes
    ).filter(pl.col("minute_mod") == 0).drop("minute_mod")

    logger.info(
        "Ritkitett mintavetelez: %d -> %d rows (interval=%d min)",
        len(df), len(df_sampled), interval_minutes,
    )
    return df_sampled


def _compute_summary_stats(values: np.ndarray, feature_name: str | None = None) -> dict[str, float]:
    """Compute summary statistics (mean, slope, std, min, max) for a 1D array.

    Args:
        values       : Array of floats (may contain NaN).
        feature_name : Optional name for logging.

    Returns:
        Dict with keys: mean, slope, std, min, max.
    """
    valid = values[~np.isnan(values)]

    if len(valid) < 2:
        return {"mean": np.nan, "slope": np.nan, "std": np.nan, "min": np.nan, "max": np.nan}

    stats_dict = {
        "mean": float(np.mean(valid)),
        "std": float(np.std(valid, ddof=1)) if len(valid) > 1 else np.nan,
        "min": float(np.min(valid)),
        "max": float(np.max(valid)),
    }

    try:
        x = np.arange(len(valid))
        slope, _, _, _, _ = stats.linregress(x, valid)
        stats_dict["slope"] = float(slope)
    except Exception as e:
        logger.debug("Slope szamitas kudarc (%s): %s", feature_name, e)
        stats_dict["slope"] = np.nan

    return stats_dict


def _normalize_within_window(values: np.ndarray) -> np.ndarray:
    """Z-score normalize values within a window (shape-only, not price-level).

    Args:
        values : 1D array (may contain NaN).

    Returns:
        Z-score normalized array (unchanged for constant/insufficient input).
    """
    valid = values[~np.isnan(values)]
    if len(valid) < 2:
        return values

    mean_val = np.mean(valid)
    std_val = np.std(valid, ddof=1)
    if std_val == 0:
        return values

    normalized = values.copy()
    non_nan_mask = ~np.isnan(values)
    normalized[non_nan_mask] = (values[non_nan_mask] - mean_val) / std_val
    return normalized


def _build_window_features(
    df: pl.DataFrame,
    feature_cols: list[str],
    window_size: int = WINDOW_SIZE,
) -> pl.DataFrame:
    """Build windowed summary statistics for each (downsampled) observation.

    For each row i in df, computes a lookback window [i-window_size+1, i]
    and extracts summary stats for each feature after within-window z-score
    normalization.

    Args:
        df           : Input DataFrame (must have 'open_time' + feature columns).
        feature_cols : Feature column names to window (from 03_feature_selection.py).
        window_size  : Lookback window size in bars (rows).

    Returns:
        DataFrame with [open_time, summary_stats_columns...]. Total new
        columns = len(feature_cols) * 5.
    """
    n_rows = len(df)
    feature_arrays = {col: df[col].to_numpy() for col in feature_cols}
    open_times = df["open_time"].to_list()

    result_rows: list[dict[str, Any]] = []

    for i in range(n_rows):
        if (i + 1) % 10000 == 0:
            logger.info("Processing row %d / %d", i + 1, n_rows)

        start_idx = max(0, i - window_size + 1)
        end_idx = i + 1

        row_dict: dict[str, Any] = {"open_time": open_times[i]}

        for feat_col in feature_cols:
            window_values = feature_arrays[feat_col][start_idx:end_idx]
            normalized = _normalize_within_window(window_values)
            stats_dict = _compute_summary_stats(normalized, feat_col)
            for stat_name, stat_value in stats_dict.items():
                row_dict[f"{feat_col}_{stat_name}"] = stat_value

        result_rows.append(row_dict)

    logger.info("Ablak-reprezentacio szamitva: %d rows", len(result_rows))
    return pl.DataFrame(result_rows)


def compute_window_features(
    asset_id: str | None = None,
    sampling_interval: int = DEFAULT_SAMPLING_INTERVAL,
    output_format: str = "parquet",
) -> str | pl.DataFrame | None:
    """Compute windowed feature representations for cluster regime research.

    Pipeline:
    1. Load the selected feature list (from 03_feature_selection.py output).
    2. Load quant_train (selected features only) with fw360 targets (if available).
    3. Downsample to every Nth minute (reduce autocorrelation).
    4. For each observation, compute a 360-bar lookback window; within each
       window, z-score normalize features (shape-only), then extract summary
       stats (mean, slope, std, min, max).
    5. Output as Parquet (or return DataFrame).

    Args:
        asset_id          : Asset key from config; default if None.
        sampling_interval : Downsampling interval in minutes (default 30).
        output_format      : "parquet" or "dataframe".

    Returns:
        Path to output Parquet file, DataFrame if output_format="dataframe",
        or None if no data is available.
    """
    selected_features = _load_selected_features()

    db_cfg = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]

    conn = get_connection(db_path)
    ensure_tables(conn)

    try:
        logger.info("Adatok betoltese...")
        df = _fetch_quant_train_with_targets(conn, selected_features)

        if df.is_empty():
            logger.warning("Ures quant_train -- ablak-reprezentacio szamitas kihagyva")
            return None

        logger.info("Ritkitett mintavetelez (%d perc / megfigyeles)...", sampling_interval)
        df = _downsample_by_interval(df, sampling_interval)

        if df.is_empty():
            logger.warning("Ritkites utan ures adat -- szamitas kihagyva")
            return None

        logger.info("Ablak-reprezentacio szamitasa (%d bar lookback)...", WINDOW_SIZE)
        df_windows = _build_window_features(df, selected_features, WINDOW_SIZE)

        target_cols = ["open_time"]
        for col in ("long_mfe_fw360", "short_mfe_fw360", "long_mfe_fw60", "short_mfe_fw60"):
            if col in df.columns:
                target_cols.append(col)

        df_targets = df.select(target_cols)
        df_result = df_windows.join(df_targets, on="open_time", how="left")

        logger.info(
            "Ablak-reprezentacio keszult: %d rows, %d cols",
            len(df_result), len(df_result.columns),
        )

        if output_format == "dataframe":
            return df_result

        if output_format == "parquet":
            artifacts_dir = Path("artifacts") / "research_cluster_regime"
            artifacts_dir.mkdir(parents=True, exist_ok=True)

            asset_name = asset_id or "solusdt"
            output_path = artifacts_dir / f"{asset_name}_window_repr.parquet"
            df_result.write_parquet(str(output_path))
            logger.info("Ablak-reprezentacio Parquetbe irva: %s", output_path)
            return str(output_path)

        raise ValueError(f"Unknown output_format: {output_format}")

    finally:
        conn.close()


def validate_window_representation(asset_id: str | None = None) -> dict[str, Any]:
    """Validate window representation quality (NaN ratio, normality check).

    Args:
        asset_id : Asset key; default if None.

    Returns:
        Dictionary with validation metrics.
    """
    selected_features = _load_selected_features()

    db_cfg = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]

    conn = get_connection(db_path)
    ensure_tables(conn)

    try:
        logger.info("Ablak-reprezentacio validacioja...")
        df = _fetch_quant_train_with_targets(conn, selected_features)
        df = _downsample_by_interval(df, DEFAULT_SAMPLING_INTERVAL)
        df_windows = _build_window_features(df, selected_features, WINDOW_SIZE)

        validation: dict[str, Any] = {}

        nan_counts = {
            col: df_windows[col].null_count() / len(df_windows)
            for col in df_windows.columns
            if col != "open_time"
        }
        validation["nan_counts"] = {col: float(ratio) for col, ratio in nan_counts.items()}

        ks_stats = []
        for col in df_windows.columns:
            if col != "open_time" and "_mean" in col:
                vals = df_windows[col].drop_nulls().to_numpy()
                if len(vals) > 100:
                    ks_stat, ks_pval = stats.kstest(vals, "norm")
                    ks_stats.append((col, ks_stat, ks_pval))

        if ks_stats:
            validation["ks_test_mean_pval"] = float(np.mean([pval for _, _, pval in ks_stats]))
            logger.info("KS test (normalized features): mean p-value = %.4f", validation["ks_test_mean_pval"])

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
