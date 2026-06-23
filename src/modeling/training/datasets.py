# =============================================================================
# Modeling dataset builder
# =============================================================================
# Purpose:
#  - Load aligned model matrices from feat_ohlcv_quant and target datasets
#  - Keep dataset preparation independent from model family and feature selection
#
# DEPRECATED — load_modeling_dataset() is a legacy path (feat_ohlcv_quant + target
# table join, time-range scoped).  The pipeline steps (search, train) use the
# snapshot-native path instead: snap."<snapshot_id>" INNER JOIN
# model."<model_id>__sample" on open_time (see lgbm_search._load_search_dataset
# and fit_lgbm._load_train_data).  This module is retained for compatibility but
# is no longer called from any pipeline step.
# =============================================================================

from dataclasses import dataclass

import pandas as pd

import utils
from data_handling.store.duckdb_query import dataset_columns, query_range


# =============================================================================
# ModelingDataset
# =============================================================================
@dataclass(frozen=True)
class ModelingDataset:
    open_time: pd.Series
    X: pd.DataFrame
    y: pd.Series
    target_col: str
    feature_cols: list[str]

    def to_frame(self) -> pd.DataFrame:
        df = pd.DataFrame({"open_time": self.open_time, self.target_col: self.y})
        return pd.concat([df.reset_index(drop=True), self.X.reset_index(drop=True)], axis=1)


# =============================================================================
# load_modeling_dataset(...) -> ModelingDataset
# =============================================================================
# Purpose:
#  - Load feature columns from feat_ohlcv_quant and target column from target table
#  - Apply optional time bounds and target-horizon embargo
# =============================================================================
def load_modeling_dataset(
    target_col      : str,
    feature_cols    : list[str] | None = None,
    start           : str | None = None,
    end             : str | None = None,
    embargo_minutes : int = 0,
    row_stride      : int = 1,
    dropna_features : bool = False,
    db_path         : str | None = None,
    asset_id        : str | None = None,
) -> ModelingDataset:
    """Load a model-ready dataset from feat_ohlcv_quant (features) and target (labels).

    Features and targets are stored in separate tables and joined on open_time.
    Only rows where both feature and target data are available are returned.

    Args:
        target_col      : Target column name (e.g. 'long_mfe_fw60').
        feature_cols    : Feature columns to load. None = all feat_* columns.
        start           : Optional lower bound (inclusive), YYYY-MM-DD HH:MM:SS.
        end             : Optional upper bound (inclusive), YYYY-MM-DD HH:MM:SS.
        embargo_minutes : Drop the last N minutes to avoid forward-looking leakage.
        row_stride      : Take every N-th row (1 = all rows).
        dropna_features : Drop rows where any feature is NaN.
        db_path         : Override .duckdb path; uses asset config if None.
        asset_id        : Asset key from config/assets.json.

    Returns:
        ModelingDataset with open_time, X, y, and metadata.

    Raises:
        ValueError: If target column is missing from target table, feature columns
            are missing from feat_ohlcv_quant, or row_stride < 1.
    """
    resolved_db: str = db_path or utils.load_asset_config(asset_id)["database"]["db_path"]
    if row_stride < 1:
        raise ValueError("row_stride must be >= 1")

    # --- validate target column exists in target table ---
    target_cols_available = dataset_columns(resolved_db, "target")
    if target_col not in target_cols_available:
        raise ValueError(f"Target column not found in target dataset: {target_col}")

    # --- resolve feature columns from feat_ohlcv_quant ---
    feat_cols_available = dataset_columns(resolved_db, "feat_ohlcv_quant")
    if feature_cols is None:
        feature_cols = [col for col in feat_cols_available if col.startswith("feat_")]

    missing_features = [col for col in feature_cols if col not in feat_cols_available]
    if missing_features:
        raise ValueError(f"Feature columns not found in feat_ohlcv_quant dataset: {missing_features}")

    # --- load features ---
    feat_select = ["open_time"] + feature_cols
    feat_df = query_range(resolved_db, "feat_ohlcv_quant", start=start, end=end, columns=feat_select)

    # --- load targets ---
    target_df = query_range(
        resolved_db, "target",
        start   = start,
        end     = end,
        columns = ["open_time", target_col],
    )

    # --- join on open_time ---
    if feat_df.empty or target_df.empty:
        return ModelingDataset(
            open_time    = pd.Series(dtype="datetime64[ns]"),
            X            = pd.DataFrame(columns=feature_cols),
            y            = pd.Series(dtype="float64"),
            target_col   = target_col,
            feature_cols = feature_cols,
        )

    feat_df["open_time"]   = pd.to_datetime(feat_df["open_time"])
    target_df["open_time"] = pd.to_datetime(target_df["open_time"])
    df = feat_df.merge(target_df, on="open_time", how="inner")

    if row_stride > 1:
        df = df.iloc[::row_stride].copy()

    if df.empty:
        return ModelingDataset(
            open_time    = pd.Series(dtype="datetime64[ns]"),
            X            = pd.DataFrame(columns=feature_cols),
            y            = pd.Series(dtype="float64"),
            target_col   = target_col,
            feature_cols = feature_cols,
        )

    # --- drop rows where target is null (last horizon rows) ---
    df = df[df[target_col].notna()].copy()

    if embargo_minutes > 0 and not df.empty:
        max_time = df["open_time"].max()
        cutoff   = max_time - pd.Timedelta(minutes=embargo_minutes)
        df       = df[df["open_time"] <= cutoff].copy()

    if dropna_features:
        df = df[pd.DataFrame(df[feature_cols]).notna().all(axis=1)].copy()

    open_time_s = pd.Series(df["open_time"]).reset_index(drop=True)
    X_df        = pd.DataFrame(df[feature_cols]).reset_index(drop=True)
    y_s         = pd.Series(df[target_col]).reset_index(drop=True)

    return ModelingDataset(
        open_time    = open_time_s,
        X            = X_df,
        y            = y_s,
        target_col   = target_col,
        feature_cols = feature_cols,
    )
