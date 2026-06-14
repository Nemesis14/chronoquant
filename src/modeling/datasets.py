# =============================================================================
# Modeling dataset builder
# =============================================================================
# Purpose:
#  - Load aligned model matrices from the features dataset
#  - Keep dataset preparation independent from model family and feature selection
# =============================================================================

from dataclasses import dataclass
from store.duckdb_query import dataset_columns, query_range

import pandas as pd

import utils


# =============================================================================
# ModelingDataset
# =============================================================================
# Purpose:
#  - Hold model-ready open_time, X, y, and metadata
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
#  - Load target and feature columns from the configured features dataset
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
    data_dir        : str | None = None,
    asset_id        : str | None = None,
) -> ModelingDataset:
    """Load a model-ready dataset from the features Parquet partitions.

    Args:
        target_col      : Target column name (e.g. 'trg_l_fw60_q90').
        feature_cols    : Feature columns to load. None = all feat_* columns.
        start           : Optional lower bound (inclusive), YYYY-MM-DD HH:MM:SS.
        end             : Optional upper bound (inclusive), YYYY-MM-DD HH:MM:SS.
        embargo_minutes : Drop the last N minutes to avoid forward-looking leakage.
        row_stride      : Take every N-th row (1 = all rows).
        dropna_features : Drop rows where any feature is NaN.
        data_dir        : Override data directory; uses asset config if None.
        asset_id        : Asset key from config/assets.json.

    Returns:
        ModelingDataset with open_time, X, y, and metadata.

    Raises:
        ValueError: If target or feature columns are missing, or row_stride < 1.
    """
    resolved_dir: str = data_dir or utils.load_asset_config(asset_id)["database"]["data_dir"]
    if row_stride < 1:
        raise ValueError("row_stride must be >= 1")

    columns = dataset_columns(resolved_dir, "features")
    if target_col not in columns:
        raise ValueError(f"Target column not found in features dataset: {target_col}")

    if feature_cols is None:
        feature_cols = [col for col in columns if col.startswith("feat_")]

    missing_features = [col for col in feature_cols if col not in columns]
    if missing_features:
        raise ValueError(f"Feature columns not found in features dataset: {missing_features}")

    select_cols = ["open_time", target_col] + feature_cols
    df = query_range(resolved_dir, "features", start=start, end=end, columns=select_cols)

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

    df["open_time"] = pd.to_datetime(df["open_time"])
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
