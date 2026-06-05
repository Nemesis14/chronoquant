# =============================================================================
# Modeling dataset builder
# =============================================================================
# Purpose:
#  - Load aligned model matrices from the features table
#  - Keep dataset preparation independent from model family and feature selection
# =============================================================================

from dataclasses import dataclass
import sqlite3

import pandas as pd

import utils
from db.table_ops import table_columns


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
#  - Load target and feature columns from the configured features table
#  - Apply optional time bounds and target-horizon embargo
# =============================================================================
def load_modeling_dataset(
    target_col: str,
    feature_cols: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    embargo_minutes: int = 0,
    row_stride: int = 1,
    dropna_features: bool = False,
    db_path: str | None = None,
    table_name: str | None = None,
    asset_id: str | None = None,
) -> ModelingDataset:
    db_cfg = utils.load_asset_config(asset_id)["database"]
    db_path = db_path or db_cfg["db_path"]
    table_name = table_name or db_cfg["tables"]["features"]
    if row_stride < 1:
        raise ValueError("row_stride must be >= 1")

    columns = table_columns(db_path, table_name)
    if target_col not in columns:
        raise ValueError(f"Target column not found in {table_name}: {target_col}")

    if feature_cols is None:
        feature_cols = [col for col in columns if col.startswith("feat_")]

    missing_features = [col for col in feature_cols if col not in columns]
    if missing_features:
        raise ValueError(f"Feature columns not found in {table_name}: {missing_features}")

    select_cols = ["open_time", target_col] + feature_cols
    query = f"""
        SELECT {', '.join(_quote_identifier(col) for col in select_cols)}
        FROM {table_name}
        WHERE (? IS NULL OR open_time >= ?)
            AND (? IS NULL OR open_time <= ?)
            AND (
                ? <= 1
                OR rowid % ? = 0
            )
        ORDER BY open_time ASC
    """

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            query,
            conn,
            params=(start, start, end, end, row_stride, row_stride),
        )

    if df.empty:
        return ModelingDataset(
            open_time=pd.Series(dtype="datetime64[ns]"),
            X=pd.DataFrame(columns=feature_cols),
            y=pd.Series(dtype="float64"),
            target_col=target_col,
            feature_cols=feature_cols,
        )

    df["open_time"] = pd.to_datetime(df["open_time"])
    df = df.dropna(subset=[target_col]).copy()

    if embargo_minutes > 0 and not df.empty:
        max_time = df["open_time"].max()
        cutoff = max_time - pd.Timedelta(minutes=embargo_minutes)
        df = df[df["open_time"] <= cutoff].copy()

    if dropna_features:
        df = df.dropna(subset=feature_cols).copy()

    return ModelingDataset(
        open_time=df["open_time"].reset_index(drop=True),
        X=df[feature_cols].reset_index(drop=True),
        y=df[target_col].reset_index(drop=True),
        target_col=target_col,
        feature_cols=feature_cols,
    )


# =============================================================================
# _quote_identifier(name: str) -> str
# =============================================================================
# Purpose:
#  - Quote SQLite identifiers safely for known column names
# =============================================================================
def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
