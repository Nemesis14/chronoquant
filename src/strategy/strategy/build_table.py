"""Strategy table builder for ChronoQuant.

Loads two LightGBM models (long + short), runs predictions on quant_train,
joins targets, and writes strategy_table.parquet to artifacts/{session_id}/.
"""

import json
import logging
import pickle
from pathlib import Path

import utils
from data_handling.store.duckdb_query import query_range

logger = logging.getLogger(__name__)

# %% Internal helpers


def _load_model_and_features(model_id: str) -> tuple:
    """Load a pickled LightGBM model and its feature list from the artifact directory.

    Args:
        model_id: Model artifact directory name under artifacts/.

    Returns:
        Tuple of (lgbm_model, feature_list) where feature_list is list[str].

    Raises:
        FileNotFoundError: If model.pkl or features.json are absent.
    """
    artifact_dir   = Path(utils._resolve_path("artifacts")) / model_id
    model_path     = artifact_dir / "model.pkl"
    features_path  = artifact_dir / "features.json"

    if not model_path.exists():
        raise FileNotFoundError(f"model.pkl not found: {model_path}")
    if not features_path.exists():
        raise FileNotFoundError(f"features.json not found: {features_path}")

    with open(model_path, "rb") as f:
        model = pickle.load(f)  # noqa: S301

    with open(features_path, encoding="utf-8") as f:
        features_cfg = json.load(f)

    feature_list: list[str] = features_cfg["features"]
    logger.debug("Loaded model %s with %d features", model_id, len(feature_list))
    return model, feature_list


def _date_to_range(date_str: str) -> tuple[str, str]:
    """Convert a YYYY-MM-DD date string to DuckDB open_time range strings.

    Args:
        date_str: Date string in YYYY-MM-DD format.

    Returns:
        Tuple of (start_ts, end_ts) in YYYY-MM-DD HH:MM:SS format,
        representing midnight to 23:59:59 on the given date.
    """
    return f"{date_str} 00:00:00", f"{date_str} 23:59:59"


# %% Public API


def build_strategy_table(
    long_model_id  : str,
    short_model_id : str,
    start          : str,
    end            : str,
    session_id     : str,
) -> Path:
    """Build strategy table from two model predictions and targets.

    Loads features from quant_train for both models, runs predict() on each,
    joins long_mfe_fw60 and short_mfe_fw60 from the target table.
    Writes strategy_table.parquet to artifacts/{session_id}/.

    Args:
        long_model_id : Model artifact ID for the long direction.
        short_model_id: Model artifact ID for the short direction.
        start         : Start date inclusive, YYYY-MM-DD.
        end           : End date inclusive, YYYY-MM-DD.
        session_id    : Strategy session identifier for artifact output path.

    Returns:
        Path to the written strategy_table.parquet file.

    Raises:
        FileNotFoundError: If model artifacts are missing.
        ValueError: If quant_train or target tables return no data for the range.
    """
    # --- Load models and feature lists ---
    logger.info("Loading long model: %s", long_model_id)
    long_model,  long_features  = _load_model_and_features(long_model_id)

    logger.info("Loading short model: %s", short_model_id)
    short_model, short_features = _load_model_and_features(short_model_id)

    # --- Union of features for a single DB query ---
    all_features = sorted(set(long_features) | set(short_features))
    logger.info("Querying %d unique features from quant_train", len(all_features))

    # --- Query quant_train ---
    db_path    = utils.load_asset_config()["database"]["db_path"]
    start_ts   = f"{start} 00:00:00"
    end_ts     = f"{end} 23:59:59"

    feat_df = query_range(
        db_path,
        "quant_train",
        start   = start_ts,
        end     = end_ts,
        columns = ["open_time"] + all_features,
    )
    if feat_df.empty:
        raise ValueError(f"quant_train returned no rows for range {start} to {end}")
    logger.info("quant_train rows loaded: %d", len(feat_df))

    # --- Predict long and short ---
    logger.info("Running long model predict()")
    feat_df["pred_long_raw"] = long_model.predict(feat_df[long_features])

    logger.info("Running short model predict()")
    feat_df["pred_short_raw"] = short_model.predict(feat_df[short_features])

    # --- Query target table ---
    logger.info("Querying target table for mfe columns")
    target_df = query_range(
        db_path,
        "target",
        start   = start_ts,
        end     = end_ts,
        columns = ["open_time", "long_mfe_fw60", "short_mfe_fw60"],
    )
    if target_df.empty:
        raise ValueError(f"target table returned no rows for range {start} to {end}")
    logger.info("target rows loaded: %d", len(target_df))

    # --- Query OHLCV for live-like charting / price-aware trade artifacts ---
    logger.info("Querying ohlcv table for price columns")
    ohlcv_df = query_range(
        db_path,
        "ohlcv",
        start   = start_ts,
        end     = end_ts,
        columns = ["open_time", "open", "high", "low", "close"],
    )
    if ohlcv_df.empty:
        raise ValueError(f"ohlcv table returned no rows for range {start} to {end}")
    logger.info("ohlcv rows loaded: %d", len(ohlcv_df))

    # --- Inner join on open_time ---
    pred_cols = feat_df[["open_time", "pred_long_raw", "pred_short_raw"]]
    strategy_df = (
        pred_cols
        .merge(target_df, on="open_time", how="inner")
        .merge(ohlcv_df, on="open_time", how="inner")
    )
    logger.info("After inner join: %d rows", len(strategy_df))

    # --- Reorder columns ---
    strategy_df = strategy_df[[
        "open_time",
        "pred_long_raw",
        "pred_short_raw",
        "long_mfe_fw60",
        "short_mfe_fw60",
        "open",
        "high",
        "low",
        "close",
    ]]

    # --- Write parquet ---
    out_dir  = Path(utils._resolve_path("artifacts")) / session_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "strategy_table.parquet"

    strategy_df.to_parquet(out_path, compression="zstd", index=False)
    logger.info("strategy_table.parquet written: %s  (%d rows)", out_path, len(strategy_df))

    return out_path
