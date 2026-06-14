"""Config loading, path resolution, time utilities, and model helpers for ChronoQuant.

Single entry point for all config access — business logic must not read JSON files directly.
"""

import copy
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

# %% Path helpers

def _repo_root() -> str:
    return str(Path(__file__).resolve().parents[1])


def _runtime_root() -> str:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return str(sys._MEIPASS)
    return _repo_root()


def _resolve_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(_runtime_root(), path)


def _config_root() -> str:
    if getattr(sys, "frozen", False):
        external_config = os.path.join(os.path.dirname(sys.executable), "config")
        if os.path.exists(external_config):
            return external_config
        return os.path.join(_runtime_root(), "config")
    return os.path.join(_repo_root(), "config")


def _load_json(path: str) -> dict:
    if os.path.isabs(path):
        load_path = path
    else:
        load_path = os.path.join(_config_root(), path)
    with open(load_path, encoding="utf-8-sig") as f:
        return json.load(f)


# %% Config loaders


def load_assets_config() -> dict:
    """Load the assets configuration from config/assets.json."""
    return _load_json("assets.json")


def default_asset_id(assets_cfg: dict | None = None) -> str:
    """Return the default asset ID from assets config."""
    assets_cfg = assets_cfg or load_assets_config()
    return assets_cfg.get("default_asset_id", "solusdt_fw60")


def resolve_asset_id(asset_id: str | None = None) -> str:
    """Return asset_id if given, otherwise the default asset ID."""
    return asset_id or default_asset_id()


def load_asset_config(asset_id: str | None = None) -> dict:
    """Return a normalized config dict for the given asset.

    Args:
        asset_id: Asset key from config/assets.json. Uses default_asset_id if None.

    Returns:
        Dict with a single "database" key containing asset metadata and data_dir.

    Raises:
        ValueError: If asset_id is not found or has no data_dir configured.
    """
    assets_cfg = load_assets_config()
    resolved   = asset_id or assets_cfg.get("default_asset_id", "solusdt_fw60")
    assets     = assets_cfg.get("assets", {})

    if resolved not in assets:
        raise ValueError(f"Asset not found in config/assets.json: {resolved}")

    asset_cfg = assets[resolved]
    data_dir  = asset_cfg.get("data_dir")
    if not data_dir:
        raise ValueError(f"No data_dir configured for asset {resolved}")

    return {
        "database": {
            "asset_id":         resolved,
            "data_dir":         _resolve_path(data_dir),
            "symbol":           asset_cfg.get("symbol"),
            "interval":         asset_cfg.get("interval", "1m"),
            "market":           asset_cfg.get("market", "spot"),
            "features_profile": asset_cfg.get("features_profile", resolved),
        }
    }


def load_features_config(asset_id: str | None = None) -> dict:
    """Load features.json and apply the feature profile for the given asset."""
    cfg        = _load_json("features.json")
    profile_id = feature_profile_id(asset_id)
    return apply_feature_profile(cfg, profile_id)


def load_unusable_features() -> list[str]:
    """Return the list of features marked unusable in the feature taxonomy.

    Unusable features must not appear in model input_features or live prediction
    inputs.  The list is sourced from config/features.json feature_taxonomy.unusable
    and may be empty when all features have valid t-1 definitions.
    """
    cfg = _load_json("features.json")
    return list(cfg.get("feature_taxonomy", {}).get("unusable", []))


def feature_profile_id(asset_id: str | None = None) -> str | None:
    """Return the features_profile key for the given asset."""
    db_cfg = load_asset_config(asset_id).get("database", {})
    return db_cfg.get("features_profile")


def apply_feature_profile(features_cfg: dict, profile_id: str | None) -> dict:
    """Merge a named feature profile into the base features config.

    Args:
        features_cfg: Full features.json dict.
        profile_id: Key under features_cfg["profiles"] to apply. If None or
            absent, returns features_cfg unchanged.

    Returns:
        Deep-copied config with the profile's targets/indicators merged in.

    Raises:
        ValueError: If profile_id is not found in profiles.
    """
    if not profile_id:
        return features_cfg

    profiles = features_cfg.get("profiles", {})
    if not profiles:
        return features_cfg
    if profile_id not in profiles:
        raise ValueError(f"Feature profile not found in config/features.json: {profile_id}")

    cfg         = copy.deepcopy(features_cfg)
    feature_cfg = cfg.setdefault("database", {}).setdefault("features", {})
    profile     = profiles[profile_id]

    if "targets" in profile:
        feature_cfg["targets"] = copy.deepcopy(profile["targets"])
    if "indicators" in profile:
        feature_cfg["indicators"] = copy.deepcopy(profile["indicators"])
    if "indicators_extend" in profile:
        # Merge new top-level indicator groups without replacing existing ones
        base = feature_cfg.setdefault("indicators", {})
        for group, group_cfg in profile["indicators_extend"].items():
            base[group] = copy.deepcopy(group_cfg)

    feature_cfg["profile_id"] = profile_id
    if profile.get("description"):
        feature_cfg["profile_description"] = profile["description"]
    return cfg


def load_models_config() -> dict:
    """Load the models configuration from config/models.json."""
    return _load_json("models.json")


def load_model_params_config() -> dict:
    """Load model hyperparameter config from config/model_params.json."""
    return _load_json("model_params.json")


def load_model_registry_config() -> dict:
    """Load the model registry from config/model_registry.json."""
    return _load_json("model_registry.json")


def load_predictions_config() -> dict:
    """Load predictions config from config/predictions.json."""
    return _load_json("predictions.json")


def load_strategies_config() -> dict:
    """Load trading strategies config from config/strategies.json."""
    return _load_json("strategies.json")


def load_trading_config() -> dict:
    """Load trading config from config/trading.json."""
    return _load_json("trading.json")


def load_env_config() -> dict:
    """Load runtime environment config from config/env.json."""
    return _load_json("env.json")


# %% Model helpers

def active_model_ids(model_cfg: dict) -> list[str]:
    """Return model IDs where active=true in the given models config."""
    models = model_cfg.get("models", {})
    return [model_id for model_id, meta in models.items() if meta.get("active")]


def prediction_col_name(model_id: str) -> str:
    """Return the prediction column name for a model ID (e.g. 'lgbm_l_p')."""
    return f"{model_id}_p"


def live_prediction_columns() -> dict[str, str]:
    """Return the live prediction column name mapping from predictions config."""
    cfg      = load_predictions_config()
    columns  = cfg.get("live_predictions", {}).get("column_names", {})
    defaults = {
        "target":     "target",
        "prediction": "prediction",
        "signal":     "signal",
    }
    return {**defaults, **columns}


def live_model_id(
    model_cfg: dict | None = None,
    env_cfg  : dict | None = None,
    asset_id : str  | None = None,
) -> str:
    """Resolve the single live model ID from env and models config.

    Args:
        model_cfg: Loaded models config; loaded from file if None.
        env_cfg:   Loaded env config; loaded from file if None.
        asset_id:  If given, checks per-asset model override in env.json first.

    Returns:
        The resolved model ID string.

    Raises:
        ValueError: If the resolved model ID is not in models.json, or if there
            is not exactly one active model and no runtime.model_id is set.
    """
    model_cfg = model_cfg or load_models_config()
    env_cfg   = env_cfg   or load_env_config()
    models    = model_cfg.get("models", {})
    runtime   = env_cfg.get("runtime", {})

    if asset_id:
        per_asset = runtime.get("models", {})
        model_id  = per_asset.get(asset_id)
        if model_id:
            if model_id not in models:
                raise ValueError(f"Runtime model not found in config/models.json: {model_id}")
            return model_id

    model_id = runtime.get("model_id")
    if model_id:
        if model_id not in models:
            raise ValueError(f"Runtime model not found in config/models.json: {model_id}")
        return model_id

    active_ids = active_model_ids(model_cfg)
    if len(active_ids) != 1:
        raise ValueError("Set config/env.json runtime.model_id for the single live model")
    return active_ids[0]


def live_model_meta(
    model_cfg: dict | None = None,
    env_cfg  : dict | None = None,
    asset_id : str  | None = None,
) -> tuple[str, dict]:
    """Return (model_id, model metadata dict) for the live model."""
    model_cfg = model_cfg or load_models_config()
    model_id  = live_model_id(model_cfg=model_cfg, env_cfg=env_cfg, asset_id=asset_id)
    return model_id, model_cfg["models"][model_id]


def target_direction_from_name(target_name: str) -> str:
    """Return 'long' or 'short' inferred from a target column name prefix."""
    if target_name.startswith("trg_l_"):
        return "long"
    if target_name.startswith("trg_s_"):
        return "short"
    raise ValueError(f"Cannot infer target direction from target name: {target_name}")


def signal_probability_threshold(predictions_cfg: dict | None = None) -> float:
    """Return the signal probability threshold from predictions config."""
    predictions_cfg = predictions_cfg or load_predictions_config()
    signal_cfg      = predictions_cfg.get("live_predictions", {}).get("signal", {})
    return float(signal_cfg.get("threshold", 0.5))


def champion_models_for_asset(
    model_cfg : dict,
    asset_id  : str | None = None,
) -> tuple[str, dict, str, dict]:
    """Return (long_model_id, long_meta, short_model_id, short_meta) for an asset.

    Resolves the default asset when asset_id is None. Long model is identified
    by target_name starting with 'trg_l_'; short by 'trg_s_'.

    Args:
        model_cfg : Loaded models config dict.
        asset_id  : Asset key; uses default_asset_id if None.

    Returns:
        Tuple of (long_model_id, long_meta, short_model_id, short_meta).

    Raises:
        ValueError: If a unique active long and short model cannot be resolved.
    """
    resolved = resolve_asset_id(asset_id)
    models   = model_cfg.get("models", {})

    long_id   : str  | None = None
    long_meta : dict | None = None
    short_id  : str  | None = None
    short_meta: dict | None = None

    for mid, meta in models.items():
        if not meta.get("active", False):
            continue
        if meta.get("asset_id") != resolved:
            continue
        target = meta.get("target_name", "")
        if target.startswith("trg_l_") and long_id is None:
            long_id, long_meta = mid, meta
        elif target.startswith("trg_s_") and short_id is None:
            short_id, short_meta = mid, meta

    if long_id is None or long_meta is None:
        raise ValueError(f"No active long model found for asset {resolved!r}")
    if short_id is None or short_meta is None:
        raise ValueError(f"No active short model found for asset {resolved!r}")

    return long_id, long_meta, short_id, short_meta


def long_short_prediction_columns(model_cfg: dict) -> tuple[str, str]:
    """Return (long_pred_col, short_pred_col) for the active model pair.

    Args:
        model_cfg: Loaded models config dict.

    Returns:
        Tuple of (long prediction column name, short prediction column name).

    Raises:
        ValueError: If both a long and short active model cannot be identified.
    """
    long_col  = None
    short_col = None

    for model_id in active_model_ids(model_cfg):
        model_meta  = model_cfg["models"][model_id]
        target_name = model_meta.get("target_name", "")

        if target_name.startswith("trg_l_") or "_l_" in model_id:
            long_col = prediction_col_name(model_id)
        elif target_name.startswith("trg_s_") or "_s_" in model_id:
            short_col = prediction_col_name(model_id)

    if long_col is None or short_col is None:
        raise ValueError("Active long and short models are required for spread-based signals")

    return long_col, short_col


def signal_cutoffs_from_config(model_cfg: dict) -> tuple[float, float]:
    """Return (long_cutoff, short_cutoff) thresholds from the trading strategy config."""
    cutoffs      = model_cfg.get("trading_strategy", {}).get("cut_offs", {})
    long_cutoff  = cutoffs.get("long_signal",  {}).get("threshold",  0.0139)
    short_cutoff = cutoffs.get("short_signal", {}).get("threshold", -0.0142)
    return float(long_cutoff), float(short_cutoff)


# %% Target column name helpers

def _format_quantile(percentile: float) -> str:
    value = int(round(float(percentile) * 100))
    return f"q{value:02d}"


def _format_target_direction(kind: str) -> str:
    if kind in {"long", "l"}:
        return "l"
    if kind in {"short", "s"}:
        return "s"
    return kind


def target_col_name(kind: str, rolling_window: int, percentile: float) -> str:
    """Build a target column name, e.g. 'trg_l_fw60_q90'."""
    direction = _format_target_direction(kind)
    quantile  = _format_quantile(percentile)
    return f"trg_{direction}_fw{rolling_window}_{quantile}"


def target_name_from_config(cfg: dict) -> str:
    """Return target column name from a target config entry, using explicit name if set."""
    name = cfg.get("name")
    if name:
        return name
    return target_col_name(cfg["direction"], cfg["rolling_window"], cfg["percentile"])


def target_columns_from_config(features_cfg: dict) -> list[str]:
    """Return all target column names defined in a loaded features config."""
    targets = features_cfg.get("database", {}).get("features", {}).get("targets", [])
    return [target_name_from_config(cfg) for cfg in targets]


# %% Time helpers
# Project canonical time unit: epoch milliseconds (open_time_ms) in UTC.
# String format is "YYYY-MM-DD HH:MM:SS" without timezone suffix — compact and
# consistent with DB storage. Naive strings are always treated as UTC.

def now_utc_ms() -> int:
    """Return current UTC time as epoch milliseconds."""
    return int(datetime.now(UTC).timestamp() * 1000)


def now_utc_str() -> str:
    """Return current UTC time as YYYY-MM-DD HH:MM:SS string."""
    dt = datetime.now(UTC).replace(microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def utc_str_to_ms(s: str) -> int:
    """Convert a UTC datetime string to epoch milliseconds.

    Args:
        s: ISO datetime string; accepts optional 'T' separator, 'Z' or '+00:00' suffix.

    Returns:
        Integer epoch milliseconds.
    """
    if s is None:
        raise ValueError("utc_str_to_ms: s must be a datetime string")

    value = str(s).strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)

    return int(dt.timestamp() * 1000)


def ms_to_utc_str(ms: int) -> str:
    """Convert epoch milliseconds to YYYY-MM-DD HH:MM:SS (UTC).

    Args:
        ms: Integer epoch milliseconds.

    Returns:
        UTC datetime string without timezone suffix.

    Raises:
        ValueError: If ms is None.
    """
    if ms is None:
        raise ValueError("ms_to_utc_str: ms must be integer milliseconds")
    dt = datetime.fromtimestamp(int(ms) / 1000.0, tz=UTC).replace(microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")
