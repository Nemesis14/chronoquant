# =============================================================================
# import modules
# =============================================================================

import copy
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# =============================================================================
# CONFIG HELPERS
# =============================================================================

# -------------------------------------------------------------------------
# _repo_root()
# -------------------------------------------------------------------------
# Purpose:
#  - Run `git rev-parse --show-toplevel` to get repository root
#  - Return stripped path string
# -------------------------------------------------------------------------
def _repo_root() -> str:
    return str(Path(__file__).resolve().parents[1])

def _runtime_root():
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return sys._MEIPASS
    return _repo_root()

def _app_root() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
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
    with open(load_path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def load_db_config() -> dict:
    cfg        = _load_json("db.json")
    db_cfg     = cfg.get("database", {})
    db_paths   = db_cfg.get("db_paths", {})
    active_env = db_cfg.get("active_env")
    if active_env in db_paths:
        db_cfg["db_path"] = db_paths[active_env]
    return cfg

def load_assets_config() -> dict:
    return _load_json("assets.json")

def default_asset_id(assets_cfg: dict | None = None) -> str:
    assets_cfg = assets_cfg or load_assets_config()
    return assets_cfg.get("default_asset_id", "bchusdt_fw240")

def resolve_asset_id(asset_id: str | None = None) -> str:
    return asset_id or default_asset_id()

def load_asset_config(asset_id: str | None = None) -> dict:
    if asset_id is None:
        cfg = load_db_config()
        db_cfg = cfg.get("database", {})
        db_cfg.setdefault("asset_id", "bchusdt_fw240")
        db_cfg.setdefault("features_profile", "bchusdt_fw240")
        return cfg

    assets_cfg = load_assets_config()
    assets = assets_cfg.get("assets", {})
    if asset_id not in assets:
        raise ValueError(f"Asset not found in config/assets.json: {asset_id}")

    base_db_cfg = load_db_config().get("database", {})
    active_env = base_db_cfg.get("active_env", "dev")
    asset_cfg = assets[asset_id]
    db_paths = asset_cfg.get("db_paths", {})
    db_path = db_paths.get(active_env, asset_cfg.get("db_path"))
    if not db_path:
        raise ValueError(f"No database path configured for asset {asset_id} env {active_env}")

    return {
        "database": {
            "active_env": active_env,
            "asset_id": asset_id,
            "db_path": db_path,
            "db_paths": dict(db_paths),
            "symbol": asset_cfg.get("symbol"),
            "interval": asset_cfg.get("interval", "1m"),
            "tables": dict(asset_cfg.get("tables", {})),
            "features_profile": asset_cfg.get("features_profile", asset_id),
        }
    }

def load_features_config(asset_id: str | None = None) -> dict:
    cfg = _load_json("features.json")
    profile_id = feature_profile_id(asset_id)
    return apply_feature_profile(cfg, profile_id)

def feature_profile_id(asset_id: str | None = None) -> str | None:
    db_cfg = load_asset_config(asset_id).get("database", {})
    return db_cfg.get("features_profile")

def apply_feature_profile(features_cfg: dict, profile_id: str | None) -> dict:
    if not profile_id:
        return features_cfg

    profiles = features_cfg.get("profiles", {})
    if not profiles:
        return features_cfg
    if profile_id not in profiles:
        raise ValueError(f"Feature profile not found in config/features.json: {profile_id}")

    cfg = copy.deepcopy(features_cfg)
    feature_cfg = cfg.setdefault("database", {}).setdefault("features", {})
    profile = profiles[profile_id]

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
    return _load_json("models.json")

def load_model_params_config() -> dict:
    return _load_json("model_params.json")

def load_model_registry_config() -> dict:
    return _load_json("model_registry.json")

def load_predictions_config() -> dict:
    return _load_json("predictions.json")

def load_strategies_config() -> dict:
    return _load_json("strategies.json")

def load_trading_config() -> dict:
    return _load_json("trading.json")

def load_env_config() -> dict:
    return _load_json("env.json")


def active_model_ids(model_cfg: dict) -> list[str]:
    models = model_cfg.get("models", {})
    return [model_id for model_id, meta in models.items() if meta.get("active")]


def prediction_col_name(model_id: str) -> str:
    return f"{model_id}_p"


def live_prediction_columns() -> dict[str, str]:
    cfg = load_predictions_config()
    columns = cfg.get("live_predictions", {}).get("column_names", {})
    defaults = {
        "target": "target",
        "prediction": "prediction",
        "signal": "signal",
    }
    return {**defaults, **columns}


def live_model_id(
    model_cfg: dict | None = None,
    env_cfg: dict | None = None,
    asset_id: str | None = None,
) -> str:
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
    env_cfg: dict | None = None,
    asset_id: str | None = None,
) -> tuple[str, dict]:
    model_cfg = model_cfg or load_models_config()
    model_id  = live_model_id(model_cfg=model_cfg, env_cfg=env_cfg, asset_id=asset_id)
    return model_id, model_cfg["models"][model_id]


def target_direction_from_name(target_name: str) -> str:
    if target_name.startswith("trg_l_"):
        return "long"
    if target_name.startswith("trg_s_"):
        return "short"
    raise ValueError(f"Cannot infer target direction from target name: {target_name}")


def signal_probability_threshold(predictions_cfg: dict | None = None) -> float:
    predictions_cfg = predictions_cfg or load_predictions_config()
    signal_cfg = predictions_cfg.get("live_predictions", {}).get("signal", {})
    return float(signal_cfg.get("threshold", 0.5))


def long_short_prediction_columns(model_cfg: dict) -> tuple[str, str]:
    long_col = None
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
    cutoffs = model_cfg.get("trading_strategy", {}).get("cut_offs", {})
    long_cutoff = cutoffs.get("long_signal", {}).get("threshold", 0.0139)
    short_cutoff = cutoffs.get("short_signal", {}).get("threshold", -0.0142)
    return float(long_cutoff), float(short_cutoff)

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
    direction = _format_target_direction(kind)
    quantile = _format_quantile(percentile)
    return f"trg_{direction}_fw{rolling_window}_{quantile}"

def target_name_from_config(cfg: dict) -> str:
    name = cfg.get("name")
    if name:
        return name
    return target_col_name(cfg["direction"], cfg["rolling_window"], cfg["percentile"])

def target_columns_from_config(features_cfg: dict) -> list:
    targets = features_cfg.get("database", {}).get("features", {}).get("targets", [])
    cols = []
    for cfg in targets:
        cols.append(target_name_from_config(cfg))
    return cols

# =============================================================================
# TIME HELPERS (UTC+0 / epoch milliseconds)
# =============================================================================
# Project canonical time unit: epoch milliseconds (open_time_ms) in UTC.
# The following helpers return values that are convenient for DB inserts:
#   - now_utc_ms()      -> int epoch ms (UTC)
#   - now_utc_str()     -> "YYYY-MM-DD HH:MM:SS" (UTC, no timezone suffix)
#   - ms_to_utc_str(ms) -> "YYYY-MM-DD HH:MM:SS" (UTC, no timezone suffix)
#   - utc_str_to_ms(s)  -> int epoch ms (accepts formats with or without 'T' or 'Z')
#
# Important:
#  - The string representation intentionally does NOT include timezone info
#    such as "+00:00" or "Z" (this keeps DB text fields compact and matches
#    the project's desired storage format).
#  - All parsing/conversion treats naive datetimes or strings without timezone
#    as UTC (per project policy: do not use local time anywhere).

# -------------------------------------------------------------------------
# now_utc_ms()
# -------------------------------------------------------------------------
# Purpose:
#  - Return current time in epoch milliseconds (UTC)
# -------------------------------------------------------------------------
def now_utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# -------------------------------------------------------------------------
# now_utc_str()
# -------------------------------------------------------------------------
# Purpose:
#  - Return current UTC time as "YYYY-MM-DD HH:MM:SS" string
#  - No timezone suffix, no microseconds
#  - Suitable for DB inserts
# -------------------------------------------------------------------------
def now_utc_str() -> str:
    dt = datetime.now(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# -------------------------------------------------------------------------
# utc_str_to_ms(s: str)
# -------------------------------------------------------------------------
# Purpose:
#  - Convert a UTC datetime string to epoch milliseconds
#  - Accept common ISO formats with optional timezone suffix
# -------------------------------------------------------------------------
def utc_str_to_ms(s: str) -> int:
    if s is None:
        raise ValueError("utc_str_to_ms: s must be a datetime string")

    value = str(s).strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return int(dt.timestamp() * 1000)


# -------------------------------------------------------------------------
# ms_to_utc_str(ms: int)
# -------------------------------------------------------------------------
# Purpose:
#  - Convert epoch milliseconds to "YYYY-MM-DD HH:MM:SS" (UTC)
#  - Raise ValueError if ms is None or not convertible
# -------------------------------------------------------------------------
def ms_to_utc_str(ms: int) -> str:
    if ms is None:
        raise ValueError("ms_to_utc_str: ms must be integer milliseconds")
    dt = datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")
