# =============================================================================
# import modules
# =============================================================================

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

def load_features_config() -> dict:
    return _load_json("features.json")

def load_models_config() -> dict:
    return _load_json("models.json")

def load_model_params_config() -> dict:
    return _load_json("model_params.json")

def load_env_config() -> dict:
    return _load_json("env.json")


def active_model_ids(model_cfg: dict) -> list[str]:
    models = model_cfg.get("models", {})
    return [model_id for model_id, meta in models.items() if meta.get("active")]


def prediction_col_name(model_id: str) -> str:
    return f"{model_id}_p"


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

def _format_percentile(percentile: float) -> str:
    text = str(percentile)
    if "." in text:
        text = text.split(".", 1)[1]
    text = text.rstrip("0") or "0"
    return text.zfill(2)

def target_col_name(kind: str, rolling_window: int, percentile: float) -> str:
    pct = _format_percentile(percentile)
    return f"trg_{kind}_rw_{rolling_window}_prc_{pct}"

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
