# =============================================================================
# import modules
# =============================================================================

import os
import json
import subprocess
import sys
from datetime import datetime, timezone

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
def _repo_root():
	return subprocess.check_output(
		["git", "rev-parse", "--show-toplevel"],
		text=True
	).strip()

def _runtime_root():
	if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
		return sys._MEIPASS
	return _repo_root()

def _resolve_path(path: str) -> str:
	if os.path.isabs(path):
		return path
	return os.path.join(_runtime_root(), path)

def _config_root() -> str:
	if getattr(sys, "frozen", False):
		return os.path.join(os.path.dirname(sys.executable), "config")
	return os.path.join(_repo_root(), "config")

def _load_json(path: str) -> dict:
	if os.path.isabs(path):
		load_path = path
	else:
		load_path = os.path.join(_config_root(), path)
	with open(load_path, "r", encoding="utf-8") as f:
		return json.load(f)

def load_db_config() -> dict:
	return _load_json("db.json")

def load_features_config() -> dict:
	return _load_json("features.json")

def load_models_config() -> dict:
	return _load_json("models.json")

def load_env_config() -> dict:
	return _load_json("env.json")


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
