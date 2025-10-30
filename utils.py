# =============================================================================
# import modules
# =============================================================================
#
# NOTE: This file contains repo helpers. All explanatory text is provided
# as comments (no string docstrings), per project convention.
#
import os
import json
import subprocess

# -----------------------------------------------------------------------------
# Helpers: read config.json from repo root
# -----------------------------------------------------------------------------
# Logic:
#   - _repo_root():
#       * runs `git rev-parse --show-toplevel`
#       * returns the repository root path as a stripped string
#   - _load_config(config_path=None):
#       * if config_path is None, constructs path as <repo_root>/config.json
#       * opens the file and json.load(...) its contents, returning the dict
#       * raises normal IO / JSON errors to the caller (no swallowing)
# -----------------------------------------------------------------------------
def _repo_root():
    return subprocess.check_output(
        ["git", "rev-parse", "--show-toplevel"],
        text=True
    ).strip()

def _load_config(config_path=None):
    if config_path is None:
        repo_root   = _repo_root()
        config_path = os.path.join(repo_root, "config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

# -----------------------------------------------------------------------------
# Time helpers (UTC+0 / epoch milliseconds)
# -----------------------------------------------------------------------------
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
# -----------------------------------------------------------------------------
from datetime import datetime, timezone

def now_utc_ms() -> int:
    # Return current time in epoch milliseconds (UTC).
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def now_utc_str() -> str:
    # Return current UTC time as a readable string suitable for DB inserts:
    # "YYYY-MM-DD HH:MM:SS" (no timezone suffix, no microseconds).
    dt = datetime.now(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def ms_to_utc_str(ms: int) -> str:
    # Convert epoch milliseconds -> "YYYY-MM-DD HH:MM:SS" (UTC).
    # Raises ValueError if ms is None or not convertible to float/int.
    if ms is None:
        raise ValueError("ms_to_utc_str: ms must be an integer number of milliseconds")
    dt = datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

