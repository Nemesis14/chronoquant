# =============================================================================
# import modules
# =============================================================================

import os
import json
import subprocess

# =============================================================================
# Helpers: read config.json from repo root
# =============================================================================
# Logic:
#   - _repo_root():
#       * runs `git rev-parse --show-toplevel`
#       * returns the repository root path as a stripped string
#   - _load_config(config_path=None):
#       * if config_path is None, constructs path as <repo_root>/config.json
#       * opens the file and json.load(...) its contents, returning the dict
#       * raises normal IO / JSON errors to the caller (no swallowing)
# =============================================================================

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