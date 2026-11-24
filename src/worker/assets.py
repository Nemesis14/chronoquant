"""Asset registry helpers shared by the API, the MCP server and the worker scheduler.

* *configured* assets: every key of ``config/assets.json``; any of them may be selected
  with ``?asset=`` (unknown -> :class:`UnknownAssetError`).
* *display order*: optional ``display_order`` list of config/assets.json (UI asset tabs,
  ``GET /api/assets``); ids not listed follow in configured order.
* *sync-active* assets: configured assets that have a live DuckDB file AND a strategy-session
  assignment in ``config/trading.json`` (``utils.trading_asset_ids``).  Only those are
  auto-synced, so an asset that is still being bootstrapped / has no deployed model yet is
  never touched by the scheduler.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import utils

logger = logging.getLogger(__name__)


class UnknownAssetError(ValueError):
    """The asset id is not present in config/assets.json."""


def configured_asset_ids() -> list[str]:
    """Asset ids of config/assets.json (default asset first)."""
    cfg = utils.load_assets_config()
    ids = list(cfg.get("assets", {}))
    default = utils.default_asset_id(cfg)
    if default in ids:
        ids.remove(default)
        ids.insert(0, default)
    return ids


def require_asset(asset_id: str | None) -> str:
    """Resolve ``asset_id`` (None/empty -> default asset); raise UnknownAssetError if not configured."""
    cfg = utils.load_assets_config()
    resolved = asset_id or utils.default_asset_id(cfg)
    if resolved not in cfg.get("assets", {}):
        raise UnknownAssetError(f"unknown asset: {resolved!r}")
    return resolved


def _has_live_db(asset_id: str) -> bool:
    try:
        return Path(utils.load_asset_config(asset_id)["database"]["db_path"]).is_file()
    except Exception:
        logger.debug("asset %s: live db path unavailable", asset_id, exc_info=True)
        return False


def sync_asset_ids() -> list[str]:
    """Assets the auto-sync scheduler serves (live DB exists and a trading session is assigned)."""
    try:
        trading = set(utils.trading_asset_ids())
    except Exception:
        logger.exception("sync_asset_ids: trading config unavailable")
        return []
    return [a for a in configured_asset_ids() if a in trading and _has_live_db(a)]


def display_asset_ids(cfg: dict[str, Any] | None = None) -> list[str]:
    """Configured asset ids in UI order: ``display_order`` of config/assets.json first (unknown ids
    skipped), then the rest in :func:`configured_asset_ids` order (default first).

    Only the UI listing uses it; the scheduler / Scout keep the configured order.
    """
    cfg = cfg or utils.load_assets_config()
    configured = configured_asset_ids()
    order = cfg.get("display_order")
    wanted = [str(a) for a in order] if isinstance(order, list) else []
    head = [a for a in dict.fromkeys(wanted) if a in configured]
    return head + [a for a in configured if a not in head]


def list_assets() -> dict[str, Any]:
    """All configured assets (UI order, :func:`display_asset_ids`) with ``has_data`` (live DB file)
    and ``active`` (sync-active) flags."""
    cfg = utils.load_assets_config()
    default = utils.default_asset_id(cfg)
    active = set(sync_asset_ids())
    items = []
    for asset_id in display_asset_ids(cfg):
        meta = cfg["assets"][asset_id]
        items.append({
            "asset_id": asset_id,
            "symbol": meta.get("symbol"),
            "interval": meta.get("interval", "1m"),
            "market": meta.get("market"),
            "is_default": asset_id == default,
            "has_data": _has_live_db(asset_id),
            "active": asset_id in active,
        })
    return {"default_asset_id": default, "items": items}


# %% Impulse Scout (epic_074 / t942)
#
# Config: optional ``scout`` block in config/trading.json -- ``model_id`` (default
# ``scout_s0c_r1``), ``assets`` (default: the frozen artifact's training assets,
# ``manifest.train.assets`` = solusdt / btcusdt / ethusdt) and ``min_interval_s`` (auto-sync
# throttle, see worker.scout_runner).  Read without importing the ``scout`` package
# (its import pulls the research code, ~2 s) -- the API only needs the manifest JSON.

DEFAULT_SCOUT_MODEL_ID = "scout_s0c_r1"


def scout_config() -> dict[str, Any]:
    """The optional ``scout`` block of config/trading.json (``{}`` when absent / unreadable)."""
    try:
        block = utils.load_trading_config().get("scout") or {}
    except Exception:
        logger.debug("scout_config: trading config unavailable", exc_info=True)
        return {}
    return block if isinstance(block, dict) else {}


def scout_model_id() -> str:
    return str(scout_config().get("model_id") or DEFAULT_SCOUT_MODEL_ID)


def scout_manifest_path(model_id: str | None = None) -> Path:
    return Path(utils.repo_root()) / "artifacts" / (model_id or scout_model_id()) / "manifest.json"


def scout_manifest(model_id: str | None = None) -> dict[str, Any]:
    """The frozen Scout artifact's manifest (``{}`` when missing / unparsable)."""
    path = scout_manifest_path(model_id)
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("scout_manifest: failed to parse %s", path)
        return {}
    return data if isinstance(data, dict) else {}


def scout_asset_ids() -> list[str]:
    """Assets the Scout runs on: ``scout.assets`` override, else the artifact's training assets.

    Restricted to configured assets (order of config/assets.json).  BCH is not a training asset,
    so it is excluded unless explicitly listed in the override.
    """
    override = scout_config().get("assets")
    wanted = override if isinstance(override, list) else (scout_manifest().get("train") or {}).get("assets") or []
    wanted_set = {str(a) for a in wanted}
    try:
        configured = configured_asset_ids()
    except Exception:
        logger.exception("scout_asset_ids: assets config unavailable")
        return []
    return [a for a in configured if a in wanted_set]
