# =============================================================================
# Impulse Scout forward-log step of the worker (epic_074 / t942)
# =============================================================================
# Purpose:
#  - Run data_handling.sync_tables.sync_scout for the Scout assets (solusdt /
#    btcusdt / ethusdt; worker.assets.scout_asset_ids), from sync_full (auto,
#    throttled) and from the standalone `sync_scout` job (manual, unthrottled)
#  - The frozen artifact is loaded lazily, once per worker process
#  - Integrity alert in the log when immutable_mismatch / missing_in_run > 0
#
# CPU: one call re-detects every event since the artifact's detect_anchor
# (~0.7 s / asset in 2026-11).  sync_full calls it only when the OHLCV step
# inserted new 1m bars AND at least `min_interval_s` (trading.json
# `scout.min_interval_s`, default 300 s) passed since the asset's last attempt
# -> ~2 s CPU per 5 min for the three assets.  The throttle only delays the
# write (scored_at - t_c); the prediction itself is deterministic.
# =============================================================================

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from worker import assets

if TYPE_CHECKING:
    from scout import ScoutArtifact

logger = logging.getLogger(__name__)

DEFAULT_MIN_INTERVAL_S = 300
#: DuckDB connect retries inside the worker (the worker is the only writer; readers are short)
CONNECT_ATTEMPTS = 3
CONNECT_WAIT_S = 1.0

_lock = threading.Lock()
_artifact: ScoutArtifact | None = None
_last_attempt: dict[str, float] = {}


class NotScoutAssetError(ValueError):
    """The asset is not a Scout asset (no scout_signals table should be created for it)."""


def get_artifact() -> ScoutArtifact:
    """The frozen Scout artifact, loaded (and hash-verified) once per process."""
    global _artifact  # noqa: PLW0603
    with _lock:
        if _artifact is None:
            from scout import load_artifact  # noqa: PLC0415 - heavy import (research code), lazy

            model_id = assets.scout_model_id()
            root = assets.scout_manifest_path(model_id).parent
            _artifact = load_artifact(root)
            logger.info("Scout artifact loaded: %s (%s)", _artifact.model_id, root)
        return _artifact


def min_interval_s() -> int:
    try:
        return int(assets.scout_config().get("min_interval_s", DEFAULT_MIN_INTERVAL_S))
    except (TypeError, ValueError):
        return DEFAULT_MIN_INTERVAL_S


def is_due(asset_id: str, now: float | None = None) -> bool:
    """True if the asset's last Scout attempt is at least ``min_interval_s`` old (or never ran)."""
    last = _last_attempt.get(asset_id)
    now = time.monotonic() if now is None else now
    return last is None or now - last >= min_interval_s()


def run_scout(asset_id: str, log: logging.Logger | None = None) -> dict[str, Any]:
    """Run ``sync_scout`` for one Scout asset; return its counters (``ScoutSyncResult.to_dict()``).

    Raises NotScoutAssetError for a non-Scout asset (e.g. bchusdt) -- ``sync_scout`` would
    otherwise create the table there.  Other errors propagate (the caller decides if fatal).
    """
    log = log or logger
    if asset_id not in assets.scout_asset_ids():
        raise NotScoutAssetError(f"not a Scout asset: {asset_id!r} (scout assets: {assets.scout_asset_ids()})")
    from data_handling.sync_tables.sync_scout import (
        sync_scout,  # noqa: PLC0415 - pulls scout (lazy)
    )

    _last_attempt[asset_id] = time.monotonic()
    t0 = time.monotonic()
    res = sync_scout(asset_id, art=get_artifact(), attempts=CONNECT_ATTEMPTS, wait_s=CONNECT_WAIT_S).to_dict()
    res["elapsed_s"] = round(time.monotonic() - t0, 2)
    log.info(
        "Scout sync %s: events=%s inserted=%s resolved=%s open=%s elapsed=%.2fs",
        asset_id, res["events"], res["inserted"], res["resolved"], res["open_events"], res["elapsed_s"],
    )
    if res["immutable_mismatch"] > 0 or res["missing_in_run"] > 0:
        log.error(
            "SCOUT ALERT %s: immutable_mismatch=%s missing_in_run=%s (forward-log integrity; stored rows kept)",
            asset_id, res["immutable_mismatch"], res["missing_in_run"],
        )
        res["alert"] = True
    return res


def maybe_run_scout(asset_id: str, new_bars: int, log: logging.Logger | None = None) -> dict[str, Any]:
    """The sync_full step: run when Scout asset + new bars + due; never raises.

    Returns ``{"status": "ok", ...counters}``, ``{"status": "skipped", "reason": ...}`` or
    ``{"status": "failed", "error": ...}``.
    """
    log = log or logger
    try:
        if asset_id not in assets.scout_asset_ids():
            return {"status": "skipped", "reason": "not_scout_asset"}
        if new_bars <= 0:
            return {"status": "skipped", "reason": "no_new_bars"}
        if not is_due(asset_id):
            return {"status": "skipped", "reason": "throttled"}
        return {"status": "ok", **run_scout(asset_id, log)}
    except Exception as exc:
        log.warning("Scout sync failed for %s (non-fatal)", asset_id, exc_info=True)
        return {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}


def reset_state() -> None:
    """Forget the cached artifact and the throttle state (tests)."""
    global _artifact  # noqa: PLW0603
    with _lock:
        _artifact = None
        _last_attempt.clear()
