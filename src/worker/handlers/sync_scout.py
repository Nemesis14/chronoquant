"""sync_scout handler (epic_074 / t942): Impulse Scout forward-log of one asset, unthrottled.

params: ``asset_id`` (default asset when omitted; must be a Scout asset, see
``worker.assets.scout_asset_ids``).  Result: ``ScoutSyncResult.to_dict()`` + ``elapsed_s``
(+ ``alert: true`` on immutable_mismatch / missing_in_run > 0).
"""

from __future__ import annotations

import logging
from typing import Any

import utils
from worker import scout_runner


def run_sync_scout(params: dict[str, Any], logger: logging.Logger) -> dict[str, Any]:
    asset_id = utils.resolve_asset_id(params.get("asset_id"))
    logger.info("Scout sync started: %s", asset_id)
    return scout_runner.run_scout(asset_id, logger)
