#!/usr/bin/env python3
"""Trigger a deploy: insert a pending deployment into reg.deployments.

This CLI inserts a ``status='pending'`` row into ``reg.deployments`` for the
given strategy session.  The live sync process (``sync_predictions``) detects
the pending row on its next run and executes the atomic backfill+swap.

Design: registry-intent model
------------------------------
The deploy trigger is separate from the swap executor.  The CLI writes the
*intent* (pending row); the live writer *executes* the cutover atomically.
This avoids a second writer competing with the live sync connection.

Rollback
--------
To roll back, re-run this CLI with the previous ``strategy_session_id``
(found in the current deployment's ``previous_strategy_id`` column).  The
next sync cycle will swap back to the old model's pred table.

Usage
-----
    # Trigger a deploy for a strategy session
    uv run python src/data_handling/06_trigger_deploy.py \\
        --strategy-session-id strat_solusdt_fw60_combo_2101_2605_v1

    # Explicit asset (default: from config)
    uv run python src/data_handling/06_trigger_deploy.py \\
        --strategy-session-id strat_solusdt_fw60_combo_2101_2605_v1 \\
        --asset-id solusdt
"""

import argparse
import logging
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import utils

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(
        level    = logging.INFO,
        format   = "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt  = "%Y-%m-%d %H:%M:%S",
        handlers = [logging.StreamHandler(sys.stdout)],
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description     = "Insert a pending deployment row into reg.deployments.",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--strategy-session-id",
        required = True,
        metavar  = "SESSION_ID",
        help     = "strategy_id / session_id to deploy (must exist in reg.strategies).",
    )
    parser.add_argument(
        "--asset-id",
        default = None,
        metavar = "ASSET_ID",
        help    = "Asset key from config/assets.json. Uses default_asset_id if omitted.",
    )
    return parser.parse_args()


def trigger_deploy(strategy_session_id: str, asset_id: str | None = None) -> str:
    """Insert a pending deployment row and return the new deployment_id.

    If the strategy does not exist in reg.strategies, the operation is aborted
    and a ValueError is raised.

    Args:
        strategy_session_id : strategy_id of the strategy to deploy.
        asset_id            : Asset key; uses default if None.

    Returns:
        The newly created deployment_id (UUID string).

    Raises:
        ValueError : If the strategy_id is not found in reg.strategies.
    """
    resolved_asset = utils.resolve_asset_id(asset_id)
    reg_path       = utils.registry_path()

    from data_handling.store.registry import get, open_crud_connection, upsert

    # Ensure registry is up-to-date (runs pending migrations including v2)
    utils.ensure_registry()

    conn = open_crud_connection(reg_path)
    try:
        # Validate: strategy must exist
        strategy_row = get(conn, "strategies", strategy_session_id)
        if strategy_row is None:
            raise ValueError(
                f"strategy_id '{strategy_session_id}' not found in reg.strategies. "
                "Run the strategy pipeline first."
            )

        deployment_id = str(uuid.uuid4())
        upsert(conn, "deployments", {
            "deployment_id" : deployment_id,
            "asset_id"      : resolved_asset,
            "strategy_id"   : strategy_session_id,
            "active"        : False,
            "status"        : "pending",
        })
        logger.info(
            "Pending deployment inserted: deployment_id=%s strategy_id=%s asset_id=%s",
            deployment_id, strategy_session_id, resolved_asset,
        )
        logger.info("A következő sync_predictions futás végrehajtja az atomikus cserét.")
    finally:
        conn.close()

    return deployment_id


def main() -> None:
    _setup_logging()
    args = _parse_args()

    try:
        deployment_id = trigger_deploy(
            strategy_session_id = args.strategy_session_id,
            asset_id            = args.asset_id,
        )
        logger.info("Deploy trigger OK: deployment_id=%s", deployment_id)
    except ValueError as exc:
        logger.error("Deploy trigger FAILED: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
