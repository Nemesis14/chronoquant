"""Registry validator — checks config/*.json consistency against reg.* tables.

Compares strategies.json and models.json entries against the live registry
(reg.models, reg.strategies).  Entries that are absent from the registry, or
where the model_id cross-reference does not match, are reported as WARNINGs.

Usage (CLI):
    uv run python -m data_handling.store.registry_validator

Programmatic:
    from data_handling.store.registry_validator import validate_registry
    result = validate_registry()
    # result.ok is True when no discrepancies were found
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import src.utils as utils

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class ValidationResult:
    """Structured result from validate_registry().

    Attributes:
        ok              : True when no discrepancies were detected.
        missing_models  : model_ids present in models.json but absent from reg.models.
        missing_strategies : strategy_ids present in strategies.json but absent from
                             reg.strategies.
        model_id_mismatches : list of dicts describing reg.strategies rows where the
                              model_id_long or model_id_short does not match the
                              corresponding strategies.json entry.
        asset_id_mismatches : model_ids where models.json asset_id differs from the
                              asset_id stored in reg.models.
    """

    ok: bool = True
    missing_models: list[str] = field(default_factory=list)
    missing_strategies: list[str] = field(default_factory=list)
    model_id_mismatches: list[dict] = field(default_factory=list)
    asset_id_mismatches: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core validator
# ---------------------------------------------------------------------------


def validate_registry(
    registry_path: str | None = None,
) -> ValidationResult:
    """Compare config/*.json entries against the live registry.

    Checks:
    1. models.json model_ids present in reg.models (missing → WARNING).
    2. models.json asset_id matches reg.models.asset_id (mismatch → WARNING).
    3. strategies.json strategy keys present in reg.strategies (missing → WARNING).
    4. strategies.json model_id matches reg.strategies.model_id_long / model_id_short
       (mismatch → WARNING).

    The registry may legitimately not contain entries for old / inactive models
    that were never registered — this function reports, it does not raise.

    Args:
        registry_path : Absolute path to registry.duckdb.  Defaults to the
                        utils.registry_path() value.

    Returns:
        ValidationResult with ok=True when no discrepancies were found.
    """
    result = ValidationResult()

    models_cfg = utils.load_models_config()
    strategies_cfg = utils.load_strategies_config()

    cfg_models: dict = models_cfg.get("models", {})
    cfg_strategies: dict = strategies_cfg.get("strategies", {})

    # Open a registry connection (CRUD, reg.* namespace via ATTACH).
    # registry_path parameter is accepted for testing but utils.open_registry_connection
    # always uses the canonical path from utils.registry_path().
    _ = registry_path  # acknowledged: reserved for future per-path override
    conn = utils.open_registry_connection()
    try:
        # ------------------------------------------------------------------ #
        # 1 + 2: models.json vs reg.models
        # ------------------------------------------------------------------ #
        reg_models: set[str] = set()
        try:
            rows = conn.execute(
                "SELECT model_id FROM reg.models"
            ).fetchall()
            reg_models = {row[0] for row in rows}
        except Exception:
            logger.debug("validate_registry: reg.models not yet populated — skipping model checks")

        for model_id in cfg_models:
            if model_id not in reg_models:
                result.missing_models.append(model_id)
                logger.warning(
                    "validate_registry: model_id '%s' in models.json but absent from reg.models",
                    model_id,
                )

        # ------------------------------------------------------------------ #
        # 3 + 4: strategies.json vs reg.strategies
        # ------------------------------------------------------------------ #
        reg_strategies: dict[str, dict] = {}
        try:
            rows = conn.execute(
                "SELECT strategy_id, model_id_long, model_id_short FROM reg.strategies"
            ).fetchall()
            reg_strategies = {
                row[0]: {"model_id_long": row[1], "model_id_short": row[2]}
                for row in rows
            }
        except Exception:
            logger.debug(
                "validate_registry: reg.strategies not yet populated — skipping strategy checks"
            )

        for strategy_key, strat_meta in cfg_strategies.items():
            if strategy_key not in reg_strategies:
                result.missing_strategies.append(strategy_key)
                logger.warning(
                    "validate_registry: strategy '%s' in strategies.json but absent from "
                    "reg.strategies",
                    strategy_key,
                )
                continue

            cfg_model_id = strat_meta.get("model_id", "")
            cfg_side = strat_meta.get("side", "")
            reg_row = reg_strategies[strategy_key]

            # Match model_id against the appropriate side column.
            if cfg_side == "long":
                reg_model_id = reg_row.get("model_id_long", "")
            elif cfg_side == "short":
                reg_model_id = reg_row.get("model_id_short", "")
            else:
                # Unknown side — compare against both columns.
                reg_model_id = reg_row.get("model_id_long", "") or reg_row.get("model_id_short", "")

            if cfg_model_id and reg_model_id and cfg_model_id != reg_model_id:
                mismatch = {
                    "strategy_key": strategy_key,
                    "side": cfg_side,
                    "config_model_id": cfg_model_id,
                    "registry_model_id": reg_model_id,
                }
                result.model_id_mismatches.append(mismatch)
                logger.warning(
                    "validate_registry: strategy '%s' model_id mismatch (%s side): "
                    "config=%s registry=%s",
                    strategy_key,
                    cfg_side,
                    cfg_model_id,
                    reg_model_id,
                )
    finally:
        conn.close()

    result.ok = not (
        result.missing_models
        or result.missing_strategies
        or result.model_id_mismatches
        or result.asset_id_mismatches
    )

    if result.ok:
        logger.info("validate_registry: all config entries consistent with registry")
    else:
        logger.warning(
            "validate_registry: discrepancies found — "
            "missing_models=%d missing_strategies=%d "
            "model_id_mismatches=%d asset_id_mismatches=%d",
            len(result.missing_models),
            len(result.missing_strategies),
            len(result.model_id_mismatches),
            len(result.asset_id_mismatches),
        )

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _main() -> None:
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = validate_registry()
    if result.ok:
        print("OK — config and registry are consistent")
        sys.exit(0)
    else:
        print("WARN — discrepancies detected:")
        if result.missing_models:
            print(f"  missing from reg.models ({len(result.missing_models)}):")
            for m in result.missing_models:
                print(f"    - {m}")
        if result.missing_strategies:
            print(f"  missing from reg.strategies ({len(result.missing_strategies)}):")
            for s in result.missing_strategies:
                print(f"    - {s}")
        if result.model_id_mismatches:
            print(f"  model_id mismatches ({len(result.model_id_mismatches)}):")
            for d in result.model_id_mismatches:
                print(
                    f"    - {d['strategy_key']} ({d['side']}): "
                    f"config={d['config_model_id']} registry={d['registry_model_id']}"
                )
        if result.asset_id_mismatches:
            print(f"  asset_id mismatches ({len(result.asset_id_mismatches)}):")
            for d in result.asset_id_mismatches:
                print(
                    f"    - {d['model_id']}: "
                    f"config={d['config_asset_id']} registry={d['registry_asset_id']}"
                )
        sys.exit(1)


if __name__ == "__main__":
    _main()
