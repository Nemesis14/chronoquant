"""Strategy artifact read/write for the calibration pipeline.

Writes and loads ``artifacts/<model_id>/strategy/strategy_artifact.json``,
the authoritative record of a calibrated strategy configuration and its
out-of-sample performance metrics.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import utils

logger = logging.getLogger(__name__)

# %% Write


def write_strategy_artifact(
    model_id     : str,
    side         : str,
    strategy_cfg : dict,
    summary      : dict,
    oos_period   : dict,
) -> Path:
    """Write strategy_artifact.json to artifacts/<model_id>/strategy/.

    Creates the directory if it does not exist. Overwrites any existing file.

    Args:
        model_id     : Artifact directory name, e.g. ``lgbm_solusdt_l_fw60_2021``.
        side         : ``"long"`` or ``"short"``.
        strategy_cfg : Strategy parameter dict (entry_threshold, max_hold_minutes, etc.).
        summary      : Metrics dict from summarize_trades().
        oos_period   : Dict with ``"start"`` and ``"end"`` keys (YYYY-MM-DD strings).

    Returns:
        Path to the written strategy_artifact.json file.
    """
    out_dir = Path(utils._resolve_path(f"artifacts/{model_id}/strategy"))
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics = {
        "trade_count":    summary.get("trade_count"),
        "win_rate":       summary.get("win_rate"),
        "total_return":   summary.get("total_return"),
        "profit":         summary.get("profit"),
        "profit_factor":  summary.get("profit_factor"),
        "max_drawdown":   summary.get("max_drawdown"),
        "avg_hold_minutes":  summary.get("avg_hold_minutes"),
        "exposure_pct":      summary.get("exposure_pct"),
        "initial_equity":    summary.get("initial_equity"),
        "final_equity":      summary.get("final_equity"),
        "exit_reasons":      summary.get("exit_reasons", {}),
    }

    artifact = {
        "model_id":      model_id,
        "side":          side,
        "calibrated_at": datetime.now(UTC).isoformat(),
        "oos_period":    oos_period,
        "strategy":      strategy_cfg,
        "metrics":       metrics,
    }

    out_path = out_dir / "strategy_artifact.json"
    out_path.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    logger.info("Strategy artifact written: %s", out_path)
    return out_path


# %% Read


def load_strategy_artifact(model_id: str) -> dict:
    """Load strategy_artifact.json for the given model.

    Args:
        model_id : Artifact directory name, e.g. ``lgbm_solusdt_l_fw60_2021``.

    Returns:
        Parsed artifact dict.

    Raises:
        FileNotFoundError: If strategy_artifact.json does not exist.
    """
    path = Path(utils._resolve_path(f"artifacts/{model_id}/strategy/strategy_artifact.json"))
    if not path.exists():
        raise FileNotFoundError(
            f"strategy_artifact.json not found for model {model_id!r}: {path}"
        )
    artifact = json.loads(path.read_text(encoding="utf-8"))
    logger.info("Loaded strategy artifact: %s", path)
    return artifact
