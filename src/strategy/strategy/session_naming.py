"""Helpers for deriving stable strategy session identifiers."""

from __future__ import annotations


def derive_strategy_session_id(long_model_id: str, short_model_id: str) -> str:
    """Return a compact session id for a long/short model pair.

    Preferred output format:
        strategy_<shared-model-stem-without-direction>

    Example:
        lgbm_solusdt_l_fw60_2101_2605
        lgbm_solusdt_s_fw60_2101_2605
        -> strategy_lgbm_solusdt_fw60_2101_2605

    Fallback:
        If the two model ids do not match the expected paired structure, fall
        back to a deterministic combined identifier.
    """
    long_tokens = long_model_id.split("_")
    short_tokens = short_model_id.split("_")

    if (
        len(long_tokens) == len(short_tokens)
        and len(long_tokens) >= 4
        and long_tokens[2] in {"l", "long"}
        and short_tokens[2] in {"s", "short"}
    ):
        base_long = long_tokens[:2] + long_tokens[3:]
        base_short = short_tokens[:2] + short_tokens[3:]
        if base_long == base_short:
            return "strategy_" + "_".join(base_long)

    return f"strategy_{long_model_id}__{short_model_id}"
