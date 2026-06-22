"""Helpers for deriving stable strategy session identifiers."""

from __future__ import annotations

import re


def derive_session_id(
    long_model_id : str,
    short_model_id: str,
    scope         : str = "combo",
) -> str:
    """Return a plan-6 strategy session_id ``strat_{asset}_fw{h}_{scope}_{range}``.

    Parses the shared ``{asset}`` / ``fw{h}`` / ``{range}`` tokens from a matching
    long/short model pair (``{family}_{asset}_{dir}_fw{h}_{range}``) and assembles
    ``strat_{asset}_fw{h}_{scope}_{range}`` (plan 6 naming table).  When the two
    model ids do not share those tokens, falls back to the compact pairing id from
    :func:`derive_strategy_session_id` so a session id is always produced.

    Args:
        long_model_id  : Long-direction model id.
        short_model_id : Short-direction model id.
        scope          : Direction scope token (``combo`` / ``l`` / ``s``).

    Returns:
        The strategy session identifier.

    Example:
        ``lgbm_solusdt_l_fw60_2101_2605`` + ``lgbm_solusdt_s_fw60_2101_2605``
        -> ``strat_solusdt_fw60_combo_2101_2605``.
    """
    long_parsed  = _parse_model_id(long_model_id)
    short_parsed = _parse_model_id(short_model_id)
    if (
        long_parsed is not None
        and short_parsed is not None
        and long_parsed[0] == short_parsed[0]      # asset
        and long_parsed[1] == short_parsed[1]      # horizon token
        and long_parsed[2] == short_parsed[2]      # range
    ):
        asset, horizon_token, range_token = long_parsed
        return f"strat_{asset}_{horizon_token}_{scope}_{range_token}"

    return derive_strategy_session_id(long_model_id, short_model_id)


def _parse_model_id(model_id: str) -> tuple[str, str, str] | None:
    """Extract ``(asset, fw{h}, range)`` from ``{family}_{asset}_{dir}_fw{h}_{range}``.

    Returns None when the id does not match the expected paired structure.
    """
    tokens = model_id.split("_")
    if len(tokens) < 5:
        return None
    if tokens[2] not in {"l", "long", "s", "short"}:
        return None
    asset = tokens[1]
    # Locate the fw{h} token after the direction.
    for i in range(3, len(tokens)):
        if re.fullmatch(r"fw\d+", tokens[i]):
            horizon_token = tokens[i]
            range_token   = "_".join(tokens[i + 1:])
            if not range_token:
                return None
            return asset, horizon_token, range_token
    return None


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
