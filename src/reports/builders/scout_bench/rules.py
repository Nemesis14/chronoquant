# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Generalised trade rules of the Impulse Scout benchmark (epic_073 / t936, round 2: trading-rule iteration).

A :class:`TradeRule` describes a long trade after the confirmed low (entry at the open of the 1m bar opening at
``t_c``, taker + slippage) with configurable barriers:

* target / stop level: ``("pstar", 0)`` = the label level ``P*``, ``("low0", 0)`` = ``low[t0]``, ``("sigma", k)`` =
  ``entry * exp(+-k * sigma_W)``, ``("atr", k)`` = ``entry +- k * ATR14(15m)``, ``("half", 0)`` = ``close_tc * exp(A / 2)``
  (half way to ``P*``);
* time stop ``time_min`` (<= ``H``; the window is the first ``time_min`` bars from ``t_c``);
* partial exit: ``partial_frac`` of the position at ``tp1`` (maker), then optionally the stop of the rest moves to the
  entry price (break-even);
* break-even trigger: when the high reaches ``entry * exp(be_trigger_sigma * sigma_W)`` the stop moves to the entry.

Conservative conventions (same as :mod:`trades` and the label): inside one bar the stop is checked first (a bar touching
both the stop and a target is a stop); a stop moved inside a bar is also checked in that bar; target fills at
``max(level, open)``, stop fills at ``min(level, open)``; costs from ``config/strategies.json`` ``costs``. The event set
is the same as :func:`trades.simulate` (an event is invalid when the full ``H`` window has a gap), so every rule trades
on the same events. With :data:`DEFAULT_RULE` the result equals :func:`trades.simulate_event` exactly (tested).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd
import polars as pl

from reports.builders import wave_events as we
from reports.builders.scout_bench.trades import Costs

LEVEL_KINDS = ("pstar", "low0", "sigma", "atr", "half")


@dataclass(frozen=True)
class TradeRule:
    name: str
    tp: tuple[str, float] = ("pstar", 0.0)
    sl: tuple[str, float] = ("low0", 0.0)
    time_min: int | None = None            # None = the event's H
    partial_frac: float = 0.0              # share closed at ``tp1`` (0 = no partial exit)
    tp1: tuple[str, float] | None = None
    be_after_tp1: bool = False             # after the partial exit the stop of the rest moves to the entry
    be_trigger_sigma: float | None = None  # high >= entry * exp(x * sigma_W) -> stop to entry
    description: str = ""

    def __post_init__(self) -> None:
        for lv in (self.tp, self.sl, *((self.tp1,) if self.tp1 else ())):
            if lv[0] not in LEVEL_KINDS:
                raise ValueError(f"{self.name}: unknown level kind {lv[0]!r}")
        if not 0.0 <= self.partial_frac < 1.0:
            raise ValueError(f"{self.name}: partial_frac must be in [0, 1)")
        if self.partial_frac > 0 and self.tp1 is None:
            raise ValueError(f"{self.name}: a partial exit needs tp1")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


DEFAULT_RULE = TradeRule("default", description="P* / low[t0] / H (5300 triple barrier)")


def _level(kind: str, k: float, side: int, *, entry: float, p_star: float, low0: float, close_tc: float, a_log: float,
           sigma_w: float, atr: float) -> float:
    if kind == "pstar":
        return p_star
    if kind == "low0":
        return low0
    if kind == "sigma":
        return entry * float(np.exp(side * k * sigma_w))
    if kind == "atr":
        return entry + side * k * atr
    if kind == "half":
        return close_tc * float(np.exp(a_log / 2.0))
    raise ValueError(kind)


def simulate_rule_event(m: we.MinuteData, t_c_min: int, h: int, ev: Mapping[str, float], rule: TradeRule,
                        costs: Costs) -> dict[str, Any]:
    """One trade under ``rule``. ``ev`` needs ``p_star, low_t0, close_tc, a_log, sigma_w, atr14_15m``."""
    i0 = t_c_min - m.m0
    if i0 < 0 or i0 + h > m.n:
        return {"exit_type": "invalid"}
    hi_h, lo_h = m.high[i0:i0 + h], m.low[i0:i0 + h]
    if np.isnan(hi_h).any() or np.isnan(lo_h).any() or np.isnan(m.open[i0]) or np.isnan(m.close[i0 + h - 1]):
        return {"exit_type": "invalid"}                       # same validity as trades.simulate_event (full H window)
    t = int(min(rule.time_min or h, h))
    hi, lo, op, cl = hi_h[:t], lo_h[:t], m.open[i0:i0 + t], m.close[i0:i0 + t]
    entry = float(op[0])
    kw = {"entry": entry, "p_star": float(ev["p_star"]), "low0": float(ev["low_t0"]), "close_tc": float(ev["close_tc"]),
          "a_log": float(ev["a_log"]), "sigma_w": float(ev["sigma_w"]), "atr": float(ev.get("atr14_15m", np.nan))}
    tp = _level(*rule.tp, +1, **kw)
    stop = _level(*rule.sl, -1, **kw)
    tp1 = _level(*rule.tp1, +1, **kw) if rule.partial_frac > 0 and rule.tp1 else None
    be_lvl = entry * float(np.exp(rule.be_trigger_sigma * kw["sigma_w"])) if rule.be_trigger_sigma is not None else None
    if not (np.isfinite(tp) and np.isfinite(stop)):
        return {"exit_type": "invalid"}
    legs: list[tuple[float, float, str]] = []                 # (fraction, exit price, kind)
    pos, tp1_done, be_on, bar = 1.0, False, False, t - 1
    for j in range(t):
        o = float(op[j])
        if lo[j] < stop:
            legs.append((pos, min(stop, o), "sl"))
            pos, bar = 0.0, j
            break
        if tp1 is not None and not tp1_done and hi[j] > tp1:
            legs.append((rule.partial_frac, max(tp1, o), "tp"))
            pos -= rule.partial_frac
            tp1_done = True
            if rule.be_after_tp1 and entry > stop:
                stop = entry
                if lo[j] < stop:                              # moved stop also checked in this bar (conservative)
                    legs.append((pos, min(stop, o), "sl"))
                    pos, bar = 0.0, j
                    break
        if hi[j] > tp:
            legs.append((pos, max(tp, o), "tp"))
            pos, bar = 0.0, j
            break
        if be_lvl is not None and not be_on and hi[j] >= be_lvl:
            be_on = True
            if entry > stop:
                stop = entry
                if lo[j] < stop:
                    legs.append((pos, min(stop, o), "sl"))
                    pos, bar = 0.0, j
                    break
    if pos > 1e-12:
        legs.append((pos, float(cl[-1]), "timeout"))
    slip = costs.slippage_bps / 1e4
    entry_eff = entry * (1 + slip) if costs.entry_order_type == "taker" else entry
    fee_in = costs.fee_bps(costs.entry_order_type) / 1e4
    net = gross = 0.0
    for frac, px, kind in legs:
        ek = costs.exit_order_type[kind]
        exit_eff = px * (1 - slip) if ek == "taker" else px
        net += frac * (exit_eff / entry_eff - 1.0 - fee_in - costs.fee_bps(ek) / 1e4)
        gross += frac * (px / entry - 1.0)
    return {"exit_type": legs[-1][2], "tp_any": any(k == "tp" for _, _, k in legs), "n_legs": len(legs),
            "bars_held": bar + 1, "same_bar": False, "entry": entry, "gross": gross, "net": net}


def simulate_rule(events: pl.DataFrame, bars: Mapping[str, we.MinuteData], rule: TradeRule, costs: Costs,
                  rows: np.ndarray | None = None) -> pd.DataFrame:
    """Trades of the frame rows (all, or ``rows`` = row positions) under ``rule``; invalid windows dropped. Same output
    columns as :func:`trades.simulate` (``row_id, asset_id, sigma_w, exit_type, net, net_bps, net_sigma, same_bar`` ...)."""
    ev = events if rows is None else events[np.asarray(rows, dtype=np.int64)]
    cols = ["row_id", "asset_id", "t_c", "h_min", "p_star", "low_t0", "close_tc", "a_log", "sigma_w", "atr14_15m"]
    if "atr14_15m" not in ev.columns:
        ev = ev.with_columns(pl.lit(float("nan")).alias("atr14_15m"))
    out = []
    for r in ev.select(cols).iter_rows(named=True):
        res = simulate_rule_event(bars[r["asset_id"]], we._epoch_min(r["t_c"]), int(r["h_min"]), r, rule, costs)
        if res["exit_type"] == "invalid":
            continue
        out.append({"row_id": int(r["row_id"]), "asset_id": r["asset_id"], "sigma_w": float(r["sigma_w"]), **res})
    df = pd.DataFrame(out)
    if df.empty:
        return pd.DataFrame(columns=["row_id", "asset_id", "sigma_w", "exit_type", "net", "net_sigma", "net_bps", "same_bar"])
    df["net_bps"] = df["net"] * 1e4
    df["net_sigma"] = df["net"] / df["sigma_w"]
    return df


__all__ = ["DEFAULT_RULE", "LEVEL_KINDS", "TradeRule", "simulate_rule", "simulate_rule_event"]
