# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Triple-barrier trade simulation of the Impulse Scout benchmark (5300 "Triple-barrier kereskedési szimuláció").

Long entry at the open of the first 1m bar after ``t_c`` (taker + slippage), target ``P*`` (maker), stop ``low[t0]``
(taker + slippage), time-out ``t_c + H`` at the close of the last window bar (taker + slippage). A bar that touches both
the target and the stop is a STOP (conservative, same as the label). Costs come from ``config/strategies.json``
``costs`` (maker / taker / slippage bps and the exit order type per barrier) — no number is hard-coded here.

Fill assumptions (documented, 5300 "feltételezés — validálásra vár"):

* target: limit order at ``P*`` fills at ``max(P*, open of the touching bar)`` (a gap above ``P*`` fills at the open);
* stop: stop order at ``low[t0]`` fills at ``min(low[t0], open of the touching bar)`` (a gap below fills at the open);
* the entry bar itself is part of the window (label window = bars ``t_c`` .. ``t_c + H - 1m`` by open time);
* no funding fee (5300: sensitivity only).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl

from reports.builders import wave_events as we

CONFIG_PATH = Path(__file__).resolve().parents[4] / "config" / "strategies.json"
_EXIT_TYPES = ("tp", "sl", "timeout")


@dataclass(frozen=True)
class Costs:
    maker_bps: float
    taker_bps: float
    slippage_bps: float
    exit_order_type: Mapping[str, str] = field(default_factory=dict)
    entry_order_type: str = "taker"          # 5300: market entry (taker + slippage)

    def fee_bps(self, kind: str) -> float:
        return self.maker_bps if kind == "maker" else self.taker_bps

    def to_json(self) -> str:
        return json.dumps({"maker_bps": self.maker_bps, "taker_bps": self.taker_bps, "slippage_bps": self.slippage_bps,
                           "exit_order_type": dict(self.exit_order_type), "entry_order_type": self.entry_order_type},
                          sort_keys=True)


def load_costs(path: Path | str | None = None) -> Costs:
    """Read the ``costs`` section of ``config/strategies.json`` (raises on missing / invalid keys)."""
    raw = json.loads(Path(path or CONFIG_PATH).read_text(encoding="utf-8"))["costs"]
    exit_type = dict(raw["exit_order_type"])
    for k in _EXIT_TYPES:
        if exit_type.get(k) not in ("maker", "taker"):
            raise ValueError(f"costs.exit_order_type.{k} must be 'maker' or 'taker'")
    return Costs(float(raw["maker_bps"]), float(raw["taker_bps"]), float(raw["slippage_bps"]), exit_type,
                 raw.get("entry_order_type", "taker"))


def simulate_event(m: we.MinuteData, t_c_min: int, h: int, p_star: float, stop: float, costs: Costs) -> dict[str, Any]:
    """One trade on the 1m grid. ``t_c_min`` = absolute epoch minute of ``t_c`` (= open of the entry bar)."""
    i0 = t_c_min - m.m0
    if i0 < 0 or i0 + h > m.n:
        return {"exit_type": "invalid"}
    hi, lo, op, cl = m.high[i0:i0 + h], m.low[i0:i0 + h], m.open[i0:i0 + h], m.close[i0:i0 + h]
    if np.isnan(hi).any() or np.isnan(lo).any() or np.isnan(op[0]) or np.isnan(cl[-1]):
        return {"exit_type": "invalid"}
    entry = float(op[0])
    tp_bars = np.flatnonzero(hi > p_star)
    sl_bars = np.flatnonzero(lo < stop)
    tp_i = int(tp_bars[0]) if tp_bars.size else None
    sl_i = int(sl_bars[0]) if sl_bars.size else None
    same_bar = tp_i is not None and tp_i == sl_i
    if sl_i is not None and (tp_i is None or sl_i <= tp_i):
        kind, bar = "sl", sl_i
        exit_px = min(stop, float(op[bar]))
    elif tp_i is not None:
        kind, bar = "tp", tp_i
        exit_px = max(p_star, float(op[bar]))
    else:
        kind, bar = "timeout", h - 1
        exit_px = float(cl[-1])
    slip = costs.slippage_bps / 1e4
    entry_eff = entry * (1 + slip) if costs.entry_order_type == "taker" else entry
    exit_kind = costs.exit_order_type[kind]
    exit_eff = exit_px * (1 - slip) if exit_kind == "taker" else exit_px
    fees = (costs.fee_bps(costs.entry_order_type) + costs.fee_bps(exit_kind)) / 1e4
    gross = exit_px / entry - 1.0
    net = exit_eff / entry_eff - 1.0 - fees
    return {"exit_type": kind, "entry": entry, "exit": exit_px, "bars_held": bar + 1, "same_bar": same_bar,
            "gap": bool((kind == "tp" and op[bar] > p_star) or (kind == "sl" and op[bar] < stop)), "gross": gross, "net": net}


def simulate(events: pl.DataFrame, bars: Mapping[str, we.MinuteData], costs: Costs) -> pd.DataFrame:
    """Trades of every event row (columns ``asset_id, t_c, h_min, p_star, low_t0, sigma_w`` + ``row_id``); invalid
    (gappy) windows are dropped. Returns a pandas frame with ``row_id``, ``asset_id``, ``t0``-free trade fields."""
    rows = []
    cols = events.select("row_id", "asset_id", "t_c", "h_min", "p_star", "low_t0", "sigma_w").iter_rows()
    for eid, asset, t_c, h, p_star, low0, sig in cols:
        r = simulate_event(bars[asset], we._epoch_min(t_c), int(h), float(p_star), float(low0), costs)
        if r["exit_type"] == "invalid":
            continue
        rows.append({"row_id": int(eid), "asset_id": asset, "sigma_w": float(sig), **r})
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["row_id", "asset_id", "sigma_w", "exit_type", "net", "net_sigma", "net_bps"])
    df["net_bps"] = df["net"] * 1e4
    df["net_sigma"] = df["net"] / df["sigma_w"]
    return df


def summarise(trades: pd.DataFrame, t0: np.ndarray | None = None, *, n_boot: int = 2000, seed: int = 20260101
              ) -> dict[str, Any]:
    """Hit / stop / time-out rates, net expectancy (bps and sigma_W) with a day-cluster bootstrap CI, profit factor, max
    drawdown (cumulative net return over the time-ordered trades), break-even hit rate. ``t0``: trade times (aligned to
    ``trades`` rows) for the day clusters."""
    from reports.builders.scout_bench.stats import cluster_bootstrap, day_clusters

    n = len(trades)
    out: dict[str, Any] = {"n": n}
    if n == 0:
        return out | {k: float("nan") for k in ("hit_rate", "stop_rate", "timeout_rate", "same_bar_rate", "mean_net_bps",
                                                "ci_lo_bps", "ci_hi_bps", "mean_net_sigma", "total_net_bps",
                                                "profit_factor", "max_dd_bps", "break_even_hit")}
    net = trades["net"].to_numpy()
    kind = trades["exit_type"].to_numpy()
    out.update({"hit_rate": float((kind == "tp").mean()), "stop_rate": float((kind == "sl").mean()),
                "timeout_rate": float((kind == "timeout").mean()), "same_bar_rate": float(trades["same_bar"].mean()),
                "mean_net_bps": float(net.mean() * 1e4), "mean_net_sigma": float(trades["net_sigma"].mean()),
                "total_net_bps": float(net.sum() * 1e4)})
    gain, loss = net[net > 0].sum(), -net[net < 0].sum()
    out["profit_factor"] = float(gain / loss) if loss > 0 else float("inf") if gain > 0 else float("nan")
    cum = np.cumsum(net)
    out["max_dd_bps"] = float((np.maximum.accumulate(np.r_[0.0, cum])[1:] - cum).max() * 1e4)
    g = net[kind == "tp"].mean() if (kind == "tp").any() else float("nan")
    lo = net[kind != "tp"].mean() if (kind != "tp").any() else float("nan")
    out["break_even_hit"] = float(-lo / (g - lo)) if np.isfinite(g) and np.isfinite(lo) and g > lo else float("nan")
    if t0 is not None and n > 1:
        cl = day_clusters(t0)
        lo_ci, hi_ci, _ = cluster_bootstrap(lambda idx: float(net[idx].mean() * 1e4), cl, n_boot=n_boot, seed=seed,
                                            salt=777_003)
        out["ci_lo_bps"], out["ci_hi_bps"] = lo_ci, hi_ci
    else:
        out["ci_lo_bps"] = out["ci_hi_bps"] = float("nan")
    return out
