# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Impulse Scout post-hoc live scoring (epic_073 / t938) — descriptive only, no persistence.

``uv run python -m reports.builders.scout_live_score --asset solusdt --since 2026-11-20``

1. Loads ONLY the minimal 1m window of one asset from its live DB (read-only): ``since - lookback`` (7 day sigma + 3 day
   pretrend + greedy warm-up) up to the last bar.
2. Detects events with the exact 5300 / 5200 v2 definition (:func:`reports.builders.scout_events.scout_params` ``a15``,
   cell ``W4h_H8h``) via :func:`reports.builders.wave_events.build_events`; only events with ``t0 >= since`` are kept —
   every other row (and its label) is dropped before anything is shown (holdout-2 hygiene: the window lies in holdout-2).
3. Fits the S0 + C baseline logit (``scout_bench.baseline_spec``) on the full prepared **train** frame (t930 a15, pooled
   SOL + BTC + ETH) and recomputes its pooled train OOF score (shared folds) for the percentile and the top-20% / 10%
   thresholds (80th / 90th percentile of the train OOF score, 5300). Nothing from OOS / holdout-2 enters the fit.
4. Trade plans: default triple barrier (TP ``P*``, SL ``low[t0]``, ``t_c + 8h``) and round-2 ``R2_03`` (TP +3 sigma_W,
   SL -1.5 sigma_W from the entry = open of the bar opening at ``t_c``); the outcome so far (first touch, stop checked
   first inside a bar) is POST-HOC and marked as such.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import duckdb
import numpy as np
import polars as pl

from reports.builders import scout_bench as sb
from reports.builders import scout_events as se
from reports.builders import wave_events as we
from reports.builders.scout_bench.trades import Costs, load_costs
from scout.freeze import (  # noqa: F401  (re-export; lifted to scout.freeze, t940)
    SEED,
    Baseline,
    fit_baseline,
)
from scout.runtime import R2_03  # noqa: F401  (re-export)

logger = logging.getLogger(__name__)

CELL = se.PRIMARY_CELL
#: 7 day sigma window + 3 day pretrend overlap; +1 day greedy non-overlap warm-up -> 8 days before ``since``
LOOKBACK_DAYS = 8


# --------------------------------------------------------------------------- data


def load_window(asset_id: str, start: datetime, *, tries: int = 10, delay: float = 3.0) -> pl.DataFrame:
    """1m OHLCV of ``asset_id`` with ``open_time >= start`` from the live DB (read-only, retries on lock)."""
    from data_handling.store.readonly_query import open_readonly_connection

    sql = "SELECT open_time, open, high, low, close, volume FROM live.ohlcv WHERE open_time >= ? ORDER BY open_time"
    last: Exception | None = None
    for attempt in range(tries):
        try:
            con = open_readonly_connection(asset_id)
        except duckdb.Error as exc:
            last = exc
            logger.warning("live db locked (%s, attempt %d/%d): %s", asset_id, attempt + 1, tries, exc)
            time.sleep(delay)
            continue
        try:
            return con.execute(sql, [start]).pl()
        finally:
            con.close()
    raise RuntimeError(f"live db unavailable for {asset_id}: {last}")


# --------------------------------------------------------------------------- events


def detect(df_1m: pl.DataFrame, asset_id: str, since: datetime) -> tuple[pl.DataFrame, we.MinuteData]:
    """Primary-cell a15 events with ``t0 >= since`` (all other rows dropped) + the minute grid."""
    p = se.scout_params(se.PRIMARY_VARIANT)
    ds = we.build_events(df_1m, p)
    ev = ds.events.filter((pl.col("cell_id") == CELL) & (pl.col("t0") >= since))
    ids = [se.event_id(asset_id, CELL, t) for t in ev["t0"].to_list()]
    ev = ev.with_columns(pl.lit(asset_id).alias("asset_id"), pl.Series("event_id", ids, dtype=pl.Utf8))
    return ev, we.to_minute_grid(df_1m)


@dataclass
class Touch:
    kind: str                  # "tp" | "sl" | "timeout" | "open"
    bar: int | None            # bar offset after t_c (0 = the bar opening at t_c)
    price: float | None
    time: datetime | None


def first_touch(m: we.MinuteData, t_c_min: int, h: int, tp: float, sl: float) -> Touch:
    """First barrier touch in the bars opening at ``t_c .. t_c + h - 1`` that exist so far (stop first inside a bar,
    same convention as the label and :mod:`scout_bench.rules`). ``open`` = no touch yet and the window is not over."""
    i0 = t_c_min - m.m0
    end = min(i0 + h, m.n)
    for j in range(max(i0, 0), end):
        o = float(m.open[j])
        if m.low[j] < sl:
            return Touch("sl", j - i0, min(sl, o), we._dt(m.m0 + j))
        if m.high[j] > tp:
            return Touch("tp", j - i0, max(tp, o), we._dt(m.m0 + j))
    if i0 + h <= m.n:
        j = i0 + h - 1
        return Touch("timeout", h - 1, float(m.close[j]), we._dt(m.m0 + j + 1))
    return Touch("open", None, None, None)


def net_bps(entry: float, touch: Touch, costs: Costs) -> float | None:
    """Net return (bp) of a long trade, same formula as :func:`scout_bench.trades.simulate_event`."""
    if touch.kind == "open" or touch.price is None:
        return None
    slip = costs.slippage_bps / 1e4
    entry_eff = entry * (1 + slip) if costs.entry_order_type == "taker" else entry
    ek = costs.exit_order_type[touch.kind]
    exit_eff = touch.price * (1 - slip) if ek == "taker" else touch.price
    fees = (costs.fee_bps(costs.entry_order_type) + costs.fee_bps(ek)) / 1e4
    return (exit_eff / entry_eff - 1.0 - fees) * 1e4


# --------------------------------------------------------------------------- baseline
# t940: the baseline fit lives in :mod:`scout.freeze` (frozen artifact ``artifacts/scout_s0c_r1``); re-exported here.


# --------------------------------------------------------------------------- scoring


def score_events(ev: pl.DataFrame, m: we.MinuteData, base: Baseline, costs: Costs) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not ev.height:
        return out
    proba = np.asarray(base.model.predict_proba(ev, None), dtype=float)
    for k, r in enumerate(ev.iter_rows(named=True)):
        s = float(proba[k, 0])
        t_c_min = we._epoch_min(r["t_c"])
        h = int(r["h_min"])
        i0 = t_c_min - m.m0
        entry = float(m.open[i0]) if 0 <= i0 < m.n else float("nan")
        sig = float(r["sigma_w"])
        label_touch = first_touch(m, t_c_min, h, float(r["p_star"]), float(r["low_t0"]))
        plans = {}
        for name, tp, sl in (("default", float(r["p_star"]), float(r["low_t0"])),
                             ("R2_03", entry * float(np.exp(3.0 * sig)), entry * float(np.exp(-1.5 * sig)))):
            t = first_touch(m, t_c_min, h, tp, sl) if np.isfinite(entry) else Touch("open", None, None, None)
            plans[name] = {"entry": entry, "tp": tp, "sl": sl, "timeout": r["t_c"] + timedelta(minutes=h),
                           "tp_pct": (tp / entry - 1) * 100, "sl_pct": (sl / entry - 1) * 100,
                           "outcome": t.kind, "exit_price": t.price, "exit_time": t.time,
                           "net_bps": net_bps(entry, t, costs)}
        cls = {"tp": "impulse", "sl": "false_start", "timeout": "sideways", "open": None}[label_touch.kind]
        out.append({
            "event_id": r["event_id"], "t0": r["t0"], "t_c": r["t_c"], "low_t0": r["low_t0"], "close_tc": r["close_tc"],
            "a_log": r["a_log"], "sigma_w": sig, "a_sigma": r["a_sigma"], "p_star": r["p_star"],
            "features": {c: r[c] for c in sb.S0C_COLS},
            "p_impulse": s, "p_false_start": float(proba[k, 1]), "p_sideways": float(proba[k, 2]),
            "oof_percentile": float((base.oof_score <= s).mean() * 100),
            "top20": s >= base.thr20, "top10": s >= base.thr10,
            "label_so_far": cls, "label_touch_time": label_touch.time,
            "label_resolved": label_touch.kind != "open", "plans": plans,
        })
    return out


def _fmt(rows: list[dict[str, Any]], base: Baseline, data_last: datetime) -> str:
    lines = [f"baseline S0+C logit: train n={base.n_train} (impulse {base.n_impulse}), train OOF AUC {base.oof_auc:.3f}, "
             f"top20 thr {base.thr20:.4f}, top10 thr {base.thr10:.4f}; data last bar {data_last}"]
    for r in rows:
        lines.append(f"\n{r['event_id']}  t0={r['t0']}  t_c={r['t_c']}  low={r['low_t0']:.4f}  close_tc={r['close_tc']:.4f}  "
                     f"A={r['a_log']:.4f}  sigma_W={r['sigma_w']:.4f}  A/sigma_W={r['a_sigma']:.2f}  P*={r['p_star']:.4f}")
        lines.append("  features: " + json.dumps({k: round(float(v), 4) for k, v in r["features"].items()}))
        lines.append(f"  P(impulse)={r['p_impulse']:.3f}  P(false_start)={r['p_false_start']:.3f}  "
                     f"P(sideways)={r['p_sideways']:.3f}  OOF pct={r['oof_percentile']:.1f}  top20={r['top20']}  top10={r['top10']}")
        lines.append(f"  label so far (POST-HOC): {r['label_so_far'] or 'open'} at {r['label_touch_time']}")
        for name, pl_ in r["plans"].items():
            nb = "—" if pl_["net_bps"] is None else f"{pl_['net_bps']:+.1f} bp"
            lines.append(f"  {name:7s} entry {pl_['entry']:.4f}  TP {pl_['tp']:.4f} ({pl_['tp_pct']:+.2f}%)  "
                         f"SL {pl_['sl']:.4f} ({pl_['sl_pct']:+.2f}%)  timeout {pl_['timeout']}  -> {pl_['outcome']} "
                         f"{'' if pl_['exit_price'] is None else f'@ {pl_['exit_price']:.4f} {pl_['exit_time']}'}  net {nb}")
    if not rows:
        lines.append("no qualifying event since the given date")
    return "\n".join(lines)


def run(asset_id: str, since: datetime, *, lookback_days: int = LOOKBACK_DAYS) -> tuple[list[dict[str, Any]], Baseline, datetime]:
    if since < datetime.fromisoformat(we.WaveParams().holdout2_start):
        raise ValueError("live scoring is meant for holdout-2-period events only (train / OOS events have their own pipeline)")
    df = load_window(asset_id, since - timedelta(days=lookback_days))
    ev, m = detect(df, asset_id, since)
    base = fit_baseline()
    last: datetime = df["open_time"].max()  # type: ignore[assignment]
    return score_events(ev, m, base, load_costs()), base, last


def main() -> None:
    ap = argparse.ArgumentParser(description="Impulse Scout post-hoc live scoring (descriptive, t938)")
    ap.add_argument("--asset", default="solusdt")
    ap.add_argument("--since", required=True, help="first t0 to report (UTC date / datetime, >= 2026-08-01)")
    ap.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    rows, base, last = run(a.asset, datetime.fromisoformat(a.since), lookback_days=a.lookback_days)
    print(_fmt(rows, base, last))


if __name__ == "__main__":
    main()
