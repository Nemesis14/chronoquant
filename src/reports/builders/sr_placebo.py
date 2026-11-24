# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportAssignmentType=false, reportReturnType=false
"""General report ``sr_placebo_study``: Osler-style placebo event study of the S/R detector (epic_071 / t913).

Question: do *real* detected levels / trendlines produce more bounces (touch), follow-throughs (break) and
holds (retest) than *placebo* objects with the same statistical structure? Placebo objects keep everything of
a real object except its price (levels) or price + slope (lines):

* **placebo level** — same scale, same confirmation bar (``confirm_idx``), same life (``expired_at``); the
  price is ``close[confirm] ± |d|·ATR[confirm]`` with ``|d|`` drawn from the real levels' ATR distance
  distribution of that scale and a random sign (methodology 1700: "az irány véletlen");
* **placebo line** — same kind (support / resistance), confirmation bar, expiry and expiry reason as the real
  line; the line price at the confirmation bar is ``close + d·ATR`` with the signed ``d`` drawn from the real
  lines of that kind, the slope (ATR/bar) drawn independently from the real slopes of that kind.

The events and outcome labels of placebo objects come from the *same* state machine and label rules as the
detector (``_levels_polars._scan_object`` / ``forward_label_events``). Because one placebo set through the
detector takes ~8 s, ``scan_batch`` / ``label_batch`` re-implement those two functions vectorised over K
placebo variants of one object; :func:`verify_against_detector` (run in every build on placebo set 0)
asserts that they give exactly the detector's events and labels. The detector library is not modified.

Statistics per cell (scale × object type × side × event type) and period (train 2021-01-01 → 2025-05-01,
OOS 2025-05-01 → data end): real rate, placebo rate (pooled over the sets), Δ = real − placebo; a paired
day-block bootstrap (block = 1 UTC day, joint resampling of days for all cells) gives the SE and a two-sided
normal p (the empirical bootstrap p is reported as well); Holm correction over the cells per period;
go / no-go by the 1700 thresholds. Plus a memory curve (Δ of touch → bounce by object age band) and a
univariate diagnostic of the ``feat_levels`` columns against the champion targets.

Regenerate::

    uv run python -m reports.builders.sr_placebo          (cwd: repo root; ~5-8 min, 1000 placebo sets)
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from data_handling.sync_tables import _levels_polars as det
from reports import ReportBuilder, theme
from reports.builders._common import fmt, source_info, themed
from reports.builders.sr_detection import load_ohlcv_1m

logger = logging.getLogger(__name__)

SLUG = "sr_placebo_study"
GENERATOR = "reports.builders.sr_placebo"
SCALES = ("s", "m", "l")
OBJ_TYPES = ("level", "line")
SIDES = ("support", "resistance")
EVENT_TYPES = ("touch", "break", "retest")
POSITIVE = {"touch": "bounce", "break": "follow_through", "retest": "hold"}
EVENT_HU = {"touch": "touch → bounce", "break": "break → follow-through", "retest": "retest → hold"}
OBJ_HU = {"level": "szint", "line": "vonal"}
SIDE_HU = {"support": "támasz", "resistance": "ellenállás"}

TRAIN_START = datetime(2021, 1, 1)
OOS_START = datetime(2025, 5, 1)
PERIODS = ("train", "oos")
PERIOD_HU = {"train": "train 2021-01 → 2025-05", "oos": "OOS 2025-05 → (vizuálisan látott)"}

N_SETS = 1000            # placebo sets (Osler: artificial level sets)
N_BOOT = 5000            # day-block bootstrap replications
SEED = 20261119
ALPHA = 0.01             # 1700: p < 0.01 (here after Holm)
MIN_DELTA = 0.05         # 1700: |Δ| >= 5 pp (GO needs Δ >= +5 pp)
MIN_EVENTS = 100         # 1700: >= 100 events per cell (required in train AND in OOS)
AGE_EDGES = (1.0, 7.0, 30.0, 180.0)          # days
AGE_LABELS = ("<1d", "1–7d", "7–30d", "30–180d", ">180d")
_NEVER = np.iinfo(np.int64).max
_DAY = 86400

ET_CODE = {e: i for i, e in enumerate(EVENT_TYPES)}


def cell_index(scale: str, obj_type: str, side: str, event_type: str) -> int:
    """Global cell index 0..35 (scale-major)."""
    return (SCALES.index(scale) * 12 + OBJ_TYPES.index(obj_type) * 6 + SIDES.index(side) * 3
            + EVENT_TYPES.index(event_type))


CELLS = [(s, o, d, e) for s in SCALES for o in OBJ_TYPES for d in SIDES for e in EVENT_TYPES]
N_CELLS = len(CELLS)


# --------------------------------------------------------------------------- vectorised detector logic


def scan_batch(arrays: tuple[np.ndarray, ...], start: int, stop: int, p0: np.ndarray, num0: np.ndarray | int,
               slope: np.ndarray | float, side0: np.ndarray, eps_k: float, brk_k: float, retest_n: int,
               kill_on_break: bool) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """``_levels_polars._scan_object`` vectorised over K variants of one object (same bars, other price/slope).

    Args:
        arrays : (high, low, close, atr, bar_num) numpy arrays of the scale bars.
        start, stop : scanned bar positions start..stop-1 (as in the detector).
        p0, num0, slope : object price p0 at bar number num0 and slope per bar; arrays of length K (or scalars).
        side0  : +1 (price above = support) / -1 per variant.

    Returns:
        (variant k, bar position j, event type code [0 touch, 1 break, 2 retest], role_up [support at the
        event, before a break flips it], object price at the bar) — one entry per emitted event.
    """
    hi, lo, cl, atr, bn = arrays
    p0 = np.asarray(p0, dtype=float)
    K = p0.size
    slope_a = np.broadcast_to(np.asarray(slope, dtype=float), (K,))
    num0_a = np.broadcast_to(np.asarray(num0, dtype=np.int64), (K,))
    side = np.asarray(side0, dtype=float).copy()
    j0 = start - 1
    obj0 = p0 + slope_a * (bn[j0] - num0_a)
    with np.errstate(invalid="ignore"):
        near_prev = np.where(side > 0, lo[j0] <= obj0 + eps_k * atr[j0], hi[j0] >= obj0 - eps_k * atr[j0])
    retest_until = np.full(K, -1, dtype=np.int64)
    broken = np.zeros(K, dtype=bool)
    alive = np.ones(K, dtype=bool)
    ks: list[np.ndarray] = []
    js: list[np.ndarray] = []
    ts: list[np.ndarray] = []
    rs: list[np.ndarray] = []
    ps: list[np.ndarray] = []
    for j in range(start, stop):
        a = atr[j]
        if not np.isfinite(a):
            continue
        if kill_on_break:
            alive &= ~(broken & (j > retest_until))
            if not alive.any():
                break
        obj = p0 + slope_a * (bn[j] - num0_a)
        eps = eps_k * a
        up = side > 0
        brk = alive & ~broken & (side * (obj - cl[j]) > brk_k * a)
        near = np.where(up, lo[j] <= obj + eps, hi[j] >= obj - eps)
        arm = alive & ~brk & near & ~near_prev
        ret = arm & (j <= retest_until)
        tch = arm & ~ret & ~broken
        for code, mask in ((1, brk), (2, ret), (0, tch)):
            if mask.any():
                idx = np.flatnonzero(mask)
                ks.append(idx)
                js.append(np.full(idx.size, j, dtype=np.int64))
                ts.append(np.full(idx.size, code, dtype=np.int8))
                rs.append(up[idx])
                ps.append(obj[idx])
        if brk.any() or ret.any():
            retest_until = np.where(ret, -1, retest_until)
            retest_until = np.where(brk, j + retest_n, retest_until)
            side = np.where(brk, -side, side)
            if kill_on_break:
                broken |= brk
        near_prev = np.where(brk, True, np.where(alive, near, near_prev))
    if not ks:
        e = np.empty(0)
        return e.astype(np.int64), e.astype(np.int64), e.astype(np.int8), e.astype(bool), e
    return (np.concatenate(ks), np.concatenate(js), np.concatenate(ts), np.concatenate(rs), np.concatenate(ps))


def label_batch(arrays: tuple[np.ndarray, ...], j: np.ndarray, etype: np.ndarray, role_up: np.ndarray,
                price: np.ndarray, slope: np.ndarray, eps_obj: np.ndarray, params: dict) -> np.ndarray:
    """``_levels_polars.forward_label_events`` vectorised; returns 1 = positive label, 0 = negative, -1 = null.

    Args:
        arrays  : (high, low, close, atr, bar_num) of the scale bars.
        j       : event bar positions; ``etype`` codes (0 touch, 1 break, 2 retest); ``role_up`` = side is support.
        price   : object price at the event bar; ``slope`` per bar; ``eps_obj`` = tolerance factor of the object type.
    """
    hi, lo, cl, atr, bn = arrays
    H = int(params["forward_bars"])
    move_k = float(params["forward_move_atr"])
    brk_k = float(params["break_atr"])
    n = bn.size
    E = j.size
    if E == 0:
        return np.empty(0, dtype=np.int8)
    ks = j[:, None] + np.arange(1, H + 1)[None, :]
    valid = ks < n
    kc = np.minimum(ks, n - 1)
    nvalid = valid.sum(axis=1)
    is_brk = etype == 1
    d = np.where(is_brk, np.where(role_up, -1.0, 1.0), np.where(role_up, 1.0, -1.0))
    a = atr[j]
    obj = price[:, None] + slope[:, None] * (bn[kc] - bn[j][:, None])
    ref = np.where(is_brk[:, None], cl[j][:, None], obj)
    fav = np.where(d[:, None] > 0, hi[kc] - ref, ref - lo[kc])
    hit = (fav >= move_k * a[:, None]) & valid
    dist = d[:, None] * (cl[kc] - obj)
    ab = atr[kc]
    with np.errstate(invalid="ignore"):
        neg_t = dist < -brk_k * ab
        neg_b = dist <= eps_obj[:, None] * ab
    neg_r = dist < 0
    neg = np.where((etype == 0)[:, None], neg_t, np.where(is_brk[:, None], neg_b, neg_r)) & valid
    i_pos = np.where(hit.any(axis=1), hit.argmax(axis=1), _NEVER)
    i_neg = np.where(neg.any(axis=1), neg.argmax(axis=1), _NEVER)
    short = nvalid < H
    lab_other = np.where(i_pos < i_neg, 1, np.where(i_neg != _NEVER, 0, np.where(short, -1, 0)))
    lab_ret = np.where(i_neg != _NEVER, 0, np.where(short, -1, 1))
    return np.where(etype == 2, lab_ret, lab_other).astype(np.int8)


# --------------------------------------------------------------------------- placebo objects


@dataclass
class ScalePlacebo:
    """Real objects of one scale + K placebo variants (matrices n_obj × K)."""

    scale: str
    level_ci: np.ndarray
    level_stop: np.ndarray
    level_origin: np.ndarray          # first touch pivot time, epoch s (age origin)
    level_price: np.ndarray           # n_levels × K placebo prices
    level_d_real: np.ndarray          # signed ATR distance of the real levels at confirmation
    line_ci: np.ndarray
    line_stop: np.ndarray
    line_origin: np.ndarray           # anchor1 time, epoch s
    line_kind_up: np.ndarray          # True = support line
    line_p0: np.ndarray               # n_lines × K price at the confirmation bar
    line_slope: np.ndarray            # n_lines × K slope per bar (price units)
    line_d_real: np.ndarray
    line_slope_atr_real: np.ndarray
    line_slope_atr_pl: np.ndarray = field(default_factory=lambda: np.empty((0, 0)))
    level_d_pl: np.ndarray = field(default_factory=lambda: np.empty((0, 0)))
    line_d_pl: np.ndarray = field(default_factory=lambda: np.empty((0, 0)))


def _bar_arrays(res: det.ScaleResult) -> tuple[np.ndarray, ...]:
    b = res.bars
    return (b["high"].to_numpy(), b["low"].to_numpy(), b["close"].to_numpy(),
            b["atr"].fill_null(np.nan).to_numpy(), b["bar_num"].to_numpy())


def _stops(res: det.ScaleResult, expired: pl.Series, extra: np.ndarray | None = None) -> np.ndarray:
    """Scan stop positions exactly as ``detect_events._stop`` (+ the retest window of broken lines)."""
    n = res.bars.height
    ct = res.bars["close_time"].to_numpy()
    exp = expired.to_numpy()
    out = np.full(exp.size, n, dtype=np.int64)
    has = ~np.isnat(exp)
    out[has] = np.searchsorted(ct, exp[has].astype("datetime64[us]"), side="right")
    if extra is not None:
        out[has] = np.minimum(n, out[has] + extra[has])
    return out


def _epoch_s(series: pl.Series) -> np.ndarray:
    return series.dt.epoch("s").to_numpy().astype(np.int64)


def make_placebo(res: det.ScaleResult, params: dict, n_sets: int, seed: int) -> ScalePlacebo:
    """Draw ``n_sets`` placebo variants of every real level and trendline of one scale (deterministic)."""
    rng = np.random.default_rng([seed, SCALES.index(res.scale) if res.scale in SCALES else 9])
    hi, lo, cl, atr, bn = _bar_arrays(res)
    lv = res.levels
    lci = lv["confirm_idx"].to_numpy().astype(np.int64)
    ld = (lv["price"].to_numpy() - cl[lci]) / atr[lci]
    pool = np.abs(ld[np.isfinite(ld)])
    n_l = lci.size
    if n_l and pool.size:
        mag = pool[rng.integers(0, pool.size, size=(n_l, n_sets))]
        sign = np.where(rng.random((n_l, n_sets)) < 0.5, -1.0, 1.0)
        lprice = cl[lci][:, None] + sign * mag * atr[lci][:, None]
    else:
        lprice = np.empty((n_l, n_sets))

    tl = res.trendlines
    tci = tl["confirm_idx"].to_numpy().astype(np.int64)
    up = (tl["kind"] == "support").to_numpy()
    line_px = tl["anchor1_price"].to_numpy() + tl["slope_per_bar"].to_numpy() * (bn[tci] - tl["anchor1_bar_num"].to_numpy())
    td = (line_px - cl[tci]) / atr[tci]
    tslope = tl["slope_per_bar_atr"].to_numpy()
    n_t = tci.size
    p0 = np.empty((n_t, n_sets))
    sl_atr = np.empty((n_t, n_sets))
    for kind_up in (True, False):
        rows = np.flatnonzero(up == kind_up)
        ok = rows[np.isfinite(td[rows]) & np.isfinite(tslope[rows])]
        if rows.size == 0 or ok.size == 0:
            continue
        dd = td[ok][rng.integers(0, ok.size, size=(rows.size, n_sets))]
        ss = tslope[ok][rng.integers(0, ok.size, size=(rows.size, n_sets))]
        p0[rows] = cl[tci[rows]][:, None] + dd * atr[tci[rows]][:, None]
        sl_atr[rows] = ss
    extra = np.where((tl["expiry_reason"] == "break").fill_null(False).to_numpy(), int(params["retest_bars"]), 0)
    return ScalePlacebo(
        scale=res.scale, level_ci=lci, level_stop=_stops(res, lv["expired_at"]),
        level_origin=_epoch_s(lv["first_touch"]), level_price=lprice, level_d_real=ld,
        line_ci=tci, line_stop=_stops(res, tl["expired_at"], extra), line_origin=_epoch_s(tl["anchor1_time"]),
        line_kind_up=up, line_p0=p0, line_slope=sl_atr * atr[tci][:, None], line_d_real=td,
        line_slope_atr_real=tslope, line_slope_atr_pl=sl_atr,
        level_d_pl=(lprice - cl[lci][:, None]) / atr[lci][:, None] if n_l else np.empty((0, n_sets)),
        line_d_pl=(p0 - cl[tci][:, None]) / atr[tci][:, None] if n_t else np.empty((0, n_sets)),
    )


def placebo_frames(res: det.ScaleResult, pb: ScalePlacebo, k: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Placebo set ``k`` as detector-compatible ``levels`` / ``trendlines`` frames (for the verification)."""
    bn = res.bars["bar_num"].to_numpy()
    lv = res.levels.select("level_id", "confirm_idx", "expired_at").with_columns(
        pl.Series("price", pb.level_price[:, k] if pb.level_price.size else np.empty(0)),
        ("P" + pl.col("level_id")).alias("level_id"))
    tl = res.trendlines.select("line_id", "kind", "confirm_idx", "expired_at", "expiry_reason").with_columns(
        ("P" + pl.col("line_id")).alias("line_id"),
        pl.Series("anchor1_price", pb.line_p0[:, k] if pb.line_p0.size else np.empty(0)),
        pl.Series("anchor1_bar_num", bn[pb.line_ci] if pb.line_ci.size else np.empty(0, dtype=np.int64)),
        pl.Series("slope_per_bar", pb.line_slope[:, k] if pb.line_slope.size else np.empty(0)))
    return lv, tl


# --------------------------------------------------------------------------- aggregation


@dataclass
class Accum:
    """Event / success counts per (cell, day, age band) and per-set totals."""

    n_days: int
    n_sets: int
    real_n: np.ndarray = field(init=False)
    real_k: np.ndarray = field(init=False)
    pl_n: np.ndarray = field(init=False)
    pl_k: np.ndarray = field(init=False)
    set_n: np.ndarray = field(init=False)   # n_sets × cells × periods
    set_k: np.ndarray = field(init=False)

    def __post_init__(self) -> None:
        shape = (N_CELLS, self.n_days, len(AGE_LABELS))
        self.real_n, self.real_k = np.zeros(shape), np.zeros(shape)
        self.pl_n, self.pl_k = np.zeros(shape), np.zeros(shape)
        self.set_n = np.zeros((self.n_sets, N_CELLS, 2))
        self.set_k = np.zeros((self.n_sets, N_CELLS, 2))


@dataclass
class Clock:
    """Event-time helpers of one scale: bar close epoch seconds -> study day / period."""

    close_s: np.ndarray
    t0: int
    oos_day: int

    def day(self, j: np.ndarray) -> np.ndarray:
        return (self.close_s[j] - self.t0) // _DAY


def _add(acc: Accum, real: bool, cell: np.ndarray, day: np.ndarray, band: np.ndarray, pos: np.ndarray,
         set_k: np.ndarray | None, oos_day: int) -> None:
    ok = (day >= 0) & (day < acc.n_days)
    cell, day, band, pos = cell[ok], day[ok], band[ok], pos[ok]
    tgt_n, tgt_k = (acc.real_n, acc.real_k) if real else (acc.pl_n, acc.pl_k)
    np.add.at(tgt_n, (cell, day, band), 1.0)
    np.add.at(tgt_k, (cell, day, band), pos)
    if set_k is not None:
        sk = set_k[ok]
        per = (day >= oos_day).astype(np.int64)
        np.add.at(acc.set_n, (sk, cell, per), 1.0)
        np.add.at(acc.set_k, (sk, cell, per), pos)


def _age_band(event_s: np.ndarray, origin_s: np.ndarray) -> np.ndarray:
    age = (event_s - origin_s) / _DAY
    return np.searchsorted(np.asarray(AGE_EDGES), age, side="right")


def accumulate_real(acc: Accum, res: det.ScaleResult, fwd: pl.DataFrame, clock: Clock) -> None:
    """Add the detector's real (labelled) events of one scale."""
    ev = fwd.filter(pl.col("forward_label").is_not_null())
    if ev.height == 0:
        return
    origin = pl.concat([
        res.levels.select(pl.col("level_id").alias("obj_id"), pl.col("first_touch").alias("_o")),
        res.trendlines.select(pl.col("line_id").alias("obj_id"), pl.col("anchor1_time").alias("_o")),
    ])
    ev = ev.join(origin, on="obj_id", how="left")
    s = res.scale
    cell = np.array([cell_index(s, o, d, e) for o, d, e in
                     zip(ev["obj_type"].to_list(), ev["side"].to_list(), ev["event_type"].to_list(), strict=True)])
    j = ev["bar_idx"].to_numpy().astype(np.int64)
    pos = np.array([lab == POSITIVE[e] for lab, e in
                    zip(ev["forward_label"].to_list(), ev["event_type"].to_list(), strict=True)], dtype=float)
    band = _age_band(clock.close_s[j], _epoch_s(ev["_o"]))
    _add(acc, True, cell, clock.day(j), band, pos, None, clock.oos_day)


def accumulate_placebo(acc: Accum, res: det.ScaleResult, pb: ScalePlacebo, params: dict, clock: Clock,
                       collect_set: int | None = None) -> pl.DataFrame | None:
    """Scan + label every placebo variant of every object of one scale and add them to ``acc``.

    With ``collect_set`` the events of that placebo set are also returned (verification against the detector).
    """
    arrays = _bar_arrays(res)
    n = res.bars.height
    eps_l, eps_t = float(params["level_tolerance_atr"]), float(params["line_tolerance_atr"])
    brk_k, rn = float(params["break_atr"]), int(params["retest_bars"])
    cl, bn = arrays[2], arrays[4]
    s = res.scale
    collected: list[dict[str, Any]] = []
    jobs: list[tuple] = []
    for i in range(pb.level_ci.size):
        jobs.append(("level", i, pb.level_ci[i], pb.level_stop[i], pb.level_price[i], 0, 0.0,
                     np.where(cl[pb.level_ci[i]] >= pb.level_price[i], 1.0, -1.0), eps_l, False, pb.level_origin[i]))
    for i in range(pb.line_ci.size):
        ci = pb.line_ci[i]
        jobs.append(("line", i, ci, pb.line_stop[i], pb.line_p0[i], int(bn[ci]), pb.line_slope[i],
                     np.full(pb.line_p0.shape[1], 1.0 if pb.line_kind_up[i] else -1.0), eps_t, True, pb.line_origin[i]))
    for obj_type, i, ci, stop, p0, num0, slope, side0, eps_k, kill, origin in jobs:
        if ci + 1 >= n or not np.all(np.isfinite(p0)):
            continue
        kk, jj, tt, rr, pp = scan_batch(arrays, int(ci) + 1, int(stop), p0, num0, slope, side0, eps_k, brk_k, rn, kill)
        if kk.size == 0:
            continue
        sl = np.zeros(kk.size) if obj_type == "level" else np.asarray(slope)[kk]
        lab = label_batch(arrays, jj, tt, rr, pp, sl, np.full(kk.size, eps_k), params)
        if collect_set is not None:
            m = kk == collect_set
            for j_, t_, r_, p_, l_ in zip(jj[m], tt[m], rr[m], pp[m], lab[m], strict=True):
                collected.append({"obj_type": obj_type, "obj": i, "bar_idx": int(j_), "event_type": EVENT_TYPES[t_],
                                  "side": "support" if r_ else "resistance", "price": float(p_), "label": int(l_)})
        ok = lab >= 0
        kk, jj, tt, rr, lab = kk[ok], jj[ok], tt[ok], rr[ok], lab[ok]
        cell = (SCALES.index(s) * 12 + OBJ_TYPES.index(obj_type) * 6 + np.where(rr, 0, 1) * 3 + tt).astype(np.int64)
        band = _age_band(clock.close_s[jj], np.full(jj.size, origin))
        _add(acc, False, cell, clock.day(jj), band, lab.astype(float), kk, clock.oos_day)
    return pl.DataFrame(collected) if collect_set is not None else None


def verify_against_detector(res: det.ScaleResult, pb: ScalePlacebo, params: dict, batch_events: pl.DataFrame,
                            k: int = 0) -> int:
    """Run placebo set ``k`` through the detector's own ``detect_events`` + ``forward_label_events`` and assert
    that the vectorised re-implementation produced exactly the same events and labels. Returns the event count."""
    lv, tl = placebo_frames(res, pb, k)
    ev = det.detect_events(res.bars, lv, tl, res.scale, params)
    fwd = det.forward_label_events(res.bars, ev, params)
    lab_map = {"bounce": 1, "follow_through": 1, "hold": 1, "break": 0, "false_break": 0, "fail": 0}
    ref = (fwd.with_columns(
        pl.col("obj_type"),
        pl.col("obj_id").str.slice(len(res.scale) + 3).cast(pl.Int64).alias("obj"),
        pl.col("forward_label").replace_strict(lab_map, default=-1, return_dtype=pl.Int64).alias("label"))
        .select("obj_type", "obj", "bar_idx", "event_type", "side", "price", "label"))
    mine = batch_events.select("obj_type", "obj", "bar_idx", "event_type", "side", "price", "label") if batch_events.height else ref.clear()
    key = ["obj_type", "obj", "bar_idx", "event_type"]
    a = ref.sort(key)
    b = mine.with_columns(pl.col("obj").cast(pl.Int64), pl.col("bar_idx").cast(pl.Int64),
                          pl.col("label").cast(pl.Int64)).sort(key)
    if a.height != b.height:
        raise AssertionError(f"placebo verification ({res.scale}): {a.height} detector events vs {b.height} batch events")
    for c in ("obj_type", "obj", "bar_idx", "event_type", "side", "label"):
        if not (a[c] == b[c]).all():
            raise AssertionError(f"placebo verification ({res.scale}): column {c} differs")
    if not np.allclose(a["price"].to_numpy(), b["price"].to_numpy(), rtol=0, atol=1e-9):
        raise AssertionError(f"placebo verification ({res.scale}): object prices differ")
    return a.height


# --------------------------------------------------------------------------- statistics


def holm(p: np.ndarray) -> np.ndarray:
    """Holm step-down adjusted p-values (NaN kept out of the family)."""
    p = np.asarray(p, dtype=float)
    out = np.full(p.size, np.nan)
    ok = np.flatnonzero(np.isfinite(p))
    m = ok.size
    if m == 0:
        return out
    order = ok[np.argsort(p[ok], kind="mergesort")]
    adj = np.minimum(1.0, (m - np.arange(m)) * p[order])
    out[order] = np.maximum.accumulate(adj)
    return out


def _p_norm(delta: float, se: float) -> float:
    if not (np.isfinite(delta) and np.isfinite(se)) or se <= 0:
        return math.nan
    return math.erfc(abs(delta / se) / math.sqrt(2.0))


def bootstrap_weights(n_days: int, n_boot: int, seed: int) -> np.ndarray:
    """Multinomial day-block resampling weights (n_boot × n_days)."""
    rng = np.random.default_rng(seed)
    return rng.multinomial(n_days, np.full(n_days, 1.0 / n_days), size=n_boot).astype(float)


def delta_stats(rn: np.ndarray, rk: np.ndarray, pn: np.ndarray, pk: np.ndarray, w: np.ndarray) -> dict[str, float]:
    """Δ = real − placebo rate and its day-block bootstrap SE / two-sided p (normal and empirical)."""
    n_r, n_p = rn.sum(), pn.sum()
    r = rk.sum() / n_r if n_r else math.nan
    q = pk.sum() / n_p if n_p else math.nan
    delta = r - q
    with np.errstate(invalid="ignore", divide="ignore"):
        db = (w @ rk) / (w @ rn) - (w @ pk) / (w @ pn)
    db = db[np.isfinite(db)]
    se = float(db.std(ddof=1)) if db.size > 10 and n_r else math.nan
    p_emp = (min(1.0, 2.0 * min((db <= 0).mean(), (db >= 0).mean()) + 1.0 / max(db.size, 1))
             if db.size > 10 and n_r else math.nan)
    return {"n_real": float(n_r), "rate_real": r, "rate_placebo": q, "delta": delta, "se": se,
            "ci_lo": delta - 1.96 * se, "ci_hi": delta + 1.96 * se, "p_boot": _p_norm(delta, se), "p_emp": p_emp,
            "n_placebo_per_set": float(n_p)}


def cell_table(acc: Accum, w_by_period: dict[str, np.ndarray], day_slices: dict[str, slice]) -> pd.DataFrame:
    """One row per cell × period with Δ, bootstrap p, placebo-rank p, Holm p."""
    rows = []
    for per_i, per in enumerate(PERIODS):
        sl, w = day_slices[per], w_by_period[per]
        for c, (s, o, d, e) in enumerate(CELLS):
            rn, rk = acc.real_n[c, sl].sum(axis=1), acc.real_k[c, sl].sum(axis=1)
            pn, pk = acc.pl_n[c, sl].sum(axis=1), acc.pl_k[c, sl].sum(axis=1)
            st = delta_stats(rn, rk, pn, pk, w)
            sn, sk = acc.set_n[:, c, per_i], acc.set_k[:, c, per_i]
            with np.errstate(invalid="ignore", divide="ignore"):
                set_rates = sk / sn
            set_rates = set_rates[np.isfinite(set_rates)]
            r = st["rate_real"]
            if set_rates.size and np.isfinite(r):
                S = set_rates.size
                p_rank = min(1.0, 2.0 * min((1 + (set_rates >= r).sum()) / (S + 1), (1 + (set_rates <= r).sum()) / (S + 1)))
                pct = float((set_rates < r).mean())
            else:
                p_rank, pct = math.nan, math.nan
            rows.append({"period": per, "cell": c, "scale": s, "obj_type": o, "side": d, "event_type": e, **st,
                         "n_placebo_per_set": st["n_placebo_per_set"] / acc.n_sets,
                         "p_rank": p_rank, "placebo_pct": pct})
    df = pd.DataFrame(rows)
    df["p_holm"] = np.nan
    for per in PERIODS:
        m = df["period"] == per
        df.loc[m, "p_holm"] = holm(df.loc[m, "p_boot"].to_numpy())
    return df


def go_table(cells: pd.DataFrame) -> pd.DataFrame:
    """Wide train / OOS table with the 1700 go / no-go decision per cell."""
    tr = cells[cells["period"] == "train"].set_index("cell")
    oo = cells[cells["period"] == "oos"].set_index("cell")
    out = tr[["scale", "obj_type", "side", "event_type"]].copy()
    for pre, src in (("tr", tr), ("oos", oo)):
        for c in ("n_real", "rate_real", "rate_placebo", "delta", "se", "ci_lo", "ci_hi", "p_boot", "p_emp", "p_holm",
                  "p_rank"):
            out[f"{pre}_{c}"] = src[c]
    enough = (out["tr_n_real"] >= MIN_EVENTS) & (out["oos_n_real"] >= MIN_EVENTS)
    sig = out["tr_p_holm"] < ALPHA
    same = np.sign(out["tr_delta"]) == np.sign(out["oos_delta"])
    big_pos = out["tr_delta"] >= MIN_DELTA
    big_abs = out["tr_delta"].abs() >= MIN_DELTA
    out["go"] = enough & sig & same & big_pos
    out["go_inverse"] = enough & sig & same & big_abs & ~big_pos
    reason = []
    for _, r in out.iterrows():
        if not (r["tr_n_real"] >= MIN_EVENTS and r["oos_n_real"] >= MIN_EVENTS):
            reason.append(f"kevés esemény (< {MIN_EVENTS})")
        elif not r["tr_p_holm"] < ALPHA:
            reason.append("nem szignifikáns (Holm p ≥ 0,01)")
        elif abs(r["tr_delta"]) < MIN_DELTA:
            reason.append("|Δ| < 5 pp")
        elif np.sign(r["tr_delta"]) != np.sign(r["oos_delta"]):
            reason.append("OOS előjel eltér")
        elif r["tr_delta"] < 0:
            reason.append("fordított hatás (Δ ≤ −5 pp)")
        else:
            reason.append("minden küszöb teljesül")
    out["decision"] = np.where(out["go"], "GO", np.where(out["go_inverse"], "NO-GO (fordított)", "NO-GO"))
    out["reason"] = reason
    return out.reset_index()


def memory_table(acc: Accum, w_by_period: dict[str, np.ndarray], day_slices: dict[str, slice]) -> pd.DataFrame:
    """Δ of touch → bounce per scale × object type × age band (sides pooled), train and OOS."""
    rows = []
    for per in PERIODS:
        sl, w = day_slices[per], w_by_period[per]
        for s in SCALES:
            for o in OBJ_TYPES:
                cs = [cell_index(s, o, d, "touch") for d in SIDES]
                for b, lab in enumerate(AGE_LABELS):
                    rn = acc.real_n[cs, sl, b].sum(axis=0)
                    rk = acc.real_k[cs, sl, b].sum(axis=0)
                    pn = acc.pl_n[cs, sl, b].sum(axis=0)
                    pk = acc.pl_k[cs, sl, b].sum(axis=0)
                    if rn.sum() == 0:
                        continue
                    st = delta_stats(rn, rk, pn, pk, w)
                    rows.append({"period": per, "scale": s, "obj_type": o, "age_band": lab, "band": b, **st,
                                 "n_placebo_per_set": st["n_placebo_per_set"] / acc.n_sets})
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Holm per period over the evaluable (n >= MIN_EVENTS) age-band cells; the rest stays NaN
    df["p_holm"] = np.nan
    for per in PERIODS:
        m = (df["period"] == per) & (df["n_real"] >= MIN_EVENTS)
        if m.any():
            df.loc[m, "p_holm"] = holm(df.loc[m, "p_boot"].to_numpy())
    return df


# --------------------------------------------------------------------------- study driver


@dataclass
class StudyResult:
    cells: pd.DataFrame
    go: pd.DataFrame
    memory: pd.DataFrame
    acc: Accum
    placebos: dict[str, ScalePlacebo]
    verified: dict[str, int]
    counts: dict[str, Any]
    data_first: datetime
    data_last: datetime


def run_study(df_1m: pl.DataFrame, params: dict, *, n_sets: int = N_SETS, n_boot: int = N_BOOT, seed: int = SEED,
              train_start: datetime = TRAIN_START, oos_start: datetime = OOS_START, verify: bool = True,
              results: dict[str, det.ScaleResult] | None = None) -> StudyResult:
    """Detector + placebo sets + aggregation + bootstrap statistics."""
    scales = [s for s in SCALES if s in params["scales"]]
    t = time.time()
    if results is None:
        results = det.detect_all_scales(df_1m, scales=scales, params=params)
    logger.info("detector: %.1f s", time.time() - t)
    last = df_1m["open_time"].max()
    t0 = int(pd.Timestamp(train_start).timestamp())
    n_days = int((pd.Timestamp(last).timestamp() - t0) // _DAY) + 2
    oos_day = int((pd.Timestamp(oos_start).timestamp() - t0) // _DAY)
    acc = Accum(n_days=n_days, n_sets=n_sets)
    placebos: dict[str, ScalePlacebo] = {}
    verified: dict[str, int] = {}
    for s in scales:
        res = results[s]
        clock = Clock(close_s=_epoch_s(res.bars["close_time"]), t0=t0, oos_day=oos_day)
        t = time.time()
        fwd = det.forward_label_events(res.bars, res.events, params)
        accumulate_real(acc, res, fwd, clock)
        pb = make_placebo(res, params, n_sets, seed)
        placebos[s] = pb
        got = accumulate_placebo(acc, res, pb, params, clock, collect_set=0 if verify else None)
        logger.info("scale %s: %d levels, %d lines, %d placebo sets: %.1f s", s, pb.level_ci.size, pb.line_ci.size,
                    n_sets, time.time() - t)
        if verify and got is not None:
            t = time.time()
            verified[s] = verify_against_detector(res, pb, params, got, 0)
            logger.info("scale %s: placebo set 0 == detector (%d events, %.1f s)", s, verified[s], time.time() - t)
    day_slices = {"train": slice(0, oos_day), "oos": slice(oos_day, n_days)}
    w = {per: bootstrap_weights(day_slices[per].stop - day_slices[per].start, n_boot, seed + i)
         for i, per in enumerate(PERIODS)}
    cells = cell_table(acc, w, day_slices)
    counts = {s: {"levels": int(results[s].levels.height), "lines": int(results[s].trendlines.height),
                  "events": int(results[s].events.height)} for s in scales}
    return StudyResult(cells=cells, go=go_table(cells), memory=memory_table(acc, w, day_slices), acc=acc,
                       placebos=placebos, verified=verified, counts=counts,
                       data_first=df_1m["open_time"].min(), data_last=last)


# --------------------------------------------------------------------------- feature diagnostics (feat_levels)


CHAMPION_MODELS = ("lgbm_solusdt_l_fw60_2101_2607", "lgbm_solusdt_s_fw60_2101_2607")
TARGETS = ("long_mfe_fw60", "short_mfe_fw60")
CHALLENGER_TABLE = "quant_train__solusdt_fw60_sr"
REDUNDANT_RHO = 0.8


def champion_features() -> list[str]:
    """Union of the champion long / short model feature lists (``features.json``)."""
    import json

    from reports.builders._common import artifact_dir_of

    feats: list[str] = []
    for mid in CHAMPION_MODELS:
        data = json.loads((artifact_dir_of(mid) / "features.json").read_text(encoding="utf-8"))
        feats += [f for f in data["features"] if f not in feats]
    return feats


def level_feature_columns() -> list[str]:
    from data_handling.sync_tables._levels_features import feature_columns

    return list(feature_columns())


def load_feature_sample(asset_id: str | None = None, *, tries: int = 10, delay: float = 3.0) -> pd.DataFrame | None:
    """Hourly (minute 0) rows of the challenger quant_train from 2021: targets + feat_levels + champion features."""
    import duckdb

    from data_handling.store.readonly_query import open_readonly_connection

    cols = list(dict.fromkeys([*TARGETS, *level_feature_columns(), *champion_features()]))
    sql = (f"SELECT open_time, {', '.join(cols)} FROM live.{CHALLENGER_TABLE} "
           f"WHERE minute(open_time) = 0 AND open_time >= '{TRAIN_START:%Y-%m-%d}' ORDER BY open_time")
    for attempt in range(tries):
        try:
            con = open_readonly_connection(asset_id)
        except duckdb.Error as exc:
            logger.warning("live db locked/unavailable (attempt %d/%d): %s", attempt + 1, tries, exc)
            if attempt == tries - 1:
                return None
            time.sleep(delay)
            continue
        try:
            return con.execute(sql).fetchdf()
        except duckdb.Error as exc:
            logger.warning("feature sample unavailable: %s", exc)
            return None
        finally:
            con.close()
    return None


def feature_group(col: str) -> tuple[str, str]:
    """(group, scale) of a feat_levels column, e.g. ``feat_sr_dist_res_atr_s`` -> (``sr_dist``, ``s``)."""
    body, scale = col.removeprefix("feat_").rsplit("_", 1)
    parts = body.split("_")
    if parts[1] in ("n", "is", "break", "retest"):
        return "_".join(parts[:3]), scale
    return "_".join(parts[:2]), scale


def decile_means(x: pd.Series, y: pd.Series) -> pd.Series:
    """Mean of y per decile of x (per value for low-cardinality x); index = decile 0..9 or the value."""
    m = x.notna() & y.notna()
    x, y = x[m], y[m]
    if x.nunique() <= 10:
        return y.groupby(x).mean()
    dec = pd.qcut(x.rank(method="first"), 10, labels=False)
    return y.groupby(dec).mean()


def _spread(x: pd.Series, y: pd.Series) -> float:
    """Top − bottom decile (or value group) mean of y."""
    if int((x.notna() & y.notna()).sum()) < 200:
        return math.nan
    g = decile_means(x, y)
    return float(g.iloc[-1] - g.iloc[0]) if len(g) > 1 else math.nan


def feature_diagnostics(df: pd.DataFrame, level_cols: list[str], champ_cols: list[str],
                        oos_start: datetime = OOS_START) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Univariate Spearman / decile spread of every feat_levels column vs the champion targets and the max
    |Spearman| with the champion features (redundancy) on the train rows; OOS Spearman for sign stability.

    Returns:
        (per-feature table, champion benchmark: |ρ| quantiles of the champion features vs each target).
    """
    tr = df[df["open_time"] < oos_start]
    oo = df[df["open_time"] >= oos_start]
    champ = [c for c in champ_cols if c in df.columns and c not in level_cols]
    champ_tr = tr[champ]
    rows = []
    for c in level_cols:
        g, s = feature_group(c)
        x = tr[c]
        row: dict[str, Any] = {"feature": c, "group": g, "scale": s, "null_pct": float(x.isna().mean())}
        for tgt, short in zip(TARGETS, ("long", "short"), strict=True):
            row[f"rho_{short}"] = float(x.corr(tr[tgt], method="spearman"))
            row[f"rho_{short}_oos"] = float(oo[c].corr(oo[tgt], method="spearman")) if len(oo) else math.nan
            row[f"spread_{short}_bp"] = 1e4 * _spread(x, tr[tgt])
        m = x.notna()
        if m.sum() > 200 and x[m].nunique() > 1 and champ:
            rc = champ_tr.loc[m].corrwith(x[m], method="spearman").abs().dropna()
            row["max_rho_champion"] = float(rc.max()) if len(rc) else math.nan
            row["max_rho_champion_feature"] = str(rc.idxmax()) if len(rc) else ""
        else:
            row["max_rho_champion"], row["max_rho_champion_feature"] = math.nan, ""
        rows.append(row)
    out = pd.DataFrame(rows)
    out["abs_rho_max"] = out[["rho_long", "rho_short"]].abs().max(axis=1)
    out["sign_stable"] = ((np.sign(out["rho_long"]) == np.sign(out["rho_long_oos"]))
                          & (np.sign(out["rho_short"]) == np.sign(out["rho_short_oos"])))
    bench = []
    for tgt in TARGETS:
        rc = champ_tr.corrwith(tr[tgt], method="spearman").abs().dropna() if champ else pd.Series(dtype=float)
        if rc.empty:
            continue
        bench.append({"target": tgt, "n_features": len(rc), "median_abs_rho": float(rc.median()),
                      "p75_abs_rho": float(rc.quantile(0.75)), "p90_abs_rho": float(rc.quantile(0.9)),
                      "max_abs_rho": float(rc.max()), "max_feature": str(rc.idxmax())})
    return out, pd.DataFrame(bench)


def feature_candidates(fd: pd.DataFrame, bench: pd.DataFrame) -> pd.DataFrame:
    """Rows of ``fd`` that pass the univariate screen: |ρ| >= champion median |ρ|, sign-stable, not redundant."""
    if fd.empty or bench.empty:
        return fd.iloc[0:0]
    med = float(bench["median_abs_rho"].mean())
    ok = ((fd["abs_rho_max"] >= med) & fd["sign_stable"] & (fd["max_rho_champion"] < REDUNDANT_RHO)
          & ~fd["scale"].isin(EXCLUDED_SCALES))
    return fd[ok].sort_values("abs_rho_max", ascending=False)


#: the l scale is left out of the t914 screen: < 100 events in most cells (train ~4 years of 1d bars) and its
#: strongest feat_levels columns flip sign / vanish in OOS (see the feat_diag table)
EXCLUDED_SCALES = ("l",)


# --------------------------------------------------------------------------- figures

SCALE_COLOR = {"s": theme.BLUE, "m": theme.ORANGE, "l": "#6f42c1"}
PERIOD_COLOR = {"train": theme.NEUTRAL, "oos": theme.BLUE}


def _cell_label(r: Any) -> str:
    return f"{r['scale']} · {OBJ_HU[r['obj_type']]} · {SIDE_HU[r['side']]} · {EVENT_HU[r['event_type']]}"


def delta_forest_fig(go_df: pd.DataFrame) -> go.Figure:
    labels = [_cell_label(r) for _, r in go_df.iterrows()]
    fig = go.Figure()
    for per, pre in (("train", "tr"), ("oos", "oos")):
        err = (100 * 1.96 * go_df[f"{pre}_se"]).fillna(0)
        fig.add_trace(go.Scatter(
            x=(100 * go_df[f"{pre}_delta"]).tolist(), y=labels, mode="markers", name=PERIOD_HU[per],
            marker={"color": PERIOD_COLOR[per], "size": 8, "symbol": "circle" if per == "train" else "diamond"},
            error_x={"type": "data", "array": err.tolist(), "thickness": 1.5, "width": 0},
            customdata=np.c_[go_df[f"{pre}_n_real"].fillna(0), go_df[f"{pre}_p_holm"].fillna(1)],
            hovertemplate="%{y}<br>Δ = %{x:.1f} pp<br>n = %{customdata[0]:.0f}<br>Holm p = %{customdata[1]:.3g}<extra>"
                          + per + "</extra>"))
    for xv in (-5, 5):
        fig.add_vline(x=xv, line_dash="dot", line_color=theme.NEUTRAL, line_width=1)
    fig.add_vline(x=0, line_color=theme.NEUTRAL, line_width=1)
    themed(fig, height=900, x_title="Δ = valódi − placebo arány (százalékpont), 95% bootstrap CI", hovermode="closest")
    fig.update_yaxes(autorange="reversed", automargin=True)
    return fig


def placebo_hist_fig(acc: Accum, go_df: pd.DataFrame, scale: str) -> go.Figure:
    titles = [f"{OBJ_HU[o]} · {SIDE_HU[d]} · {EVENT_HU[e]}" for o in OBJ_TYPES for d in SIDES for e in EVENT_TYPES]
    fig = make_subplots(rows=4, cols=3, subplot_titles=titles, vertical_spacing=0.08, horizontal_spacing=0.06)
    for o_i, o in enumerate(OBJ_TYPES):
        for d_i, d in enumerate(SIDES):
            for e_i, e in enumerate(EVENT_TYPES):
                c = cell_index(scale, o, d, e)
                row, col = o_i * 2 + d_i + 1, e_i + 1
                g = go_df[go_df["cell"] == c].iloc[0]
                for per_i, (per, pre) in enumerate((("train", "tr"), ("oos", "oos"))):
                    with np.errstate(invalid="ignore", divide="ignore"):
                        rates = acc.set_k[:, c, per_i] / acc.set_n[:, c, per_i]
                    rates = rates[np.isfinite(rates)]
                    first = row == 1 and col == 1
                    if rates.size:
                        fig.add_trace(go.Histogram(
                            x=(100 * rates).round(3).tolist(), nbinsx=30, name=f"placebo készletek ({per})",
                            legendgroup=per, showlegend=first, marker={"color": PERIOD_COLOR[per]}, opacity=0.55,
                            hovertemplate="placebo arány %{x}%<br>készletek: %{y}<extra>" + per + "</extra>"),
                            row=row, col=col)
                    r = g[f"{pre}_rate_real"]
                    if np.isfinite(r):
                        fig.add_trace(go.Scatter(
                            x=[100 * r, 100 * r], y=[0, max(1, rates.size // 6)], mode="lines",
                            name=f"valódi arány ({per})", legendgroup=per + "_r", showlegend=first,
                            line={"color": theme.SHORT if per == "train" else theme.AMBER, "width": 3,
                                  "dash": "solid" if per == "train" else "dot"},
                            hovertemplate=f"valódi arány {100 * r:.1f}% (n = {g[f'{pre}_n_real']:.0f})<extra>{per}</extra>"),
                            row=row, col=col)
    themed(fig, height=1000, barmode="overlay", hovermode="closest")
    fig.update_annotations(font_size=11)
    return fig


def memory_fig(mem: pd.DataFrame) -> go.Figure:
    fig = make_subplots(rows=1, cols=2, subplot_titles=["Szintek: touch → bounce", "Trendvonalak: touch → bounce"],
                        shared_yaxes=True, horizontal_spacing=0.05)
    for o_i, o in enumerate(OBJ_TYPES):
        for s in SCALES:
            for per in PERIODS:
                m = mem[(mem["obj_type"] == o) & (mem["scale"] == s) & (mem["period"] == per)
                        & (mem["band"] < len(AGE_EDGES))].sort_values("band")
                if m.empty:
                    continue
                fig.add_trace(go.Scatter(
                    x=m["age_band"].tolist(), y=(100 * m["delta"]).tolist(), mode="lines+markers",
                    name=f"{s} · {per}", legendgroup=f"{s}{per}", showlegend=o_i == 0 or o == "line" and s == "l",
                    line={"color": SCALE_COLOR[s], "width": 2, "dash": "solid" if per == "train" else "dot"},
                    marker={"size": 9, "symbol": ["circle" if n >= MIN_EVENTS else "circle-open" for n in m["n_real"]]},
                    error_y={"type": "data", "array": (100 * 1.96 * m["se"]).fillna(0).tolist(), "thickness": 1, "width": 3},
                    customdata=np.c_[m["n_real"], 100 * m["rate_real"], 100 * m["rate_placebo"]],
                    hovertemplate="%{x}: Δ = %{y:.1f} pp<br>valódi %{customdata[1]:.1f}% / placebo %{customdata[2]:.1f}%"
                                  "<br>n = %{customdata[0]:.0f}<extra>" + f"{s} {per}</extra>"),
                    row=1, col=o_i + 1)
    for c in (1, 2):
        fig.add_hline(y=0, line_color=theme.NEUTRAL, line_width=1, row=1, col=c)
        for yv in (-5, 5):
            fig.add_hline(y=yv, line_dash="dot", line_color=theme.NEUTRAL, line_width=1, row=1, col=c)
    themed(fig, height=460, hovermode="closest", y_title="Δ bounce (pp), 95% CI")
    fig.update_xaxes(categoryorder="array", categoryarray=list(AGE_LABELS[:-1]))
    return fig


def train_oos_fig(go_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for s in SCALES:
        g = go_df[go_df["scale"] == s]
        fig.add_trace(go.Scatter(
            x=(100 * g["tr_delta"]).tolist(), y=(100 * g["oos_delta"]).tolist(), mode="markers", name=f"lépték {s}",
            marker={"color": SCALE_COLOR[s], "size": np.clip(np.sqrt(g["tr_n_real"].fillna(0)) / 3, 8, 22).tolist(),
                    "line": {"width": 2, "color": "rgba(255,255,255,0.6)"}},
            text=[_cell_label(r) for _, r in g.iterrows()],
            hovertemplate="%{text}<br>train Δ %{x:.1f} pp / OOS Δ %{y:.1f} pp<extra></extra>"))
    vals = np.abs(100 * go_df[["tr_delta", "oos_delta"]].to_numpy(dtype=float))
    lim = float(np.nanmax(vals)) * 1.1 + 1 if np.isfinite(vals).any() else 10.0
    fig.add_trace(go.Scatter(x=[-lim, lim], y=[-lim, lim], mode="lines", name="train = OOS",
                             line={"color": theme.NEUTRAL, "dash": "dot", "width": 1}, hoverinfo="skip"))
    themed(fig, height=460, x_title="train Δ (pp)", y_title="OOS Δ (pp)", hovermode="closest")
    fig.update_xaxes(range=[-lim, lim], zeroline=True)
    fig.update_yaxes(range=[-lim, lim], zeroline=True)
    return fig


def feat_rho_fig(fd: pd.DataFrame, bench: pd.DataFrame) -> go.Figure:
    d = fd.sort_values("abs_rho_max", ascending=True)
    fig = go.Figure()
    for short, color, name in (("long", theme.LONG, "long_mfe_fw60"), ("short", theme.SHORT, "short_mfe_fw60")):
        fig.add_trace(go.Bar(y=d["feature"].tolist(), x=d[f"rho_{short}"].tolist(), orientation="h", name=name,
                             marker={"color": color},
                             hovertemplate="%{y}<br>Spearman ρ = %{x:.3f}<extra>" + name + "</extra>"))
    med = float(bench["median_abs_rho"].mean()) if not bench.empty else math.nan
    if np.isfinite(med):
        for xv in (-med, med):
            fig.add_vline(x=xv, line_dash="dot", line_color=theme.NEUTRAL, line_width=1)
    themed(fig, height=1100, barmode="group", hovermode="closest",
           x_title=f"Spearman ρ (train, óránkénti minta); pontozott: champion feature-ök medián |ρ| = {fmt(med, 3)}")
    fig.update_yaxes(automargin=True, tickfont={"size": 10})
    return fig


def feat_redundancy_fig(fd: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for s in SCALES:
        g = fd[fd["scale"] == s]
        fig.add_trace(go.Scatter(
            x=g["max_rho_champion"].tolist(), y=g["abs_rho_max"].tolist(), mode="markers", name=f"lépték {s}",
            marker={"color": SCALE_COLOR[s], "size": 9, "line": {"width": 2, "color": "rgba(255,255,255,0.6)"}},
            text=[f"{f}<br>legközelebbi champion: {c}" for f, c in zip(g["feature"], g["max_rho_champion_feature"], strict=True)],
            hovertemplate="%{text}<br>max |ρ| champion = %{x:.2f}<br>max |ρ| target = %{y:.3f}<extra></extra>"))
    fig.add_vline(x=REDUNDANT_RHO, line_dash="dot", line_color=theme.NEUTRAL, line_width=1)
    themed(fig, height=440, hovermode="closest", x_title="max |Spearman ρ| a champion feature-ökkel (redundancia)",
           y_title="max |ρ| a targettel (train)")
    return fig


def feat_decile_fig(df: pd.DataFrame, fd: pd.DataFrame, n_top: int = 6) -> go.Figure:
    tr = df[df["open_time"] < OOS_START]
    top = fd.sort_values("abs_rho_max", ascending=False)["feature"].head(n_top).tolist()
    fig = make_subplots(rows=2, cols=3, subplot_titles=top, vertical_spacing=0.16)
    for i, f in enumerate(top):
        r, c = i // 3 + 1, i % 3 + 1
        for tgt, color in (("long_mfe_fw60", theme.LONG), ("short_mfe_fw60", theme.SHORT)):
            g = decile_means(tr[f], tr[tgt])
            low_card = tr[f].nunique() <= 10
            xs = [fmt(v, 0) if low_card else f"D{int(v) + 1}" for v in g.index]
            fig.add_trace(go.Scatter(x=xs, y=(1e4 * g).tolist(), mode="lines+markers", name=tgt, legendgroup=tgt,
                                     showlegend=i == 0, line={"color": color, "width": 2}, marker={"size": 8},
                                     hovertemplate="%{x}: %{y:.1f} bp<extra>" + tgt + "</extra>"), row=r, col=c)
    themed(fig, height=580, hovermode="closest", y_title="target átlag (bp)")
    fig.update_annotations(font_size=11)
    return fig


# --------------------------------------------------------------------------- tables


def placebo_check_table(study: StudyResult) -> pd.DataFrame:
    """Real vs placebo quantiles of the matched quantities (1700 validation checklist)."""
    qs = (0.05, 0.25, 0.5, 0.75, 0.95)
    rows: list[dict[str, Any]] = []

    def add(scale: str, what: str, real: np.ndarray, plc: np.ndarray) -> None:
        real, plc = real[np.isfinite(real)], plc[np.isfinite(plc)]
        if real.size == 0 or plc.size == 0:
            return
        for src, arr in (("valódi", real), ("placebo", plc)):
            rows.append({"scale": scale, "quantity": what, "source": src, "n": int(arr.size),
                         **{f"q{int(q * 100):02d}": float(np.quantile(arr, q)) for q in qs}})

    for s, pb in study.placebos.items():
        add(s, "szint |távolság| (ATR)", np.abs(pb.level_d_real), np.abs(pb.level_d_pl).ravel())
        add(s, "szint előjeles távolság (ATR)", pb.level_d_real, pb.level_d_pl.ravel())
        for up, kind in ((True, "támasz-vonal"), (False, "ellenállás-vonal")):
            m = pb.line_kind_up == up
            add(s, f"{kind} távolság (ATR)", pb.line_d_real[m], pb.line_d_pl[m].ravel())
            add(s, f"{kind} meredekség (ATR/bar)", pb.line_slope_atr_real[m], pb.line_slope_atr_pl[m].ravel())
    return pd.DataFrame(rows)


GO_COLUMNS: dict[str, Any] = {
    "scale": "Lépték", "obj_type": "Objektum", "side": "Irány", "event_type": "Esemény",
    "tr_n_real": {"label": "n train", "type": "int"}, "tr_rate_real": "Valódi (train)",
    "tr_rate_placebo": "Placebo (train)", "tr_delta": {"label": "Δ train", "type": "pct", "digits": 1},
    "tr_p_boot": {"label": "p boot (train)", "digits": 4}, "tr_p_holm": {"label": "Holm p (train)", "digits": 4},
    "tr_p_rank": {"label": "placebo-rang p (train)", "digits": 3},
    "oos_n_real": {"label": "n OOS", "type": "int"}, "oos_rate_real": "Valódi (OOS)",
    "oos_rate_placebo": "Placebo (OOS)", "oos_delta": {"label": "Δ OOS", "type": "pct", "digits": 1},
    "oos_p_holm": {"label": "Holm p (OOS)", "digits": 4}, "decision": "Döntés", "reason": "Ok",
}


def _go_view(go_df: pd.DataFrame) -> pd.DataFrame:
    v = go_df[list(GO_COLUMNS)].copy()
    v["obj_type"] = v["obj_type"].map(OBJ_HU)
    v["side"] = v["side"].map(SIDE_HU)
    v["event_type"] = v["event_type"].map(EVENT_HU)
    for c in ("tr_n_real", "oos_n_real"):
        v[c] = v[c].fillna(0).astype(int)
    return v


def recommendation(go_df: pd.DataFrame, fd: pd.DataFrame | None, bench: pd.DataFrame | None) -> dict[str, Any]:
    """Data-driven t914 recommendation: GO cells per scale / group and the univariate feature screen."""
    go_cells = go_df[go_df["go"]]
    inv_cells = go_df[go_df["go_inverse"]]
    groups: dict[str, list[str]] = {}
    for _, r in go_cells.iterrows():
        groups.setdefault(f"feat_{'sr' if r['obj_type'] == 'level' else 'tl'}_*_{r['scale']}", []).append(_cell_label(r))
    cands = feature_candidates(fd, bench) if fd is not None and bench is not None else pd.DataFrame()
    return {"n_go": len(go_cells), "n_inverse": len(inv_cells), "groups": groups,
            "candidates": cands["feature"].tolist() if len(cands) else [],
            "n_evaluable": int(((go_df["tr_n_real"] >= MIN_EVENTS) & (go_df["oos_n_real"] >= MIN_EVENTS)).sum())}


# --------------------------------------------------------------------------- build


def build(root: Path | None = None, *, df_1m: pl.DataFrame | None = None, params: dict | None = None,
          asset_id: str | None = None, n_sets: int = N_SETS, n_boot: int = N_BOOT,
          feature_df: pd.DataFrame | None = None, with_features: bool = True,
          oos_start: datetime = OOS_START, train_start: datetime = TRAIN_START) -> Path | None:
    """Run the study and write the report; ``None`` when no OHLCV data is available.

    Args:
        root         : Report root override (tests); default ``_doc_/reports``.
        df_1m        : 1m OHLCV (tests); default: the live ``ohlcv`` table, read-only.
        params       : Detector params; default ``config/features.json`` ``levels_detector`` (v1, frozen).
        n_sets       : Placebo sets (default 1000); ``n_boot``: bootstrap replications.
        feature_df   : Hourly sample for the feature diagnostic (tests); default read from the live DB.
        with_features: False skips the feature diagnostic section.
    """
    p = det.load_params(params)
    if df_1m is None:
        df_1m = load_ohlcv_1m(asset_id)
    if df_1m is None or df_1m.height == 0:
        logger.warning("%s skipped: no OHLCV data", SLUG)
        return None
    t_start = time.time()
    st = run_study(df_1m, p, n_sets=n_sets, n_boot=n_boot, train_start=train_start, oos_start=oos_start)
    go_df = st.go
    fd = bench = fdf = None
    if with_features:
        fdf = feature_df if feature_df is not None else load_feature_sample(asset_id)
        if fdf is not None and len(fdf):
            lc = [c for c in level_feature_columns() if c in fdf.columns]
            cc = [c for c in fdf.columns if c.startswith("feat_") and c not in lc]
            t = time.time()
            fd, bench = feature_diagnostics(fdf, lc, cc, oos_start)
            logger.info("feature diagnostics: %.1f s", time.time() - t)
    rec = recommendation(go_df, fd, bench)
    elapsed = time.time() - t_start

    rb = ReportBuilder(
        scope="general", slug=SLUG, order=21, root=root,
        title="S/R szintek és trendvonalak — placebo eseménytanulmány és memória-görbe",
        summary=(f"Osler-féle placebo-teszt ({n_sets} véletlen szint-/vonalkészlet, napi blokk-bootstrap, Holm): "
                 f"{rec['n_go']} / {N_CELLS} cella GO a 1700 küszöbök szerint. Memória-görbe a szint kora szerint, "
                 "feat_levels egyváltozós diagnosztika és javaslat a t914 feature-válogatásra."),
        tags=["sr", "trendline", "levels", "epic_071", "placebo", "event_study"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    rb.add_markdown(
        "## Cél\n\n"
        "**epic_071 / t913.** Hordoznak-e a detektált támasz/ellenállás szintek és trendvonalak *többlet* információt "
        "a véletlenhez képest? Osler (2000) módszerével minden valódi objektumhoz véletlen **placebo** objektumot "
        "készítünk azonos statisztikai szerkezettel, és ugyanazzal az esemény- és címke-logikával mérjük a "
        "touch → bounce, break → follow-through és retest → hold arányokat. Ha a valódi arány nem tér el a placebótól, "
        "a „szint” csak a volatilitást és az ár-dinamikát méri, nem egy valódi memóriát.\n\n"
        "**Háttér:** módszertan és go/no-go küszöbök `_doc_/methodology_doc/1700_features_sr_trendline.md`; detektor "
        "`src/data_handling/sync_tables/_levels_polars.py` (config `levels_detector` **v1, befagyasztva**); bar-szintű "
        "feature-ök `feat_levels` (t912, `_levels_features.py`); a detektor vizuális riportja `sr_trendline_detection` (t911).\n\n"
        f"**Adat:** SOLUSDT `ohlcv` 1m {st.data_first:%Y-%m-%d} → {st.data_last:%Y-%m-%d %H:%M} UTC ({df_1m.height:,} sor, "
        "read-only); a detektor a teljes történeten fut (bemelegedés), az események közül csak a "
        f"**train = {train_start:%Y-%m-%d} → {oos_start:%Y-%m-%d}** és az **OOS = {oos_start:%Y-%m-%d} → adat vége** "
        "számít. ⚠️ **Az OOS időszakot a t911 riport vizuálisan megmutatta** (a user látta a 2025–2026-os ábrákat), "
        "ezért ez „vizuálisan látott”, nem teljesen tiszta OOS; a detektor paraméterei viszont a t911 után nem változtak. "
        "Az OOS határ az epic döntése (2025-05-01, a champion korábbi valid kezdete); a `config/models.json` szerint a "
        "jelenlegi champion train 2021-01 → 2026-01, valid 2026-02 → 2026-07, vagyis a champion a 2025-05 → 2026-01 "
        "szakaszon tanult — a feature-diagnosztika OOS oszlopa ezért nem a champion OOS-a.\n\n"
        f"Generálás: `uv run python -m reports.builders.sr_placebo` (~{elapsed / 60:.0f} perc)."
    )
    rb.add_markdown(
        "## Módszer\n\n"
        "**Placebo objektumok** (minden valódi objektumhoz készletenként egy; a készletek determinisztikus seed-ből):\n\n"
        "- *Placebo szint:* ugyanaz a lépték, ugyanaz a megerősítő bar (`confirm_idx`) és élettartam (`expired_at`), mint a "
        "valódi szinté; az ár `close[confirm] ± |d|·ATR[confirm]`, ahol `|d|` a lépték valódi szintjeinek "
        "megerősítéskori ATR-távolság-eloszlásából húzott, az előjel véletlen (1700: „az irány véletlen”).\n"
        "- *Placebo vonal:* ugyanaz a típus (támasz/ellenállás), megerősítő bar, elévülés és elévülési ok; a vonal ára a "
        "megerősítő baron `close + d·ATR` (előjeles `d` az azonos típusú valódi vonalakból), a meredekség (ATR/bar) "
        "függetlenül húzva az azonos típusú valódi vonalak meredekség-eloszlásából.\n\n"
        "**Azonos esemény- és címke-logika:** a placebo objektumokon a detektor állapotgépe (`_scan_object`: touch "
        "re-arm, break + szerepcsere, 12 bar retest-ablak, vonal halála a törés után) és a `forward_label_events` "
        "címkéi futnak. A sebesség miatt a builder ezeket vektorizálva futtatja a készleteken (`scan_batch`, "
        "`label_batch`); **minden futásban** a 0. placebo készletet a detektor saját függvényein is átküldi, és "
        "eseményre és címkére pontos egyezést követel (lásd az ellenőrző táblát). A detektor könyvtár nem változott.\n\n"
        "**Cellák:** lépték (s/m/l) × objektum (szint/vonal) × irány (az objektum szerepe az eseménykor: támasz/ellenállás) "
        f"× eseménytípus (touch/break/retest) = {N_CELLS} cella. Pozitív címke: bounce, follow-through, hold. A jövőbe "
        "nem látó (null címkéjű) események kimaradnak.\n\n"
        f"**Statisztika:** Δ = valódi arány − placebo arány (a {n_sets} készleten összevonva). *Blokk-bootstrap:* "
        f"{n_boot} ismétlés, blokk = 1 UTC nap; a napokat periódusonként együtt húzzuk újra az összes cellára (a valódi és "
        "a placebo esemény ugyanazon a napon párban marad). p = kétoldalas, a bootstrap SE-vel normál közelítéssel "
        "(az empirikus bootstrap p felbontása 1/ismétlésszám, ez a Holm-küszöbhöz kevés); *placebo-rang p* = a valódi "
        f"arány helye a {n_sets} placebo-készlet arányának eloszlásában (Osler-féle, kiegészítő). **Holm-korrekció** a "
        f"{N_CELLS} cellán, periódusonként.\n\n"
        "**Go/no-go (1700):** GO, ha (1) n ≥ 100 a train *és* az OOS időszakban is (feltételezés: a „cellánként” mindkét "
        "periódusra vonatkozik), (2) Holm p (train) < 0,01, (3) Δ train ≥ +5 pp, (4) az OOS Δ előjele megegyezik a "
        "trainével. Kiegészítő jelölés: **NO-GO (fordított)**, ha az 1, 2 és 4 teljesül, de Δ ≤ −5 pp (a hatás a "
        "hipotézissel ellentétes, de feature-ként informatív lehet)."
    )
    ver = pd.DataFrame([{"scale": s, "events_set0": n, "levels": st.counts[s]["levels"], "lines": st.counts[s]["lines"],
                         "real_events": st.counts[s]["events"]} for s, n in st.verified.items()])
    if len(ver):
        rb.add_table("verification", ver, title="Ellenőrzés: a 0. placebo készlet a detektor függvényein = vektorizált futás",
                     columns={"scale": "Lépték", "events_set0": "Placebo események (0. készlet, egyezik)",
                              "levels": "Valódi szint", "lines": "Valódi vonal", "real_events": "Valódi esemény (teljes)"})
    rb.add_markdown("### A placebo eloszlások egyezése\n\nA placebo objektumok a valódi távolság- és meredekség-eloszlást "
                    "kvantilisekben reprodukálják (1700 validációs checklist). A szint előjeles távolsága szándékosan "
                    "szimmetrizált (véletlen irány).")
    rb.add_table("placebo_check", placebo_check_table(st), title="Valódi vs placebo kvantilisek (ATR-egység)", columns={
        "scale": "Lépték", "quantity": "Mennyiség", "source": "Forrás", "n": "n",
        **{f"q{q:02d}": {"label": f"q{q:02d}", "digits": 3} for q in (5, 25, 50, 75, 95)}})

    # ---- 1. go / no-go
    gv = go_df
    rb.add_markdown(
        "## 1. Eredmény cellánként (go/no-go)\n\n"
        f"**{rec['n_go']} / {N_CELLS} cella GO**, {rec['n_inverse']} fordított; értékelhető (n ≥ {MIN_EVENTS} mindkét "
        f"periódusban): {rec['n_evaluable']} cella. Az arányok a pozitív címke aránya (bounce / follow-through / hold); "
        "Δ százalékpontban."
    )
    rb.add_table("go_nogo", _go_view(gv), title="Valódi vs placebo arányok, Δ, p-értékek és döntés cellánként",
                 columns=GO_COLUMNS)
    rb.add_chart("delta_forest", delta_forest_fig(gv), title="Δ (valódi − placebo) cellánként, train vs OOS, 95% CI", height=900)
    rb.add_chart("train_vs_oos", train_oos_fig(gv), title="Train vs OOS Δ cellánként (pont mérete ~ √n)", height=460)
    rb.add_markdown("### Placebo-eloszlás vs valódi arány\n\nCellánként a placebo készletek arányának hisztogramja "
                    "(szürke = train, kék = OOS) és a valódi arány (piros folytonos = train, sárga pontozott = OOS). "
                    "Ha a valódi vonal a hisztogram belsejébe esik, a szint nem különbözik egy véletlen szinttől.")
    for s in SCALES:
        if s in st.placebos:
            rb.add_chart(f"placebo_hist_{s}", placebo_hist_fig(st.acc, gv, s),
                         title=f"Lépték {s}: placebo készletek arány-eloszlása és a valódi arány", height=1000)

    # ---- 2. memory curve
    mem = st.memory
    rb.add_markdown(
        "## 2. Memória-görbe\n\n"
        "A touch → bounce hatás (Δ = valódi − placebo) a szint/vonal kora szerint (kor = esemény ideje − az első érintés "
        "pivot-ideje, vonalnál az első horgonyé; a placebo ikre ugyanazt a kort kapja). Irányok összevonva. Üres marker: "
        f"n < {MIN_EVENTS}. Ha egy szintnek van „memóriája”, a friss szinteknél nagyobb Δ-t várnánk, ami a korral csökken."
    )
    rb.add_chart("memory_curve", memory_fig(mem), title="Memória-görbe: Δ bounce a kor szerint, léptékenként (train folytonos, OOS pontozott)", height=460)
    mv = mem[["period", "scale", "obj_type", "age_band", "n_real", "rate_real", "rate_placebo", "delta", "ci_lo", "ci_hi",
              "p_boot", "p_holm"]].copy()
    mv["obj_type"] = mv["obj_type"].map(OBJ_HU)
    mv["n_real"] = mv["n_real"].astype(int)
    rb.add_table("memory", mv, title="Memória-görbe táblázat (touch → bounce)", columns={
        "period": "Periódus", "scale": "Lépték", "obj_type": "Objektum", "age_band": "Kor", "n_real": "n",
        "rate_real": "Valódi", "rate_placebo": "Placebo", "delta": {"label": "Δ", "type": "pct", "digits": 1},
        "ci_lo": {"label": "CI alsó", "type": "pct", "digits": 1}, "ci_hi": {"label": "CI felső", "type": "pct", "digits": 1},
        "p_boot": {"label": "p boot (korrigálatlan)", "digits": 4},
        "p_holm": {"label": f"Holm p (n ≥ {MIN_EVENTS} sávok)", "digits": 4}})

    # ---- 3. feature diagnostics
    if fd is not None and bench is not None and fdf is not None:
        n_tr = int((fdf["open_time"] < oos_start).sum())
        rb.add_markdown(
            "## 3. Kiegészítő feature-diagnosztika (`feat_levels`)\n\n"
            f"A challenger tábla (`{CHALLENGER_TABLE}`) óránkénti (perc = 0) sorai, train {n_tr:,} sor (a fw60 target "
            "így nem fed át). Minden `feat_levels` oszlopra: Spearman ρ a champion targetekkel (`long_mfe_fw60`, "
            "`short_mfe_fw60`) a nem-NULL sorokon, decilis-terjedelem (felső − alsó decilis target-átlaga bp-ben; "
            "kevés értékű oszlopnál a legnagyobb − legkisebb érték csoportja), OOS ρ az előjel-stabilitáshoz, és a "
            f"redundancia: a legnagyobb |ρ| a {int(bench['n_features'].max())} champion feature bármelyikével. Benchmark: a "
            "champion feature-ök saját |ρ|-eloszlása ugyanezen a mintán."
        )
        rb.add_table("champion_benchmark", bench, title="Champion feature-ök |Spearman ρ| a targettel (train, óránként)", columns={
            "target": "Target", "n_features": "Feature-ök", "median_abs_rho": {"label": "Medián |ρ|", "digits": 3},
            "p75_abs_rho": {"label": "p75 |ρ|", "digits": 3}, "p90_abs_rho": {"label": "p90 |ρ|", "digits": 3},
            "max_abs_rho": {"label": "Max |ρ|", "digits": 3}, "max_feature": "Legerősebb"})
        rb.add_chart("feat_rho", feat_rho_fig(fd, bench), title="feat_levels: Spearman ρ a champion targetekkel (train)", height=1100)
        rb.add_chart("feat_redundancy", feat_redundancy_fig(fd), title="Hasznosság vs redundancia", height=440)
        rb.add_chart("feat_deciles", feat_decile_fig(fdf, fd), title="A 6 legerősebb feat_levels oszlop decilis-átlagai (train)", height=580)
        fv = fd.sort_values("abs_rho_max", ascending=False)[[
            "feature", "group", "scale", "null_pct", "rho_long", "rho_short", "rho_long_oos", "rho_short_oos",
            "spread_long_bp", "spread_short_bp", "max_rho_champion", "max_rho_champion_feature", "sign_stable"]]
        rb.add_table("feat_diag", fv, title="feat_levels egyváltozós diagnosztika (|ρ| szerint rendezve)", columns={
            "feature": "Feature", "group": "Csoport", "scale": "Lépték", "null_pct": "NULL",
            "rho_long": {"label": "ρ long", "digits": 3}, "rho_short": {"label": "ρ short", "digits": 3},
            "rho_long_oos": {"label": "ρ long OOS", "digits": 3}, "rho_short_oos": {"label": "ρ short OOS", "digits": 3},
            "spread_long_bp": {"label": "D10−D1 long (bp)", "digits": 1}, "spread_short_bp": {"label": "D10−D1 short (bp)", "digits": 1},
            "max_rho_champion": {"label": "max |ρ| champion", "digits": 2}, "max_rho_champion_feature": "Legközelebbi champion",
            "sign_stable": "Előjel stabil"})

    # ---- interpretation
    rb.add_markdown(_interpretation(st, rec, fd, bench, n_sets))
    return rb.write()


def _interpretation(st: StudyResult, rec: dict[str, Any], fd: pd.DataFrame | None, bench: pd.DataFrame | None,
                    n_sets: int) -> str:
    g = st.go
    tr_abs = 100 * g["tr_delta"].abs()
    big = g[(g["tr_n_real"] >= MIN_EVENTS)]
    lines = ["## Értelmezés", ""]
    lines.append(
        f"**Placebo-teszt.** A {N_CELLS} cellából {rec['n_go']} GO és {rec['n_inverse']} fordított. A train |Δ| "
        f"medián {fmt(tr_abs.median(), 1)} pp, maximum {fmt(tr_abs.max(), 1)} pp; a legalább {MIN_EVENTS} eseményes "
        f"cellákban a legkisebb Holm p (train) {fmt(big['tr_p_holm'].min(), 3)}. A placebo-rang p alapján a valódi arány "
        f"a cellák {int((g['tr_p_rank'] < 0.05).sum())} esetében esik a {n_sets} placebo-készlet eloszlásának szélső 5%-ába "
        "(korrigálatlanul, 36 cellánál véletlenül ~2 várható).")
    if rec["n_go"]:
        lines.append("")
        lines.append("**GO cellák:** " + "; ".join(f"`{k}`: {', '.join(v)}" for k, v in rec["groups"].items()) + ".")
    mem_tr = st.memory[(st.memory["period"] == "train") & (st.memory["obj_type"] == "level") & (st.memory["n_real"] >= MIN_EVENTS)]
    if len(mem_tr):
        parts = [f"`{r.scale}` {r.age_band}: {fmt(100 * r.delta, 1)} pp" for r in mem_tr.itertuples()]
        lines += ["", f"**Memória-görbe (szintek, train, n ≥ {MIN_EVENTS}):** " + ", ".join(parts) + "."]
        mm = st.memory[st.memory["n_real"] >= MIN_EVENTS]
        hot = mm[(mm["period"] == "train") & (mm["p_boot"] < 0.01)]
        if len(hot):
            oos = st.memory[st.memory["period"] == "oos"].set_index(["scale", "obj_type", "age_band"])
            descr = []
            for r in hot.itertuples():
                key = (r.scale, r.obj_type, r.age_band)
                od = f"{fmt(100 * oos.loc[key, 'delta'], 1)} pp" if key in oos.index else "nincs adat"
                descr.append(f"`{r.scale}` {OBJ_HU[r.obj_type]} {r.age_band}: train {fmt(100 * r.delta, 1)} pp "
                             f"(korrigálatlan p {fmt(r.p_boot, 3)}, Holm p {fmt(r.p_holm, 2)}), OOS {od}")
            lines.append(f"Korrigálatlanul p < 0,01 sávok: {'; '.join(descr)}. A Holm-korrekció után egyik sem "
                         "szignifikáns, ezért ezek legfeljebb hipotézisek." if (hot["p_holm"] >= ALPHA).all() else
                         f"Korrigálatlanul p < 0,01 sávok: {'; '.join(descr)}.")
    if fd is not None and bench is not None and not bench.empty:
        med = float(bench["median_abs_rho"].mean())
        top = fd.sort_values("abs_rho_max", ascending=False).head(5)
        lines += ["", f"**Feature-diagnosztika.** A champion feature-ök medián |ρ|-ja {fmt(med, 3)} (max "
                      f"{fmt(bench['max_abs_rho'].max(), 3)}); a legerősebb feat_levels oszlopok: "
                      + ", ".join(f"`{r.feature}` {fmt(r.abs_rho_max, 3)} (redundancia {fmt(r.max_rho_champion, 2)})"
                                  for r in top.itertuples())
                      + f". A szűrőn (|ρ| ≥ champion medián, előjel-stabil, redundancia < {REDUNDANT_RHO}) "
                      f"{len(rec['candidates'])} oszlop megy át" + (": " + ", ".join(f"`{c}`" for c in rec["candidates"][:15])
                                                                    if rec["candidates"] else "") + "."]
    lines += ["", "## Döntési szempontok", ""]
    if rec["n_go"] == 0:
        lines.append(
            "- **A 1700 szabálya szerint a teljes detektor NO-GO:** egyetlen cellában sem nagyobb a valódi szintek és "
            "vonalak bounce / follow-through / hold aránya a placebo objektumokénál. A detektált struktúra eseményszinten "
            "nem hordoz a véletlen szinteken túli információt (SOLUSDT, v1 paraméterek).\n"
            "- **⛔ DÖNTÉSI PONT (user):** (A) **megállás** — a t914 nem fut, az epic negatív eredménnyel zárul (a 1700 "
            "szerinti alapértelmezés); (B) **t914 exploratív futása** a teljes 50 feature-rel, mert az eseménytanulmány "
            "csak a címkézett események *irányított* hatását méri, a bar-szintű feature-ök (pl. távolság, erő, aktív "
            "szintszám) nemlineárisan vagy interakcióban még adhatnak jelet — ezt csak a tiszta A/B mutatná meg, de a "
            "várható haszon a fenti eredmény alapján kicsi; (C) **szűkített t914** csak a feature-diagnosztika szűrőjén "
            "átment oszlopokkal (kisebb zaj, gyorsabb).")
        if rec["candidates"]:
            lines.append(
                f"- **Elemzői javaslat: (C), kifejezetten exploratív címkével.** A szűrőn átment {len(rec['candidates'])} "
                f"oszlop ({', '.join(EXCLUDED_SCALES)} lépték nélkül): " + ", ".join(f"`{c}`" for c in rec["candidates"])
                + ". Ezek ρ-ja a long és a short targettel ellentétes előjelű (irány/pozíció-információ, nem volatilitás), "
                "és a legközelebbi champion társuk jellemzően a `feat_weekly_open_return` / előző session távolság — "
                "vagyis inkább „hol van az ár a közelmúlt tartományában” jellegű információt hordoznak, mint S/R-memóriát. "
                "A t914 akkor indokolt, ha a user ezt a gyengébb hipotézist is tesztelni akarja; az `l` lépték kimarad "
                "(kevés esemény, OOS-ben előjelet váltó ρ).")
    else:
        lines.append("- A GO cellák léptékei / csoportjai mehetnek a t914 challengerbe (fent felsorolva); a NO-GO léptékek "
                     "feature-csoportjai kimaradnak.")
    lines.append("- Az OOS „vizuálisan látott”; a döntés a train Holm p-n és a train/OOS előjel-egyezésen alapul.")
    lines.append("- Korlátok: a placebo nem rezsim-stratifikált; az események nincsenek dedupolva (a blokk-bootstrap kezeli "
                 "az autokorrelációt); a léptékek közti duplikáció cellánként külön értékelt; a retest → hold definíció "
                 "szigorú (1700 feltételezés).")
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(build())
