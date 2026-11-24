"""Wave-start event dataset (epic_072 / t918 v1, t925 v2) — definitions of ``_doc_/methodology_doc/5200_wave_start_image_model.md``.

Pure library (no report): detection of right-confirmed local lows on SOLUSDT 1m, scalar baseline ``S0`` and context
``C`` features (only data closed at or before ``t_c``), the impulse / correction label (only ``(t_c, t_c + H]``),
the train / OOS / holdout-2 split with full-interval purge, the 15m prefix-image window and persistence to the
lab DB (``research."wave_events_v1"``). The report is ``reports.builders.wave_event_dataset``; t919 / t920 reuse
:func:`build_events`, :func:`load_events` and :func:`prefix_image`.

Time conventions (all naive UTC, 1m bar index ``i`` = bar opening at ``t0``):

* left window   : bars ``i - B .. i - 1`` (open ``t0 - B`` .. ``t0 - 1m``), ``B = W``
* right window  : bars ``i + 1 .. i + W - 1``; ``t_c = t0 + W`` = close time of bar ``i + W - 1`` (index ``tc``)
* candidate     : ``low[i] < min(left)`` (earliest of equal lows wins) and ``low[i] <= min(right)``
* ``sigma_1m``  : std of 1m log returns of the bars closed ``<= t0`` in the previous 7 days (bars ``i - 10080 .. i - 1``)
* label window  : bars ``tc + 1 .. tc + H`` (open ``t_c`` .. ``t_c + H - 1m``, close ``<= t_c + H``)
* non-overlap   : greedy in time order, an event is accepted when ``t0 - B >= t_c(prev) + H`` (epic decision:
  the whole interval ``[t0 - B, t_c + H]`` of two events never overlaps; stricter than 5200's ``t0' >= t_c + H``)
* v2 (t925)     : ``WaveParams.version == "v2"`` (see :func:`params_v2`): grid W4h_H4h / W4h_H8h, three-class
  first-touch label (impulse / false_start / sideways; same-bar double touch = false_start), ``cv_block`` (6 contiguous
  time blocks over the train period) and the purged / embargoed fold helper :func:`cv_fold`. v1 stays reproducible.
* prefix image  : clock-aligned 15m bars with open ``>= t0 - B`` and close ``<= t_c`` (the 15m bar containing
  ``t_c`` is dropped); ATR14 = SMA of the true range of the last 14 complete 15m bars closed ``<= t_c``
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)

VERSION = "v1"                  # default version (v1 behaviour is frozen); v2 via :func:`params_v2`
TABLE = f"wave_events_{VERSION}"
SCHEMA = "research"
GENERATOR = "reports.builders.wave_events"

#: (cell_id, W minutes, H minutes) — the fixed 5200 grid
CELLS: tuple[tuple[str, int, int], ...] = (
    ("W4h_H4h", 240, 240),
    ("W4h_H8h", 240, 480),
    ("W8h_H8h", 480, 480),
    ("W12h_H12h", 720, 720),
)
#: v2 grid (5200 v2): W = 4h, H in {4h, 8h}
CELLS_V2: tuple[tuple[str, int, int], ...] = (
    ("W4h_H4h", 240, 240),
    ("W4h_H8h", 240, 480),
)
CLASS_ORDER = ("impulse", "t0_below", "sideways")
CLASS_HU = {"impulse": "impulzus", "t0_below": "korrekció / t0 alá esett", "sideways": "korrekció / oldalazott"}
CLASS_ORDER_V2 = ("impulse", "false_start", "sideways")
CLASS_HU_V2 = {"impulse": "impulzus", "false_start": "hamis indulás", "sideways": "oldalazás"}


def table_name(version: str) -> str:
    return f"wave_events_{version}"


def class_order(version: str) -> tuple[str, ...]:
    return CLASS_ORDER if version == "v1" else CLASS_ORDER_V2


def class_hu(version: str) -> dict[str, str]:
    return CLASS_HU if version == "v1" else CLASS_HU_V2


#: feature columns (only data closed <= t_c); label / outcome columns are listed separately for leak checks
S0_COLS = ("a_sigma", "velocity_1h", "pretrend_3d", "log_sigma_1m")
C_COLS = ("drop_depth", "volume_ratio")
FEATURE_COLS = (*S0_COLS, *C_COLS)
EVENT_COLS = ("low_t0", "close_tc", "a_log", "sigma_1m", "sigma_w", "vol7d_mean_1m", "atr14_15m",
              "prefix_start", "prefix_end", "n_prefix_15m", "drop_depth_sigma")
LABEL_COLS = ("p_star", "label_impulse", "label_class", "label_sub", "fwd_max_high", "fwd_min_low",
              "fwd_mfe_log", "fwd_mae_log", "mins_to_target", "mins_to_t0_break", "first_hit")
LABEL_COLS_V2 = (*LABEL_COLS, "same_bar")
CV_COLS = ("cv_block", "cv_straddle")


@dataclass(frozen=True)
class WaveParams:
    """Frozen 5200 v1 parameters (overridable in tests only)."""

    cells: tuple[tuple[str, int, int], ...] = CELLS
    sigma_window: int = 10080           # 7 days of 1m bars, closed <= t0
    sigma_min_frac: float = 0.9         # min. share of non-missing bars in the sigma / volume window
    min_a_sigma: float = 1.5            # A >= 1.5 * sigma_W
    impulse_mult: float = 1.0           # P* = close[t_c] * exp(1.0 * A)
    trend_bars: int = 4320              # 3 days
    velocity_bars: int = 60             # 1 hour
    bar15: int = 15
    atr_n: int = 14
    train_start: str = "2021-01-01"
    oos_start: str = "2026-02-01"       # = train end (exclusive)
    holdout2_start: str = "2026-08-01"  # = OOS end (exclusive)
    #: non-overlap rule: ``full_interval`` (epic decision, v1), ``label_window`` (5200 text: t0' >= t_c + H),
    #: ``none`` (every confirmed candidate) — the latter two only for the count sensitivity table
    overlap_rule: str = "full_interval"
    version: str = "v1"                 # "v1" (frozen) or "v2" (5200 v2: first-touch 3 classes, cv blocks)
    tie_break: str = "false_start"      # v2 same-bar double touch: "false_start" (5200 v2) or "impulse" (sensitivity)
    cv_blocks: int = 6                  # v2: contiguous time blocks over the train period
    extra: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, default=str)


def params_v2(**overrides: Any) -> WaveParams:
    """Frozen 5200 v2 parameters (grid W4h_H4h / W4h_H8h, first-touch label, 6 cv blocks)."""
    kw: dict[str, Any] = {"cells": CELLS_V2, "version": "v2"}
    kw.update(overrides)
    return WaveParams(**kw)


def first_touch_class(hit: int | None, brk: int | None, tie_break: str = "false_start") -> str:
    """v2 label from the first bar index (time order) with ``high > P*`` (``hit``) / ``low < low[t0]`` (``brk``).

    The earlier bar decides; the same bar for both levels (order inside the bar unknown) is a ``false_start``
    (conservative, 5200 v2) unless ``tie_break == "impulse"`` (sensitivity only).
    """
    if hit is None and brk is None:
        return "sideways"
    if brk is None:
        return "impulse"
    if hit is None or brk < hit:
        return "false_start"
    if hit < brk:
        return "impulse"
    return "impulse" if tie_break == "impulse" else "false_start"


# --------------------------------------------------------------------------- minute grid


@dataclass
class MinuteData:
    """Complete 1m grid (missing minutes are NaN); ``m0`` = absolute epoch minute of index 0."""

    m0: int
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray

    @property
    def n(self) -> int:
        return int(self.close.size)

    def time(self, idx: int | np.ndarray) -> Any:
        """Open time(s) of bar index(es) as ``datetime64[m]``."""
        return (np.asarray(self.m0 + idx, dtype="int64")).astype("datetime64[m]")


_EPOCH = datetime(1970, 1, 1)


def _epoch_min(ts: Any) -> int:
    """Absolute epoch minute of a naive-UTC timestamp (str / datetime / numpy / pandas)."""
    return int(pd.Timestamp(ts).value // 60_000_000_000)


def _dt(epoch_min: int) -> datetime:
    return _EPOCH + timedelta(minutes=int(epoch_min))


def to_minute_grid(df_1m: pl.DataFrame) -> MinuteData:
    """1m OHLCV (``open_time, open, high, low, close, volume``) -> complete minute grid."""
    df = df_1m.sort("open_time")
    t = df["open_time"].to_numpy().astype("datetime64[m]").astype("int64")
    m0 = int(t[0])
    n = int(t[-1] - m0 + 1)
    pos = t - m0
    out = {}
    for col in ("open", "high", "low", "close", "volume"):
        arr = np.full(n, np.nan)
        arr[pos] = df[col].cast(pl.Float64).to_numpy()
        out[col] = arr
    return MinuteData(m0=m0, **out)


@dataclass
class Bars15:
    """Clock-aligned complete 15m bars; ``k0`` = absolute 15m bucket of index 0; NaN for incomplete buckets."""

    k0: int
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    atr: np.ndarray       # SMA true range over the last ``atr_n`` complete bars (incl. this one)


def to_bars15(m: MinuteData, p: WaveParams) -> Bars15:
    step = p.bar15
    first = -(-m.m0 // step)                      # first bucket starting at/after the grid start
    last = (m.m0 + m.n) // step                   # exclusive
    nb = max(last - first, 0)
    s = first * step - m.m0
    sl = slice(s, s + nb * step)

    def blk(a: np.ndarray) -> np.ndarray:
        return a[sl].reshape(nb, step)

    o, h, lo, c, v = (blk(a) for a in (m.open, m.high, m.low, m.close, m.volume))
    complete = ~np.isnan(c).any(axis=1)
    op = np.where(complete, o[:, 0], np.nan)
    hi = np.where(complete, h.max(axis=1, initial=-np.inf), np.nan)
    lw = np.where(complete, lo.min(axis=1, initial=np.inf), np.nan)
    cl = np.where(complete, c[:, -1], np.nan)
    vo = np.where(complete, np.nansum(v, axis=1), np.nan)
    prev_c = np.concatenate(([np.nan], cl[:-1]))
    tr = np.nanmax(np.vstack([hi - lw, np.abs(hi - prev_c), np.abs(lw - prev_c)]), axis=0)
    tr = np.where(complete & ~np.isnan(prev_c), tr, np.nan)
    atr = np.asarray(pd.Series(tr).rolling(p.atr_n, min_periods=p.atr_n).mean(), dtype=float)
    return Bars15(k0=first, open=op, high=hi, low=lw, close=cl, volume=vo, atr=atr)


# --------------------------------------------------------------------------- causal rolling inputs


@dataclass
class Rolling:
    sigma_1m: np.ndarray       # at bar i: std of log returns of bars i - window .. i - 1
    vol_mean: np.ndarray       # at bar i: mean 1m volume of bars i - window .. i - 1


def _shift1(a: np.ndarray) -> np.ndarray:
    """Value at i := a[i - 1] (window ending at the previous bar)."""
    return np.concatenate(([np.nan], a[:-1]))


def rolling_inputs(m: MinuteData, p: WaveParams) -> Rolling:
    ret = np.concatenate(([np.nan], np.diff(np.log(m.close))))
    mp = int(p.sigma_window * p.sigma_min_frac)
    sig = _shift1(np.asarray(pd.Series(ret).rolling(p.sigma_window, min_periods=mp).std(ddof=1), dtype=float))
    vol = _shift1(np.asarray(pd.Series(m.volume).rolling(p.sigma_window, min_periods=mp).mean(), dtype=float))
    return Rolling(sigma_1m=sig, vol_mean=vol)


# --------------------------------------------------------------------------- detection


def candidates(m: MinuteData, w: int, b: int) -> np.ndarray:
    """Bar indexes ``i`` whose low is the minimum of ``[t0 - B, t_c]`` (earliest equal low wins)."""
    low = pd.Series(m.low)
    left = low.rolling(b, min_periods=b).min().shift(1).to_numpy()
    right = low.rolling(w - 1, min_periods=w - 1).min().shift(-(w - 1)).to_numpy()
    with np.errstate(invalid="ignore"):
        ok = (m.low < left) & (m.low <= right)
    return np.flatnonzero(ok)


def _split_of(start_min: int, end_min: int, data_end_min: int, p: WaveParams, label_ok: bool) -> tuple[str, str | None]:
    """(split, purge_reason) — an event belongs to a split only when its whole interval is inside it."""
    tr0, oos0, ho0 = (_epoch_min(x) for x in (p.train_start, p.oos_start, p.holdout2_start))
    if not label_ok:
        return "purged", "incomplete_label"
    if start_min >= tr0 and end_min <= oos0:
        return "train", None
    if start_min >= oos0 and end_min <= ho0:
        return "oos", None
    if start_min >= ho0 and end_min <= data_end_min:
        return "holdout2", None
    if start_min < tr0 and end_min <= tr0:
        return "purged", "pre_train"
    return "purged", "split_boundary"


def _first_true(mask: np.ndarray) -> int | None:
    idx = np.flatnonzero(mask)
    return int(idx[0]) if idx.size else None


def build_cell(m: MinuteData, b15: Bars15, roll: Rolling, cell: tuple[str, int, int], p: WaveParams) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Events of one (W, H) cell + funnel counts."""
    cell_id, w, h = cell
    b = w
    n = m.n
    cand = candidates(m, w, b)
    cand = cand[cand + w - 1 < n]
    tc = cand + w - 1
    sig = roll.sigma_1m[cand]
    sigma_w = sig * np.sqrt(w)
    with np.errstate(invalid="ignore", divide="ignore"):
        a_log = np.log(m.close[tc] / m.low[cand])
        passing = np.isfinite(sigma_w) & (sigma_w > 0) & (a_log >= p.min_a_sigma * sigma_w)
    funnel = {"candidates": int(cand.size), "passing_a": int(passing.sum())}

    data_end_min = m.m0 + n  # close time of the last bar
    rows: list[dict[str, Any]] = []
    next_free = -(10**12)
    lead = {"full_interval": b, "label_window": 0, "none": None}[p.overlap_rule]
    for i in cand[passing]:
        i = int(i)
        if lead is not None:
            if i - lead < next_free:
                continue
            next_free = i + w + h
        rows.append(event_row(m, b15, roll, i, cell, p, data_end_min))
    funnel["accepted"] = len(rows)
    return rows, funnel


def event_row(m: MinuteData, b15: Bars15, roll: Rolling, i: int, cell: tuple[str, int, int], p: WaveParams,
              data_end_min: int) -> dict[str, Any]:
    """One event: identification, features (<= t_c), prefix window, label ((t_c, t_c + H]) and split."""
    cell_id, w, h = cell
    b = w
    tc = i + w - 1
    t0_min = m.m0 + i
    tc_min = t0_min + w                     # close time of bar tc
    low0 = float(m.low[i])
    close_tc = float(m.close[tc])
    a_log = float(np.log(close_tc / low0))
    sigma_1m = float(roll.sigma_1m[i])
    sigma_w = sigma_1m * float(np.sqrt(w))
    vol7 = float(roll.vol_mean[i])

    # --- S0 / C features: bars closed <= t_c only
    vb = p.velocity_bars
    velocity = (float(np.log(close_tc / m.close[tc - vb]) / (sigma_1m * np.sqrt(vb)))
                if tc - vb >= 0 else float("nan"))
    tb = p.trend_bars
    pretrend = float(np.log(m.close[i] / m.close[i - tb]) / sigma_w) if i - tb >= 0 else float("nan")
    drop_depth = float(np.log(np.nanmax(m.high[i - b:i + 1]) / low0))
    volume_ratio = float(np.nanmean(m.volume[i - b:tc + 1]) / vol7) if vol7 > 0 else float("nan")
    gap = bool(np.isnan(m.close[max(i - b, 0):min(tc + h + 1, m.n)]).any())

    # --- prefix image window (15m, clock aligned, close <= t_c)
    step = p.bar15
    k_first = -(-(t0_min - b) // step)
    k_last = tc_min // step - 1               # last 15m bucket whose close <= t_c
    kk = k_last - b15.k0
    atr = float(b15.atr[kk]) if 0 <= kk < b15.atr.size else float("nan")

    # --- label: bars tc + 1 .. tc + H only
    p_star = close_tc * float(np.exp(p.impulse_mult * a_log))
    label_ok = tc + h < m.n
    v2 = p.version != "v1"
    lab: dict[str, Any] = dict.fromkeys(LABEL_COLS_V2 if v2 else LABEL_COLS)
    lab["p_star"] = p_star
    if label_ok:
        hi = m.high[tc + 1:tc + h + 1]
        lo = m.low[tc + 1:tc + h + 1]
        label_ok = not (np.isnan(hi).any() or np.isnan(lo).any())
        if label_ok:
            hit = _first_true(hi > p_star)
            brk = _first_true(lo < low0)
            if v2:
                cls = first_touch_class(hit, brk, p.tie_break)
                impulse = cls == "impulse"
                lab["same_bar"] = hit is not None and hit == brk
            else:
                impulse = hit is not None
                cls = "impulse" if impulse else ("t0_below" if brk is not None else "sideways")
            if hit is None and brk is None:
                first = "none"
            elif brk is None or (hit is not None and hit < brk):
                first = "target"
            elif hit is None or brk < hit:
                first = "t0_break"
            else:
                first = "same_bar"
            mx, mn = float(hi.max()), float(lo.min())
            lab.update({
                "label_impulse": impulse, "label_class": cls, "label_sub": None if impulse else cls,
                "fwd_max_high": mx, "fwd_min_low": mn,
                "fwd_mfe_log": float(np.log(mx / close_tc)), "fwd_mae_log": float(np.log(mn / close_tc)),
                "mins_to_target": None if hit is None else hit + 1,
                "mins_to_t0_break": None if brk is None else brk + 1, "first_hit": first,
            })
    split, reason = _split_of(t0_min - b, tc_min + h, data_end_min, p, label_ok)
    if gap and split != "purged":
        split, reason = "purged", "gap"

    return {
        "cell_id": cell_id, "w_min": w, "h_min": h, "b_min": b,
        "t0": _dt(t0_min), "t_c": _dt(tc_min), "event_start": _dt(t0_min - b), "label_end": _dt(tc_min + h),
        "split": split, "purge_reason": reason,
        "low_t0": low0, "close_tc": close_tc, "a_log": a_log, "sigma_1m": sigma_1m, "sigma_w": sigma_w,
        "a_sigma": a_log / sigma_w, "velocity_1h": velocity, "pretrend_3d": pretrend,
        "log_sigma_1m": float(np.log(sigma_1m)), "drop_depth": drop_depth, "drop_depth_sigma": drop_depth / sigma_w,
        "volume_ratio": volume_ratio, "vol7d_mean_1m": vol7, "atr14_15m": atr,
        "prefix_start": _dt(k_first * step), "prefix_end": _dt((k_last + 1) * step),
        "n_prefix_15m": int(k_last - k_first + 1),
        **lab,
    }


_SCHEMA: dict[str, Any] = {
    "cell_id": pl.Utf8, "w_min": pl.Int32, "h_min": pl.Int32, "b_min": pl.Int32,
    "t0": pl.Datetime("us"), "t_c": pl.Datetime("us"), "event_start": pl.Datetime("us"), "label_end": pl.Datetime("us"),
    "split": pl.Utf8, "purge_reason": pl.Utf8,
    "low_t0": pl.Float64, "close_tc": pl.Float64, "a_log": pl.Float64, "sigma_1m": pl.Float64, "sigma_w": pl.Float64,
    "a_sigma": pl.Float64, "velocity_1h": pl.Float64, "pretrend_3d": pl.Float64, "log_sigma_1m": pl.Float64,
    "drop_depth": pl.Float64, "drop_depth_sigma": pl.Float64, "volume_ratio": pl.Float64, "vol7d_mean_1m": pl.Float64,
    "atr14_15m": pl.Float64, "prefix_start": pl.Datetime("us"), "prefix_end": pl.Datetime("us"), "n_prefix_15m": pl.Int32,
    "p_star": pl.Float64, "label_impulse": pl.Boolean, "label_class": pl.Utf8, "label_sub": pl.Utf8,
    "fwd_max_high": pl.Float64, "fwd_min_low": pl.Float64, "fwd_mfe_log": pl.Float64, "fwd_mae_log": pl.Float64,
    "mins_to_target": pl.Int32, "mins_to_t0_break": pl.Int32, "first_hit": pl.Utf8,
}


_SCHEMA_V2: dict[str, Any] = {**_SCHEMA, "same_bar": pl.Boolean, "cv_block": pl.Int32, "cv_straddle": pl.Boolean}


def schema_of(version: str) -> dict[str, Any]:
    return _SCHEMA if version == "v1" else _SCHEMA_V2


@dataclass
class EventDataset:
    events: pl.DataFrame
    funnel: pl.DataFrame
    params: WaveParams
    data_first: datetime
    data_last: datetime          # open time of the last 1m bar
    n_rows: int


def build_events(df_1m: pl.DataFrame, params: WaveParams | None = None) -> EventDataset:
    """All cells' events (sorted by cell, t0) + the detection funnel."""
    p = params or WaveParams()
    m = to_minute_grid(df_1m)
    b15 = to_bars15(m, p)
    roll = rolling_inputs(m, p)
    rows: list[dict[str, Any]] = []
    funnel = []
    for cell in p.cells:
        r, f = build_cell(m, b15, roll, cell, p)
        rows.extend(r)
        funnel.append({"cell_id": cell[0], **f})
    schema = schema_of(p.version)
    if p.version != "v1":
        for r in rows:
            r.setdefault("cv_block", None)
            r.setdefault("cv_straddle", None)
    ev = pl.DataFrame(rows, schema=schema, orient="row") if rows else pl.DataFrame(schema=schema)
    order = {c[0]: k for k, c in enumerate(p.cells)}
    ev = (ev.with_columns(pl.col("cell_id").replace_strict(order, return_dtype=pl.Int32).alias("_o"))
          .sort(["_o", "t0"]).drop("_o"))
    if p.version != "v1" and ev.height:
        ev = assign_cv_blocks(ev, p)
    by_split = (ev.group_by("cell_id", "split").len().pivot(on="split", index="cell_id", values="len").fill_null(0)
                if ev.height else pl.DataFrame({"cell_id": []}))
    fn = pl.DataFrame(funnel).join(by_split, on="cell_id", how="left")
    for s in ("train", "oos", "holdout2", "purged"):
        if s not in fn.columns:
            fn = fn.with_columns(pl.lit(0).alias(s))
    fn = fn.with_columns(pl.col(s).fill_null(0) for s in ("train", "oos", "holdout2", "purged"))
    return EventDataset(events=ev, funnel=fn.select("cell_id", "candidates", "passing_a", "accepted",
                                                     "train", "oos", "holdout2", "purged"),
                        params=p, data_first=_dt(m.m0), data_last=_dt(m.m0 + m.n - 1), n_rows=df_1m.height)


# --------------------------------------------------------------------------- v2: CV blocks, purge, embargo


def cv_block_bounds(p: WaveParams) -> list[datetime]:
    """``cv_blocks + 1`` boundaries over ``[train_start, oos_start)``: whole-day, near equal calendar length."""
    d0, d1 = pd.Timestamp(p.train_start), pd.Timestamp(p.oos_start)
    days = (d1 - d0).days
    return [_dt(_epoch_min(d0 + pd.Timedelta(days=round(k * days / p.cv_blocks)))) for k in range(p.cv_blocks + 1)]


def assign_cv_blocks(ev: pl.DataFrame, p: WaveParams) -> pl.DataFrame:
    """Add ``cv_block`` (block of ``t0``; train events only) and ``cv_straddle`` (interval ``[event_start, label_end)``
    leaves its block; such events enter no fold at all: 5200 "a blokkhatart atlogo esemenyek sehova nem kerulnek")."""
    b = cv_block_bounds(p)
    t0 = ev["t0"].to_numpy().astype("datetime64[us]")
    edges = np.array(b[1:-1], dtype="datetime64[us]")
    idx = np.searchsorted(edges, t0, side="right").astype(np.int32)       # block k: [b_k, b_k+1)
    lo = np.array(b[:-1], dtype="datetime64[us]")[idx]
    hi = np.array(b[1:], dtype="datetime64[us]")[idx]
    es = ev["event_start"].to_numpy().astype("datetime64[us]")
    le = ev["label_end"].to_numpy().astype("datetime64[us]")
    straddle = (es < lo) | (le > hi)
    train = pl.col("split") == "train"
    return ev.with_columns(
        pl.when(train).then(pl.Series(idx)).otherwise(None).cast(pl.Int32).alias("cv_block"),
        pl.when(train).then(pl.Series(straddle)).otherwise(None).cast(pl.Boolean).alias("cv_straddle"),
    )


@dataclass
class CVFold:
    test_block: int
    train: pl.DataFrame
    test: pl.DataFrame
    n_straddle: int          # events of the other blocks dropped because they straddle a block boundary
    n_purged: int            # train candidates whose interval overlaps the test block range
    n_embargoed: int         # additional train candidates inside the H embargo band around the test block


def cv_fold(ev: pl.DataFrame, test_block: int, bounds: list[datetime], embargo_min: int) -> CVFold:
    """One purged + embargoed fold of ONE cell's train events (pure function).

    * test  = events with ``cv_block == test_block`` that do not straddle a block boundary;
    * purge = train candidates whose whole interval ``[event_start, label_end)`` overlaps the test-block range
      ``[bounds[k], bounds[k+1])`` are removed;
    * embargo = additionally removed when the interval overlaps the ``embargo_min`` band on either edge of the
      test block (``H``, per cell);
    * boundary-straddling events are in no fold (neither test nor train).
    """
    lo, hi = bounds[test_block], bounds[test_block + 1]
    emb = timedelta(minutes=embargo_min)
    tr = ev.filter(pl.col("split") == "train")
    test = tr.filter((pl.col("cv_block") == test_block) & ~pl.col("cv_straddle"))
    other = tr.filter(pl.col("cv_block") != test_block)
    n_straddle = int(other["cv_straddle"].sum())
    cand = other.filter(~pl.col("cv_straddle"))
    overlap = (pl.col("event_start") < hi) & (pl.col("label_end") > lo)
    overlap_emb = (pl.col("event_start") < hi + emb) & (pl.col("label_end") > lo - emb)
    n_purged = cand.filter(overlap).height
    n_emb = cand.filter(overlap_emb & ~overlap).height
    return CVFold(test_block, cand.filter(~overlap_emb), test, n_straddle, n_purged, n_emb)


def cv_summary(ev: pl.DataFrame, bounds: list[datetime], *, min_block: int = 40, min_pooled: int = 100
               ) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Per-block event / class counts of one cell + the 5200 v2 minimum checks (>= ``min_block`` usable test events per
    block, pooled >= ``min_pooled`` impulse and non-impulse)."""
    tr = ev.filter(pl.col("split") == "train")
    rows = []
    for k in range(len(bounds) - 1):
        b = tr.filter(pl.col("cv_block") == k)
        u = b.filter(~pl.col("cv_straddle"))
        rows.append({
            "block": k + 1, "start": bounds[k].date().isoformat(),
            "end": (bounds[k + 1] - timedelta(days=1)).date().isoformat(),   # inclusive last day
            "n_events": b.height, "n_straddle": b.height - u.height, "n_test": u.height,
            "n_impulse": int((u["label_class"] == "impulse").sum()),
            "n_false_start": int((u["label_class"] == "false_start").sum()),
            "n_sideways": int((u["label_class"] == "sideways").sum()),
        })
    df = pl.DataFrame(rows)
    n_imp, n_test = int(df["n_impulse"].sum()), int(df["n_test"].sum())
    checks: dict[str, Any] = {
        "min_block_events": int(min(df["n_test"].to_list())), "block_ok": bool((df["n_test"] >= min_block).all()),
        "pooled_impulse": n_imp, "pooled_non_impulse": n_test - n_imp,
        "pooled_ok": bool(n_imp >= min_pooled and n_test - n_imp >= min_pooled),
    }
    checks["pass"] = checks["block_ok"] and checks["pooled_ok"]
    return df, checks


# --------------------------------------------------------------------------- prefix image (t919 input)


def prefix_image(b15: Bars15, row: dict[str, Any]) -> dict[str, np.ndarray]:
    """ATR-normalised 15m OHLC (relative to ``low[t0]``) + volume (/ 7d mean 15m volume) of one event's prefix.

    Uses only the 15m bars listed by ``prefix_start`` / ``prefix_end`` (close <= t_c) and ``atr14_15m``.
    """
    step = 15
    k_first = _epoch_min(row["prefix_start"]) // step - b15.k0
    k_end = _epoch_min(row["prefix_end"]) // step - b15.k0      # exclusive
    atr, low0 = float(row["atr14_15m"]), float(row["low_t0"])
    vol15 = float(row["vol7d_mean_1m"]) * step
    sl = slice(k_first, k_end)
    return {
        "open": (b15.open[sl] - low0) / atr, "high": (b15.high[sl] - low0) / atr,
        "low": (b15.low[sl] - low0) / atr, "close": (b15.close[sl] - low0) / atr,
        "volume": b15.volume[sl] / vol15 if vol15 > 0 else b15.volume[sl] * np.nan,
    }


# --------------------------------------------------------------------------- persistence (lab DB)


def content_hash(events: pl.DataFrame) -> str:
    return hashlib.sha256(events.write_csv(float_precision=10).encode("utf-8")).hexdigest()[:16]


def persist(ds: EventDataset, asset_id: str | None = None, *, tries: int = 20, delay: float = 3.0,
            lab_path: str | None = None) -> dict[str, Any]:
    """Write ``research."wave_events_<version>"`` (+ ``__meta``, ``__funnel``) into the asset's LAB DB (never the live DB).

    Retries while another process holds the lab DB lock; raises the last error when all tries fail.
    """
    import utils

    path = lab_path or utils.lab_db_path(asset_id)
    version = ds.params.version
    table = table_name(version)
    meta: dict[str, Any] = {
        "version": version, "table": f'{SCHEMA}."{table}"', "generator": GENERATOR,
        "params_json": ds.params.to_json(), "ohlcv_first": ds.data_first, "ohlcv_last": ds.data_last,
        "ohlcv_rows": ds.n_rows, "n_events": ds.events.height, "content_hash": content_hash(ds.events),
        "created_at": datetime.now().replace(microsecond=0),
    }
    if version != "v1":
        meta["cv_bounds"] = json.dumps([b.date().isoformat() for b in cv_block_bounds(ds.params)])
        meta["tie_break"] = ds.params.tie_break
    last_exc: Exception | None = None
    for attempt in range(tries):
        try:
            con = duckdb.connect(path)
        except duckdb.Error as exc:
            last_exc = exc
            logger.warning("lab db locked/unavailable (attempt %d/%d): %s", attempt + 1, tries, exc)
            time.sleep(delay)
            continue
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            ev, fn, mt = ds.events.to_arrow(), ds.funnel.to_arrow(), pl.DataFrame([meta]).to_arrow()  # noqa: F841
            con.execute(f'CREATE OR REPLACE TABLE {SCHEMA}."{table}" AS SELECT * FROM ev')
            con.execute(f'CREATE OR REPLACE TABLE {SCHEMA}."{table}__funnel" AS SELECT * FROM fn')
            con.execute(f'CREATE OR REPLACE TABLE {SCHEMA}."{table}__meta" AS SELECT * FROM mt')
            meta["attempts"] = attempt + 1
            meta["lab_path"] = path
            return meta
        finally:
            con.close()
    raise RuntimeError(f"lab db write failed after {tries} tries: {last_exc}")


def load_events(asset_id: str | None = None, *, split: str | None = None, version: str = VERSION) -> pl.DataFrame:
    """Read ``research."wave_events_<version>"`` from the lab DB (read-only); optional split filter."""
    import utils

    con = duckdb.connect(utils.lab_db_path(asset_id), read_only=True)
    try:
        sql = f'SELECT * FROM {SCHEMA}."{table_name(version)}"' + (" WHERE split = ?" if split else "") + " ORDER BY cell_id, t0"
        return con.execute(sql, [split] if split else []).pl()
    finally:
        con.close()
