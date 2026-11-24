"""Support/resistance level and trendline detector (epic_071 / t910).

Pure, side-effect-free library: 1m OHLCV in, Polars DataFrames out. No DB
access, no file writes. Parameters come from ``config/features.json``
``levels_detector`` (read via ``utils.load_levels_detector_config``) unless an
explicit ``params`` dict is passed.

Pipeline per scale (``s`` = 1h / 7d, ``m`` = 4h / 30d, ``l`` = 1d / 180d)::

    1m OHLCV -> scale bars (+ ATR14) -> pivots -> levels  -+-> events -> forward labels
                                              \\-> trendlines -/            (research only)

Look-ahead rule
---------------
Every object carries ``confirmed_at``: the earliest wall-clock time at which
it is fully determined by *closed* scale bars. For a pivot on bar ``i`` with
``n`` bars on each side that is the CLOSE time of bar ``i + n`` (i.e. the pivot
bar's open time + ``(n+1)`` bar lengths). Levels / lines are confirmed by the
confirmation of the pivot that completes them; events exist at the close of
the bar that produced them. Detection is strictly online (pivots processed in
``confirmed_at`` order, decisions only use information known at that time),
so running on data truncated at ``T`` yields exactly the objects of the full
run with ``confirmed_at <= T``; the ``*_as_of`` helpers return the evolving
state (touch count, breaks, expiry, role) of objects as known at ``T``.

Timestamps are ``pl.Datetime`` (UTC, naive, like the ``ohlcv`` table); scale
bars are labelled by their open time and are epoch-aligned (00:00 UTC for 1d).

The ``forward_*`` function looks into the future by design; its output is a
research label (t913 event study) and must never become a feature.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import polars as pl

import utils

logger = logging.getLogger(__name__)

_BAR_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800,
                "1h": 3600, "2h": 7200, "4h": 14400, "1d": 86400}

_NEVER = np.iinfo(np.int64).max  # sentinel bar position: "does not happen"


# %% Params and result container


def load_params(params: dict | None = None) -> dict:
    """Return detector params: the explicit dict, else the config block."""
    return params if params is not None else utils.load_levels_detector_config()


@dataclass
class ScaleResult:
    """All detector outputs of one scale. Schemas: see ``detect_scale``."""

    scale         : str
    bars          : pl.DataFrame
    pivots        : pl.DataFrame
    levels        : pl.DataFrame
    level_touches : pl.DataFrame
    trendlines    : pl.DataFrame
    line_touches  : pl.DataFrame
    events        : pl.DataFrame


def _scale_cfg(params: dict, scale: str) -> tuple[str, int, int]:
    """Return (bar string, bar seconds, lookback in seconds) of a scale."""
    sc = params["scales"][scale]
    bar = sc["bar"]
    if bar not in _BAR_SECONDS:
        raise ValueError(f"Unsupported scale bar: {bar}")
    return bar, _BAR_SECONDS[bar], int(float(sc["lookback_days"]) * 86400)


# %% Scale bars


def resample_ohlcv(df_1m: pl.DataFrame, bar: str, atr_window: int = 14) -> pl.DataFrame:
    """Aggregate 1m OHLCV into epoch-aligned scale bars with Wilder ATR.

    The trailing bar is dropped when it is not yet closed (its close time is
    after the last 1m bar's close), so a truncated input never produces a bar
    that differs from the full run.

    Args:
        df_1m      : Columns open_time (Datetime or 'YYYY-MM-DD HH:MM:SS' str),
                     open, high, low, close, volume. Any order; gaps allowed.
        bar        : Scale bar, one of 1m/5m/15m/30m/1h/2h/4h/1d.
        atr_window : Wilder ATR window on the scale bars.

    Returns:
        Columns: open_time, close_time (Datetime), open, high, low, close,
        volume (f64), n_minutes (u32), bar_num (i64, open epoch / bar seconds,
        gap-robust bar index), atr (f64, null for the first atr_window-1 bars).
    """
    secs = _BAR_SECONDS[bar]
    df = df_1m.select("open_time", "open", "high", "low", "close", "volume")
    if df.schema["open_time"] == pl.Utf8:
        df = df.with_columns(pl.col("open_time").str.to_datetime("%Y-%m-%d %H:%M:%S"))
    df = df.with_columns(pl.col("open_time").cast(pl.Datetime("us"))).sort("open_time")
    if df.height == 0:
        return _empty_bars()
    last_close = df["open_time"][-1] + timedelta(minutes=1)

    bars = (
        df.with_columns(pl.col("open_time").dt.truncate(bar).alias("_b"))
        .group_by("_b", maintain_order=True)
        .agg(
            pl.col("open").first(), pl.col("high").max(), pl.col("low").min(),
            pl.col("close").last(), pl.col("volume").sum(),
            pl.len().cast(pl.UInt32).alias("n_minutes"),
        )
        .rename({"_b": "open_time"})
        .with_columns((pl.col("open_time") + pl.duration(seconds=secs)).alias("close_time"))
        .filter(pl.col("close_time") <= last_close)
        .with_columns(
            (pl.col("open_time").dt.epoch("s") // secs).cast(pl.Int64).alias("bar_num"),
            *(pl.col(c).cast(pl.Float64) for c in ("open", "high", "low", "close", "volume")),
        )
    )
    atr = _wilder_atr(bars["high"].to_numpy(), bars["low"].to_numpy(),
                      bars["close"].to_numpy(), atr_window)
    return bars.with_columns(pl.Series("atr", atr).fill_nan(None)).select(
        "open_time", "close_time", "open", "high", "low", "close", "volume",
        "n_minutes", "bar_num", "atr",
    )


def _empty_bars() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "open_time": pl.Datetime("us"), "close_time": pl.Datetime("us"),
        "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
        "volume": pl.Float64, "n_minutes": pl.UInt32, "bar_num": pl.Int64, "atr": pl.Float64,
    })


def _wilder_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, w: int) -> np.ndarray:
    """Causal Wilder ATR (RMA of true range); NaN until w bars are available."""
    n = high.size
    out = np.full(n, np.nan)
    if n < w:
        return out
    prev = np.concatenate(([np.nan], close[:-1]))
    tr = np.nanmax(np.vstack([high - low, np.abs(high - prev), np.abs(low - prev)]), axis=0)
    atr = tr[:w].mean()
    out[w - 1] = atr
    for i in range(w, n):
        atr = (atr * (w - 1) + tr[i]) / w
        out[i] = atr
    return out


# %% Pivots


_PIVOT_SCHEMA = {
    "pivot_id": pl.Utf8, "scale": pl.Utf8, "kind": pl.Utf8, "bar_idx": pl.Int64,
    "bar_num": pl.Int64, "pivot_time": pl.Datetime("us"), "price": pl.Float64,
    "atr": pl.Float64, "prominence_atr": pl.Float64, "confirm_idx": pl.Int64,
    "confirmed_at": pl.Datetime("us"),
}


def detect_pivots(bars: pl.DataFrame, scale: str, params: dict | None = None) -> pl.DataFrame:
    """Fractal pivots with ATR prominence filter.

    A high pivot on bar i: high[i] > max(high[i-n..i-1]) and
    high[i] >= max(high[i+1..i+n]) and high[i] - min(low[i-n..i+n]) >= k*ATR[i]
    (mirror for lows). Confirmed at the close of bar i+n.

    Args:
        bars   : Output of ``resample_ohlcv``.
        scale  : Scale key (s/m/l), copied into the output.
        params : Detector params (default: config).

    Returns:
        Columns: pivot_id (str, '<scale>-P<seq>'), scale, kind ('high'/'low'),
        bar_idx (row position in bars), bar_num, pivot_time (pivot bar open),
        price (high or low), atr (ATR at pivot bar), prominence_atr,
        confirm_idx (= bar_idx + n), confirmed_at (close of bar i+n).
        Sorted by (confirmed_at, kind, bar_idx).
    """
    p = load_params(params)
    n = int(p["pivot_n"])
    k = float(p["pivot_min_prominence_atr"])
    hi = bars["high"].to_numpy()
    lo = bars["low"].to_numpy()
    atr = bars["atr"].fill_null(np.nan).to_numpy()
    m = hi.size
    rows: list[tuple] = []
    if m >= 2 * n + 1:
        w = 2 * n + 1
        hw = np.lib.stride_tricks.sliding_window_view(hi, w)
        lw = np.lib.stride_tricks.sliding_window_view(lo, w)
        c_hi, c_lo = hi[n:m - n], lo[n:m - n]
        a = atr[n:m - n]
        win_min = lw.min(axis=1)
        win_max = hw.max(axis=1)
        is_hi = ((c_hi > hw[:, :n].max(axis=1)) & (c_hi >= hw[:, n + 1:].max(axis=1))
                 & (c_hi - win_min >= k * a))
        is_lo = ((c_lo < lw[:, :n].min(axis=1)) & (c_lo <= lw[:, n + 1:].min(axis=1))
                 & (win_max - c_lo >= k * a))
        for kind, mask, price, prom in (
            ("high", is_hi, c_hi, c_hi - win_min),
            ("low", is_lo, c_lo, win_max - c_lo),
        ):
            for j in np.flatnonzero(mask & np.isfinite(a)):
                i = int(j) + n
                rows.append((kind, i, float(price[j]), float(a[j]), float(prom[j] / a[j])))
    if not rows:
        return pl.DataFrame(schema=_PIVOT_SCHEMA)
    rows.sort(key=lambda r: (r[1], r[0]))
    bar_num = bars["bar_num"].to_numpy()
    o_t = bars["open_time"].to_list()
    c_t = bars["close_time"].to_list()
    out = pl.DataFrame({
        "pivot_id": [f"{scale}-P{q:06d}" for q in range(len(rows))],
        "scale": [scale] * len(rows),
        "kind": [r[0] for r in rows],
        "bar_idx": [r[1] for r in rows],
        "bar_num": [int(bar_num[r[1]]) for r in rows],
        "pivot_time": [o_t[r[1]] for r in rows],
        "price": [r[2] for r in rows],
        "atr": [r[3] for r in rows],
        "prominence_atr": [r[4] for r in rows],
        "confirm_idx": [r[1] + n for r in rows],
        "confirmed_at": [c_t[r[1] + n] for r in rows],
    }, schema=_PIVOT_SCHEMA)
    return out


# %% Horizontal levels


_LEVEL_SCHEMA = {
    "level_id": pl.Utf8, "scale": pl.Utf8, "price": pl.Float64, "tolerance": pl.Float64,
    "atr_at_confirm": pl.Float64, "n_touches": pl.Int64, "first_touch": pl.Datetime("us"),
    "last_touch": pl.Datetime("us"), "confirm_idx": pl.Int64, "confirmed_at": pl.Datetime("us"),
    "expired_at": pl.Datetime("us"), "strength": pl.Float64, "is_round": pl.Boolean,
    "is_round_secondary": pl.Boolean, "role": pl.Utf8, "n_role_flips": pl.Int64,
    "broken_at": pl.Datetime("us"),
}
_TOUCH_SCHEMA = {
    "obj_id": pl.Utf8, "pivot_id": pl.Utf8, "pivot_time": pl.Datetime("us"),
    "price": pl.Float64, "confirmed_at": pl.Datetime("us"),
}


def _round_flags(price: float, tol: float, step: float, step2: float) -> tuple[bool, bool]:
    near10 = abs(price - round(price / step) * step) <= tol and round(price / step) != 0
    near5 = abs(price - round(price / step2) * step2) <= tol and round(price / step2) != 0
    return near10, (near5 and not near10)


def detect_levels(
    bars   : pl.DataFrame,
    pivots : pl.DataFrame,
    scale  : str,
    params : dict | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Online clustering of confirmed pivots (both kinds) into horizontal levels.

    Pivots are processed in confirmed_at order. A pivot joins the nearest live
    cluster within ``level_tolerance_atr * pivot ATR``, else seeds a new one. A
    cluster becomes a level when it reaches ``level_min_touches``; its price is
    the mean of those touches and is then frozen (later touches only count).
    A cluster dies ``lookback`` after the confirmation of its last touch.

    Returns:
        (levels, level_touches). ``levels`` columns: level_id ('<scale>-L<seq>'),
        scale, price, tolerance (0.25*ATR at confirm), atr_at_confirm,
        n_touches, first_touch, last_touch (pivot open times), confirm_idx,
        confirmed_at, expired_at (null if still alive at data end), and the
        end-of-life state (at min(expired_at, data end)): strength
        (sum exp(-age/tau), tau=lookback*strength_tau_frac), is_round /
        is_round_secondary, role ('support'/'resistance' vs close),
        n_role_flips, broken_at (first break). The last three are filled by
        ``detect_scale`` from the events; here they are null / 0.
        ``level_touches``: obj_id, pivot_id, pivot_time, price, confirmed_at.
    """
    p = load_params(params)
    _, _, lookback = _scale_cfg(p, scale)
    tol_k = float(p["level_tolerance_atr"])
    min_t = int(p["level_min_touches"])
    lb = timedelta(seconds=lookback)

    clusters: list[dict[str, Any]] = []   # live clusters
    levels: list[dict[str, Any]] = []
    touches: list[tuple] = []
    for r in pivots.iter_rows(named=True):
        c = r["confirmed_at"]
        clusters = [cl for cl in clusters if cl["last_conf"] + lb > c]
        tol = tol_k * r["atr"]
        best, best_d = None, math.inf
        for cl in clusters:
            d = abs(r["price"] - cl["price"])
            if d <= tol and d < best_d:
                best, best_d = cl, d
        if best is None:
            clusters.append({"price": r["price"], "touches": [r], "last_conf": c, "level": None})
            continue
        best["touches"].append(r)
        best["last_conf"] = c
        if best["level"] is None and len(best["touches"]) >= min_t:
            best["price"] = float(np.mean([t["price"] for t in best["touches"]]))
            lvl = {"level_id": f"{scale}-L{len(levels):06d}", "price": best["price"],
                   "tolerance": tol, "atr_at_confirm": r["atr"],
                   "confirm_idx": r["confirm_idx"], "confirmed_at": c, "cluster": best}
            best["level"] = lvl
            levels.append(lvl)
            touches.extend((lvl["level_id"], t["pivot_id"], t["pivot_time"], t["price"], c)
                           for t in best["touches"])
        elif best["level"] is not None:
            touches.append((best["level"]["level_id"], r["pivot_id"], r["pivot_time"], r["price"], c))

    data_end = bars["close_time"][-1] if bars.height else None
    step, step2 = float(p["round_step"]), float(p["round_step_secondary"])
    rows = []
    for lvl in levels:
        cl = lvl["cluster"]
        exp = cl["last_conf"] + lb
        rnd, rnd2 = _round_flags(lvl["price"], lvl["tolerance"], step, step2)
        rows.append({
            "level_id": lvl["level_id"], "scale": scale, "price": lvl["price"],
            "tolerance": lvl["tolerance"], "atr_at_confirm": lvl["atr_at_confirm"],
            "n_touches": len(cl["touches"]), "first_touch": cl["touches"][0]["pivot_time"],
            "last_touch": cl["touches"][-1]["pivot_time"], "confirm_idx": lvl["confirm_idx"],
            "confirmed_at": lvl["confirmed_at"],
            "expired_at": exp if data_end is not None and exp <= data_end else None,
            "strength": None, "is_round": rnd, "is_round_secondary": rnd2,
            "role": None, "n_role_flips": 0, "broken_at": None,
        })
    lv = pl.DataFrame(rows, schema=_LEVEL_SCHEMA) if rows else pl.DataFrame(schema=_LEVEL_SCHEMA)
    tt = (pl.DataFrame(touches, schema=_TOUCH_SCHEMA, orient="row") if touches
          else pl.DataFrame(schema=_TOUCH_SCHEMA))
    return lv, tt


# %% Trendlines


_LINE_SCHEMA = {
    "line_id": pl.Utf8, "scale": pl.Utf8, "kind": pl.Utf8,
    "anchor1_time": pl.Datetime("us"), "anchor1_price": pl.Float64, "anchor1_bar_num": pl.Int64,
    "anchor2_time": pl.Datetime("us"), "anchor2_price": pl.Float64, "anchor2_bar_num": pl.Int64,
    "slope_per_bar": pl.Float64, "slope_per_bar_atr": pl.Float64, "atr_at_confirm": pl.Float64,
    "n_touches": pl.Int64, "last_touch": pl.Datetime("us"), "confirm_idx": pl.Int64,
    "confirmed_at": pl.Datetime("us"), "expired_at": pl.Datetime("us"),
    "expiry_reason": pl.Utf8, "broken_at": pl.Datetime("us"),
}


def _first_violation(close: np.ndarray, eps: np.ndarray, bar_num: np.ndarray,
                     a_num: int, a_price: float, slope: float, start: int,
                     sign: float) -> int:
    """First bar position >= start whose close is beyond the line by > eps.

    sign = +1 for support (violation below), -1 for resistance (above).
    Returns _NEVER if none in the data.
    """
    line = a_price + slope * (bar_num[start:] - a_num)
    viol = sign * (line - close[start:]) > eps[start:]
    j = np.flatnonzero(viol)
    return int(start + j[0]) if j.size else _NEVER


def detect_trendlines(
    bars   : pl.DataFrame,
    pivots : pl.DataFrame,
    scale  : str,
    params : dict | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Online trendline detection from same-kind pivot pairs.

    Support lines: rising lows; resistance lines: falling highs. Each new
    confirmed pivot (a) adds a touch to every live candidate / line it lies on
    (within ``eps = line_tolerance_atr * pivot ATR``) provided no close crossed
    the line by more than eps (per-bar ATR) since the first anchor, and (b)
    pairs with every earlier same-kind pivot within ``lookback`` to form new
    candidates (intermediate pivots on the line count as touches; candidates
    with a close violation between the anchors are rejected). A candidate is
    confirmed when it reaches ``line_min_touches`` (3) unless it was broken
    (close beyond ``break_atr`` ATR) before its confirmation, or it duplicates a
    live confirmed line (shares >= 2 touch pivots, or same kind with price
    within eps at the confirm bar and slope within ``line_dedup_slope_atr``);
    the older, already-confirmed line wins (it has >= as many touches).
    Lines die at their first break or ``lookback`` after their last touch's
    confirmation. 2-touch candidates are internal only and never emitted
    (bounds the output; the spec allowed keeping them behind a flag).

    Returns:
        (trendlines, line_touches). ``trendlines`` columns: line_id
        ('<scale>-T<seq>'), scale, kind ('support'/'resistance'),
        anchor1_/anchor2_ time / price / bar_num (the defining pivot pair),
        slope_per_bar (price per scale bar), slope_per_bar_atr (/ ATR at
        confirm), atr_at_confirm, n_touches, last_touch, confirm_idx,
        confirmed_at, expired_at (null if alive at data end), expiry_reason
        ('break'/'stale', null while alive), broken_at
        (filled by ``detect_scale`` from the events). Line price at a bar:
        anchor1_price + slope_per_bar * (bar_num - anchor1_bar_num).
        ``line_touches``: obj_id, pivot_id, pivot_time, price, confirmed_at.
    """
    p = load_params(params)
    _, _, lookback = _scale_cfg(p, scale)
    lb = timedelta(seconds=lookback)
    eps_k = float(p["line_tolerance_atr"])
    brk_k = float(p["break_atr"])
    min_t = int(p["line_min_touches"])
    dedup_slope = float(p["line_dedup_slope_atr"])

    close = bars["close"].to_numpy()
    atr = bars["atr"].fill_null(np.nan).to_numpy()
    eps_bar = eps_k * atr
    brk_bar = brk_k * atr
    bar_num = bars["bar_num"].to_numpy()
    close_t = bars["close_time"].to_list()

    seen: dict[str, list[dict[str, Any]]] = {"low": [], "high": []}
    cands: dict[str, list[dict[str, Any]]] = {"low": [], "high": []}
    lines: list[dict[str, Any]] = []
    live_lines: dict[str, list[dict[str, Any]]] = {"low": [], "high": []}

    def _price_at(obj: dict[str, Any], num: int) -> float:
        return obj["a1"]["price"] + obj["slope"] * (num - obj["a1"]["bar_num"])

    def _break_pos(obj: dict[str, Any], start: int) -> int:
        line = _price_at(obj, 0) + obj["slope"] * bar_num[start:]
        j = np.flatnonzero(obj["sign"] * (line - close[start:]) > brk_bar[start:])
        return int(start + j[0]) if j.size else _NEVER

    def _try_confirm(cd: dict[str, Any], r: dict[str, Any], kind: str) -> None:
        # broken between the last touch bar and the confirm bar -> dead
        bpos = _break_pos(cd, r["bar_idx"] + 1)
        if bpos <= r["confirm_idx"]:
            cd["dead"] = True
            return
        ci = r["confirm_idx"]
        a_c = atr[ci] if np.isfinite(atr[ci]) else r["atr"]
        ids = {t["pivot_id"] for t in cd["touches"]}
        for ln in live_lines[kind]:
            shared = len(ids & {t["pivot_id"] for t in ln["touches"]})
            close_px = abs(_price_at(ln, int(bar_num[ci])) - _price_at(cd, int(bar_num[ci])))
            same_slope = abs(ln["slope"] - cd["slope"]) <= dedup_slope * a_c
            if shared >= 2 or (close_px <= eps_k * a_c and same_slope):
                cd["dead"] = True
                return
        cd.update(line_id=f"{scale}-T{len(lines):06d}", confirm_idx=ci,
                  confirmed_at=r["confirmed_at"], atr_c=a_c, break_pos=bpos,
                  n_at_confirm=len(cd["touches"]))
        lines.append(cd)
        live_lines[kind].append(cd)

    for r in pivots.iter_rows(named=True):
        kind, c, m = r["kind"], r["confirmed_at"], r["bar_idx"]
        # --- expire / prune ---
        live_lines[kind] = [ln for ln in live_lines[kind]
                            if ln["last_conf"] + lb > c
                            and (ln["break_pos"] == _NEVER or close_t[ln["break_pos"]] > c)]
        cands[kind] = [cd for cd in cands[kind]
                       if not cd["dead"] and cd["last_conf"] + lb > c and cd["viol"] > m]
        eps = eps_k * r["atr"]
        # --- (a) touch existing candidates and lines ---
        for obj in cands[kind] + live_lines[kind]:
            if obj["viol"] > m and abs(r["price"] - _price_at(obj, r["bar_num"])) <= eps:
                obj["touches"].append(r)
                obj["last_conf"] = c
                if "line_id" not in obj and len(obj["touches"]) >= min_t:
                    _try_confirm(obj, r, kind)
        cands[kind] = [cd for cd in cands[kind] if "line_id" not in cd and not cd["dead"]]
        # --- (b) new candidates with earlier same-kind pivots ---
        sign = 1.0 if kind == "low" else -1.0
        seen[kind] = [q for q in seen[kind] if r["pivot_time"] - q["pivot_time"] <= lb]
        for q in seen[kind]:
            rising = r["price"] > q["price"]
            if (kind == "low") != rising or q["bar_num"] == r["bar_num"]:
                continue
            slope = (r["price"] - q["price"]) / (r["bar_num"] - q["bar_num"])
            cd: dict[str, Any] = {"a1": q, "a2": r, "slope": slope, "sign": sign,
                                  "kind": kind, "dead": False, "last_conf": c}
            viol = _first_violation(close, eps_bar, bar_num, q["bar_num"], q["price"],
                                    slope, q["bar_idx"], sign)
            if viol <= m:
                continue
            cd["viol"] = viol
            mids = [t for t in seen[kind] if q["bar_idx"] < t["bar_idx"] < m
                    and abs(t["price"] - _price_at(cd, t["bar_num"])) <= eps_k * t["atr"]]
            cd["touches"] = [q, *mids, r]
            if len(cd["touches"]) >= min_t:
                _try_confirm(cd, r, kind)
            elif not cd["dead"]:
                cands[kind].append(cd)
        seen[kind].append(r)

    data_end = close_t[-1] if close_t else None
    rows, touches = [], []
    for ln in lines:
        tch = ln["touches"]
        exp, reason = ln["last_conf"] + lb, "stale"
        # lines also die at their break: expired_at = earlier of the two
        if ln["break_pos"] != _NEVER and close_t[ln["break_pos"]] < exp:
            exp, reason = close_t[ln["break_pos"]], "break"
        rows.append({
            "line_id": ln["line_id"], "scale": scale,
            "kind": "support" if ln["kind"] == "low" else "resistance",
            "anchor1_time": ln["a1"]["pivot_time"], "anchor1_price": ln["a1"]["price"],
            "anchor1_bar_num": ln["a1"]["bar_num"],
            "anchor2_time": ln["a2"]["pivot_time"], "anchor2_price": ln["a2"]["price"],
            "anchor2_bar_num": ln["a2"]["bar_num"],
            "slope_per_bar": ln["slope"], "slope_per_bar_atr": ln["slope"] / ln["atr_c"],
            "atr_at_confirm": ln["atr_c"], "n_touches": len(tch),
            "last_touch": tch[-1]["pivot_time"], "confirm_idx": ln["confirm_idx"],
            "confirmed_at": ln["confirmed_at"],
            "expired_at": exp if data_end is not None and exp <= data_end else None,
            "expiry_reason": reason if data_end is not None and exp <= data_end else None,
            "broken_at": None,
        })
        for i, t in enumerate(tch):
            conf = ln["confirmed_at"] if i < ln["n_at_confirm"] else t["confirmed_at"]
            touches.append((ln["line_id"], t["pivot_id"], t["pivot_time"], t["price"], conf))
    tl = pl.DataFrame(rows, schema=_LINE_SCHEMA) if rows else pl.DataFrame(schema=_LINE_SCHEMA)
    tt = (pl.DataFrame(touches, schema=_TOUCH_SCHEMA, orient="row") if touches
          else pl.DataFrame(schema=_TOUCH_SCHEMA))
    return tl, tt


# %% Events


_EVENT_SCHEMA = {
    "event_time": pl.Datetime("us"), "bar_idx": pl.Int64, "bar_num": pl.Int64,
    "scale": pl.Utf8, "obj_type": pl.Utf8, "obj_id": pl.Utf8, "event_type": pl.Utf8,
    "side": pl.Utf8, "price": pl.Float64, "obj_slope_per_bar": pl.Float64,
    "bar_close": pl.Float64, "atr": pl.Float64,
}


def _scan_object(
    out      : list[tuple],
    arrays   : tuple[np.ndarray, ...],
    close_t  : list[datetime],
    scale    : str,
    obj_type : str,
    obj_id   : str,
    start    : int,
    stop     : int,
    p0       : float,
    num0     : int,
    slope    : float,
    side0    : int,
    eps_k    : float,
    brk_k    : float,
    retest_n : int,
    kill_on_break : bool,
) -> None:
    """Bar-by-bar touch / break / retest state machine for one object.

    side = +1: price above the object (support); -1: below (resistance).
    Bars start..stop-1 are scanned (all closed after the object's confirmation).
    """
    hi, lo, cl, atr, bar_num = arrays
    side = side0
    # "near" on the confirmation bar arms/disarms the first touch
    j0 = start - 1
    obj0 = p0 + slope * (bar_num[j0] - num0)
    near_prev = (lo[j0] <= obj0 + eps_k * atr[j0]) if side > 0 else (hi[j0] >= obj0 - eps_k * atr[j0])
    retest_until = -1
    line_broken = False
    for j in range(start, stop):
        a = atr[j]
        if not np.isfinite(a):
            continue
        obj = p0 + slope * (bar_num[j] - num0)
        eps = eps_k * a
        role = "support" if side > 0 else "resistance"
        if line_broken and j > retest_until:
            return                                 # line dead after its retest window
        if not line_broken and side * (obj - cl[j]) > brk_k * a:  # close beyond break_atr*ATR
            out.append((close_t[j], j, int(bar_num[j]), scale, obj_type, obj_id, "break",
                        role, obj, slope, cl[j], a))
            side = -side
            retest_until = j + retest_n
            near_prev = True                       # the break bar itself is not an approach
            line_broken = kill_on_break
            continue
        near = (lo[j] <= obj + eps) if side > 0 else (hi[j] >= obj - eps)
        if near and not near_prev:
            if j <= retest_until:
                out.append((close_t[j], j, int(bar_num[j]), scale, obj_type, obj_id, "retest",
                            role, obj, slope, cl[j], a))
                retest_until = -1
            elif not line_broken:
                out.append((close_t[j], j, int(bar_num[j]), scale, obj_type, obj_id, "touch",
                            role, obj, slope, cl[j], a))
        near_prev = near


def detect_events(
    bars       : pl.DataFrame,
    levels     : pl.DataFrame,
    trendlines : pl.DataFrame,
    scale      : str,
    params     : dict | None = None,
) -> pl.DataFrame:
    """Touch / break / retest events of levels and trendlines.

    Each object is scanned on the bars after its confirmation bar up to its
    expiry (levels) or break + retest window (lines). Definitions (ATR and
    eps = line/level tolerance * ATR at the event bar):

    * ``break``: close beyond the object by >= ``break_atr`` * ATR on the
      far side; the role flips (support <-> resistance). Levels survive breaks
      (role flip); lines die after the retest window.
    * ``retest``: within ``retest_bars`` after a break the bar extreme comes
      back within eps from the new side (at most once per break).
    * ``touch``: the bar extreme comes within eps from the approach side, and
      the previous bar did not (re-arm, so hovering is one touch).

    Returns:
        Columns: event_time (close of the event bar), bar_idx, bar_num, scale,
        obj_type ('level'/'line'), obj_id, event_type ('touch'/'break'/'retest'),
        side (object role at the event, before a break flips it:
        'support'/'resistance'), price (object price at the bar),
        obj_slope_per_bar (0 for levels), bar_close, atr.
        Sorted by (event_time, obj_type, obj_id).
    """
    p = load_params(params)
    eps_k = float(p["level_tolerance_atr"])
    eps_line = float(p["line_tolerance_atr"])
    brk_k = float(p["break_atr"])
    retest_n = int(p["retest_bars"])
    arrays = (bars["high"].to_numpy(), bars["low"].to_numpy(), bars["close"].to_numpy(),
              bars["atr"].fill_null(np.nan).to_numpy(), bars["bar_num"].to_numpy())
    cl = arrays[2]
    close_t = bars["close_time"].to_list()
    n = bars.height
    ct_np = bars["close_time"].to_numpy()
    out: list[tuple] = []

    def _stop(expired_at: datetime | None) -> int:
        if expired_at is None:
            return n
        return int(np.searchsorted(ct_np, np.datetime64(expired_at, "us"), side="right"))

    for r in levels.iter_rows(named=True):
        ci = r["confirm_idx"]
        if ci + 1 >= n:
            continue
        side0 = 1 if cl[ci] >= r["price"] else -1
        _scan_object(out, arrays, close_t, scale, "level", r["level_id"], ci + 1,
                     _stop(r["expired_at"]), r["price"], 0, 0.0, side0, eps_k, brk_k,
                     retest_n, kill_on_break=False)
    for r in trendlines.iter_rows(named=True):
        ci = r["confirm_idx"]
        if ci + 1 >= n:
            continue
        side0 = 1 if r["kind"] == "support" else -1
        exp = r["expired_at"]
        # a broken line is still scanned through its retest window
        extra = retest_n if r["expiry_reason"] == "break" else 0
        stop = n if exp is None else min(n, _stop(exp) + extra)
        _scan_object(out, arrays, close_t, scale, "line", r["line_id"], ci + 1, stop,
                     r["anchor1_price"], int(r["anchor1_bar_num"]), r["slope_per_bar"],
                     side0, eps_line, brk_k, retest_n, kill_on_break=True)
    if not out:
        return pl.DataFrame(schema=_EVENT_SCHEMA)
    return (pl.DataFrame(out, schema=_EVENT_SCHEMA, orient="row")
            .sort("event_time", "obj_type", "obj_id"))


# %% Forward labels (research only — never a feature)


_FWD_SCHEMA = {"forward_label": pl.Utf8, "forward_bars": pl.Int64,
               "forward_timeout": pl.Boolean, "forward_mfe_atr": pl.Float64,
               "forward_mae_atr": pl.Float64}


def forward_label_events(
    bars   : pl.DataFrame,
    events : pl.DataFrame,
    params : dict | None = None,
) -> pl.DataFrame:
    """Attach forward-looking outcome labels to events (USES THE FUTURE).

    Horizon H = ``forward_bars`` scale bars after the event bar, move threshold
    M = ``forward_move_atr`` * event ATR; object price follows its slope.
    Direction d = +1 when the expected reaction is upward (touch/retest of a
    support, break of a resistance), else -1.

    * touch  -> 'bounce' if the extreme moves >= M away from the object in d
      before a break (close beyond break_atr*ATR); else 'break'.
    * break  -> 'follow_through' if the extreme moves >= M beyond the break
      close in d before a close returns within eps of the object (or beyond
      it); else 'false_break'.
    * retest -> 'fail' if a close returns to the old side of the object; else
      'hold'.

    Unresolved within H -> the negative label with ``forward_timeout = True``;
    events with fewer than H bars of future and no resolution -> null label.

    Returns:
        ``events`` plus: forward_label (str), forward_bars (bars to resolution),
        forward_timeout (bool), forward_mfe_atr / forward_mae_atr (max
        favourable / adverse excursion over H in ATR, measured from the object
        price for touch/retest and from the break close for break).
    """
    p = load_params(params)
    H = int(p["forward_bars"])
    move_k = float(p["forward_move_atr"])
    brk_k = float(p["break_atr"])
    hi, lo, cl = bars["high"].to_numpy(), bars["low"].to_numpy(), bars["close"].to_numpy()
    atr_b = bars["atr"].fill_null(np.nan).to_numpy()
    num = bars["bar_num"].to_numpy()
    n = bars.height
    eps_map = {"level": float(p["level_tolerance_atr"]), "line": float(p["line_tolerance_atr"])}
    labels: list[tuple] = []
    for e in events.iter_rows(named=True):
        j, a, et = e["bar_idx"], e["atr"], e["event_type"]
        role_up = e["side"] == "support"
        d = (1.0 if role_up else -1.0) if et in ("touch", "retest") else (-1.0 if role_up else 1.0)
        ks = np.arange(j + 1, min(n, j + H + 1))
        if ks.size == 0:
            labels.append((None, None, None, None, None))
            continue
        obj = e["price"] + e["obj_slope_per_bar"] * (num[ks] - e["bar_num"])
        ref = e["bar_close"] if et == "break" else obj
        fav = (hi[ks] - ref) if d > 0 else (ref - lo[ks])
        adv = (ref - lo[ks]) if d > 0 else (hi[ks] - ref)
        hit = fav >= move_k * a
        if et == "touch":
            neg = d * (cl[ks] - obj) < -brk_k * atr_b[ks]           # broken through
            pos_lab, neg_lab = "bounce", "break"
        elif et == "break":
            neg = d * (cl[ks] - obj) <= eps_map[e["obj_type"]] * atr_b[ks]  # back within eps
            pos_lab, neg_lab = "follow_through", "false_break"
        else:  # retest: fail = close back on the old side
            neg = d * (cl[ks] - obj) < 0
            pos_lab, neg_lab = "hold", "fail"
        i_pos = int(np.argmax(hit)) if hit.any() else _NEVER
        i_neg = int(np.argmax(neg)) if neg.any() else _NEVER
        if et == "retest":
            # hold = no fail within H (no positive trigger needed)
            if i_neg != _NEVER:
                lab, nb, to = neg_lab, i_neg + 1, False
            elif ks.size < H:
                lab, nb, to = None, None, None
            else:
                lab, nb, to = pos_lab, H, False
        elif i_pos < i_neg:
            lab, nb, to = pos_lab, i_pos + 1, False
        elif i_neg != _NEVER:
            lab, nb, to = neg_lab, i_neg + 1, False
        elif ks.size < H:
            lab, nb, to = None, None, None
        else:
            lab, nb, to = neg_lab, H, True
        labels.append((lab, nb, to, float(fav.max() / a), float(adv.max() / a)))
    fwd = pl.DataFrame(labels, schema=_FWD_SCHEMA, orient="row")
    return pl.concat([events, fwd], how="horizontal")


# %% Orchestration


def _attach_event_state(res: ScaleResult, params: dict) -> ScaleResult:
    """Fill levels' role / n_role_flips / broken_at / strength and lines' broken_at."""
    _, _, lookback = _scale_cfg(params, res.scale)
    tau = lookback * float(params["strength_tau_frac"])
    data_end = res.bars["close_time"][-1] if res.bars.height else None
    brk = res.events.filter(pl.col("event_type") == "break")
    lv_b = brk.filter(pl.col("obj_type") == "level").group_by("obj_id").agg(
        pl.len().cast(pl.Int64).alias("_flips"), pl.col("event_time").min().alias("_broken"))
    levels = res.levels
    if levels.height:
        end = pl.coalesce(pl.col("expired_at"), pl.lit(data_end))
        state = _levels_state(res, end_expr_df=levels.select("level_id", end.alias("_end")),
                              tau=tau)
        levels = (levels.drop("n_role_flips", "broken_at", "role", "strength")
                  .join(lv_b, left_on="level_id", right_on="obj_id", how="left")
                  .join(state, on="level_id", how="left")
                  .with_columns(pl.col("_flips").fill_null(0).alias("n_role_flips"),
                                pl.col("_broken").alias("broken_at"))
                  .select(list(_LEVEL_SCHEMA)))
    ln_b = brk.filter(pl.col("obj_type") == "line").group_by("obj_id").agg(
        pl.col("event_time").min().alias("_broken"))
    lines = res.trendlines
    if lines.height:
        lines = (lines.drop("broken_at")
                 .join(ln_b, left_on="line_id", right_on="obj_id", how="left")
                 .rename({"_broken": "broken_at"}).select(list(_LINE_SCHEMA)))
    res.levels, res.trendlines = levels, lines
    return res


def _levels_state(res: ScaleResult, end_expr_df: pl.DataFrame, tau: float) -> pl.DataFrame:
    """strength and role of each level evaluated at its own '_end' time."""
    bars = res.bars.select("close_time", "close").sort("close_time")
    t = (res.level_touches.join(end_expr_df, left_on="obj_id", right_on="level_id")
         .filter(pl.col("confirmed_at") <= pl.col("_end"))
         .with_columns(((pl.col("_end") - pl.col("pivot_time")).dt.total_seconds() / tau)
                       .alias("_age"))
         .group_by("obj_id").agg((-pl.col("_age")).exp().sum().alias("strength"))
         .rename({"obj_id": "level_id"}))
    px = (end_expr_df.sort("_end")
          .join_asof(bars, left_on="_end", right_on="close_time", strategy="backward")
          .join(res.levels.select("level_id", "price"), on="level_id")
          .select("level_id", pl.when(pl.col("close") >= pl.col("price"))
                  .then(pl.lit("support")).otherwise(pl.lit("resistance")).alias("role")))
    return px.join(t, on="level_id", how="left")


def detect_scale(df_1m: pl.DataFrame, scale: str, params: dict | None = None) -> ScaleResult:
    """Run the full detector on one scale.

    Args:
        df_1m  : 1m OHLCV (open_time, open, high, low, close, volume).
        scale  : Key of ``params['scales']`` (default config: 's', 'm', 'l').
        params : Detector params (default: ``config/features.json`` levels_detector).

    Returns:
        ScaleResult with ``bars`` (resample_ohlcv), ``pivots`` (detect_pivots),
        ``levels`` + ``level_touches`` (detect_levels, with event-derived
        role / n_role_flips / broken_at / strength filled), ``trendlines`` +
        ``line_touches`` (detect_trendlines, broken_at filled) and ``events``
        (detect_events). See those functions for column schemas. Forward
        labels are NOT included: call ``forward_label_events`` explicitly.
    """
    p = load_params(params)
    bar, _, _ = _scale_cfg(p, scale)
    bars = resample_ohlcv(df_1m, bar, int(p["atr_window"]))
    pivots = detect_pivots(bars, scale, p)
    levels, level_touches = detect_levels(bars, pivots, scale, p)
    lines, line_touches = detect_trendlines(bars, pivots, scale, p)
    events = detect_events(bars, levels, lines, scale, p)
    res = ScaleResult(scale, bars, pivots, levels, level_touches, lines, line_touches, events)
    return _attach_event_state(res, p)


def detect_all_scales(
    df_1m  : pl.DataFrame,
    scales : list[str] | None = None,
    params : dict | None = None,
) -> dict[str, ScaleResult]:
    """Convenience entry point: ``{scale: ScaleResult}`` for every configured scale.

    Args:
        df_1m  : 1m OHLCV DataFrame (open_time, open, high, low, close, volume).
        scales : Subset of scale keys; default all of ``params['scales']``.
        params : Detector params (default: config).
    """
    p = load_params(params)
    return {s: detect_scale(df_1m, s, p) for s in (scales or list(p["scales"]))}


# %% As-of snapshots (state known at time T)


def levels_as_of(res: ScaleResult, as_of: datetime, active_only: bool = True,
                 params: dict | None = None) -> pl.DataFrame:
    """Level state as known at ``as_of`` (only objects with confirmed_at <= as_of).

    Returns columns: level_id, scale, price, tolerance, confirmed_at,
    is_round, is_round_secondary, n_touches, first_touch, last_touch,
    strength, role (vs the last close <= as_of), n_role_flips, broken_at,
    expired_at (null unless <= as_of), is_active.
    """
    p = load_params(params)
    _, _, lookback = _scale_cfg(p, res.scale)
    tau = lookback * float(p["strength_tau_frac"])
    T = pl.lit(as_of).cast(pl.Datetime("us"))
    lv = res.levels.filter(pl.col("confirmed_at") <= T)
    t = (res.level_touches.filter(pl.col("confirmed_at") <= T)
         .with_columns(((T - pl.col("pivot_time")).dt.total_seconds() / tau).alias("_age"))
         .group_by("obj_id").agg(
             pl.len().cast(pl.Int64).alias("n_touches"),
             pl.col("pivot_time").min().alias("first_touch"),
             pl.col("pivot_time").max().alias("last_touch"),
             (-pl.col("_age")).exp().sum().alias("strength")))
    b = (res.events.filter((pl.col("event_time") <= T) & (pl.col("obj_type") == "level")
                           & (pl.col("event_type") == "break"))
         .group_by("obj_id").agg(pl.len().cast(pl.Int64).alias("n_role_flips"),
                                 pl.col("event_time").min().alias("broken_at")))
    last = res.bars.filter(pl.col("close_time") <= T)
    px = float(last["close"][-1]) if last.height else math.nan
    out = (lv.select("level_id", "scale", "price", "tolerance", "confirmed_at", "is_round",
                     "is_round_secondary", "expired_at")
           .join(t, left_on="level_id", right_on="obj_id", how="left")
           .join(b, left_on="level_id", right_on="obj_id", how="left")
           .with_columns(
               pl.col("n_role_flips").fill_null(0),
               pl.when(pl.col("expired_at") <= T).then(pl.col("expired_at"))
               .otherwise(None).alias("expired_at"),
               pl.when(pl.lit(px) >= pl.col("price")).then(pl.lit("support"))
               .otherwise(pl.lit("resistance")).alias("role"))
           .with_columns(pl.col("expired_at").is_null().alias("is_active"))
           .select("level_id", "scale", "price", "tolerance", "confirmed_at", "is_round",
                   "is_round_secondary", "n_touches", "first_touch", "last_touch", "strength",
                   "role", "n_role_flips", "broken_at", "expired_at", "is_active")
           .sort("level_id"))
    return out.filter(pl.col("is_active")) if active_only else out


def trendlines_as_of(res: ScaleResult, as_of: datetime, active_only: bool = True) -> pl.DataFrame:
    """Trendline state as known at ``as_of`` (only confirmed_at <= as_of).

    Returns the ``trendlines`` definition columns with n_touches / last_touch
    recomputed from touches confirmed <= as_of, broken_at / expired_at nulled
    when later than as_of, is_active, and price_at (line price on the last
    bar closed <= as_of).
    """
    T = pl.lit(as_of).cast(pl.Datetime("us"))
    t = (res.line_touches.filter(pl.col("confirmed_at") <= T).group_by("obj_id")
         .agg(pl.len().cast(pl.Int64).alias("_n"), pl.col("pivot_time").max().alias("_last")))
    last = res.bars.filter(pl.col("close_time") <= T)
    num = int(last["bar_num"][-1]) if last.height else 0
    out = (res.trendlines.filter(pl.col("confirmed_at") <= T)
           .join(t, left_on="line_id", right_on="obj_id", how="left")
           .with_columns(
               pl.col("_n").alias("n_touches"), pl.col("_last").alias("last_touch"),
               pl.when(pl.col("broken_at") <= T).then(pl.col("broken_at")).otherwise(None)
               .alias("broken_at"),
               pl.when(pl.col("expired_at") <= T).then(pl.col("expired_at")).otherwise(None)
               .alias("expired_at"),
               pl.when(pl.col("expired_at") <= T).then(pl.col("expiry_reason")).otherwise(None)
               .alias("expiry_reason"),
               (pl.col("anchor1_price") + pl.col("slope_per_bar")
                * (num - pl.col("anchor1_bar_num"))).alias("price_at"))
           .drop("_n", "_last")
           .with_columns(pl.col("expired_at").is_null().alias("is_active"))
           .sort("line_id"))
    return out.filter(pl.col("is_active")) if active_only else out
