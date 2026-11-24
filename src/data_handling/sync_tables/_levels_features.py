"""Bar-level (1m) S/R level + trendline features (epic_071 / t912).

Pure library: 1m OHLCV + detector output in, one feature row per 1m bar out.
No DB access. The DB sync lives in ``sync_levels.py``.

Look-ahead rule (t-1 lag)
-------------------------
The row at ``open_time = t`` describes the detector state known at wall-clock
time ``T = t`` -- i.e. after the close of the 1m bar ``t-1`` and before bar
``t`` -- exactly what ``levels_as_of(res, T)`` / ``trendlines_as_of(res, T)``
return, combined with the reference price ``close[t-1]`` (previous row). The
end-of-life columns of the detector's ``levels`` / ``trendlines`` frames
(n_touches, strength, role, n_role_flips, broken_at, expired_at as a value)
are never read directly; every state is rebuilt from time-stamped change
points (touch ``confirmed_at``, break / retest ``event_time``, expiry):

* an object is active on rows with ``confirmed_at <= T < expired_at``
  (the same rule as ``*_as_of(active_only=True)``; an expiry > T is only
  used as "still alive at T", which a truncated run also concludes);
* touch count / strength / role-flip count are step functions over the
  touches / breaks with ``confirmed_at`` / ``event_time <= T``;
* the scale ATR and the trendline price come from the last scale bar closed
  ``<= T`` (as in ``trendlines_as_of``).

Computation is vectorised: active objects are packed into a few "slots"
(interval colouring, slots <= max concurrently active objects, ~20), rows are
processed in chunks as (rows x slots) matrices, and the per-object state is
looked up with composite-key ``searchsorted``. No per-minute detector call.

NULL semantics: a feature is NULL when its object does not exist at T (no
active level above / below the price, no active line of that kind, no break
within the lookback), when the scale ATR is not yet known, and on the first
row (no previous close). No value is invented. Feature definitions:
``FEATURE_DOCS`` and ``_doc_/database_and_code_doc`` / the t912 ticket.
"""
from __future__ import annotations

import heapq
import logging
from collections.abc import Sequence

import numpy as np
import polars as pl

from data_handling.sync_tables import _levels_polars as lv

logger = logging.getLogger(__name__)

# %% Constants

#: Scales that get horizontal-level features (all detector scales).
LEVEL_SCALES: tuple[str, ...] = ("s", "m", "l")
#: Scales with a round-number flag (the 10-step is meaningless at 1d ATR size).
ROUND_SCALES: tuple[str, ...] = ("s", "m")
#: Scales with trendline features (the l scale has ~140 lines in 6 years).
LINE_SCALES: tuple[str, ...] = ("s", "m")

_US_PER_MIN = 60_000_000
_KEY_BASE   = 100_000_000          # composite key: obj_idx * base + minute (minutes < 1e8)
_CHUNK_ROWS = 262_144


def feature_columns(
    level_scales : Sequence[str] = LEVEL_SCALES,
    round_scales : Sequence[str] = ROUND_SCALES,
    line_scales  : Sequence[str] = LINE_SCALES,
) -> list[str]:
    """Ordered ``feat_*`` column list of the feat_levels table (v1 = defaults)."""
    cols: list[str] = []
    for x in level_scales:
        cols += [f"feat_sr_dist_res_atr_{x}", f"feat_sr_dist_sup_atr_{x}",
                 f"feat_sr_strength_res_{x}", f"feat_sr_strength_sup_{x}",
                 f"feat_sr_touches_res_{x}", f"feat_sr_touches_sup_{x}",
                 f"feat_sr_flips_res_{x}", f"feat_sr_flips_sup_{x}",
                 f"feat_sr_n_active_{x}"]
        if x in round_scales:
            cols += [f"feat_sr_is_round_res_{x}", f"feat_sr_is_round_sup_{x}"]
        cols += [f"feat_sr_break_age_{x}", f"feat_sr_break_dir_{x}",
                 f"feat_sr_retest_flag_{x}"]
    for x in line_scales:
        cols += [f"feat_tl_dist_sup_atr_{x}", f"feat_tl_dist_res_atr_{x}",
                 f"feat_tl_break_age_{x}", f"feat_tl_break_dir_{x}",
                 f"feat_tl_retest_flag_{x}"]
    return cols


#: One-line definitions ({x} = scale). Mirrored in the t912 ticket Notes.
FEATURE_DOCS: dict[str, str] = {
    "feat_sr_dist_res_atr_{x}": "(price of nearest active level ABOVE ref close - ref) / ATR_x; >0",
    "feat_sr_dist_sup_atr_{x}": "(ref - price of nearest active level AT/BELOW ref close) / ATR_x; >=0",
    "feat_sr_strength_{res,sup}_{x}": "strength(T) = sum exp(-(T-pivot_time)/tau) over touches confirmed <= T",
    "feat_sr_touches_{res,sup}_{x}": "touch count of that level confirmed <= T",
    "feat_sr_flips_{res,sup}_{x}": "role flips of that level so far = its break events <= T",
    "feat_sr_n_active_{x}": "number of active levels at T (0 = none; never NULL after row 0)",
    "feat_sr_is_round_{res,sup}_{x}": "1 if that level is within its tolerance of a multiple of 10, else 0",
    "feat_{sr,tl}_break_age_{x}": "scale bars since the latest break (level / line) <= T; NULL if > lookback",
    "feat_{sr,tl}_break_dir_{x}": "+1 resistance broken upward, -1 support broken downward (same break)",
    "feat_{sr,tl}_retest_flag_{x}": "1 if that broken object had a retest event in (break, T], else 0",
    "feat_tl_dist_sup_atr_{x}": "(ref - nearest active support line price) / ATR_x; signed",
    "feat_tl_dist_res_atr_{x}": "(nearest active resistance line price - ref) / ATR_x; signed",
}


# %% Helpers


def _to_min(s: pl.Series) -> np.ndarray:
    """Datetime series -> int64 minutes since epoch (null -> int64 max)."""
    us = s.cast(pl.Datetime("us")).dt.epoch("us")
    return (us // _US_PER_MIN).fill_null(np.iinfo(np.int64).max).to_numpy().astype(np.int64)


def _assign_slots(i0: np.ndarray, i1: np.ndarray) -> tuple[np.ndarray, int]:
    """Greedy interval colouring of [i0, i1) row intervals; returns (slot, n_slots).

    Processed in (i0, index) order; a slot is reused only when its previous
    interval ended at or before the new start -- a decision that depends only
    on intervals that started earlier, so it is truncation-invariant.
    """
    order = np.lexsort((np.arange(i0.size), i0))
    slot = np.full(i0.size, -1, dtype=np.int64)
    busy: list[tuple[int, int]] = []     # (end, slot)
    free: list[int] = []
    n_slots = 0
    for k in order:
        if i1[k] <= i0[k]:
            continue
        while busy and busy[0][0] <= i0[k]:
            heapq.heappush(free, heapq.heappop(busy)[1])
        if free:
            s = heapq.heappop(free)
        else:
            s = n_slots
            n_slots += 1
        slot[k] = s
        heapq.heappush(busy, (int(i1[k]), s))
    return slot, n_slots


def _step_lookup(
    keys_obj : np.ndarray,
    keys_t   : np.ndarray,
    obj      : np.ndarray,
    t        : np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """For each (obj, t): index of the last change point of obj with time <= t.

    ``keys_obj`` / ``keys_t`` must be sorted by (obj, t). Returns (pos, count):
    pos = row index into the keys (-1 if none), count = number of change
    points of obj with time <= t.
    """
    keys = keys_obj * _KEY_BASE + keys_t
    q = np.where(obj >= 0, obj, 0) * _KEY_BASE
    hi = np.searchsorted(keys, q + t, side="right")
    lo = np.searchsorted(keys, q, side="left")
    cnt = np.where(obj >= 0, hi - lo, 0)
    pos = np.where(cnt > 0, hi - 1, -1)
    return pos, cnt


def _rows_of(t_rows: np.ndarray, start_min: np.ndarray) -> np.ndarray:
    """First row index with T >= start (row times sorted)."""
    return np.searchsorted(t_rows, start_min, side="left")


def _scale_context(res: lv.ScaleResult, t: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """ATR and bar_num of the last scale bar closed <= T (NaN / -1 if none)."""
    ct = _to_min(res.bars["close_time"])
    idx = np.searchsorted(ct, t, side="right") - 1
    atr_b = res.bars["atr"].fill_null(np.nan).to_numpy().astype(np.float64)
    num_b = res.bars["bar_num"].to_numpy().astype(np.int64)
    ok = idx >= 0
    atr = np.where(ok, atr_b[np.clip(idx, 0, None)], np.nan) if atr_b.size else np.full(t.size, np.nan)
    num = np.where(ok, num_b[np.clip(idx, 0, None)], -1) if num_b.size else np.full(t.size, -1)
    return atr, num


# %% Level features


def _level_features(
    res       : lv.ScaleResult,
    t         : np.ndarray,
    ref       : np.ndarray,
    atr       : np.ndarray,
    tau_min   : float,
    with_round: bool,
) -> dict[str, np.ndarray]:
    """Nearest support / resistance level state per row (see module docstring)."""
    x = res.scale
    n = t.size
    levels = res.levels
    out_names = [f"feat_sr_dist_res_atr_{x}", f"feat_sr_dist_sup_atr_{x}",
                 f"feat_sr_strength_res_{x}", f"feat_sr_strength_sup_{x}",
                 f"feat_sr_touches_res_{x}", f"feat_sr_touches_sup_{x}",
                 f"feat_sr_flips_res_{x}", f"feat_sr_flips_sup_{x}"]
    out = {c: np.full(n, np.nan) for c in out_names}
    out[f"feat_sr_n_active_{x}"] = np.where(np.isfinite(ref), 0.0, np.nan)
    if with_round:
        out[f"feat_sr_is_round_res_{x}"] = np.full(n, np.nan)
        out[f"feat_sr_is_round_sup_{x}"] = np.full(n, np.nan)
    if levels.height == 0 or n == 0:
        return out

    # --- object table: definition (fixed at confirmation) + activity interval ---
    ids = levels["level_id"].to_list()
    id_idx = {k: i for i, k in enumerate(ids)}
    price = levels["price"].to_numpy().astype(np.float64)
    is_round = levels["is_round"].fill_null(False).to_numpy().astype(np.float64)
    i0 = _rows_of(t, _to_min(levels["confirmed_at"]))
    i1 = _rows_of(t, _to_min(levels["expired_at"]))        # null -> alive at data end
    slot, n_slots = _assign_slots(i0, i1)

    # --- touch change points: count + strength step value K ---
    tt = (res.level_touches.with_columns(
            pl.col("obj_id").replace_strict(id_idx, return_dtype=pl.Int64).alias("_o"),
            pl.col("confirmed_at").cast(pl.Datetime("us")).dt.epoch("us").alias("_c"),
            pl.col("pivot_time").cast(pl.Datetime("us")).dt.epoch("us").alias("_p"))
          .sort("_o", "_c", "_p"))
    t_obj = tt["_o"].to_numpy().astype(np.int64)
    t_c = (tt["_c"].to_numpy() // _US_PER_MIN).astype(np.int64)
    t_p = tt["_p"].to_numpy().astype(np.float64) / _US_PER_MIN
    k_val = np.empty(t_obj.size)
    for j in range(t_obj.size):                   # ~10k touches: cheap scalar recurrence
        own = np.exp(-(t_c[j] - t_p[j]) / tau_min)
        if j and t_obj[j] == t_obj[j - 1]:
            k_val[j] = k_val[j - 1] * np.exp(-(t_c[j] - t_c[j - 1]) / tau_min) + own
        else:
            k_val[j] = own

    # --- break change points of levels (role flips) ---
    br = (res.events.filter((pl.col("obj_type") == "level") & (pl.col("event_type") == "break"))
          .with_columns(pl.col("obj_id").replace_strict(id_idx, return_dtype=pl.Int64).alias("_o"))
          .sort("_o", "event_time"))
    b_obj = br["_o"].to_numpy().astype(np.int64)
    b_t = _to_min(br["event_time"])

    for c0 in range(0, n, _CHUNK_ROWS):
        c1 = min(n, c0 + _CHUNK_ROWS)
        m = c1 - c0
        P = np.full((m, max(n_slots, 1)), np.nan)
        objm = np.full((m, max(n_slots, 1)), -1, dtype=np.int64)
        for k in np.flatnonzero((i0 < c1) & (i1 > c0) & (slot >= 0)):
            a, b = max(i0[k], c0) - c0, min(i1[k], c1) - c0
            P[a:b, slot[k]] = price[k]
            objm[a:b, slot[k]] = k
        r = ref[c0:c1, None]
        valid = np.isfinite(P) & np.isfinite(r)
        out[f"feat_sr_n_active_{x}"][c0:c1] = np.where(
            np.isfinite(ref[c0:c1]), valid.sum(axis=1).astype(np.float64), np.nan)
        tc = t[c0:c1]
        a_c = atr[c0:c1]
        for side in ("res", "sup"):
            if side == "res":
                d = np.where(valid & (r < P), P - r, np.inf)
            else:
                d = np.where(valid & (r >= P), r - P, np.inf)
            j = np.argmin(d, axis=1)
            dmin = d[np.arange(m), j]
            has = np.isfinite(dmin)
            obj = np.where(has, objm[np.arange(m), j], -1)
            out[f"feat_sr_dist_{side}_atr_{x}"][c0:c1] = np.where(has, dmin / a_c, np.nan)
            pos, cnt = _step_lookup(t_obj, t_c, obj, tc)
            okp = has & (pos >= 0)
            pp = np.clip(pos, 0, None)
            strength = k_val[pp] * np.exp(-(tc - t_c[pp]) / tau_min)
            out[f"feat_sr_strength_{side}_{x}"][c0:c1] = np.where(okp, strength, np.nan)
            out[f"feat_sr_touches_{side}_{x}"][c0:c1] = np.where(okp, cnt, np.nan)
            _, bcnt = _step_lookup(b_obj, b_t, obj, tc)
            out[f"feat_sr_flips_{side}_{x}"][c0:c1] = np.where(has, bcnt, np.nan)
            if with_round:
                out[f"feat_sr_is_round_{side}_{x}"][c0:c1] = np.where(
                    has, is_round[np.clip(obj, 0, None)], np.nan)
    return out


# %% Trendline features


def _line_features(
    res : lv.ScaleResult,
    t   : np.ndarray,
    ref : np.ndarray,
    atr : np.ndarray,
    num : np.ndarray,
) -> dict[str, np.ndarray]:
    """Signed ATR distance to the nearest active support / resistance line."""
    x = res.scale
    n = t.size
    out = {f"feat_tl_dist_sup_atr_{x}": np.full(n, np.nan),
           f"feat_tl_dist_res_atr_{x}": np.full(n, np.nan)}
    lines = res.trendlines
    if lines.height == 0 or n == 0:
        return out
    for kind, side in (("support", "sup"), ("resistance", "res")):
        sub = lines.filter(pl.col("kind") == kind)
        if sub.height == 0:
            continue
        a1p = sub["anchor1_price"].to_numpy().astype(np.float64)
        a1n = sub["anchor1_bar_num"].to_numpy().astype(np.float64)
        slope = sub["slope_per_bar"].to_numpy().astype(np.float64)
        i0 = _rows_of(t, _to_min(sub["confirmed_at"]))
        i1 = _rows_of(t, _to_min(sub["expired_at"]))
        slot, n_slots = _assign_slots(i0, i1)
        col = out[f"feat_tl_dist_{side}_atr_{x}"]
        for c0 in range(0, n, _CHUNK_ROWS):
            c1 = min(n, c0 + _CHUNK_ROWS)
            m = c1 - c0
            objm = np.full((m, max(n_slots, 1)), -1, dtype=np.int64)
            for k in np.flatnonzero((i0 < c1) & (i1 > c0) & (slot >= 0)):
                objm[max(i0[k], c0) - c0:min(i1[k], c1) - c0, slot[k]] = k
            oc = np.clip(objm, 0, None)
            px = a1p[oc] + slope[oc] * (num[c0:c1, None].astype(np.float64) - a1n[oc])
            r = ref[c0:c1, None]
            signed = (r - px) if side == "sup" else (px - r)
            d = np.where((objm >= 0) & np.isfinite(r), np.abs(signed), np.inf)
            j = np.argmin(d, axis=1)
            has = np.isfinite(d[np.arange(m), j])
            col[c0:c1] = np.where(has, signed[np.arange(m), j] / atr[c0:c1], np.nan)
    return out


# %% Break / retest features


def _break_features(
    res          : lv.ScaleResult,
    obj_type     : str,
    prefix       : str,
    t            : np.ndarray,
    ref          : np.ndarray,
    bar_min      : int,
    lookback_min : int,
) -> dict[str, np.ndarray]:
    """Age / direction / retest flag of the latest break (event_time <= T)."""
    x = res.scale
    n = t.size
    age = np.full(n, np.nan)
    dirn = np.full(n, np.nan)
    flag = np.full(n, np.nan)
    ev = res.events.filter(pl.col("obj_type") == obj_type)
    brk = ev.filter(pl.col("event_type") == "break").sort("event_time", "obj_id")
    if brk.height and n:
        bt = _to_min(brk["event_time"])
        bdir = np.where(brk["side"].to_numpy() == "resistance", 1.0, -1.0)
        # first retest of the same object after each break
        ids = sorted(set(ev["obj_id"].to_list()))
        id_idx = {k: i for i, k in enumerate(ids)}
        rt = (ev.filter(pl.col("event_type") == "retest")
              .with_columns(pl.col("obj_id").replace_strict(id_idx, return_dtype=pl.Int64)
                            .alias("_o"))
              .sort("_o", "event_time"))
        r_obj = rt["_o"].to_numpy().astype(np.int64)
        r_t = _to_min(rt["event_time"])
        b_obj = np.array([id_idx[k] for k in brk["obj_id"].to_list()], dtype=np.int64)
        never = np.iinfo(np.int64).max
        first_rt = np.full(bt.size, never, dtype=np.int64)
        if r_obj.size:
            keys = r_obj * _KEY_BASE + r_t
            nxt = np.searchsorted(keys, b_obj * _KEY_BASE + bt, side="right")
            nc = np.clip(nxt, 0, keys.size - 1)
            has_rt = (nxt < keys.size) & (r_obj[nc] == b_obj)
            first_rt = np.where(has_rt, r_t[nc], never)
        idx = np.searchsorted(bt, t, side="right") - 1
        ok = (idx >= 0) & np.isfinite(ref)
        ic = np.clip(idx, 0, None)
        dt = t - bt[ic]
        ok &= dt <= lookback_min
        age = np.where(ok, dt / bar_min, np.nan)
        dirn = np.where(ok, bdir[ic], np.nan)
        flag = np.where(ok, (first_rt[ic] <= t).astype(np.float64), np.nan)
    return {f"feat_{prefix}_break_age_{x}": age, f"feat_{prefix}_break_dir_{x}": dirn,
            f"feat_{prefix}_retest_flag_{x}": flag}


# %% Public API


def level_features_from_results(
    df_1m   : pl.DataFrame,
    results : dict[str, lv.ScaleResult],
    params  : dict | None = None,
    start   : str | None = None,
    end     : str | None = None,
) -> pl.DataFrame:
    """Bar-level feature frame from precomputed detector results.

    Args:
        df_1m   : The 1m OHLCV the detector ran on (open_time, close, ...).
        results : ``detect_all_scales`` output (any subset of scales).
        params  : Detector params (default: config ``levels_detector``).
        start   : Optional inclusive lower bound of the OUTPUT rows (YYYY-MM-DD HH:MM:SS).
        end     : Optional inclusive upper bound of the output rows.

    Returns:
        open_time (Datetime us), available_ts (= open_time) and the
        ``feature_columns`` of the scales present in ``results`` (Float64).
    """
    p = lv.load_params(params)
    df = df_1m.select("open_time", "close")
    if df.schema["open_time"] == pl.Utf8:
        df = df.with_columns(pl.col("open_time").str.to_datetime("%Y-%m-%d %H:%M:%S"))
    df = df.with_columns(pl.col("open_time").cast(pl.Datetime("us"))).sort("open_time")
    # t-1 lag: the reference price of row t is the previous row's close
    df = df.with_columns(pl.col("close").cast(pl.Float64).shift(1).alias("_ref"))
    if start is not None:
        df = df.filter(pl.col("open_time") >= pl.lit(start).str.to_datetime("%Y-%m-%d %H:%M:%S"))
    if end is not None:
        df = df.filter(pl.col("open_time") <= pl.lit(end).str.to_datetime("%Y-%m-%d %H:%M:%S"))
    t = _to_min(df["open_time"])
    ref = df["_ref"].fill_null(np.nan).to_numpy().astype(np.float64)

    present = [s for s in p["scales"] if s in results]
    cols: dict[str, np.ndarray] = {}
    for x in present:
        res = results[x]
        _, bar_secs, lookback = lv._scale_cfg(p, x)
        tau_min = lookback * float(p["strength_tau_frac"]) / 60.0
        atr, num = _scale_context(res, t)
        if x in LEVEL_SCALES:
            cols.update(_level_features(res, t, ref, atr, tau_min, x in ROUND_SCALES))
            cols.update(_break_features(res, "level", "sr", t, ref, bar_secs // 60,
                                        lookback // 60))
        if x in LINE_SCALES:
            cols.update(_line_features(res, t, ref, atr, num))
            cols.update(_break_features(res, "line", "tl", t, ref, bar_secs // 60,
                                        lookback // 60))
    order = feature_columns([s for s in LEVEL_SCALES if s in present],
                            [s for s in ROUND_SCALES if s in present],
                            [s for s in LINE_SCALES if s in present])
    feats = pl.DataFrame({c: pl.Series(c, cols[c], dtype=pl.Float64).fill_nan(None)
                          for c in order})
    return pl.concat([df.select("open_time", pl.col("open_time").alias("available_ts")), feats],
                     how="horizontal")


def compute_level_features(
    df_1m  : pl.DataFrame,
    params : dict | None = None,
    scales : list[str] | None = None,
    start  : str | None = None,
    end    : str | None = None,
) -> pl.DataFrame:
    """Run the detector on ``df_1m`` and return the bar-level feature frame.

    The detector needs the FULL history up to the last row (its state is
    path-dependent); ``start`` / ``end`` only restrict the output rows.

    Args:
        df_1m  : 1m OHLCV (open_time, open, high, low, close, volume), full history.
        params : Detector params (default: config ``levels_detector``, frozen v1).
        scales : Scale subset (default: all configured scales).
        start  : Optional inclusive lower bound of the output rows.
        end    : Optional inclusive upper bound of the output rows.

    Returns:
        See ``level_features_from_results``.
    """
    p = lv.load_params(params)
    results = lv.detect_all_scales(df_1m, scales=scales, params=p)
    return level_features_from_results(df_1m, results, p, start=start, end=end)
