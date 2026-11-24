"""Impulse Scout multi-asset event dataset (epic_073 / t930) — definitions of ``_doc_/methodology_doc/5300_impulse_scout.md``.

Pure library (no report). The event definition, first-touch label, non-overlap rule, splits and CV blocks are *exactly*
those of :mod:`reports.builders.wave_events` v2 (5200 v2), applied **per asset** (own sigma / ATR / greedy filter) on the
asset's own 1m series; the pooled table is the union (two assets never filter each other). The CV block boundaries come
from the parameters only (calendar), so they are shared by every asset.

Additions over 5200 v2:

* ``asset_id``, ``event_id`` (``asset|cell|t0``) and ``flow_key`` (``asset|t0``; key of the order-flow companion table);
* first-touch time ``tau_min`` (minutes after ``t_c`` of the bar that first touches a level; sideways censored at ``H``,
  the 5300 / ``scout_bench.frame`` convention) and ``first_touch_min`` (the same, NULL for sideways);
* **holdout-2 hygiene**: every label-dependent column of an event whose label window reaches into holdout-2
  (``label_end > holdout2_start``) is NULL (``label_masked``); such rows exist in the table only as flagged,
  label-free rows (counting is allowed by 5300, labels are not);
* A-threshold sensitivity variants (``a15`` = primary 1.5 sigma_W, ``a125``, ``a10``), each its own table with the same
  schema (the greedy non-overlap makes the event sets differ, so a column would not be enough);
* order-flow / microstructure features of the prefix ``[t0 - B, t_c]`` (:func:`build_flow`), stored in the companion
  table ``research."scout_flow_v1"``; only bars closed at or before ``t_c`` and trailing (past) baselines are used.

Lab DB tables (SOL lab DB, like ``wave_events_v2``): ``scout_events_v1`` (+ ``__funnel``, ``__meta``),
``scout_events_v1_a125``, ``scout_events_v1_a10``, ``scout_flow_v1``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import polars as pl

from reports.builders import wave_events as we

logger = logging.getLogger(__name__)

VERSION = "v1"
SCHEMA = "research"
GENERATOR = "reports.builders.scout_events"
ASSETS: tuple[str, ...] = ("solusdt", "btcusdt", "ethusdt")
PRIMARY_CELL = "W4h_H8h"
#: variant id -> A >= x * sigma_W (``a15`` is the 5300 primary definition)
VARIANTS: dict[str, float] = {"a15": 1.5, "a125": 1.25, "a10": 1.0}
PRIMARY_VARIANT = "a15"
#: lab DB that holds the tables (the SOL lab DB, next to ``wave_events_v2``)
LAB_ASSET = "solusdt"

FLOW_COLS: tuple[str, ...] = (
    "tb_ratio_prefix", "tb_ratio_decline", "tb_ratio_rebound", "tb_ratio_diff", "tb_ratio_slope_1h",
    "tb_ratio_last1h", "tb_ratio_low15", "delta_decline_norm", "delta_rebound_norm",
    "trades_rel_prefix", "trades_rel_decline", "trades_rel_rebound", "trade_size_rel",
    "vol_decline_rel", "vol_rebound_rel", "vol_rebound_over_decline", "vol_last1h_rel", "vol_peak_rel", "vol_peak_pos",
    "qv_logz_prefix",
)
#: label-dependent columns (NULL for rows whose label window reaches holdout-2)
MASKED_COLS: tuple[str, ...] = (*we.LABEL_COLS_V2, "tau_min", "first_touch_min")
MASKED_KEEP = ("p_star",)          # P* = close[t_c] * exp(A): known at t_c, not a label
FLOW_RAW = ("quote_volume", "trades", "taker_buy_base", "taker_buy_quote")


def table_name(variant: str = PRIMARY_VARIANT) -> str:
    return f"scout_events_{VERSION}" if variant == PRIMARY_VARIANT else f"scout_events_{VERSION}_{variant}"


# --------------------------------------------------------------------------- data


def load_ohlcv_flow(asset_id: str, *, tries: int = 10, delay: float = 3.0) -> pl.DataFrame:
    """Full 1m OHLCV + trades / taker-buy columns of an asset from its live DB (strictly read-only, retries on lock)."""
    from data_handling.store.readonly_query import open_readonly_connection

    cols = "open_time, open, high, low, close, volume, quote_volume, trades, taker_buy_base, taker_buy_quote"
    sql = f"SELECT {cols} FROM live.ohlcv ORDER BY open_time"
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
            return con.execute(sql).pl()
        finally:
            con.close()
    raise RuntimeError(f"live db unavailable for {asset_id}: {last}")


# --------------------------------------------------------------------------- order-flow features


@dataclass
class FlowGrid:
    """Raw flow arrays on the complete 1m grid of :class:`wave_events.MinuteData` plus causal rolling baselines."""

    qv: np.ndarray
    trades: np.ndarray
    tbb: np.ndarray            # taker buy base volume
    trades_mean7: np.ndarray   # at bar i: mean trades / min of bars i - 10080 .. i - 1
    log_sr: np.ndarray         # log of the rolling ``win``-bar quote-volume sum ending at the bar (inclusive)
    mu_lsr: np.ndarray         # rolling mean of log_sr over 7 days ending at the bar (inclusive)
    sd_lsr: np.ndarray
    win: int


def _grid_col(df: pl.DataFrame, m: we.MinuteData, col: str) -> np.ndarray:
    t = df["open_time"].to_numpy().astype("datetime64[m]").astype("int64") - m.m0
    arr = np.full(m.n, np.nan)
    arr[t] = df[col].cast(pl.Float64).to_numpy()
    return arr


def flow_grid(df_1m: pl.DataFrame, m: we.MinuteData, p: we.WaveParams, win: int) -> FlowGrid:
    qv = _grid_col(df_1m, m, "quote_volume")
    tr = _grid_col(df_1m, m, "trades")
    tbb = _grid_col(df_1m, m, "taker_buy_base")
    mp = int(p.sigma_window * p.sigma_min_frac)
    tm7 = we._shift1(np.asarray(pd.Series(tr).rolling(p.sigma_window, min_periods=mp).mean(), dtype=float))
    sr = np.asarray(pd.Series(qv).rolling(win, min_periods=win).sum(), dtype=float)
    with np.errstate(invalid="ignore", divide="ignore"):
        lsr = np.where(sr > 0, np.log(sr), np.nan)
    s = pd.Series(lsr)
    mu = np.asarray(s.rolling(p.sigma_window, min_periods=mp).mean(), dtype=float)
    sd = np.asarray(s.rolling(p.sigma_window, min_periods=mp).std(ddof=1), dtype=float)
    return FlowGrid(qv=qv, trades=tr, tbb=tbb, trades_mean7=tm7, log_sr=lsr, mu_lsr=mu, sd_lsr=sd, win=win)


def _ratio(num: float, den: float) -> float:
    return float(num / den) if np.isfinite(num) and np.isfinite(den) and den > 0 else float("nan")


def flow_row(m: we.MinuteData, g: FlowGrid, roll: we.Rolling, i: int, w: int) -> dict[str, float]:
    """Order-flow features of ONE event (bar index ``i`` = t0); uses bars ``i - B .. i + W - 1`` (closed <= t_c) and
    trailing baselines of bars closed before ``t0`` / before the prefix start only (``B = W``)."""
    b = w
    a, tc = i - b, i + w - 1
    nan = float("nan")
    out = dict.fromkeys(FLOW_COLS, nan)
    if a < 0 or tc >= m.n:
        return out
    v, tbb, trd = m.volume, g.tbb, g.trades
    vol7 = float(roll.vol_mean[i])
    trd7 = float(g.trades_mean7[i])

    def seg(arr: np.ndarray, lo: int, hi: int) -> np.ndarray:        # bars lo .. hi inclusive
        return arr[lo:hi + 1]

    def nsum(arr: np.ndarray, lo: int, hi: int) -> float:
        s = seg(arr, lo, hi)
        return float(np.nansum(s)) if not np.isnan(s).all() else nan

    def nmean(arr: np.ndarray, lo: int, hi: int) -> float:
        s = seg(arr, lo, hi)
        return float(np.nanmean(s)) if not np.isnan(s).all() else nan

    # legs: decline [t0 - B, t0] (bars a .. i), rebound (t0, t_c] (bars i + 1 .. tc)
    out["tb_ratio_prefix"] = _ratio(nsum(tbb, a, tc), nsum(v, a, tc))
    out["tb_ratio_decline"] = _ratio(nsum(tbb, a, i), nsum(v, a, i))
    out["tb_ratio_rebound"] = _ratio(nsum(tbb, i + 1, tc), nsum(v, i + 1, tc))
    out["tb_ratio_diff"] = out["tb_ratio_rebound"] - out["tb_ratio_decline"]
    out["tb_ratio_last1h"] = _ratio(nsum(tbb, tc - 59, tc), nsum(v, tc - 59, tc))
    out["tb_ratio_low15"] = _ratio(nsum(tbb, max(i - 7, a), min(i + 7, tc)), nsum(v, max(i - 7, a), min(i + 7, tc)))
    # trend of the hourly taker-buy ratio over the whole prefix (OLS slope per hour)
    nh = (tc - a + 1) // 60
    hb = np.array([_ratio(nsum(tbb, a + 60 * k, a + 60 * k + 59), nsum(v, a + 60 * k, a + 60 * k + 59)) for k in range(nh)])
    ok = np.isfinite(hb)
    if ok.sum() >= 3:
        out["tb_ratio_slope_1h"] = float(np.polyfit(np.arange(nh)[ok], hb[ok], 1)[0])
    # signed flow (taker buy - taker sell) relative to the trailing 7d mean volume
    if vol7 > 0:
        dd = 2 * nsum(tbb, a, i) - nsum(v, a, i)
        dr = 2 * nsum(tbb, i + 1, tc) - nsum(v, i + 1, tc)
        out["delta_decline_norm"] = dd / (vol7 * (i - a + 1))
        out["delta_rebound_norm"] = dr / (vol7 * (tc - i))
        out["vol_decline_rel"] = nmean(v, a, i) / vol7
        out["vol_rebound_rel"] = nmean(v, i + 1, tc) / vol7
        out["vol_last1h_rel"] = nmean(v, tc - 59, tc) / vol7
        v15 = np.nansum(v[a:a + 15 * ((tc - a + 1) // 15)].reshape(-1, 15), axis=1)
        out["vol_peak_rel"] = float(v15.max() / (vol7 * 15))
        out["vol_peak_pos"] = float(np.argmax(v15) / max(v15.size - 1, 1))
    out["vol_rebound_over_decline"] = _ratio(nmean(v, i + 1, tc), nmean(v, a, i))
    if trd7 > 0:
        out["trades_rel_prefix"] = nmean(trd, a, tc) / trd7
        out["trades_rel_decline"] = nmean(trd, a, i) / trd7
        out["trades_rel_rebound"] = nmean(trd, i + 1, tc) / trd7
        size_now = _ratio(nsum(v, a, tc), nsum(trd, a, tc))
        size7 = _ratio(vol7, trd7)
        out["trade_size_rel"] = _ratio(size_now, size7)
    # quote-volume z-score of the prefix vs the 7d history of equal-length windows ending before the prefix started
    e = a - 1
    if e >= 0 and tc - g.win + 1 == a and np.isfinite(g.sd_lsr[e]) and g.sd_lsr[e] > 0:
        out["qv_logz_prefix"] = float((g.log_sr[tc] - g.mu_lsr[e]) / g.sd_lsr[e])
    return out


def build_flow(df_1m: pl.DataFrame, events: pl.DataFrame, p: we.WaveParams, asset_id: str) -> pl.DataFrame:
    """Order-flow companion rows of ONE asset: one row per unique ``t0`` of ``events`` (cell independent for a fixed W)."""
    schema: dict[str, Any] = {"flow_key": pl.Utf8, "asset_id": pl.Utf8, "t0": pl.Datetime("us"), "t_c": pl.Datetime("us"),
                              **dict.fromkeys(FLOW_COLS, pl.Float64)}
    ev = events.select("t0", "t_c", "w_min").unique(subset=["t0", "w_min"]).sort("t0")
    if ev.height == 0:
        return pl.DataFrame(schema=schema)
    ws = ev["w_min"].unique().to_list()
    if len(ws) != 1:
        raise ValueError(f"flow features need a single W, got {ws}")
    w = int(ws[0])
    m = we.to_minute_grid(df_1m)
    roll = we.rolling_inputs(m, p)
    g = flow_grid(df_1m, m, p, 2 * w)
    rows = []
    for r in ev.iter_rows(named=True):
        i = we._epoch_min(r["t0"]) - m.m0
        rows.append({"flow_key": flow_key(asset_id, r["t0"]), "asset_id": asset_id, "t0": r["t0"], "t_c": r["t_c"],
                     **flow_row(m, g, roll, i, w)})
    return pl.DataFrame(rows, schema=schema, orient="row").with_columns(pl.col(c).fill_nan(None) for c in FLOW_COLS)


def flow_key(asset_id: str, t0: Any) -> str:
    return f"{asset_id}|{pd.Timestamp(t0):%Y%m%dT%H%M}"


def event_id(asset_id: str, cell_id: str, t0: Any) -> str:
    return f"{asset_id}|{cell_id}|{pd.Timestamp(t0):%Y%m%dT%H%M}"


# --------------------------------------------------------------------------- events


def scout_params(variant: str = PRIMARY_VARIANT, **overrides: Any) -> we.WaveParams:
    """5200 v2 parameters with the variant's A threshold (``a15`` == ``wave_events.params_v2()``)."""
    return we.params_v2(min_a_sigma=VARIANTS[variant], **overrides)


def mask_holdout2(ev: pl.DataFrame, p: we.WaveParams) -> pl.DataFrame:
    """Add ``label_masked`` / ``is_holdout2`` and NULL every label-dependent column of rows whose label window reaches
    holdout-2 (``label_end > holdout2_start``). ``p_star`` stays (known at ``t_c``)."""
    ho0 = datetime.fromisoformat(p.holdout2_start)
    masked = pl.col("label_end") > ho0
    cols = [c for c in MASKED_COLS if c in ev.columns and c not in MASKED_KEEP]
    return ev.with_columns(
        masked.alias("label_masked"), (pl.col("split") == "holdout2").alias("is_holdout2"),
    ).with_columns(pl.when(pl.col("label_masked")).then(None).otherwise(pl.col(c)).alias(c) for c in cols)


def add_tau(ev: pl.DataFrame) -> pl.DataFrame:
    """``tau_min`` (sideways censored at H) and ``first_touch_min`` (NULL for sideways) from the 5200 v2 touch minutes."""
    first = (pl.when(pl.col("label_class") == "impulse").then(pl.col("mins_to_target"))
             .when(pl.col("label_class") == "false_start").then(pl.col("mins_to_t0_break"))
             .otherwise(None).cast(pl.Float64))
    return ev.with_columns(first.alias("first_touch_min")).with_columns(
        pl.when(pl.col("label_class").is_null()).then(None)
        .when(pl.col("label_class") == "sideways").then(pl.col("h_min").cast(pl.Float64))
        .otherwise(pl.col("first_touch_min")).alias("tau_min"))


def build_asset(df_1m: pl.DataFrame, asset_id: str, variant: str = PRIMARY_VARIANT,
                params: we.WaveParams | None = None) -> tuple[pl.DataFrame, pl.DataFrame, we.WaveParams]:
    """(events, funnel, params) of one asset / variant, masked and enriched. No flow columns (see :func:`build_flow`)."""
    p = params or scout_params(variant)
    ds = we.build_events(df_1m, p)
    ev = add_tau(mask_holdout2(ds.events, p))
    ids = [event_id(asset_id, c, t) for c, t in zip(ev["cell_id"].to_list(), ev["t0"].to_list(), strict=True)]
    fks = [flow_key(asset_id, t) for t in ev["t0"].to_list()]
    ev = ev.with_columns(pl.lit(asset_id).alias("asset_id"), pl.lit(variant).alias("a_variant"),
                         pl.Series("event_id", ids, dtype=pl.Utf8), pl.Series("flow_key", fks, dtype=pl.Utf8))
    first = ["asset_id", "event_id", "flow_key", "a_variant"]
    ev = ev.select(*first, *[c for c in ev.columns if c not in first])
    fn = ds.funnel.with_columns(pl.lit(asset_id).alias("asset_id"), pl.lit(variant).alias("a_variant"))
    fn = fn.select("asset_id", "a_variant", *[c for c in fn.columns if c not in ("asset_id", "a_variant")])
    return ev, fn, p


@dataclass
class ScoutDataset:
    variants: dict[str, pl.DataFrame]       # variant -> pooled events (all assets)
    funnel: pl.DataFrame                    # all variants / assets / cells
    flow: pl.DataFrame                      # companion rows (union over variants, one per asset / t0)
    meta: pl.DataFrame
    params: dict[str, we.WaveParams]
    data_span: dict[str, tuple[datetime, datetime, int]]   # asset -> (first open, last open, rows)

    @property
    def events(self) -> pl.DataFrame:
        return self.variants[PRIMARY_VARIANT]


def content_hash(df: pl.DataFrame) -> str:
    return hashlib.sha256(df.write_csv(float_precision=10).encode("utf-8")).hexdigest()[:16]


def build_dataset(dfs: dict[str, pl.DataFrame], variants: tuple[str, ...] = tuple(VARIANTS),
                  flow: bool = True) -> ScoutDataset:
    """All assets x variants (+ order-flow companion rows of the union of the event start times)."""
    ev_by: dict[str, pl.DataFrame] = {}
    fns: list[pl.DataFrame] = []
    params: dict[str, we.WaveParams] = {}
    for var in variants:
        parts = []
        for asset, df in dfs.items():
            ev, fn, p = build_asset(df, asset, var)
            parts.append(ev)
            fns.append(fn)
            params[var] = p
        ev_by[var] = pl.concat(parts, how="vertical_relaxed").sort("asset_id", "cell_id", "t0")
    flows = []
    if flow:
        for asset, df in dfs.items():
            union = pl.concat([e.filter(pl.col("asset_id") == asset).select("t0", "t_c", "w_min") for e in ev_by.values()])
            flows.append(build_flow(df, union, next(iter(params.values())), asset))
    fl = pl.concat(flows) if flows else pl.DataFrame()
    span: dict[str, tuple[datetime, datetime, int]] = {
        a: (d["open_time"].min(), d["open_time"].max(), d.height) for a, d in dfs.items()}  # type: ignore[misc]
    fn_all = pl.concat(fns)
    meta_rows = []
    for var, ev in ev_by.items():
        meta_rows.append({
            "table": f'{SCHEMA}."{table_name(var)}"', "a_variant": var, "min_a_sigma": VARIANTS[var],
            "version": VERSION, "generator": GENERATOR, "params_json": params[var].to_json(), "n_events": ev.height,
            "content_hash": content_hash(ev.drop("event_id", "flow_key")),
            "cv_bounds": json.dumps([b.date().isoformat() for b in we.cv_block_bounds(params[var])]),
            "assets": ",".join(dfs), "holdout2_labels": "NULL (label_end > holdout2_start)",
            "ohlcv_last": json.dumps({a: s[1].isoformat() for a, s in span.items()}),
            "created_at": datetime.now().replace(microsecond=0),
        })
    return ScoutDataset(variants=ev_by, funnel=fn_all, flow=fl, meta=pl.DataFrame(meta_rows), params=params, data_span=span)


# --------------------------------------------------------------------------- persistence (lab DB)


def persist(ds: ScoutDataset, *, tries: int = 20, delay: float = 3.0, lab_path: str | None = None) -> dict[str, Any]:
    """Write all tables into the SOL lab DB (never a live DB); retries while the lab DB is locked."""
    import utils

    path = lab_path or utils.lab_db_path(LAB_ASSET)
    last: Exception | None = None
    for attempt in range(tries):
        try:
            con = duckdb.connect(path)
        except duckdb.Error as exc:
            last = exc
            logger.warning("lab db locked (attempt %d/%d): %s", attempt + 1, tries, exc)
            time.sleep(delay)
            continue
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            for var, ev in ds.variants.items():
                a = ev.to_arrow()  # noqa: F841
                con.execute(f'CREATE OR REPLACE TABLE {SCHEMA}."{table_name(var)}" AS SELECT * FROM a')
            fn, mt, fl = ds.funnel.to_arrow(), ds.meta.to_arrow(), ds.flow.to_arrow()  # noqa: F841
            main = table_name(PRIMARY_VARIANT)
            con.execute(f'CREATE OR REPLACE TABLE {SCHEMA}."{main}__funnel" AS SELECT * FROM fn')
            con.execute(f'CREATE OR REPLACE TABLE {SCHEMA}."{main}__meta" AS SELECT * FROM mt')
            con.execute(f'CREATE OR REPLACE TABLE {SCHEMA}."scout_flow_{VERSION}" AS SELECT * FROM fl')
            return {"lab_path": path, "attempts": attempt + 1, "tables": [table_name(v) for v in ds.variants]}
        finally:
            con.close()
    raise RuntimeError(f"lab db write failed after {tries} tries: {last}")


def load_scout(variant: str = PRIMARY_VARIANT, *, with_flow: bool = False) -> pl.DataFrame:
    """Read a ``scout_events_v1*`` table (all splits, label-masked) from the SOL lab DB, read-only."""
    import utils

    con = duckdb.connect(utils.lab_db_path(LAB_ASSET), read_only=True)
    try:
        ev = con.execute(f'SELECT * FROM {SCHEMA}."{table_name(variant)}" ORDER BY asset_id, cell_id, t0').pl()
        if not with_flow:
            return ev
        fl = con.execute(f'SELECT * FROM {SCHEMA}."scout_flow_{VERSION}"').pl().drop("asset_id", "t0", "t_c")
        return ev.join(fl, on="flow_key", how="left")
    finally:
        con.close()
