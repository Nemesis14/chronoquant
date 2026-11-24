# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Event frame of the Impulse Scout benchmark (epic_073 / t931) — multi-asset aware, adapter based.

The benchmark never talks to a particular event table: it works on a *standard frame* (polars) that carries an
``asset_id`` column. Today the frame is built from SOL ``research."wave_events_v2"`` through
:func:`from_wave_events_v2`; t930's ``research."scout_events_v1"`` (SOL + BTC + ETH + order flow) drops in through
:func:`from_scout_events` with the same column contract (:data:`REQUIRED_COLS`).

Only the **train** split takes part (5300: all development on the train period); :func:`guard_no_holdout` refuses any
holdout-2 row / interval. The OOS period is not used by the harness at all.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

import numpy as np
import polars as pl

from reports.builders import wave_events as we
from reports.builders.wave_oos_eval import assert_no_holdout

CLASSES: tuple[str, ...] = we.CLASS_ORDER_V2          # ("impulse", "false_start", "sideways")
CLASS_INDEX = {c: i for i, c in enumerate(CLASSES)}
HOLDOUT2_START = datetime.fromisoformat(we.WaveParams().holdout2_start)
PRIMARY_CELL = "W4h_H8h"

#: label-dependent columns: permuted TOGETHER in the day-bundle permutation test
LABEL_COLS = ("label_class", "label_impulse", "tau_min")
#: scalar baseline features (S0 + C, 5200 v2)
S0C_COLS: tuple[str, ...] = tuple(we.FEATURE_COLS)
#: columns every event table adapter must deliver
REQUIRED_COLS: tuple[str, ...] = (
    "asset_id", "cell_id", "t0", "t_c", "event_start", "label_end", "split", "h_min",
    "label_class", "tau_min", "p_star", "low_t0", "close_tc", "sigma_w", *S0C_COLS,
)


def _tau_min() -> pl.Expr:
    """First-touch time ``tau`` in minutes after ``t_c`` (sideways: censored at ``H``; 5300)."""
    return (pl.when(pl.col("label_class") == "impulse").then(pl.col("mins_to_target"))
            .when(pl.col("label_class") == "false_start").then(pl.col("mins_to_t0_break"))
            .otherwise(pl.col("h_min")).cast(pl.Float64).alias("tau_min"))


def from_wave_events_v2(ev: pl.DataFrame, asset_id: str = "solusdt", cell: str = PRIMARY_CELL) -> pl.DataFrame:
    """Adapter: epic_072 ``research."wave_events_v2"`` rows of ONE asset / cell -> standard frame."""
    e = ev.filter(pl.col("cell_id") == cell)
    e = e.with_columns(pl.lit(asset_id).alias("asset_id"))
    e = e.with_columns(_tau_min())
    keep = [c for c in (*REQUIRED_COLS, "label_impulse", "same_bar", "atr14_15m", "vol7d_mean_1m") if c in e.columns]
    return e.select(keep)


def from_scout_events(ev: pl.DataFrame, cell: str = PRIMARY_CELL, variant: str = "a15") -> pl.DataFrame:
    """Adapter for t930 ``research."scout_events_v1"`` (already multi-asset, carries ``asset_id`` and ``tau_min``).

    The table may hold several A-threshold variants (``a_variant``); the primary one (``a15``) is selected. The
    table's own string ``event_id`` is kept untouched (the harness uses the positional ``row_id``)."""
    if "a_variant" in ev.columns:
        ev = ev.filter(pl.col("a_variant") == variant)
    missing = [c for c in REQUIRED_COLS if c not in ev.columns]
    if missing:
        raise KeyError(f"scout_events frame misses columns: {missing}")
    return ev.filter(pl.col("cell_id") == cell)


def load_wave_events_v2(asset_id: str | None = None, cell: str = PRIMARY_CELL) -> pl.DataFrame:
    """SOL (or any asset with a v2 table) train events from its lab DB; the SQL filter keeps OOS / holdout-2 out of memory."""
    ev = we.load_events(asset_id, split="train", version="v2")
    return from_wave_events_v2(ev, asset_id or "solusdt", cell)


def load_scout_events(table: str = "scout_events_v1", cell: str = PRIMARY_CELL, asset_id: str | None = None
                      ) -> pl.DataFrame:
    """Read t930's multi-asset table (train split only) from the lab DB; raises a clear error while it does not exist."""
    import duckdb

    import utils

    con = duckdb.connect(utils.lab_db_path(asset_id), read_only=True)
    try:
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='research' AND table_name=?",
            [table]).fetchone()
        if not exists or not exists[0]:
            raise LookupError(f'research."{table}" does not exist yet (t930 not delivered); use load_wave_events_v2 meanwhile')
        df = con.execute(f"SELECT * FROM research.\"{table}\" WHERE split = 'train' AND cell_id = ?", [cell]).pl()
    finally:
        con.close()
    return from_scout_events(df, cell)


def guard_no_holdout(df: pl.DataFrame, holdout_start: datetime = HOLDOUT2_START) -> None:
    """Refuse a frame that contains holdout-2 events or an event interval reaching into holdout-2."""
    assert_no_holdout(df, holdout_start)
    if df.height and (df["t0"].max() >= holdout_start or df["t_c"].max() > holdout_start):
        raise ValueError("event at / after the holdout-2 start present")


def validate_frame(df: pl.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise KeyError(f"event frame misses columns: {missing}")
    bad = set(df["label_class"].unique().to_list()) - set(CLASSES)
    if bad:
        raise ValueError(f"unknown label classes: {bad}")


def assign_blocks(df: pl.DataFrame, bounds: list[datetime]) -> pl.DataFrame:
    """Shared calendar CV blocks (same ``bounds`` for every asset): ``cv_block`` of ``t0`` and ``cv_straddle``
    (the interval ``[event_start, label_end)`` leaves its block; such events enter no fold at all)."""
    t0 = df["t0"].to_numpy().astype("datetime64[us]")
    edges = np.array(bounds[1:-1], dtype="datetime64[us]")
    idx = np.searchsorted(edges, t0, side="right").astype(np.int32)
    lo = np.array(bounds[:-1], dtype="datetime64[us]")[idx]
    hi = np.array(bounds[1:], dtype="datetime64[us]")[idx]
    es = df["event_start"].to_numpy().astype("datetime64[us]")
    le = df["label_end"].to_numpy().astype("datetime64[us]")
    outside = (t0 < np.datetime64(bounds[0], "us")) | (t0 >= np.datetime64(bounds[-1], "us"))
    return df.with_columns(
        pl.Series("cv_block", idx),
        pl.Series("cv_straddle", (es < lo) | (le > hi) | outside),
    )


def prepare_frame(df: pl.DataFrame, bounds: list[datetime] | None = None) -> pl.DataFrame:
    """Usable events: train split, holdout guard, shared blocks, no boundary straddlers; sorted by (t0, asset_id) and
    numbered with ``row_id`` (= row position; the key into any ``prefix_data`` a model brings)."""
    validate_frame(df)
    guard_no_holdout(df)
    bounds = bounds or we.cv_block_bounds(we.params_v2())
    d = assign_blocks(df.filter(pl.col("split") == "train"), bounds)
    d = d.filter(~pl.col("cv_straddle")).sort(["t0", "asset_id"])
    d = d.with_columns(
        (pl.col("label_class") == "impulse").alias("label_impulse"),
        pl.Series("row_id", np.arange(d.height, dtype=np.int64)),
    )
    return d


def y_binary(df: pl.DataFrame) -> np.ndarray:
    return (df["label_class"] == "impulse").to_numpy().astype(float)


def y_class(df: pl.DataFrame) -> np.ndarray:
    return df["label_class"].replace_strict(CLASS_INDEX, return_dtype=pl.Int64).to_numpy()


def frame_hash(df: pl.DataFrame, cols: tuple[str, ...] | None = None) -> str:
    """Content hash of the key / label / feature columns (run metadata)."""
    use = [c for c in (cols or (*REQUIRED_COLS, "cv_block")) if c in df.columns]
    return hashlib.sha256(df.select(use).write_csv(float_precision=10).encode()).hexdigest()[:16]


def describe(df: pl.DataFrame) -> dict[str, Any]:
    return {"n_events": df.height, "n_impulse": int((df["label_class"] == "impulse").sum()),
            "assets": sorted(df["asset_id"].unique().to_list())}
