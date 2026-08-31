#!/usr/bin/env python3
"""Baseline feature set for the multi-coin panel (epic_064, alapmodell).

A deliberate reset after the first model turned out to be memorising the
calendar: `feat_dayofweek_sin` was its strongest input at 9.4% of gain, train
hit rate ran 92-98% against 43-58% on validation, and 40% of features still
carried enough coin-specific scale to identify which coin the row belonged to.

Four changes, all decided before looking at any result:

**One horizon family.** Every windowed indicator gets exactly four variants --
4, 8, 16 and 24 hours -- against a 24-hour target. Previously the windows were
inherited from a 60-minute target and landed anywhere from 5 to 140 hours.

**No calendar.** Day of week, hour, weekend, session membership and the
session/day/week anchored returns are all dropped. They have no lookback window
at all: they do not measure anything, they only say *when* -- which on a few
hundred effective observations is a perfect surface for memorisation.

**OHLCV only.** The derivatives and share blocks are out of the baseline, so
what is left can be attributed. They come back once the base is trustworthy.

**Rolling percentile normalisation.** Each feature is replaced by its rank
inside that coin's own trailing 720-hour window:

    norm(coin, t) = P( x(coin, s) <= x(coin, t) ),  s in [t-720h, t)

Tree models are invariant to monotone rescaling, so a global StandardScaler
would have changed nothing. The problem was never scale -- it was that the same
number meant different things on BTC and on a memecoin. A within-coin
percentile fixes exactly that, stays causal, and shrugs off the 99% wicks that
a z-score would chase. It pairs with the target, which is already expressed in
each coin's own volatility units.

⚠️ It also discards the absolute level: "volatility sits at its 90th
percentile" no longer says whether that is 2% or 20%. With a volatility-scaled
barrier that is acceptable, but it is a real loss.

Usage:
    python -m modeling.experiments.baseline_features --build
    python -m modeling.experiments.baseline_features --select
"""

import argparse
import json
import logging
import os
import pathlib
import re
import sys

import duckdb
import numpy as np
import polars as pl
from sklearn.feature_selection import mutual_info_classif

import utils
from data_handling.sync_tables._features_polars import (
    _rolling_rank_arr,
    compute_features_polars,
)

logger = logging.getLogger(__name__)

DB_REL = os.path.join("database", "_universe", "universe.duckdb")
TABLE = "feat_base_1h"
PANEL_START = "2021-12-01"
WINDOWS = (4, 8, 16, 24)          # hours, against a 24h target
# Hard ceiling on how far any feature may look back. Config-driven windows
# land on WINDOWS exactly, but several families (donchian, support/resistance
# distance, autocorrelation, regime rank) carry windows hardcoded inside the
# feature module at 10/30/60 bars. The 10-hour variants are kept -- they are
# inside the horizon and dropping them would delete whole families -- while
# 30 and 60 exceed the target horizon and go.
MAX_LOOKBACK_H = 24
NORM_WINDOW = 720                 # 30 days of hourly bars
WARMUP_BARS = NORM_WINDOW + 48    # normalisation window plus the longest feature

# Whole groups that exist only to encode when in the week or session a bar
# falls. Switched off at source; the name filter below is the safety net.
CALENDAR_GROUPS = ("time_session", "session_relative")

# Anything that only says *when* rather than measuring something.
CALENDAR_MARKERS = (
    "dayofweek", "hour_sin", "hour_cos", "weekend", "session",
    "day_open_return", "weekly_open_return", "day_range_position",
    "bars_into", "month", "quarter", "time_of_day",
)

# Features whose real reach is hidden from their name. Ichimoku uses the
# standard 9/26/52 periods hardcoded in the feature module, so kijun reaches 26
# hours and senkou_b 52 -- past the horizon, and invisible to a name-based
# filter. OBV and the A/D line accumulate from the first bar. So does
# `obv_roc_w`, despite the window in its name: its numerator is a clean w-bar
# flow but its denominator is the running total, so the same flow scores large
# early in a series and small later -- a coin-and-era fingerprint rather than a
# measurement.
HIDDEN_LOOKBACK = (
    "tenkan", "kijun", "senkou", "ichimoku", "feat_obv", "feat_ad_line",
)

# Pre-registered, matching the thresholds used earlier in this project so the
# numbers stay comparable.
MI_MIN = 0.001
CORR_MAX = 0.98
MI_SAMPLE = 150_000
SEED = 42


def _conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(
        os.path.join(utils._repo_root(), DB_REL), read_only=read_only
    )


def build_indicator_config() -> dict:
    """The stock indicator config, re-cut onto the 4/8/16/24-hour family.

    The runtime config is richer than `config/features.json`: an asset feature
    profile is merged in, adding groups that are plain booleans rather than
    parameter lists. Two of those groups exist only to produce calendar
    features, so they are switched off at the source rather than filtered out
    by name afterwards.
    """
    base = utils.load_features_config()["database"]["features"]["indicators"]
    out: dict = {}
    for group, inds in base.items():
        if group in CALENDAR_GROUPS:
            out[group] = False
            continue
        if not isinstance(inds, dict):
            out[group] = inds          # a bare on/off group
            continue

        out[group] = {}
        for name, variants in inds.items():
            if not isinstance(variants, list):
                out[group][name] = variants
                continue
            if name == "macd":
                # A MACD needs a fast/slow pair rather than one window; these
                # span the same 4-24h range as everything else.
                out[group][name] = [
                    {"fast": 4, "slow": 8, "signal": 3},
                    {"fast": 8, "slow": 16, "signal": 6},
                    {"fast": 12, "slow": 24, "signal": 9},
                ]
                continue
            # Several indicators ship with more than one window (roc has 14 and
            # 140); expanding each of them onto the same four windows would ask
            # for feat_roc_4 twice, which polars rejects. Dedupe on the whole
            # parameter set so genuinely different variants still survive.
            new: list[dict] = []
            seen: set[tuple] = set()
            for v in variants:
                if not isinstance(v, dict) or "window" not in v:
                    new.append(v)      # windowless: keep as is
                    continue
                for w in WINDOWS:
                    item = dict(v)
                    item["window"] = w
                    if "smooth_window" in item:
                        item["smooth_window"] = max(2, w // 4)
                    if name == "kama":
                        item["fast"], item["slow"] = 2, w
                    key = tuple(sorted(item.items()))
                    if key not in seen:
                        seen.add(key)
                        new.append(item)
            out[group][name] = new
    return out


def is_calendar(col: str) -> bool:
    return any(m in col for m in CALENDAR_MARKERS)


def exceeds_horizon(col: str) -> bool:
    """True when the feature reaches back beyond the target horizon.

    Two cases. The name may carry the window, in which case only the last
    number matters: `feat_macd_signal_12_24_9` smooths a 12/24 MACD over 9
    bars, so 24 is how deep it actually reaches. Or the window may be hidden
    inside the feature module, which is what HIDDEN_LOOKBACK covers -- those
    look windowless but are not.
    """
    if any(h in col for h in HIDDEN_LOOKBACK):
        return True
    m = re.findall(r"_(\d+)$", col)
    return bool(m) and int(m[0]) > MAX_LOOKBACK_H


def _normalise(frame: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """Replace each column by its trailing within-coin percentile rank.

    Binary flags are left alone: a 0/1 already means the same thing on every
    coin, and ranking one would silently turn it into "how often did this fire
    lately", which is a different feature wearing the same name.
    """
    out = {}
    for c in cols:
        a = frame[c].to_numpy().astype("float64")
        if np.unique(a[~np.isnan(a)]).size <= 3:
            out[c] = a
            continue
        out[c] = _rolling_rank_arr(a, NORM_WINDOW)
    return frame.with_columns([pl.Series(c, v) for c, v in out.items()])


def build() -> None:
    """Compute, normalise and store the baseline features for every coin."""
    indicators = build_indicator_config()
    conn = _conn()
    try:
        symbols = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT symbol FROM ohlcv_5m ORDER BY 1"
            ).fetchall()
        ]
        first = True
        for i, symbol in enumerate(symbols, 1):
            bars = conn.execute("""
                SELECT date_trunc('hour', open_time) AS open_time,
                       arg_min(open, open_time)  AS open,
                       max(high) AS high, min(low) AS low,
                       arg_max(close, open_time) AS close,
                       sum(volume) AS volume, sum(quote_volume) AS quote_volume,
                       sum(n_trades) AS trades,
                       sum(taker_buy_volume) AS taker_buy_base
                FROM ohlcv_5m WHERE symbol = ? GROUP BY 1 ORDER BY 1
            """, [symbol]).pl()
            if bars.height < WARMUP_BARS * 2:
                logger.warning("%s: tul keves bar (%d)", symbol, bars.height)
                continue

            feats = compute_features_polars(
                df_ohlcv=bars, indicators=indicators, feat_prefix="feat_",
                available_activity=["quote_volume", "trades", "taker_buy_base"],
                targets_cfg=[],
            )
            keep = sorted(
                c for c in feats.columns
                if c.startswith("feat_") and not is_calendar(c)
                and not exceeds_horizon(c)
            )
            feats = _normalise(feats, keep)

            out = (
                feats.with_columns(pl.lit(symbol).alias("symbol"))
                .filter(pl.col("open_time") >= pl.lit(PANEL_START).str.to_datetime())
                .select(["symbol", "open_time", *keep])
                .with_columns([pl.col(c).cast(pl.Float32) for c in keep])
            )

            conn.register("in_", out.to_arrow())
            if first:
                conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
                conn.execute(f"CREATE TABLE {TABLE} AS SELECT * FROM in_")
                first = False
            else:
                conn.execute(f"DELETE FROM {TABLE} WHERE symbol = ?", [symbol])
                conn.execute(f"INSERT INTO {TABLE} SELECT * FROM in_")
            conn.unregister("in_")
            logger.info("[%2d/%d] %-14s %d sor, %d feature",
                        i, len(symbols), symbol, out.height, len(keep))
    finally:
        conn.close()


def select() -> None:
    """Mutual-information and correlation filter, then report what survives."""
    conn = _conn(read_only=True)
    try:
        # The table was materialised before the hidden-lookback list existed, so
        # the filter is applied here as well rather than trusting its contents.
        cols = [
            r[0] for r in conn.execute(f"DESCRIBE {TABLE}").fetchall()
            if r[0].startswith("feat_")
            and not is_calendar(r[0]) and not exceeds_horizon(r[0])
        ]
        sel = ", ".join(f'f."{c}"' for c in cols)
        df = conn.execute(f"""
            SELECT {sel}, t.tb_label_1440_vol AS label
            FROM {TABLE} f JOIN target_5m t USING (symbol, open_time)
            WHERE t.tb_label_1440_vol IS NOT NULL
              AND f.open_time <= '2023-06-30'
            ORDER BY hash(epoch_ms(f.open_time) + {SEED})
            LIMIT {MI_SAMPLE}
        """).df()
    finally:
        conn.close()

    df = df.dropna()
    logger.info("MI-minta: %s sor, %d jelolt feature", f"{len(df):,}", len(cols))

    x = df[cols].to_numpy(dtype="float32")
    y = df["label"].to_numpy()
    mi = mutual_info_classif(x, y, discrete_features="auto", random_state=SEED)
    mi = dict(zip(cols, mi, strict=True))

    survivors = [c for c in cols if mi[c] > MI_MIN]
    dropped_mi = len(cols) - len(survivors)
    survivors.sort(key=lambda c: -mi[c])

    # Correlation pruning keeps the higher-MI member of any pair that is
    # effectively the same column twice.
    corr = np.corrcoef(df[survivors].to_numpy(dtype="float64").T)
    keep: list[str] = []
    for j, c in enumerate(survivors):
        if all(abs(corr[j, survivors.index(k)]) < CORR_MAX for k in keep):
            keep.append(c)
    dropped_corr = len(survivors) - len(keep)

    print("\n" + "=" * 74)
    print("FEATURE SELECTION — ELORE ROGZITETT KUSZOBOK")
    print("=" * 74)
    print(f"  MI > {MI_MIN}    korrelacio < {CORR_MAX}    minta: {len(df):,} sor")
    print(f"\n  jelolt feature              {len(cols):>5}")
    print(f"  kiesett MI miatt            {dropped_mi:>5}")
    print(f"  kiesett korrelacio miatt    {dropped_corr:>5}")
    print(f"  MARAD                       {len(keep):>5}")

    print(f"\n=== A BENTMARADO {len(keep)} VALTOZO (MI szerint) ===")
    for i, c in enumerate(keep, 1):
        print(f"  {i:>3}. {c:<38}MI={mi[c]:.4f}")

    out = pathlib.Path(utils._repo_root()) / os.path.join(
        "artifacts", "_experiments", "e064_base_selected.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(keep, indent=2), encoding="utf-8")
    print(f"\n  mentve: {out.name}")

    print("\n=== ABLAK SZERINTI BONTAS ===")
    from collections import Counter
    cnt = Counter()
    for c in keep:
        m = re.findall(r"_(\d+)$", c)
        cnt[f"{m[0]} ora" if m else "ablak nelkul"] += 1
    for k, v in sorted(cnt.items()):
        print(f"  {k:<16}{v:>4}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Alapmodell feature-keszlet")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--select", action="store_true")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.build:
        build()
    if args.select:
        select()


if __name__ == "__main__":
    main()
