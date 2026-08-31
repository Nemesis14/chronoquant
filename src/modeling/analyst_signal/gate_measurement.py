#!/usr/bin/env python3
"""Gate measurement -- does Reddit attention carry information about BTC fw60
magnitude? (epic_053 / t205)

This is the gate. Both inputs are finished and are read as-is, never rebuilt:

  social_features   (analyst_signal.duckdb) -- six t-1-lagged, trailing-
                     normalised hourly attention features per subreddit.
  target             (btcusdt.duckdb) -- 1-minute BTC forward-window outcomes,
                     `long_mfe_fw60` / `short_mfe_fw60` / `fw60_close_logret`.

Sampling: one observation per hour, the :00 bar only
-----------------------------------------------------
The feature is hourly; the target's fw60 window is 60 minutes. Using every
minute bar would both oversample the hourly feature 60x and overlap every
target window with the next 59, which deflates the naive standard error by
roughly an order of magnitude -- exactly the mistake the fixed-horizon target
family is prone to.

Sampling only the `:00` minute bar each hour removes both problems at once:
the fw60 window of the `h:00` bar is `h:01 .. h+1:00`, and the fw60 window of
the *next* sampled bar, `h+1:00`, is `h+1:01 .. h+2:00`. These do not overlap.
With non-overlapping windows the naive (non-block) standard error is already
close to correct for the *target* side of the pairing -- there is no
autocorrelation to correct via HAC/Newey-West once overlap is gone. ~68,000
independent hourly observations remain across the two regimes, which is far
more than this gate needs.

What overlap does *not* remove: the feature side. `att_z` etc. are 30-day
trailing z-scores, so adjacent hourly values are highly autocorrelated even
though the target windows no longer touch. A naive row-shuffle placebo would
therefore be a too-easy baseline -- it would destroy the feature's real
autocorrelation structure, making every permutation look nothing like a
plausible alternate history, and inflate apparent significance. The permuted
null must preserve that structure: see `_block_permutation` below.

Statistics, per (regime, series, feature, target)
---------------------------------------------------
IC   Spearman rank correlation, tested one-sided (>0) for the magnitude
     targets (the stated hypothesis) and two-sided for the direction control
     (the stated null hypothesis is exactly zero).
MI   Mutual information on quantile-binned codes (10 bins), tested one-sided
     (>=) for every target -- MI has no sign, so "no relationship" is the
     single relevant null regardless of target kind.

Both are tested against a block-permutation null (block length see
`BLOCK_LEN_HOURS`), 5000 permutations, then Benjamini-Hochberg FDR across each
regime's full feature x series x target family (100 tests). The FDR-corrected
p-value, not the raw permutation p-value, is what the go/no-go reads.

Usage:
    python -m modeling.analyst_signal.gate_measurement
    python -m modeling.analyst_signal.gate_measurement --dry-run
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from datetime import UTC, datetime
from statistics import NormalDist

import duckdb
import numpy as np
import polars as pl

import utils
from data_handling.store.analyst_signal_store import (
    DB_SUBPATH,
    ensure_tables,
    get_connection,
    upsert_table,
)
from modeling.analyst_signal.validate import bh_fdr

logger = logging.getLogger(__name__)

SUBREDDITS: tuple[str, ...] = ("Bitcoin", "BitcoinMarkets", "CryptoCurrency", "CryptoMarkets")

FEATURE_COLS: tuple[str, ...] = (
    "att_z", "att_accel", "authors_z", "new_author_share", "items_per_author", "chars_per_item",
)

#: CAVEAT (found during t205, not corrected here -- correcting it would be
#: t204 rework, out of this ticket's scope, and re-normalising after seeing
#: results would be exactly the p-hacking the epic_051 discipline forbids):
#: despite the t205 ticket describing every social_features column as
#: "trailing-normalised", only att_z, att_accel, authors_z and
#: new_author_share actually are (see social_features.py). `items_per_author`
#: and `chars_per_item` are raw un-normalised ratios carrying only the t-1
#: lag. Both show a multi-year secular decline (e.g. Bitcoin items_per_author:
#: 1.73 in 2021 -> 1.38 in 2025) that BTC fw60 magnitude also declines over
#: the same window, which inflates their pooled Spearman IC relative to their
#: (weaker, sign-inconsistent) within-year IC -- classic between-year
#: confounding, not an hour-to-hour relationship. Treat their gate_validation
#: rows as lower-confidence than att_z/authors_z/new_author_share/att_accel.
#: See the t205 ticket Notes for the within-year diagnostic.

#: Cross-subreddit average is only built for att_z -- the ticket calls out
#: individual subreddits as noisier and asks for one averaged series, not a
#: full averaged feature set.
AGGREGATE_SERIES  = "ALL_AVG"
AGGREGATE_FEATURE = "att_z"

#: regime name -> [start, end] inclusive, UTC timestamps.
REGIMES: dict[str, tuple[datetime, datetime]] = {
    "main_2021_2025":       (datetime(2021, 1, 1, 0, 0, 0), datetime(2025, 12, 31, 23, 0, 0)),
    "robustness_2017_2020": (datetime(2017, 8, 17, 4, 0, 0), datetime(2020, 12, 31, 23, 0, 0)),
}
#: Only this regime's FDR family drives the go/no-go decision.
PRIMARY_REGIME = "main_2021_2025"

#: target column produced for the panel -> ('source expression', 'kind')
#: magnitude targets carry the stated hypothesis (positive IC expected);
#: direction is the placebo control (zero expected).
TARGETS: dict[str, str] = {
    "long_mfe_fw60":         "magnitude",
    "abs_short_mfe_fw60":    "magnitude",
    "abs_fw60_close_logret": "magnitude",
    "fw60_close_logret":     "direction",
}

#: Block length for the permutation null. Deliberately 169, not 168 (one
#: calendar week) -- this was found empirically, not chosen a priori, and is
#: important enough to spell out.
#:
#: Block-order permutation only reorders *which* block lands in each slot; it
#: never touches the offset *within* a block. If the block length is an exact
#: multiple of 24, every permutation preserves each position's hour-of-day
#: (and, at 168, day-of-week) phase -- slot (block, offset) always receives
#: some block's same offset. That means the null is only unbiased if the
#: feature has exactly zero mean at every hour-of-week phase. att_z and
#: authors_z are deseasonalised against hour-of-week in social_features.py,
#: but via a *causal, expanding* trailing mean (see that module's docstring),
#: which leaves a real, nonzero residual seasonal phase -- confirmed
#: empirically: with block_len=168, the permutation null for e.g.
#: BitcoinMarkets/authors_z sits at mean=-0.062 (should be ~0), which made an
#: essentially-zero observed IC (-0.0007) register as p=0.0002. Shifting to
#: block_len=169 (or any length not a multiple of 24) breaks the phase lock;
#: null_mean collapses to ~0 in every case checked, while every *observed*
#: correlation is unchanged (block length never touches the real data, only
#: the null) -- e.g. Bitcoin/att_z/long_mfe_fw60 observed=0.03009 at 168 vs.
#: 0.03003 at 169. 169 hours still spans the same real-world duration as the
#: original weekly-autocorrelation rationale (`_log_acf_check`), just without
#: the exact-period phase lock.
BLOCK_LEN_HOURS = 169

N_BINS          = 10
N_PERMUTATIONS  = 5000
SEED            = 20260821
MIN_CALLS       = 500  # minimum paired observations to bother testing at all


# %% Data loading

def _load_hourly_target(btc_db_path: str) -> pl.DataFrame:
    """Load one BTC row per hour (the `:00` minute bar only).

    Args:
        btc_db_path : Absolute path to btcusdt.duckdb.

    Returns:
        Columns: ts_utc, long_mfe_fw60, abs_short_mfe_fw60, abs_fw60_close_logret,
        fw60_close_logret.
    """
    conn = duckdb.connect(btc_db_path, read_only=True)
    try:
        df = conn.execute("""
            SELECT
                open_time AS ts_utc,
                long_mfe_fw60,
                abs(short_mfe_fw60)     AS abs_short_mfe_fw60,
                abs(fw60_close_logret)  AS abs_fw60_close_logret,
                fw60_close_logret
            FROM target
            WHERE date_part('minute', open_time) = 0
            ORDER BY open_time
        """).pl()
    finally:
        conn.close()
    logger.info("target: %d hourly (:00) rows loaded", df.height)
    return df


def _load_social_features(signal_db_path: str) -> pl.DataFrame:
    """Load the full social_features table.

    Args:
        signal_db_path : Absolute path to analyst_signal.duckdb.

    Returns:
        Raw social_features rows (ts_utc, subreddit, six feature columns).
    """
    conn = duckdb.connect(signal_db_path, read_only=True)
    try:
        df = conn.execute(f"""
            SELECT ts_utc, subreddit, {", ".join(FEATURE_COLS)}
            FROM social_features
        """).pl()
    finally:
        conn.close()
    logger.info("social_features: %d rows loaded", df.height)
    return df


def build_panel(target_df: pl.DataFrame, features_df: pl.DataFrame) -> pl.DataFrame:
    """Join hourly target and per-subreddit features into one wide panel.

    Args:
        target_df   : Output of `_load_hourly_target`.
        features_df : Output of `_load_social_features`.

    Returns:
        One row per hour. Columns: ts_utc, the four target columns, one
        `{subreddit}__{feature}` column per (subreddit, feature) pair, and
        `ALL_AVG__att_z` -- the mean att_z across the four subreddits.
    """
    wide = features_df.pivot(
        on="subreddit", index="ts_utc", values=list(FEATURE_COLS),
    )
    # polars pivot with multiple value columns names outputs "{value}_{subreddit}" ->
    # normalise to "{subreddit}__{feature}" for a single, unambiguous convention.
    rename = {}
    for col in wide.columns:
        if col == "ts_utc":
            continue
        for feat in FEATURE_COLS:
            prefix = f"{feat}_"
            if col.startswith(prefix):
                sub = col[len(prefix):]
                rename[col] = f"{sub}__{feat}"
                break
    wide = wide.rename(rename)

    avg_expr = pl.mean_horizontal([f"{sub}__att_z" for sub in SUBREDDITS]).alias(
        f"{AGGREGATE_SERIES}__{AGGREGATE_FEATURE}"
    )
    wide = wide.with_columns(avg_expr)

    panel = target_df.join(wide, on="ts_utc", how="inner").sort("ts_utc")
    logger.info(
        "panel: %d rows (target %d x features %d, inner join on ts_utc)",
        panel.height, target_df.height, wide.height,
    )
    return panel


# %% Statistics

def _rank_avg(x: np.ndarray) -> np.ndarray:
    """Average-tie ranks, 0-based."""
    order   = np.argsort(x, kind="mergesort")
    ranks   = np.empty_like(order, dtype=np.float64)
    sorted_x = x[order]
    # Average rank within blocks of equal value.
    n = len(x)
    i = 0
    while i < n:
        j = i
        while j + 1 < n and sorted_x[j + 1] == sorted_x[i]:
            j += 1
        avg_rank = (i + j) / 2.0
        ranks[order[i:j + 1]] = avg_rank
        i = j + 1
    return ranks


def _quantile_codes(x: np.ndarray, n_bins: int = N_BINS) -> tuple[np.ndarray, int]:
    """Quantile-bin `x` into integer codes 0..k-1 (k <= n_bins, ties collapse bins)."""
    quantiles = np.unique(np.quantile(x, np.linspace(0, 1, n_bins + 1)))
    if len(quantiles) < 3:
        # Degenerate (near-constant) series -- everything in one bin.
        return np.zeros(len(x), dtype=np.int64), 1
    edges = quantiles[1:-1]
    codes = np.searchsorted(edges, x, side="right")
    return codes.astype(np.int64), len(quantiles) - 1


def _mi_bits(code_x: np.ndarray, code_y: np.ndarray, kx: int, ky: int) -> float:
    """Mutual information in bits from two integer-coded arrays."""
    n = len(code_x)
    idx = code_x * ky + code_y
    counts = np.bincount(idx, minlength=kx * ky).reshape(kx, ky).astype(np.float64)
    pxy = counts / n
    px  = pxy.sum(axis=1, keepdims=True)
    py  = pxy.sum(axis=0, keepdims=True)
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.where(pxy > 0, pxy / (px * py), 1.0)
        terms = pxy * np.log2(ratio)
    return float(np.nansum(np.where(pxy > 0, terms, 0.0)))


def _log_acf_check(x: np.ndarray, lag: int) -> float:
    """Sample autocorrelation of `x` at `lag`, for the block-length rationale log line."""
    if lag >= len(x):
        return float("nan")
    a, b = x[:-lag], x[lag:]
    a, b = a - a.mean(), b - b.mean()
    denom = np.sqrt((a * a).sum() * (b * b).sum())
    return float((a * b).sum() / denom) if denom > 0 else float("nan")


def _block_permutation(
    feat: np.ndarray,
    target: np.ndarray,
    kind: str,
    rng: np.random.Generator,
    block_len: int = BLOCK_LEN_HOURS,
    n_perm: int = N_PERMUTATIONS,
) -> dict:
    """Block-permutation placebo for Spearman IC and MI.

    Only the feature series is permuted, in contiguous blocks of `block_len`
    (block order shuffled, in-block order kept), which preserves the
    feature's own short-range autocorrelation while destroying its alignment
    with the target. The target stays in its original time order. A trailing
    remainder shorter than one block is dropped (documented, not silent --
    logged by the caller via `n_obs`).

    Args:
        feat      : Feature values, time-ordered, no NaNs.
        target    : Target values, same order/length, no NaNs.
        kind      : 'magnitude' (one-sided, IC > 0 expected) or 'direction'
                     (two-sided, IC == 0 expected).
        rng       : Random generator, shared across calls for reproducibility.
        block_len : Block length in observations (hours).
        n_perm    : Number of block permutations.

    Returns:
        Dict with n_obs, spearman_ic, ic_perm_p, mi_bits, mi_perm_p.
    """
    n = len(feat)
    n_blocks = n // block_len
    n_used = n_blocks * block_len
    feat_used, target_used = feat[:n_used], target[:n_used]

    rank_feat   = _rank_avg(feat_used)
    rank_target = _rank_avg(target_used)
    feat_c   = rank_feat - rank_feat.mean()
    target_c = rank_target - rank_target.mean()
    feat_ss   = math.sqrt(float((feat_c * feat_c).sum()))
    target_ss = math.sqrt(float((target_c * target_c).sum()))
    observed_ic = float(np.dot(feat_c, target_c) / (feat_ss * target_ss)) if feat_ss > 0 and target_ss > 0 else 0.0

    code_feat, kx = _quantile_codes(feat_used)
    code_target, ky = _quantile_codes(target_used)
    observed_mi = _mi_bits(code_feat, code_target, kx, ky)

    feat_c_blocks    = feat_c.reshape(n_blocks, block_len)
    code_feat_blocks = code_feat.reshape(n_blocks, block_len)

    null_ic = np.empty(n_perm)
    null_mi = np.empty(n_perm)
    for i in range(n_perm):
        order = rng.permutation(n_blocks)
        perm_feat_c = feat_c_blocks[order].reshape(-1)
        null_ic[i] = np.dot(perm_feat_c, target_c) / (feat_ss * target_ss) if feat_ss > 0 and target_ss > 0 else 0.0
        perm_code_feat = code_feat_blocks[order].reshape(-1)
        null_mi[i] = _mi_bits(perm_code_feat, code_target, kx, ky)

    if kind == "magnitude":
        ic_p = float((np.sum(null_ic >= observed_ic) + 1) / (n_perm + 1))
    else:
        ic_p = float((np.sum(np.abs(null_ic) >= abs(observed_ic)) + 1) / (n_perm + 1))
    mi_p = float((np.sum(null_mi >= observed_mi) + 1) / (n_perm + 1))

    return {
        "n_obs":       n_used,
        "spearman_ic": observed_ic,
        "ic_perm_p":   ic_p,
        "mi_bits":     observed_mi,
        "mi_perm_p":   mi_p,
    }


# %% Power analysis

def min_detectable_rho(n: int, alpha: float, power: float = 0.80) -> float:
    """Minimum |Spearman rho| detectable at `power` for two-sided `alpha`, via
    the Fisher z approximation (SE = 1/sqrt(n-3)).

    Args:
        n     : Sample size (paired, non-null observations).
        alpha : Two-sided significance level.
        power : Target power (default 0.80).

    Returns:
        Minimum detectable |rho|.
    """
    dist = NormalDist()
    z_alpha = dist.inv_cdf(1 - alpha / 2)
    z_power = dist.inv_cdf(power)
    se = 1.0 / math.sqrt(n - 3)
    return math.tanh((z_alpha + z_power) * se)


# %% Orchestration

def _series_list() -> list[tuple[str, str]]:
    """All (series, feature) pairs to test: 4 subreddits x 6 features, plus ALL_AVG x att_z."""
    pairs = [(sub, feat) for sub in SUBREDDITS for feat in FEATURE_COLS]
    pairs.append((AGGREGATE_SERIES, AGGREGATE_FEATURE))
    return pairs


def run(panel: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Run the full gate measurement over both regimes.

    Args:
        panel : Output of `build_panel`.

    Returns:
        (gate_validation rows, gate_power rows) as Polars DataFrames.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    rng = np.random.default_rng(SEED)
    series_list = _series_list()

    # One-time ACF sanity check logged for the block-length rationale.
    main_start, main_end = REGIMES[PRIMARY_REGIME]
    acf_probe = panel.filter(
        (pl.col("ts_utc") >= main_start) & (pl.col("ts_utc") <= main_end)
    )[f"{AGGREGATE_SERIES}__{AGGREGATE_FEATURE}"].drop_nulls().to_numpy()
    acf_24    = _log_acf_check(acf_probe, 24)
    acf_block = _log_acf_check(acf_probe, BLOCK_LEN_HOURS)
    logger.info(
        "ACF(ALL_AVG att_z, main regime): lag=24h -> %.3f, lag=%dh -> %.3f "
        "(block length chosen at %dh)", acf_24, BLOCK_LEN_HOURS, acf_block, BLOCK_LEN_HOURS,
    )

    validation_rows: list[dict] = []
    power_rows: list[dict] = []

    for regime, (start, end) in REGIMES.items():
        regime_panel = panel.filter((pl.col("ts_utc") >= start) & (pl.col("ts_utc") <= end))
        regime_results: list[dict] = []

        for series, feature in series_list:
            col = f"{series}__{feature}"
            if col not in regime_panel.columns:
                continue
            for target_name, kind in TARGETS.items():
                sub = regime_panel.select(["ts_utc", col, target_name]).drop_nulls()
                if sub.height < max(MIN_CALLS, BLOCK_LEN_HOURS * 4):
                    logger.warning(
                        "%s / %s / %s / %s: only %d paired obs, skipped",
                        regime, series, feature, target_name, sub.height,
                    )
                    continue
                feat_arr   = sub[col].to_numpy().astype(np.float64)
                target_arr = sub[target_name].to_numpy().astype(np.float64)
                stats = _block_permutation(feat_arr, target_arr, kind, rng)
                regime_results.append({
                    "regime":          regime,
                    "series":          series,
                    "feature":         feature,
                    "target":          target_name,
                    "target_kind":     kind,
                    "block_len_hours": BLOCK_LEN_HOURS,
                    "n_permutations":  N_PERMUTATIONS,
                    "computed_at_utc": now,
                    **stats,
                })
        logger.info("%s: %d tests computed", regime, len(regime_results))

        if regime_results:
            ic_p = np.array([r["ic_perm_p"] for r in regime_results])
            mi_p = np.array([r["mi_perm_p"] for r in regime_results])
            ic_fdr = bh_fdr(ic_p)
            mi_fdr = bh_fdr(mi_p)
            for row, ic_adj, mi_adj in zip(regime_results, ic_fdr, mi_fdr, strict=True):
                row["ic_fdr_p"] = float(ic_adj)
                row["mi_fdr_p"] = float(mi_adj)
            validation_rows.extend(regime_results)

            family_size = len(regime_results)
            n_obs = regime_results[0]["n_obs"]
            max_abs_ic = float(np.max(np.abs([r["spearman_ic"] for r in regime_results])))
            alpha_bonf = 0.05 / family_size
            power_rows.append({
                "regime":                        regime,
                "n_obs":                         n_obs,
                "family_size":                   family_size,
                "alpha_nominal":                 0.05,
                "alpha_bonferroni":              alpha_bonf,
                "min_detectable_rho_nominal":    min_detectable_rho(n_obs, 0.05),
                "min_detectable_rho_bonferroni": min_detectable_rho(n_obs, alpha_bonf),
                "max_observed_abs_ic":           max_abs_ic,
                "computed_at_utc":               now,
            })

    return pl.DataFrame(validation_rows), pl.DataFrame(power_rows)


def summarise(validation_df: pl.DataFrame, power_df: pl.DataFrame) -> None:
    """Print the gate verdict."""
    print("\n=== GATE MEASUREMENT -- IC/MI, block-permutation placebo, FDR ===")
    for regime in REGIMES:
        sub = validation_df.filter(pl.col("regime") == regime).sort("ic_fdr_p")
        if sub.height == 0:
            continue
        print(f"\n--- {regime} (n={sub['n_obs'][0]}) ---")
        header = (f"{'series':>14s} {'feature':>17s} {'target':>22s} {'ic':>7s} "
                  f"{'ic_p':>7s} {'ic_fdr':>7s} {'mi':>7s} {'mi_fdr':>7s}")
        print(header)
        print("-" * len(header))
        for row in sub.head(15).iter_rows(named=True):
            print(f"{row['series']:>14s} {row['feature']:>17s} {row['target']:>22s} "
                  f"{row['spearman_ic']:>7.4f} {row['ic_perm_p']:>7.4f} {row['ic_fdr_p']:>7.4f} "
                  f"{row['mi_bits']:>7.4f} {row['mi_fdr_p']:>7.4f}")

        n_ic_fdr05 = sub.filter(pl.col("ic_fdr_p") < 0.05).height
        n_ic_fdr10 = sub.filter(pl.col("ic_fdr_p") < 0.10).height
        n_mi_fdr05 = sub.filter(pl.col("mi_fdr_p") < 0.05).height
        print(f"\n  IC survivors: FDR<0.05 = {n_ic_fdr05}/{sub.height}, FDR<0.10 = {n_ic_fdr10}/{sub.height}")
        print(f"  MI survivors: FDR<0.05 = {n_mi_fdr05}/{sub.height}")

    print("\n=== POWER ANALYSIS ===")
    for row in power_df.iter_rows(named=True):
        print(f"{row['regime']:>22s}: n={row['n_obs']:>6d}  "
              f"min |rho| @80% power, alpha=0.05 -> {row['min_detectable_rho_nominal']:.4f}  "
              f"alpha=0.05/{row['family_size']} -> {row['min_detectable_rho_bonferroni']:.4f}  "
              f"max observed |IC| = {row['max_observed_abs_ic']:.4f}")


# %% CLI

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute and report, but do not write the database.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    btc_db_path    = utils.load_asset_config("btcusdt")["database"]["db_path"]
    signal_db_path = os.path.join(utils._repo_root(), DB_SUBPATH)

    target_df   = _load_hourly_target(btc_db_path)
    features_df = _load_social_features(signal_db_path)
    panel = build_panel(target_df, features_df)

    validation_df, power_df = run(panel)
    summarise(validation_df, power_df)

    if args.dry_run:
        logger.info("dry-run: nincs iras")
        return 0

    out = get_connection(signal_db_path)
    try:
        ensure_tables(out)
        upsert_table(out, "gate_validation", validation_df)
        upsert_table(out, "gate_power", power_df)
    finally:
        out.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
