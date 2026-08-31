#!/usr/bin/env python3
"""Placebo and multiple-testing validation of author skill (epic_051 / t6).

The scoring step produces a ranking whatever the data contains, so the ranking
alone proves nothing. This module asks the only question that matters: does the
real data separate authors any better than shuffled author labels do?

Three tests, all at every horizon:

  beat_benchmark   How many authors beat buy-and-hold, against a permutation
                   null that preserves each author's call count. If skill
                   exists, the real count exceeds the shuffled count.

  significant      How many authors reach t > 2 individually, against the same
                   null. Chance alone produces roughly 5% of them.

  best_author      The single best t-statistic, against the distribution of the
                   best t-statistic under shuffling. This is an exact
                   family-wise test: it asks whether the standout author is
                   better than the standout of a random partition.

Plus Benjamini-Hochberg FDR across the per-author one-sided p-values, which
answers the practical question by name: is any specific author identifiable as
skilled once we account for having tested all of them?

Permutation preserves the per-author call counts, so a rank driven purely by
sample size cannot masquerade as skill.

Usage:
    python -m modeling.analyst_signal.validate
    python -m modeling.analyst_signal.validate --dry-run
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from datetime import UTC, datetime

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

logger = logging.getLogger(__name__)

HORIZONS_MIN = (60, 240, 1440, 4320, 10080)

#: Authors below this call count carry too little evidence to test at all.
MIN_CALLS = 30

N_PERMUTATIONS = 5000
SEED = 20260820


# %% Statistics

def group_stats(values: np.ndarray, groups: np.ndarray, k: int) -> tuple[np.ndarray, np.ndarray]:
    """Per-group mean and t-statistic against zero.

    Args:
        values : Per-observation values.
        groups : Zero-based group index per observation.
        k      : Number of groups.

    Returns:
        (mean, t_statistic) arrays of length k.
    """
    n = np.bincount(groups, minlength=k)
    total = np.bincount(groups, weights=values, minlength=k)
    mean = np.divide(total, n, out=np.zeros(k), where=n > 0)
    sq = np.bincount(groups, weights=values * values, minlength=k)
    var = np.maximum(np.divide(sq, n, out=np.zeros(k), where=n > 0) - mean ** 2, 0.0)
    se = np.divide(np.sqrt(var), np.sqrt(np.maximum(n - 1, 1)), out=np.zeros(k), where=n > 1)
    return mean, np.divide(mean, se, out=np.zeros(k), where=se > 0)


def bh_fdr(pvals: np.ndarray) -> np.ndarray:
    """Benjamini-Hochberg adjusted p-values."""
    order = np.argsort(pvals)
    m = len(pvals)
    adj = pvals[order] * m / (np.arange(m) + 1)
    adj = np.minimum.accumulate(adj[::-1])[::-1]
    out = np.empty(m)
    out[order] = np.minimum(adj, 1.0)
    return out


# %% Validation

def validate_horizon(
    values: np.ndarray,
    groups: np.ndarray,
    k: int,
    keep: np.ndarray,
    horizon: int,
    now: datetime,
    rng: np.random.Generator,
) -> list[dict]:
    """Run the placebo battery and FDR for one horizon.

    Args:
        values  : Per-call excess return, in percent.
        groups  : Zero-based author index per call.
        k       : Number of distinct authors.
        keep    : Boolean mask of authors with enough calls.
        horizon : Horizon in minutes.
        now     : Timestamp for the output rows.
        rng     : Random generator.

    Returns:
        One dict per test, matching the oos_validation schema.
    """
    idx = np.where(keep)[0]
    n_authors = len(idx)
    mean, tstat = group_stats(values, groups, k)

    real = {
        "beat_benchmark": float((mean[idx] > 0).sum()),
        "significant": float((tstat[idx] > 2).sum()),
        "best_author": float(tstat[idx].max()),
    }
    null: dict[str, np.ndarray] = {key: np.empty(N_PERMUTATIONS) for key in real}

    for i in range(N_PERMUTATIONS):
        shuffled = rng.permutation(groups)
        m_, t_ = group_stats(values, shuffled, k)
        null["beat_benchmark"][i] = (m_[idx] > 0).sum()
        null["significant"][i] = (t_[idx] > 2).sum()
        null["best_author"][i] = t_[idx].max()

    # One-sided per-author p-values, then FDR across the whole family.
    pvals = np.array([0.5 * math.erfc(x / 2 ** 0.5) for x in tstat[idx]])
    adjusted = bh_fdr(pvals)

    rows = []
    for name, observed in real.items():
        draws = null[name]
        rows.append({
            "test_name": name,
            "horizon_min": horizon,
            "min_calls": MIN_CALLS,
            "n_authors": n_authors,
            "statistic_real": observed,
            "null_mean": float(draws.mean()),
            "null_p05": float(np.percentile(draws, 5)),
            "null_p95": float(np.percentile(draws, 95)),
            "p_value": float((draws >= observed).mean()),
            "n_permutations": N_PERMUTATIONS,
            "n_raw_p05": int((pvals < 0.05).sum()),
            "n_fdr10": int((adjusted < 0.10).sum()),
            "n_fdr05": int((adjusted < 0.05).sum()),
            "computed_at_utc": now,
        })
    return rows


def run(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Validate every horizon.

    Args:
        conn : Read-only connection to the signal store.

    Returns:
        Polars DataFrame matching the oos_validation schema.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    rng = np.random.default_rng(SEED)
    rows: list[dict] = []

    for horizon in HORIZONS_MIN:
        df = conn.execute(f"""
            SELECT c.author_id, r.r_excess
            FROM call_returns r JOIN calls c USING (call_id)
            WHERE r.horizon_min = {horizon} AND r.r_excess IS NOT NULL
              AND c.author_id IS NOT NULL
        """).pl()
        author = df["author_id"].to_numpy()
        values = df["r_excess"].to_numpy().astype(float) * 100
        _, groups, counts = np.unique(author, return_inverse=True, return_counts=True)
        keep = counts >= MIN_CALLS
        rows.extend(validate_horizon(values, groups, len(counts), keep, horizon, now, rng))
        logger.info("h=%-6d kesz (%d szerzo)", horizon, int(keep.sum()))

    return pl.DataFrame(rows)


def summarise(df: pl.DataFrame) -> None:
    """Print the placebo battery verdict."""
    print("\n=== PLACEBO BATTERY — valodi vs permutalt szerzo-cimkek ===")
    header = (f"{'h':>6s} {'szerzo':>7s} {'teszt':>16s} {'valodi':>9s} "
              f"{'veletlen':>10s} {'[5%..95%]':>18s} {'p':>7s}")
    print(header)
    print("-" * len(header))
    for horizon in HORIZONS_MIN:
        for row in df.filter(pl.col("horizon_min") == horizon).iter_rows(named=True):
            band = f"[{row['null_p05']:.2f}..{row['null_p95']:.2f}]"
            print(f"{horizon:>6d} {row['n_authors']:>7d} {row['test_name']:>16s} "
                  f"{row['statistic_real']:>9.3f} {row['null_mean']:>10.3f} "
                  f"{band:>18s} {row['p_value']:>7.3f}")
        print("-" * len(header))

    print("\n=== EGYENI SZERZOK — Benjamini-Hochberg FDR ===")
    print(f"{'h':>6s} {'szerzo':>7s} {'nyers p<0.05':>13s} {'veletlenul':>11s} "
          f"{'FDR 10%':>8s} {'FDR 5%':>7s}")
    print("-" * 56)
    for horizon in HORIZONS_MIN:
        row = df.filter(pl.col("horizon_min") == horizon).row(0, named=True)
        expected = 0.05 * row["n_authors"]
        print(f"{horizon:>6d} {row['n_authors']:>7d} {row['n_raw_p05']:>13d} "
              f"{expected:>11.1f} {row['n_fdr10']:>8d} {row['n_fdr05']:>7d}")


# %% CLI

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute and report, but do not write the database.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    signal_db = os.path.join(utils._repo_root(), DB_SUBPATH)

    conn = duckdb.connect(signal_db, read_only=True)
    try:
        df = run(conn)
    finally:
        conn.close()

    summarise(df)

    if args.dry_run:
        logger.info("dry-run: nincs iras")
        return 0

    out = get_connection(signal_db)
    try:
        ensure_tables(out)
        upsert_table(out, "oos_validation", df)
    finally:
        out.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
