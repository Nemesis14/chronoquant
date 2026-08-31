#!/usr/bin/env python3
"""Rolling out-of-sample author-skill scoring (epic_051 / t5).

Turns per-call drift-corrected returns into a per-author skill estimate, and --
the part that actually answers the epic -- measures whether that estimate
predicts the author's *future* calls.

Why shrinkage
-------------
The median author has 7 usable calls. Ranking on the raw mean therefore ranks
luck: on this data the naive top ten are all authors with one to three calls.
Empirical Bayes pulls each author's mean toward the population mean in inverse
proportion to how much evidence they have:

    shrunk_i = mu + w_i * (mean_i - mu),   w_i = n_i / (n_i + k),   k = sigma2 / tau2

sigma2 is the within-author call variance, tau2 the between-author variance of
true skill. Both are estimated, not assumed. If tau2 collapses to zero there is
no detectable spread in author skill at all, every w_i is zero, and the model
says so by ranking everyone identically -- which is itself a result.

Why rolling out-of-sample
-------------------------
The question is not who did well, but whether whoever did well keeps doing well.
At each as-of date the score uses strictly earlier calls, and the outcome is
measured on the calls that follow it. Evaluation windows are non-overlapping, so
the resulting series is not autocorrelated and t6 can test it honestly.

Outputs author_scores (one row per as-of date, horizon and author, carrying both
the prior score and the realised forward outcome) and score_params (the fitted
empirical-Bayes constants per fit, for diagnostics).

Usage:
    python -m modeling.analyst_signal.score_authors
    python -m modeling.analyst_signal.score_authors --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, date, datetime

import duckdb
import polars as pl

import utils
from data_handling.store.analyst_signal_store import (
    DB_SUBPATH,
    ensure_tables,
    get_connection,
    upsert_table,
)

logger = logging.getLogger(__name__)

#: Primary horizon for the go/no-go decision. The other horizons are computed
#: too, but only as robustness checks -- testing all five as equals would
#: multiply the number of hypotheses and weaken t6's correction.
PRIMARY_HORIZON_MIN = 1440

HORIZONS_MIN = (60, 240, 1440, 4320, 10080)

#: First as-of date. Earlier months carry too few calls to rank anyone.
START_AS_OF = date(2022, 1, 1)

#: An author needs at least this many prior calls to be ranked at all.
MIN_TRAIN_CALLS = 5

#: Size of the top and bottom buckets in the summary.
BUCKET_K = 20


# %% Empirical Bayes

def fit_shrinkage(n: pl.Series, mean: pl.Series, var: pl.Series) -> tuple[float, float, float, float]:
    """Estimate the empirical-Bayes constants for one set of author means.

    Args:
        n    : Calls per author.
        mean : Mean z per author.
        var  : Within-author variance of z per author.

    Returns:
        (grand_mean, sigma2_within, tau2_between, k). k is the shrinkage
        constant; it is inf when no between-author variance is detectable.
    """
    n_arr = n.to_numpy()
    total = int(n_arr.sum())
    grand = float((mean.to_numpy() * n_arr).sum() / total) if total else 0.0

    # Pooled within-author variance, weighted by degrees of freedom.
    dof = n_arr - 1
    usable = dof > 0
    sigma2 = (
        float((var.to_numpy()[usable] * dof[usable]).sum() / dof[usable].sum())
        if usable.any() else 0.0
    )

    # Observed spread of author means overstates true spread by the sampling
    # noise sigma2/n, so subtract it back out.
    observed = float(mean.to_numpy().var(ddof=1)) if len(n_arr) > 1 else 0.0
    noise = float((sigma2 / n_arr).mean()) if sigma2 else 0.0
    tau2 = max(observed - noise, 0.0)

    k = (sigma2 / tau2) if tau2 > 0 else float("inf")
    return grand, sigma2, tau2, k


# %% Scoring

def score_one(
    train: pl.DataFrame,
    forward: pl.DataFrame,
    as_of: date,
    horizon: int,
    now: datetime,
) -> tuple[pl.DataFrame, dict] | None:
    """Score every eligible author at one as-of date and horizon.

    Args:
        train   : Calls strictly before as_of, columns author_id/author_name/z.
        forward : Calls in the evaluation window, same columns.
        as_of   : Evaluation date.
        horizon : Horizon in minutes.
        now     : Timestamp stamped on the output rows.

    Returns:
        (scores, params) or None when nobody is eligible.
    """
    agg = (
        train.group_by("author_id", "author_name")
        .agg(
            pl.len().alias("n_calls"),
            pl.col("z").mean().alias("raw_mean"),
            pl.col("z").var().fill_null(0.0).alias("var_z"),
        )
        .filter(pl.col("n_calls") >= MIN_TRAIN_CALLS)
    )
    if agg.height < 2:
        return None

    grand, sigma2, tau2, k = fit_shrinkage(agg["n_calls"], agg["raw_mean"], agg["var_z"])

    weight = (
        pl.lit(0.0) if k == float("inf")
        else pl.col("n_calls") / (pl.col("n_calls") + k)
    )
    scored = (
        agg.with_columns(weight.alias("shrink_weight"))
        .with_columns(
            (grand + pl.col("shrink_weight") * (pl.col("raw_mean") - grand)).alias("shrunk_score")
        )
        .with_columns(
            pl.col("shrunk_score").rank("ordinal", descending=True).cast(pl.Int32).alias("rank")
        )
    )

    fwd = forward.group_by("author_id").agg(
        pl.len().alias("fwd_n_calls"),
        pl.col("z").mean().alias("fwd_mean_z"),
    )

    out = (
        scored.join(fwd, on="author_id", how="left")
        .with_columns(
            pl.lit(as_of).cast(pl.Date).alias("as_of_date"),
            pl.lit(horizon).cast(pl.Int32).alias("horizon_min"),
            pl.col("fwd_n_calls").fill_null(0).cast(pl.Int32),
            pl.lit(now).cast(pl.Datetime("us")).alias("computed_at_utc"),
            pl.col("n_calls").cast(pl.Int32),
        )
        .select(
            "as_of_date", "horizon_min", "author_id", "author_name",
            "n_calls", "raw_mean", "shrink_weight", "shrunk_score", "rank",
            "fwd_n_calls", "fwd_mean_z", "computed_at_utc",
        )
    )

    params = {
        "as_of_date": as_of,
        "horizon_min": horizon,
        "n_authors": scored.height,
        "n_train_calls": int(agg["n_calls"].sum()),
        "grand_mean": grand,
        "sigma2_within": sigma2,
        "tau2_between": tau2,
        "shrink_k": None if k == float("inf") else k,
        "computed_at_utc": now,
    }
    return out, params


def month_starts(first: date, last: date) -> list[date]:
    """Return the first day of every month in [first, last]."""
    out, y, m = [], first.year, first.month
    while date(y, m, 1) <= last:
        out.append(date(y, m, 1))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def run(conn: duckdb.DuckDBPyConnection) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Compute rolling scores for every horizon.

    Args:
        conn : Read-only connection to the signal store.

    Returns:
        (author_scores, score_params) as Polars DataFrames.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    data = conn.execute("""
        SELECT c.author_id, c.author_name, c.ts_utc, r.horizon_min, r.z
        FROM call_returns r JOIN calls c USING (call_id)
        WHERE r.z IS NOT NULL AND c.author_id IS NOT NULL
    """).pl()

    last_ts = data["ts_utc"].max()
    assert isinstance(last_ts, datetime)
    periods = month_starts(START_AS_OF, last_ts.date())

    all_scores: list[pl.DataFrame] = []
    all_params: list[dict] = []

    for horizon in HORIZONS_MIN:
        subset = data.filter(pl.col("horizon_min") == horizon)
        fitted = 0
        for i, as_of in enumerate(periods[:-1]):
            nxt = periods[i + 1]
            train = subset.filter(pl.col("ts_utc") < pl.lit(as_of).cast(pl.Datetime("us")))
            forward = subset.filter(
                (pl.col("ts_utc") >= pl.lit(as_of).cast(pl.Datetime("us")))
                & (pl.col("ts_utc") < pl.lit(nxt).cast(pl.Datetime("us")))
            )
            result = score_one(train, forward, as_of, horizon, now)
            if result is None:
                continue
            scores, params = result
            all_scores.append(scores)
            all_params.append(params)
            fitted += 1
        logger.info("h=%-6d %d idopont felmerve", horizon, fitted)

    return pl.concat(all_scores), pl.DataFrame(all_params)


# %% Reporting

def _spearman(df: pl.DataFrame) -> float | None:
    """Rank correlation between the prior score and the forward outcome."""
    part = df.filter(pl.col("fwd_n_calls") > 0)
    if part.height < 5:
        return None
    a = part["shrunk_score"].rank().to_numpy()
    b = part["fwd_mean_z"].rank().to_numpy()
    if a.std() == 0 or b.std() == 0:
        return None
    return float(((a - a.mean()) * (b - b.mean())).mean() / (a.std() * b.std()))


def summarise(scores: pl.DataFrame, params: pl.DataFrame) -> None:
    """Print the persistence evidence per horizon."""
    print("\n=== SZERZO-SKILL PERZISZTENCIA (rolling out-of-sample) ===")
    header = (f"{'h':>6s} {'fit':>4s} {'szerzo/fit':>11s} {'tau2>0':>7s} {'k med':>8s} "
              f"{'IC atlag':>9s} {'IC>0':>7s} {'top20':>8s} {'mezony':>8s} {'bottom20':>9s}")
    print(header)
    print("-" * len(header))

    for horizon in HORIZONS_MIN:
        sc = scores.filter(pl.col("horizon_min") == horizon)
        pr = params.filter(pl.col("horizon_min") == horizon)
        if not sc.height:
            continue

        ics = [
            ic for _, grp in sc.group_by("as_of_date")
            if (ic := _spearman(grp)) is not None
        ]
        ic_mean = sum(ics) / len(ics) if ics else float("nan")
        ic_pos = sum(1 for x in ics if x > 0) / len(ics) * 100 if ics else float("nan")

        evaluated = sc.filter(pl.col("fwd_n_calls") > 0)
        top = evaluated.filter(pl.col("rank") <= BUCKET_K)["fwd_mean_z"].mean()
        allm = evaluated["fwd_mean_z"].mean()
        worst = evaluated.group_by("as_of_date").agg(pl.col("rank").max().alias("mx"))
        bot = (
            evaluated.join(worst, on="as_of_date")
            .filter(pl.col("rank") > pl.col("mx") - BUCKET_K)["fwd_mean_z"].mean()
        )

        tau_pos = (pr["tau2_between"] > 0).sum() / pr.height * 100
        k_med = pr["shrink_k"].median()
        tag = " <-- elsodleges" if horizon == PRIMARY_HORIZON_MIN else ""
        print(f"{horizon:>6d} {pr.height:>4d} {pr['n_authors'].mean():>11.0f} "
              f"{tau_pos:>6.0f}% {_fmt(k_med):>8s} {ic_mean:>9.4f} {ic_pos:>6.0f}% "
              f"{_fmt(top):>8s} {_fmt(allm):>8s} {_fmt(bot):>9s}{tag}")

    print("\nIC = Spearman rangkorrelacio a pontszam es a KOVETKEZO havi teljesitmeny kozott.")
    print("Ha nincs perzisztens szerzo-skill, az IC atlaga 0 korul szor, es a")
    print("top20 nem valik el a mezonytol. A dontest a t6 placebo + FDR mondja ki.")


def _fmt(value: object) -> str:
    return f"{value:.4f}" if isinstance(value, (int, float)) else "-"


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
        scores, params = run(conn)
    finally:
        conn.close()

    summarise(scores, params)

    if args.dry_run:
        logger.info("dry-run: nincs iras")
        return 0

    out = get_connection(signal_db)
    try:
        ensure_tables(out)
        upsert_table(out, "author_scores", scores)
        upsert_table(out, "score_params", params)
    finally:
        out.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
