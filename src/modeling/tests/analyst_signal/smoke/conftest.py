"""Synthetic social_hourly generator shared by the t204 leak-safety tests.

No real `social_hourly` data exists yet (t202 is still blocked on the Reddit
dump ingest), so every test in this package runs against synthetic data that
deliberately reproduces the two measured regimes from the t204 ticket:
a dense subreddit (rate in the hundreds/thousands per hour) and a sparse one
(rate 6-12/hour, where Poisson noise alone is +/-29%) -- including hours with
zero activity, which the sparse regime produces routinely.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TypedDict

import numpy as np
import polars as pl


class RegimeSpec(TypedDict, total=False):
    rate_before       : float
    rate_after        : float
    break_at          : datetime | None
    items_per_author  : float
    new_author_frac   : float
    chars_per_item    : float


def make_synthetic_social_hourly(
    start   : datetime,
    end     : datetime,
    regimes : dict[str, RegimeSpec],
    seed    : int = 0,
) -> pl.DataFrame:
    """Generate synthetic hourly Reddit counts for one or more subreddits.

    Each subreddit's Poisson rate follows `rate_before` until `break_at` (if
    given), then jumps to `rate_after` -- simulating regimes like the
    measured 8-16x comment-rate drop from 2021-05 to 2025-01, or a level
    break like the 2023-06 API lockout. A weekly/daily seasonal multiplier is
    layered on top so hour-of-week deseasonalisation has something real to
    remove.

    Args:
        start   : First hour, inclusive.
        end     : Last hour, inclusive.
        regimes : subreddit name -> RegimeSpec. `rate_after`/`break_at`
                   default to no break (constant `rate_before`).
        seed    : RNG seed, for reproducible tests.

    Returns:
        One row per (subreddit, hour) -- including zero-count hours --
        matching the social_hourly schema: ts_utc, subreddit, kind, source,
        n_items, n_authors, n_new_authors, n_chars, ingested_at_utc.
    """
    rng      = np.random.default_rng(seed)
    n_hours  = int((end - start) / timedelta(hours=1)) + 1
    hours    = [start + timedelta(hours=i) for i in range(n_hours)]
    dow      = np.array([h.weekday() for h in hours])   # 0=Mon .. 6=Sun
    hour_of_day = np.array([h.hour for h in hours])
    weekend_mult = np.where(dow >= 5, 0.6, 1.0)
    daytime_mult = 1.0 + 0.3 * np.cos((hour_of_day - 15) / 24 * 2 * np.pi)

    frames: list[pl.DataFrame] = []
    for subreddit, spec in regimes.items():
        rate_before      = spec["rate_before"]
        rate_after       = spec.get("rate_after", rate_before)
        break_at         = spec.get("break_at")
        items_per_author = spec.get("items_per_author", 2.0)
        new_author_frac  = spec.get("new_author_frac", 0.15)
        chars_per_item   = spec.get("chars_per_item", 120.0)

        rates = np.where(
            [(break_at is not None and h >= break_at) for h in hours],
            rate_after, rate_before,
        )
        eff_rate = np.clip(rates * weekend_mult * daytime_mult, 0.01, None)

        n_items   = rng.poisson(eff_rate)
        n_authors = np.minimum(n_items, rng.poisson(np.clip(eff_rate / items_per_author, 0.01, None)))
        n_authors = np.where((n_items > 0) & (n_authors == 0), 1, n_authors)
        n_new_authors = rng.binomial(n_authors, new_author_frac)

        n_chars = n_items * chars_per_item * (1 + rng.normal(0, 0.1, n_hours))
        n_chars = np.clip(n_chars, 0, None).round().astype(np.int64)
        n_chars = np.where(n_items == 0, 0, n_chars)

        frames.append(pl.DataFrame({
            "ts_utc"          : hours,
            "subreddit"       : subreddit,
            "kind"            : "comment",
            "source"          : "dump",
            "n_items"         : n_items.astype(np.int64),
            "n_authors"       : n_authors.astype(np.int64),
            "n_new_authors"   : n_new_authors.astype(np.int64),
            "n_chars"         : n_chars,
            "ingested_at_utc" : datetime(2026, 1, 1),
        }))

    return pl.concat(frames).with_columns(pl.col("ts_utc").cast(pl.Datetime("us")))


#: Dense regime, modelled on measured r/CryptoCurrency (2069/hr in 2021-05,
#: 129/hr in 2025-01 -- a ~16x fall).
DENSE_REGIME: RegimeSpec = {"rate_before": 2000.0, "rate_after": 130.0}

#: Sparse regime, modelled on measured r/BitcoinMarkets (48/hr in 2021-05,
#: 6/hr in 2025-01). At this rate Poisson noise alone is +/-29% and
#: zero-count hours are routine.
SPARSE_REGIME: RegimeSpec = {"rate_before": 10.0, "rate_after": 6.0, "items_per_author": 1.3}
