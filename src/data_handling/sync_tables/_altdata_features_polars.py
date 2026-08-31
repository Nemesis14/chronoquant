"""Alt-data derivatives features on the 1-minute grid (epic_054 / t308).

Computes the 22 modelling variables from `altdata_5m` and maps them onto the
1-minute grid used by `feat_ohlcv_quant`, so they can join into `quant_train`
like any other feature.

Three rules drive every decision here:

1. **No window exceeds 240 minutes**, normalization included. The project's
   `feature_lookback_embargo_minutes` is 240, so a longer trailing baseline
   (a 30-day z-score, say) would reach behind the embargo. Where a long
   baseline would normally be used, a dimensionless ratio takes its place --
   `oi_to_volume`, `retail_vs_top`, `basis` are all trend-free by construction.

2. **A 1-minute bar only sees COMPLETED 5-minute bars.** The 5m bar opening at
   T covers [T, T+5m), so its value is known only at T+5m. Each 5m row is
   therefore stamped `available_at = open_time + 5m` and joined backward onto
   the minute grid. That is strictly stronger than the project's t-1 lag rule.

3. **The 5m grid is upsampled to be gap-free before any rolling window.**
   Row-based windows silently mean the wrong time span across a missing bar;
   on a complete grid they do not.
"""

import logging

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

# Window sizes in 5-minute bars.
W_15M = 3
W_60M = 12
W_240M = 48

# Binance's baseline funding component: 0.01% per 8h. Funding resting exactly
# here means neither side is paying a premium, so deviation is the signal.
FUNDING_BASELINE = 0.0001

ALTDATA_FEATURES = [
    # A) leverage stock
    "feat_oi_to_volume_60",
    "feat_oi_delta_15",
    "feat_oi_delta_60",
    "feat_oi_delta_240",
    "feat_oi_accel_60",
    "feat_oi_unwind_60",
    "feat_oi_price_div_60",
    # B) positioning skew
    "feat_ls_retail_vs_top_60",
    "feat_ls_count_z_240",
    "feat_ls_top_sum_z_240",
    "feat_ls_top_conc_60",
    "feat_ls_count_delta_60",
    # C) price of leverage
    "feat_prem_mean_15",
    "feat_prem_mean_60",
    "feat_prem_std_60",
    "feat_prem_pctile_240",
    "feat_funding_countdown",
    "feat_funding_dev",
    # D) spot vs perp
    "feat_basis_60",
    "feat_basis_z_240",
    "feat_spot_perp_vol_ratio_60",
    "feat_spot_taker_imb_60",
    # E) order-book depth (epic_058; bookDepth archive, 2023-01 onwards)
    "feat_depth_imb_1",
    "feat_depth_imb_5",
    "feat_depth_total_z_240",
    "feat_depth_slope",
    "feat_depth_drain_60",
]


def _safe_log(col: pl.Expr) -> pl.Expr:
    """log() of a ratio column, nulling out non-positive values.

    The long/short ratios are strictly positive in practice, but an archive
    gap can surface as 0.0; log(0) would poison the whole rolling window.
    """
    return pl.when(col > 0).then(col.log()).otherwise(None)


def _zscore(col: pl.Expr, window: int) -> pl.Expr:
    """Trailing z-score over `window` bars, inclusive of the current bar."""
    mean = col.rolling_mean(window_size=window, min_samples=window // 2)
    std = col.rolling_std(window_size=window, min_samples=window // 2)
    return pl.when(std > 0).then((col - mean) / std).otherwise(None)


def _rolling_pctile_rank(values: np.ndarray, window: int) -> np.ndarray:
    """Percentile rank of each value within its trailing window.

    Done in numpy rather than Polars' rolling_map: the latter calls back into
    Python per window, which is minutes of runtime over half a million rows.
    """
    n = values.size
    out = np.full(n, np.nan, dtype=np.float64)
    if n < window:
        return out

    windows = np.lib.stride_tricks.sliding_window_view(values, window)
    current = values[window - 1:]
    with np.errstate(invalid="ignore"):
        valid = ~np.isnan(windows)
        below = np.nansum(windows <= current[:, None], axis=1)
        count = valid.sum(axis=1)
    ranks = np.where(count > 0, below / np.maximum(count, 1), np.nan)
    ranks[np.isnan(current)] = np.nan
    out[window - 1:] = ranks
    return out


def compute_altdata_features(altdata_5m: pl.DataFrame) -> pl.DataFrame:
    """Compute the 22 alt-data features on the 5-minute grid.

    Args:
        altdata_5m: Raw frame as stored in the `altdata_5m` table.

    Returns:
        Frame with `open_time`, `available_at` and the 22 feature columns.
    """
    df = altdata_5m.sort("open_time").upsample(time_column="open_time", every="5m")

    basis = (pl.col("perp_close") - pl.col("spot_close")) / pl.col("spot_close")

    df = df.with_columns(
        # --- A) leverage stock -------------------------------------------
        (pl.col("oi_coin") / pl.col("perp_volume").rolling_sum(W_60M, min_samples=W_60M // 2))
        .alias("feat_oi_to_volume_60"),
        (pl.col("oi_coin") / pl.col("oi_coin").shift(W_15M) - 1).alias("feat_oi_delta_15"),
        (pl.col("oi_coin") / pl.col("oi_coin").shift(W_60M) - 1).alias("feat_oi_delta_60"),
        (pl.col("oi_coin") / pl.col("oi_coin").shift(W_240M) - 1).alias("feat_oi_delta_240"),
        (
            pl.col("oi_coin").rolling_min(W_60M, min_samples=W_60M // 2)
            / pl.col("oi_coin").rolling_max(W_60M, min_samples=W_60M // 2)
        ).alias("feat_oi_unwind_60"),
        # --- B) positioning skew ------------------------------------------
        (_safe_log(pl.col("ls_count")) - _safe_log(pl.col("ls_top_sum")))
        .rolling_mean(W_60M, min_samples=W_60M // 2)
        .alias("feat_ls_retail_vs_top_60"),
        _zscore(pl.col("ls_count"), W_240M).alias("feat_ls_count_z_240"),
        _zscore(pl.col("ls_top_sum"), W_240M).alias("feat_ls_top_sum_z_240"),
        (_safe_log(pl.col("ls_top_count")) - _safe_log(pl.col("ls_top_sum")))
        .rolling_mean(W_60M, min_samples=W_60M // 2)
        .alias("feat_ls_top_conc_60"),
        (pl.col("ls_count") - pl.col("ls_count").shift(W_60M)).alias("feat_ls_count_delta_60"),
        # --- C) price of leverage -----------------------------------------
        pl.col("prem_close").rolling_mean(W_15M, min_samples=1).alias("feat_prem_mean_15"),
        pl.col("prem_close").rolling_mean(W_60M, min_samples=W_60M // 2).alias("feat_prem_mean_60"),
        pl.col("prem_close").rolling_std(W_60M, min_samples=W_60M // 2).alias("feat_prem_std_60"),
        # The funding schedule is published ahead of time, so the countdown to
        # the next settlement is knowable at t -- this is not lookahead. Note
        # the interval is not always 8h: SOLUSDT has had 2h and 4h stretches.
        ((pl.col("funding_next_ts") - pl.col("open_time")).dt.total_minutes())
        .cast(pl.Float64)
        .alias("feat_funding_countdown"),
        (pl.col("funding_last") - FUNDING_BASELINE).alias("feat_funding_dev"),
        # --- D) spot vs perp ----------------------------------------------
        basis.rolling_mean(W_60M, min_samples=W_60M // 2).alias("feat_basis_60"),
        (
            pl.col("spot_volume").rolling_sum(W_60M, min_samples=W_60M // 2)
            / pl.col("perp_volume").rolling_sum(W_60M, min_samples=W_60M // 2)
        ).alias("feat_spot_perp_vol_ratio_60"),
        (
            pl.col("spot_taker_buy").rolling_sum(W_60M, min_samples=W_60M // 2)
            / pl.col("spot_volume").rolling_sum(W_60M, min_samples=W_60M // 2)
        ).alias("feat_spot_taker_imb_60"),
    )

    # --- E) order-book depth ------------------------------------------
    # The most direct read on how far a market order can push price: a thin
    # book means the same flow moves further. NULL before 2023-01, where the
    # bookDepth archive starts.
    total_1 = pl.col("bid_1") + pl.col("ask_1")
    total_5 = pl.col("bid_5") + pl.col("ask_5")
    df = df.with_columns(
        # Which side has less resistance. Positive = thinner asks above.
        ((pl.col("bid_1") - pl.col("ask_1")) / total_1).alias("feat_depth_imb_1"),
        ((pl.col("bid_5") - pl.col("ask_5")) / total_5).alias("feat_depth_imb_5"),
        # How fast the book thins out: a steep drop means gappy price action.
        (total_5 / total_1).alias("feat_depth_slope"),
    )
    df = df.with_columns(
        _zscore(total_1, W_240M).alias("feat_depth_total_z_240"),
        # Market makers pull quotes ahead of a move; a draining book is the
        # clearest leading signal the order book offers.
        (total_1 / total_1.shift(W_60M) - 1).alias("feat_depth_drain_60"),
    )

    # Second pass: features derived from first-pass columns.
    perp_ret_60 = pl.col("perp_close") / pl.col("perp_close").shift(W_60M) - 1
    df = df.with_columns(
        (pl.col("feat_oi_delta_60") - pl.col("feat_oi_delta_60").shift(W_60M))
        .alias("feat_oi_accel_60"),
        # Signed leverage flow: positive means leverage is building WITH the
        # move (new money, continuation); negative means the move is driven by
        # closing positions (short covering or long capitulation).
        (pl.col("feat_oi_delta_60") * perp_ret_60.sign()).alias("feat_oi_price_div_60"),
        _zscore(pl.col("feat_basis_60"), W_240M).alias("feat_basis_z_240"),
    )

    prem = df["prem_close"].to_numpy().astype(np.float64)
    df = df.with_columns(
        pl.Series("feat_prem_pctile_240", _rolling_pctile_rank(prem, W_240M))
    )

    # NaN is not NULL in Polars, and a division by a zero-volume window can
    # produce either NaN or +/-inf. Both would be counted as present values by
    # SQL count() and would misreport coverage, so normalize them to NULL --
    # the ticket's "warmup is NULL, not 0" rule applies to NaN just as much.
    df = df.with_columns(
        pl.when(pl.col(c).is_finite()).then(pl.col(c)).otherwise(None).alias(c)
        for c in ALTDATA_FEATURES
    )

    # A 5m bar closes 5 minutes after it opens; only then is it usable.
    df = df.with_columns(
        (pl.col("open_time") + pl.duration(minutes=5)).alias("available_at")
    )
    return df.select(["open_time", "available_at", *ALTDATA_FEATURES])


def expand_to_minute_grid(
    features_5m: pl.DataFrame,
    minute_index: pl.DataFrame,
) -> pl.DataFrame:
    """Map 5-minute features onto the 1-minute grid via a backward as-of join.

    Args:
        features_5m: Output of `compute_altdata_features`.
        minute_index: Single-column frame of the target `open_time` minutes.

    Returns:
        `minute_index` widened with the 22 feature columns; minutes before the
        first completed 5m bar stay NULL rather than 0.
    """
    src = features_5m.drop("open_time").sort("available_at")
    return (
        minute_index.sort("open_time")
        .join_asof(src, left_on="open_time", right_on="available_at", strategy="backward")
        .drop("available_at")
        .select(["open_time", *ALTDATA_FEATURES])
    )
