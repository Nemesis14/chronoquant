"""Triple-barrier labelling (epic_058 / t701).

Answers the question the trade actually asks: starting at bar t, does price
reach the take-profit barrier before the stop-loss barrier, within the holding
window?

    +1  upper barrier touched first   -> long
    -1  lower barrier touched first   -> short
     0  neither touched in `horizon`  -> do not trade

This replaces the continuous `long_mfe_fw240` target, which measured only the
UPSIDE excursion. A model trained on that learned to predict volatility rather
than direction: measured out-of-sample on the top 1% of its own scores, the
maximum downside (-350.6 bps) exceeded the maximum upside (+258.8 bps), while
the actual 4-hour close return was only +20.7 bps.

Two conventions match the strategy simulator exactly, so a label can never
promise something the backtest would not deliver:

* **Intrabar touch decides**, not the close: `high` triggers the upper barrier
  and `low` the lower one.
* **Same-bar conflict resolves to the lower barrier** (conservative), the same
  `same_bar_conflict_rule: sl_first` rule the strategy uses.
"""

import logging

import numpy as np
import polars as pl

logger = logging.getLogger(__name__)

DEFAULT_HORIZON = 240
DEFAULT_BARRIER_BPS = 100.0
# Chunking keeps the working set bounded without changing the result. The batch
# is sized by horizon rather than fixed: a 1440-bar window at 200k rows would
# allocate ~290MB per boolean mask, and there are two of them.
CHUNK_CELLS = 50_000_000

# Volatility-scaled barriers (epic_060). The EWM span is one day of 1m bars:
# long enough that a single spike cannot move the barrier, short enough to
# follow a regime change within days rather than months.
DEFAULT_VOL_SPAN = 1440
DEFAULT_MIN_BPS = 60.0
DEFAULT_MAX_BPS = 400.0


def _chunk_rows(horizon: int) -> int:
    return max(10_000, CHUNK_CELLS // max(horizon, 1))


def triple_barrier_labels(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    horizon: int = DEFAULT_HORIZON,
    up_bps: float | np.ndarray = DEFAULT_BARRIER_BPS,
    dn_bps: float | np.ndarray = DEFAULT_BARRIER_BPS,
    chunk_rows: int | None = None,
) -> np.ndarray:
    """Label each bar by which barrier its forward window touches first.

    Args:
        close: Reference price per bar; barriers are placed around it.
        high: Bar highs, used for the upper barrier.
        low: Bar lows, used for the lower barrier.
        horizon: Forward window in bars (the vertical barrier).
        up_bps: Upper barrier distance in basis points. A scalar places the
            same barrier on every bar; an array of length ``close.size`` places
            a per-bar barrier, which is how a volatility-scaled label is built.
            A NaN entry yields a NaN label -- the bar has no usable barrier.
        dn_bps: Lower barrier distance in basis points, same rules as up_bps.
        chunk_rows: Rows processed per batch; sized from the horizon if omitted.

    Returns:
        Float array of +1.0 / -1.0 / 0.0, with NaN for the final `horizon` bars
        where the window is incomplete. NaN rather than 0.0 on purpose: 0 is a
        real class (timeout), and conflating it with "no data" would silently
        poison the training set.
    """
    rows_per_chunk = chunk_rows or _chunk_rows(horizon)
    n = close.size
    out = np.full(n, np.nan, dtype=np.float64)
    if n <= horizon:
        return out

    m = n - horizon  # labelable rows: 0 .. m-1
    # Row i of these views spans bars i+1 .. i+horizon, i.e. strictly the future.
    high_win = np.lib.stride_tricks.sliding_window_view(high[1:], horizon)
    low_win = np.lib.stride_tricks.sliding_window_view(low[1:], horizon)

    # Broadcasting a scalar to full length keeps one code path for both cases;
    # the array is a view-sized float buffer, negligible next to the windows.
    up_arr = np.broadcast_to(np.asarray(up_bps, dtype="float64"), (n,))
    dn_arr = np.broadcast_to(np.asarray(dn_bps, dtype="float64"), (n,))
    never = horizon  # sentinel: barrier not touched inside the window

    for start in range(0, m, rows_per_chunk):
        stop = min(start + rows_per_chunk, m)
        ref = close[start:stop]
        up_lvl = ref * (1.0 + up_arr[start:stop] / 10_000.0)
        dn_lvl = ref * (1.0 - dn_arr[start:stop] / 10_000.0)

        up_hit = high_win[start:stop] >= up_lvl[:, None]
        dn_hit = low_win[start:stop] <= dn_lvl[:, None]

        first_up = np.where(up_hit.any(axis=1), up_hit.argmax(axis=1), never)
        first_dn = np.where(dn_hit.any(axis=1), dn_hit.argmax(axis=1), never)

        # Strict `<` means a same-bar touch falls through to -1, matching the
        # simulator's sl_first rule.
        label = np.where(first_up < first_dn, 1.0, -1.0)
        label[(first_up == never) & (first_dn == never)] = 0.0
        # A bar whose barrier is undefined must not become a training row.
        label[np.isnan(up_lvl) | np.isnan(dn_lvl)] = np.nan
        out[start:stop] = label

    return out


def horizon_barrier_bps(
    close: np.ndarray,
    horizon: int = DEFAULT_HORIZON,
    k: float = 1.0,
    span: int = DEFAULT_VOL_SPAN,
    min_bps: float = DEFAULT_MIN_BPS,
    max_bps: float = DEFAULT_MAX_BPS,
) -> np.ndarray:
    """Per-bar barrier width, scaled to the volatility the bar was entered in.

    A fixed barrier makes the label mean different things in different eras:
    on this asset the timeout class was 1.6% in 2021 and 39.3% in 2026, so a
    model trained across both learns which era it is looking at rather than
    which way price will go. Scaling the barrier to trailing volatility removes
    that: ``+1`` means "moved one horizon-sigma up first" in every regime.

    The width is ``k * sigma_1m * sqrt(horizon)`` -- the square-root-of-time
    scaling of a per-minute volatility up to the holding window.

    Args:
        close: Reference price per bar.
        horizon: Forward window in bars, used for the sqrt-time scaling.
        k: Multiplier on the horizon sigma. Callers normalise this so the
            median width matches the fixed barrier being compared against.
        span: EWM span of the per-minute volatility estimate, in bars.
        min_bps: Floor. A barrier narrower than this is eaten by the fixed
            per-trade fee no matter how quiet the market is.
        max_bps: Ceiling, so one dislocation cannot place an unreachable
            barrier for the rest of the day.

    Returns:
        Float array of barrier widths in basis points, NaN until the EWM has
        seen a full span. The estimate is lagged one bar: the barrier for bar
        t is known at t-1, so it can be placed at entry without lookahead.
    """
    logret = np.diff(np.log(close), prepend=np.nan)
    sigma = (
        # NaN and null are different things to polars: a NaN propagates through
        # the EWM and blanks the whole series, so the leading gap from `diff`
        # has to become a null before it is ignored.
        pl.Series("r", logret)
        .fill_nan(None)
        .ewm_std(span=span, ignore_nulls=True, min_samples=span)
        .shift(1)  # the barrier for bar t must be knowable at t-1
        .to_numpy()
        .astype("float64")
    )
    width = k * sigma * np.sqrt(horizon) * 10_000.0
    return np.clip(width, min_bps, max_bps)


def normalising_k(
    close: np.ndarray,
    target_median_bps: float = DEFAULT_BARRIER_BPS,
    horizon: int = DEFAULT_HORIZON,
    span: int = DEFAULT_VOL_SPAN,
) -> float:
    """The ``k`` that makes the median scaled barrier match a fixed one.

    Chosen this way the volatility-scaled label is directly comparable to the
    fixed-barrier label it replaces: the same amount of price movement is being
    asked for on average, and the only thing that changed is that the demand
    now follows the regime. Picking k any other way would confound "adaptive
    barrier" with "wider barrier".
    """
    unit = horizon_barrier_bps(close, horizon=horizon, k=1.0, span=span,
                               min_bps=0.0, max_bps=np.inf)
    med = float(np.nanmedian(unit))
    if not np.isfinite(med) or med <= 0:
        raise ValueError("a volatilitas-becsles ures -- tul rovid a sorozat?")
    return target_median_bps / med


def label_column_name(
    horizon: int = DEFAULT_HORIZON,
    barrier_bps: float = DEFAULT_BARRIER_BPS,
) -> str:
    """Canonical column name, e.g. ``tb_label_240_100``."""
    return f"tb_label_{horizon}_{int(barrier_bps)}"


def class_distribution(labels: np.ndarray) -> dict[str, float]:
    """Percentage split across the three classes, ignoring NaN."""
    valid = labels[~np.isnan(labels)]
    if valid.size == 0:
        return {"+1": 0.0, "-1": 0.0, "0": 0.0, "n": 0}
    return {
        "+1": round(float((valid == 1).mean() * 100), 2),
        "-1": round(float((valid == -1).mean() * 100), 2),
        "0": round(float((valid == 0).mean() * 100), 2),
        "n": int(valid.size),
    }
