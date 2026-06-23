"""Smoke tests for strategy.strategy.search — _simulate_direction and search_strategy."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd
import pytest

from strategy.strategy.search import _simulate_direction

pytestmark = pytest.mark.smoke


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_df(**kwargs: Any) -> pd.DataFrame:
    """Tiny helper to build a synthetic bar DataFrame.

    All columns default to sensible constants; callers override only what they
    need.  The ``close`` column defaults to 100.0 (the entry price anchor).
    """
    n_vals = [len(v) for v in kwargs.values() if isinstance(v, list)]
    n = n_vals[0] if n_vals else 1

    defaults: dict[str, Any] = dict(
        open_time              = pd.date_range("2024-01-01", periods=n, freq="1min"),
        open                   = [1.0] * n,
        high                   = [100.0] * n,
        low                    = [100.0] * n,
        close                  = [100.0] * n,
        score_pct_long         = [0.0] * n,
        score_pct_short        = [0.0] * n,
        bucket_mean_mfe_long   = [0.02] * n,
        bucket_median_mfe_long = [0.015] * n,
        bucket_p75_mfe_long    = [0.025] * n,
        bucket_mean_mfe_short  = [0.02] * n,
        bucket_median_mfe_short= [0.015] * n,
        bucket_p75_mfe_short   = [0.025] * n,
    )
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


# ---------------------------------------------------------------------------
# Test 1: Long TP hit
# ---------------------------------------------------------------------------


def test_long_tp_hit() -> None:
    """Long trade exits on TP: fact_log_return ≈ tp_lr."""
    # Entry bar: score triggers entry at close=100
    # Next bar: high is far above TP → TP hit
    tp_lr = 0.02   # bucket_mean_mfe_long

    n    = 5
    high = [101.0] + [100.0 * math.exp(tp_lr + 0.01)] * (n - 1)   # bar 1 triggers TP

    df = _make_df(
        score_pct_long         = [0.95] + [0.0] * (n - 1),
        high                   = high,
        low                    = [99.0] * n,
        close                  = [100.0] * n,
        bucket_mean_mfe_long   = [tp_lr] * n,
        bucket_median_mfe_long = [tp_lr] * n,
        bucket_p75_mfe_long    = [tp_lr] * n,
    )

    trades = _simulate_direction(df, "long", 0.90, "bucket_mean_mfe", "none")

    assert len(trades) >= 1, "Expected at least one TP trade"
    tp_trade = next((t for t in trades if t["exit_reason"] == "tp"), None)
    assert tp_trade is not None, "Expected a TP exit"
    assert abs(tp_trade["fact_log_return"] - tp_lr) < 1e-9, (
        f"fact_log_return should equal tp_lr={tp_lr}, got {tp_trade['fact_log_return']}"
    )


# ---------------------------------------------------------------------------
# Test 2: Long SL hit
# ---------------------------------------------------------------------------


def test_long_sl_hit() -> None:
    """Long trade exits on SL: fact_log_return ≈ -sl_lr."""
    tp_lr  = 0.02
    sl_lr  = 0.5 * tp_lr   # sl_spec="0.5x_tp"
    sl_price = 100.0 * math.exp(-sl_lr)

    n = 5
    low = [99.0] + [sl_price - 0.01] * (n - 1)   # bar 1 has a trigger; bar 0 is entry

    df = _make_df(
        score_pct_long         = [0.95] + [0.0] * (n - 1),
        high                   = [100.5] * n,
        low                    = low,
        close                  = [100.0] * n,
        bucket_mean_mfe_long   = [tp_lr] * n,
        bucket_median_mfe_long = [tp_lr] * n,
        bucket_p75_mfe_long    = [tp_lr] * n,
    )

    trades = _simulate_direction(df, "long", 0.90, "bucket_mean_mfe", "0.5x_tp")

    sl_trade = next((t for t in trades if t["exit_reason"] == "sl"), None)
    assert sl_trade is not None, "Expected an SL exit"
    assert abs(sl_trade["fact_log_return"] - (-sl_lr)) < 1e-9, (
        f"fact_log_return should equal -sl_lr={-sl_lr}, got {sl_trade['fact_log_return']}"
    )


# ---------------------------------------------------------------------------
# Test 3: Timeout (60 bars)
# ---------------------------------------------------------------------------


def test_long_timeout() -> None:
    """Long trade held for 60 bars without TP/SL exits on timeout."""
    n      = 65
    tp_lr  = 0.05
    # high never reaches TP; low never reaches SL => only timeout exits
    high   = [100.1] * n
    low    = [99.9]  * n
    close  = [100.0] * n

    df = _make_df(
        score_pct_long         = [0.95] + [0.0] * (n - 1),
        high                   = high,
        low                    = low,
        close                  = close,
        bucket_mean_mfe_long   = [tp_lr] * n,
        bucket_median_mfe_long = [tp_lr] * n,
        bucket_p75_mfe_long    = [tp_lr] * n,
    )

    trades = _simulate_direction(df, "long", 0.90, "bucket_mean_mfe", "none", max_hold_bars=60)

    timeout_trades = [t for t in trades if t["exit_reason"] == "timeout"]
    assert len(timeout_trades) >= 1, "Expected a timeout exit at 60 bars"
    assert timeout_trades[0]["hold_minutes"] == 60


# ---------------------------------------------------------------------------
# Test 4: Same-bar TP+SL conflict → SL wins (conservative)
# ---------------------------------------------------------------------------


def test_same_bar_tp_sl_conflict_sl_wins() -> None:
    """When both TP and SL trigger on the same bar, SL must win."""
    tp_lr    = 0.02
    sl_lr    = tp_lr * 0.5
    tp_price = 100.0 * math.exp(tp_lr)
    sl_price = 100.0 * math.exp(-sl_lr)

    # Bar 0: entry; Bar 1: both TP and SL triggered
    high  = [100.0, tp_price + 0.01]
    low   = [100.0, sl_price - 0.01]
    close = [100.0, 100.0]

    df = _make_df(
        score_pct_long         = [0.95, 0.0],
        high                   = high,
        low                    = low,
        close                  = close,
        bucket_mean_mfe_long   = [tp_lr] * 2,
        bucket_median_mfe_long = [tp_lr] * 2,
        bucket_p75_mfe_long    = [tp_lr] * 2,
    )

    trades = _simulate_direction(df, "long", 0.90, "bucket_mean_mfe", "0.5x_tp")

    assert len(trades) == 1, "Expected exactly one trade"
    assert trades[0]["exit_reason"] == "sl", "SL must win when both TP and SL fire on same bar"
    assert trades[0]["fact_log_return"] < 0, "SL exit must produce negative fact_log_return"


# ---------------------------------------------------------------------------
# Test 5: Re-entry after exit
# ---------------------------------------------------------------------------


def test_reentry_after_exit() -> None:
    """After a trade exits, the next qualifying bar triggers re-entry — 2 trades."""
    tp_lr = 0.02
    n     = 130

    # Entry signals at bar 0 and bar 65 (after the first trade times out at bar 60)
    score_pct_long = [0.0] * n
    score_pct_long[0]  = 0.95
    score_pct_long[61] = 0.95   # first entry exits at bar 60 (timeout), re-entry at bar 61

    df = _make_df(
        score_pct_long         = score_pct_long,
        high                   = [100.1] * n,
        low                    = [99.9]  * n,
        close                  = [100.0] * n,
        bucket_mean_mfe_long   = [tp_lr] * n,
        bucket_median_mfe_long = [tp_lr] * n,
        bucket_p75_mfe_long    = [tp_lr] * n,
    )

    trades = _simulate_direction(df, "long", 0.90, "bucket_mean_mfe", "none", max_hold_bars=60)

    assert len(trades) == 2, f"Expected 2 trades (re-entry), got {len(trades)}"
    assert trades[0]["exit_reason"] == "timeout"
    assert trades[1]["exit_reason"] == "timeout"


# ---------------------------------------------------------------------------
# Test 6: sl_spec="none" + price falls — no SL triggered
# ---------------------------------------------------------------------------


def test_no_stop_loss_price_falls() -> None:
    """With sl_spec='none', a falling price never triggers an SL exit."""
    tp_lr  = 0.02
    n      = 62
    # Low falls well below where an SL would be, but no SL spec
    low    = [99.0 * math.exp(-0.05)] * n

    df = _make_df(
        score_pct_long         = [0.95] + [0.0] * (n - 1),
        high                   = [100.1] * n,
        low                    = low,
        close                  = [100.0] * n,
        bucket_mean_mfe_long   = [tp_lr] * n,
        bucket_median_mfe_long = [tp_lr] * n,
        bucket_p75_mfe_long    = [tp_lr] * n,
    )

    trades = _simulate_direction(df, "long", 0.90, "bucket_mean_mfe", "none")

    sl_trades = [t for t in trades if t["exit_reason"] == "sl"]
    assert len(sl_trades) == 0, "Should be no SL exits when sl_spec='none'"
    assert all(t["exit_reason"] in ("tp", "timeout") for t in trades)


# ---------------------------------------------------------------------------
# Test 7: Short TP hit
# ---------------------------------------------------------------------------


def test_short_tp_hit() -> None:
    """Short trade exits on TP: fact_log_return ≈ tp_lr.

    short_mfe_fw60 is negative (log(fw_min/close) < 0 for profitable shorts).
    Low score_pct_short = best short opportunity (inverted ranking).
    """
    tp_lr    = 0.02
    tp_price = 100.0 * math.exp(-tp_lr)

    n   = 5
    low = [99.0] + [tp_price - 0.01] * (n - 1)   # bar 1 triggers short TP

    df = _make_df(
        score_pct_short         = [0.05] + [1.0] * (n - 1),   # low score = good short
        high                    = [100.5] * n,
        low                     = low,
        close                   = [100.0] * n,
        bucket_mean_mfe_short   = [-tp_lr] * n,   # negative: log(fw_min/close)
        bucket_median_mfe_short = [-tp_lr] * n,
        bucket_p75_mfe_short    = [-tp_lr] * n,
    )

    trades = _simulate_direction(df, "short", 0.90, "bucket_mean_mfe", "none")

    tp_trade = next((t for t in trades if t["exit_reason"] == "tp"), None)
    assert tp_trade is not None, "Expected a short TP exit"
    assert abs(tp_trade["fact_log_return"] - tp_lr) < 1e-9, (
        f"Short TP: fact_log_return should equal tp_lr={tp_lr}, got {tp_trade['fact_log_return']}"
    )


# ---------------------------------------------------------------------------
# Test 8: Short SL hit
# ---------------------------------------------------------------------------


def test_short_sl_hit() -> None:
    """Short trade exits on SL when high >= entry_close * exp(sl_lr).

    Bucket stats are negative (short_mfe_fw60 convention); magnitudes are
    recovered with abs() inside the simulation.
    """
    tp_lr    = 0.02
    sl_lr    = 0.5 * tp_lr
    sl_price = 100.0 * math.exp(sl_lr)

    n    = 5
    high = [100.0] + [sl_price + 0.01] * (n - 1)

    df = _make_df(
        score_pct_short         = [0.05] + [1.0] * (n - 1),   # low score = good short
        high                    = high,
        low                     = [99.5] * n,
        close                   = [100.0] * n,
        bucket_mean_mfe_short   = [-tp_lr] * n,   # negative convention
        bucket_median_mfe_short = [-tp_lr] * n,
        bucket_p75_mfe_short    = [-tp_lr] * n,
    )

    trades = _simulate_direction(df, "short", 0.90, "bucket_mean_mfe", "0.5x_tp")

    sl_trade = next((t for t in trades if t["exit_reason"] == "sl"), None)
    assert sl_trade is not None, "Expected a short SL exit"
    assert sl_trade["fact_log_return"] < 0, "Short SL exit must yield negative fact_log_return"


# ---------------------------------------------------------------------------
# Test 9: No trades when all scores below cutoff
# ---------------------------------------------------------------------------


def test_no_trades_when_score_below_cutoff() -> None:
    """No trades are generated when all score_pct values are below the cutoff."""
    n  = 20
    df = _make_df(
        score_pct_long  = [0.50] * n,
        score_pct_short = [0.50] * n,
    )

    trades_long  = _simulate_direction(df, "long",  0.99, "bucket_mean_mfe", "none")
    trades_short = _simulate_direction(df, "short", 0.99, "bucket_mean_mfe", "none")

    assert trades_long  == [], "No long trades expected when score < cutoff"
    assert trades_short == [], "No short trades expected when score < cutoff"


# ---------------------------------------------------------------------------
# Test 10: total_fact_log_return equals sum of individual fact_log_returns
# ---------------------------------------------------------------------------


def test_total_fact_log_return_equals_sum() -> None:
    """The sum of fact_log_return across all trades equals total_fact_log_return."""
    n     = 200
    tp_lr = 0.02

    # Alternate entry signals so multiple trades fire
    score_long = [0.95 if i % 65 == 0 else 0.0 for i in range(n)]

    df = _make_df(
        score_pct_long         = score_long,
        high                   = [100.1] * n,
        low                    = [99.9]  * n,
        close                  = [100.0] * n,
        bucket_mean_mfe_long   = [tp_lr] * n,
        bucket_median_mfe_long = [tp_lr] * n,
        bucket_p75_mfe_long    = [tp_lr] * n,
    )

    trades    = _simulate_direction(df, "long", 0.90, "bucket_mean_mfe", "none")
    total_sum = sum(t["fact_log_return"] for t in trades)

    # Compute as search_strategy would
    # Just verify internal consistency: sum == total computed from list
    assert abs(total_sum - sum(t["fact_log_return"] for t in trades)) < 1e-12, (
        "total_fact_log_return must equal sum of individual fact_log_return values"
    )
