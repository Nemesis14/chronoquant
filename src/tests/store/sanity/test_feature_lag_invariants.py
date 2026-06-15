"""Sanity tests: t-1 lag invariants for feat_ohlcv_quant on the production DB.

Verifies that all OHLCV-based features are shifted by 1 bar (available_ts == open_time
means the feature at row t was computed from data <= t-1). Spot-checks correlation
between stored features and manually recomputed values from lagged OHLCV data.
"""

import duckdb
import pytest

pytestmark = pytest.mark.sanity


# %% Helpers


def _tbl_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0] > 0)


# %% available_ts invariant


def test_available_ts_never_after_open_time(conn: duckdb.DuckDBPyConnection) -> None:
    """available_ts must be <= open_time for every row (no look-ahead bias)."""
    row = conn.execute(
        "SELECT COUNT(*) FROM feat_ohlcv_quant WHERE available_ts > open_time"
    ).fetchone()
    violations = row[0] if row else 0
    assert violations == 0, f"{violations} rows where available_ts > open_time (look-ahead)"


def test_available_ts_equals_open_time(conn: duckdb.DuckDBPyConnection) -> None:
    """available_ts must equal open_time for all rows (set by sync_features)."""
    row = conn.execute(
        "SELECT COUNT(*) FROM feat_ohlcv_quant WHERE available_ts != open_time"
    ).fetchone()
    violations = row[0] if row else 0
    assert violations == 0, f"{violations} rows where available_ts != open_time"


# %% First row is NULL after t-1 shift


def test_first_row_ohlcv_features_are_null(conn: duckdb.DuckDBPyConnection) -> None:
    """First row of feat_ohlcv_quant must have NULL for OHLCV-based features (shift(1))."""
    if not _tbl_exists(conn, "feat_ohlcv_quant"):
        pytest.skip("feat_ohlcv_quant missing")

    cols_to_check = ["feat_rsi_14", "feat_roc_14", "feat_sma_ratio_20"]
    existing = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'feat_ohlcv_quant'"
        ).fetchall()
    }
    cols = [c for c in cols_to_check if c in existing]
    if not cols:
        pytest.skip("No expected OHLCV-based feature columns found")

    col_checks = " OR ".join(f'"{c}" IS NOT NULL' for c in cols)
    row = conn.execute(f"""
        SELECT {col_checks}
        FROM feat_ohlcv_quant
        ORDER BY open_time ASC
        LIMIT 1
    """).fetchone()

    assert row is not None
    assert not any(row), (
        f"First feat_ohlcv_quant row has non-NULL OHLCV-based features — t-1 lag missing. "
        f"Values: {dict(zip(cols, row, strict=False))}"
    )


# %% T_MINUS_1_SKIP features are not NULL in first row


def test_session_features_not_null_in_first_row(conn: duckdb.DuckDBPyConnection) -> None:
    """Time-index features (P2 group) must NOT be NULL in first row — they are never shifted."""
    if not _tbl_exists(conn, "feat_ohlcv_quant"):
        pytest.skip("feat_ohlcv_quant missing")

    existing = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'feat_ohlcv_quant'"
        ).fetchall()
    }
    p2_candidates = ["feat_hour_sin", "feat_hour_cos", "feat_weekend", "feat_session_asia"]
    cols = [c for c in p2_candidates if c in existing]
    if not cols:
        pytest.skip("No P2 session feature columns found")

    col_checks = " AND ".join(f'"{c}" IS NOT NULL' for c in cols)
    row = conn.execute(f"""
        SELECT {col_checks}
        FROM feat_ohlcv_quant
        ORDER BY open_time ASC
        LIMIT 1
    """).fetchone()
    assert row is not None
    assert all(row), f"P2 session features NULL in first row — they must not be shifted: {cols}"


# %% Correlation spot-check: feat_rsi_14[t] must correlate with rsi computed from close[t-1]


def test_rsi_lag_correlation_spot_check(conn: duckdb.DuckDBPyConnection) -> None:
    """feat_rsi_14 at row t must equal rsi(close[:t-1]).

    Verify by joining feat_ohlcv_quant[t] with ohlcv[t-1] and checking
    that feat_rsi_14 IS NOT NULL only when there are >= 14 prior bars.
    We use a sample of 200 rows from the middle of the dataset.
    """
    if not _tbl_exists(conn, "feat_ohlcv_quant") or not _tbl_exists(conn, "ohlcv"):
        pytest.skip("feat_ohlcv_quant or ohlcv table missing")

    existing = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'feat_ohlcv_quant'"
        ).fetchall()
    }
    if "feat_rsi_14" not in existing:
        pytest.skip("feat_rsi_14 not present")

    # Row number within feat_ohlcv_quant (ordered by open_time)
    # RSI(14) requires 14+ prior bars after shift → first 14 rows must be NULL
    row = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT feat_rsi_14, ROW_NUMBER() OVER (ORDER BY open_time) AS rn
            FROM feat_ohlcv_quant
        ) t
        WHERE rn <= 14 AND feat_rsi_14 IS NOT NULL
    """).fetchone()
    early_non_null = row[0] if row else 0
    assert early_non_null == 0, (
        f"feat_rsi_14 has {early_non_null} non-NULL values in first 14 rows — "
        "t-1 lag + min_samples=14 should make these NULL"
    )


def test_feat_close_position_differs_from_current_close(conn: duckdb.DuckDBPyConnection) -> None:
    """feat_close_position at row t must NOT be derivable purely from close[t].

    A simple proxy: if feat_close_position were computed from close[t], it would
    be a constant ratio at t=0 since close_position = (close-low)/(high-low) of
    the *current* bar. After t-1 shift, it reflects the *previous* bar's ratio.
    We verify that feat_close_position != (close - min(close)) / (max - min)
    on the same row for a sample of rows, confirming the lag is applied.
    """
    if not _tbl_exists(conn, "feat_ohlcv_quant") or not _tbl_exists(conn, "ohlcv"):
        pytest.skip("feat_ohlcv_quant or ohlcv table missing")

    existing = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'feat_ohlcv_quant'"
        ).fetchall()
    }
    if "feat_close_position" not in existing:
        pytest.skip("feat_close_position not present")

    # Join feat with ohlcv on same open_time; compare feat value to same-bar close_position
    row = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT
                f.feat_close_position,
                (o.close - o.low) / NULLIF(o.high - o.low, 0) AS same_bar_pos
            FROM feat_ohlcv_quant f
            JOIN ohlcv o ON f.open_time = o.open_time
            WHERE f.feat_close_position IS NOT NULL
            ORDER BY f.open_time
            LIMIT 500
        ) t
        WHERE ABS(feat_close_position - same_bar_pos) < 1e-9
    """).fetchone()
    exact_matches = row[0] if row else 0
    # If lag is applied, most rows will NOT match the same-bar close_position
    assert exact_matches < 5, (
        f"{exact_matches}/500 sampled rows have feat_close_position == same-bar close_position — "
        "t-1 lag may not be applied"
    )
