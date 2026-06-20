"""Smoke tests for strategy.strategy.calibrate.fit_calibration()."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from strategy.strategy.calibrate import fit_calibration

pytestmark = pytest.mark.smoke


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_strategy_table(n: int = 200) -> pd.DataFrame:
    """Return a synthetic strategy_table DataFrame."""
    rng = np.random.default_rng(42)
    open_times = pd.date_range("2025-10-01", periods=n, freq="min")
    return pd.DataFrame({
        "open_time"      : open_times.astype(str),
        "pred_long_raw"  : rng.uniform(0.0, 0.01, n),
        "pred_short_raw" : rng.uniform(0.0, 0.01, n),
        "long_mfe_fw60"  : rng.uniform(0.0, 0.02, n),
        "short_mfe_fw60" : rng.uniform(0.0, 0.02, n),
    })


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_fit_calibration_adds_cal_columns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """fit_calibration() runs without error and adds all expected columns."""
    import utils

    session_id   = "test_calib_session"
    artifact_dir = tmp_path / "artifacts" / session_id
    artifact_dir.mkdir(parents=True)

    # Write synthetic strategy_table.parquet
    df = _make_strategy_table(200)
    parquet_path = artifact_dir / "strategy_table.parquet"
    df.to_parquet(parquet_path, compression="zstd", index=False)

    # Patch utils._resolve_path to point at tmp_path
    monkeypatch.setattr(utils, "_resolve_path", lambda p: str(tmp_path / p))

    # start / end cover first 100 rows (~100 minutes from 2025-10-01)
    iso_long, iso_short = fit_calibration(
        session_id = session_id,
        start      = "2025-10-01",
        end        = "2025-10-01",
    )

    # Reload updated parquet
    updated = pd.read_parquet(parquet_path)

    # Isotonic columns still present
    assert "pred_long_cal"  in updated.columns, "pred_long_cal missing after calibration"
    assert "pred_short_cal" in updated.columns, "pred_short_cal missing after calibration"

    # Rank columns present
    assert "score_pct_long"  in updated.columns, "score_pct_long missing"
    assert "score_pct_short" in updated.columns, "score_pct_short missing"
    assert "bucket_long"     in updated.columns, "bucket_long missing"
    assert "bucket_short"    in updated.columns, "bucket_short missing"

    # Bucket values are ints in 1-10
    assert updated["bucket_long"].between(1, 10).all(),  "bucket_long out of 1-10 range"
    assert updated["bucket_short"].between(1, 10).all(), "bucket_short out of 1-10 range"
    assert updated["bucket_long"].dtype  in (np.dtype("int32"), np.dtype("int64")), \
        "bucket_long not int dtype"
    assert updated["bucket_short"].dtype in (np.dtype("int32"), np.dtype("int64")), \
        "bucket_short not int dtype"

    # Row count unchanged
    assert len(updated) == 200, "Row count changed unexpectedly"
    assert iso_long  is not None
    assert iso_short is not None

    # Rank lookup parquets exist
    lookup_long_path  = artifact_dir / "rank_lookup_long.parquet"
    lookup_short_path = artifact_dir / "rank_lookup_short.parquet"
    assert lookup_long_path.exists(),  "rank_lookup_long.parquet not created"
    assert lookup_short_path.exists(), "rank_lookup_short.parquet not created"

    # Lookup parquet has required columns
    lookup_long = pd.read_parquet(lookup_long_path)
    for col in ("score_raw", "score_pct", "bucket_id"):
        assert col in lookup_long.columns, f"rank_lookup_long missing column: {col}"

    lookup_short = pd.read_parquet(lookup_short_path)
    for col in ("score_raw", "score_pct", "bucket_id"):
        assert col in lookup_short.columns, f"rank_lookup_short missing column: {col}"
