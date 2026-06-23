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


def _make_scored_table(n: int = 200) -> pd.DataFrame:
    """Return a synthetic scored table (output of build_scored_table)."""
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
    """fit_calibration() returns a calibrated DataFrame with all expected columns."""
    import utils

    session_id = "test_calib_session"

    # Patch utils._resolve_path to point at tmp_path (artifact files land here)
    monkeypatch.setattr(utils, "_resolve_path", lambda p: str(tmp_path / p))

    scored = _make_scored_table(200)
    calibrated, iso_long, iso_short = fit_calibration(
        session_id = session_id,
        scored_df  = scored,
        start      = "2025-10-01",
        end        = "2025-10-01",
    )

    # Isotonic columns present
    assert "pred_long_cal"  in calibrated.columns, "pred_long_cal missing after calibration"
    assert "pred_short_cal" in calibrated.columns, "pred_short_cal missing after calibration"

    # Rank columns present
    assert "score_pct_long"  in calibrated.columns, "score_pct_long missing"
    assert "score_pct_short" in calibrated.columns, "score_pct_short missing"
    assert "bucket_long"     in calibrated.columns, "bucket_long missing"
    assert "bucket_short"    in calibrated.columns, "bucket_short missing"

    # Bucket values are ints in 1-10
    assert calibrated["bucket_long"].between(1, 10).all(),  "bucket_long out of 1-10 range"
    assert calibrated["bucket_short"].between(1, 10).all(), "bucket_short out of 1-10 range"
    assert calibrated["bucket_long"].dtype  in (np.dtype("int32"), np.dtype("int64")), \
        "bucket_long not int dtype"
    assert calibrated["bucket_short"].dtype in (np.dtype("int32"), np.dtype("int64")), \
        "bucket_short not int dtype"

    # Row count unchanged
    assert len(calibrated) == 200, "Row count changed unexpectedly"
    assert iso_long  is not None
    assert iso_short is not None

    # Rank lookup parquets exist
    artifact_dir      = tmp_path / "artifacts" / session_id
    lookup_long_path  = artifact_dir / "rank_lookup_long.parquet"
    lookup_short_path = artifact_dir / "rank_lookup_short.parquet"
    assert lookup_long_path.exists(),  "rank_lookup_long.parquet not created"
    assert lookup_short_path.exists(), "rank_lookup_short.parquet not created"

    # isotonic pkl files exist
    assert (artifact_dir / "isotonic_long.pkl").exists(),  "isotonic_long.pkl not created"
    assert (artifact_dir / "isotonic_short.pkl").exists(), "isotonic_short.pkl not created"

    # New bucket stats columns present on calibrated df
    for col in (
        "bucket_mean_mfe_long", "bucket_mean_mfe_short",
        "bucket_median_mfe_long", "bucket_median_mfe_short",
        "bucket_p75_mfe_long", "bucket_p75_mfe_short",
    ):
        assert col in calibrated.columns, f"calibrated df missing column: {col}"

    # Lookup parquet has required columns including new stats
    lookup_long = pd.read_parquet(lookup_long_path)
    for col in ("score_raw", "score_pct", "bucket_id", "bucket_median_mfe", "bucket_p75_mfe"):
        assert col in lookup_long.columns, f"rank_lookup_long missing column: {col}"
