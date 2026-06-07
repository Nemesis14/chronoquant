# =============================================================================
# Elliott event study tests
# =============================================================================
# Purpose:
#  - Verify point-in-time pivot confirmation
#  - Verify long nested 1-2-1-2 setup detection and outcome labeling
# =============================================================================

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from analysis.elliott_event_study import (  # noqa: E402
    add_forward_outcomes,
    detect_confirmed_pivots,
    detect_long_1212_setups,
    detect_taew_long_1212_setups,
)


def _bars_from_closes(closes: list[float], freq: str = "1h") -> pd.DataFrame:
    times = pd.date_range("2025-01-01 00:00:00", periods=len(closes), freq=freq)
    return pd.DataFrame(
        {
            "open_time": times.strftime("%Y-%m-%d %H:%M:%S"),
            "open": closes,
            "high": [c * 1.001 for c in closes],
            "low": [c * 0.999 for c in closes],
            "close": closes,
            "volume": 1.0,
        }
    )


def test_detect_long_nested_1212_setup() -> None:
    closes = [
        100.0,
        103.0,
        110.0,
        105.0,
        103.0,
        106.0,
        108.0,
        105.5,
        104.5,
        107.0,
    ]
    df = _bars_from_closes(closes)

    pivots = detect_confirmed_pivots(df, threshold=0.02)
    events = detect_long_1212_setups(pivots, timeframe="1h", threshold=0.02)

    nested = events[events["event_type"] == "wave3_setup_nested_1212"]
    assert not nested.empty
    assert set(events["direction"]) == {"long"}
    assert set(events["expected_wave"]) == {"3"}
    assert nested["open_time"].iloc[0] == "2025-01-01 09:00:00"


def test_taew_detector_returns_comparable_frame() -> None:
    closes = [
        100.0,
        104.0,
        107.0,
        105.0,
        103.2,
        106.0,
        109.0,
        107.0,
        105.7,
        108.0,
    ]
    df = _bars_from_closes(closes)

    events = detect_taew_long_1212_setups(df, timeframe="1h")

    assert set(events.columns).issuperset({"event_id", "engine", "event_type", "direction"})
    if not events.empty:
        assert set(events["engine"]) == {"taew"}
        assert set(events["direction"]) == {"long"}


def test_add_forward_outcomes_uses_raw_price_movement() -> None:
    events = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "open_time": "2025-01-01 00:00:00",
                "asset_id": "solusdt_fw60",
                "timeframe": "1h",
                "engine": "test",
                "engine_version": "v1",
                "profile_id": "test",
                "event_type": "wave3_setup_nested_1212",
                "direction": "long",
                "setup_score": 1.0,
                "expected_wave": "3",
                "confirmation_lag_bars": 1,
                "source_pivots_json": "[]",
                "diagnostics_json": "{}",
            }
        ]
    )
    times = pd.date_range("2025-01-01 00:00:00", periods=61, freq="min")
    close = [100.0] + [100.5] * 59 + [101.0]
    high = [100.0] + [101.5] * 60
    df_1m = pd.DataFrame(
        {
            "open_time": times,
            "open": close,
            "high": high,
            "low": [99.5] * 61,
            "close": close,
            "volume": 1.0,
        }
    )

    labeled = add_forward_outcomes(events, df_1m, raw_up_threshold=0.01)

    assert labeled["price_up_60m"].iloc[0] == 1
    assert labeled["raw_up_hit_60m"].iloc[0] == 1
    assert round(labeled["forward_close_return_60m"].iloc[0], 4) == 0.01
