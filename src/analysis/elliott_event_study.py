# =============================================================================
# SOLUSDT Elliott Wave event study
# =============================================================================
# Purpose:
#  - Detect deterministic long 1-2 and nested 1-2-1-2 setups from OHLCV only
#  - Store event-study rows in a separate Elliott event table
#  - Measure raw 60-minute price outcomes without reading feature/prediction tables
# =============================================================================

from __future__ import annotations

import json
import contextlib
import io
from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import pandas as pd

import utils
from db.table_ops import sqlite_connect


EVENT_TABLE = "solusdt_elliott_events"
ENGINE = "in_house_confirmed_pivot"
ENGINE_VERSION = "v1"
PROFILE_ID = "long_1212_v1"
TAEW_ENGINE = "taew"
TAEW_ENGINE_VERSION = "0.0.3"
TAEW_PROFILE_ID = "practical_wave3_upward_1212_v1"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

TIMEFRAME_PROFILES = {
    "30m": {"freq": "30min", "pivot_threshold": 0.012},
    "1h": {"freq": "1h", "pivot_threshold": 0.018},
    "2h": {"freq": "2h", "pivot_threshold": 0.024},
}
TAEW_TIMEFRAMES = {"1h", "2h"}
TAEW_MAX_POINTS = 90
TAEW_OVERLAP_POINTS = 20


@dataclass(frozen=True)
class Pivot:
    pivot_time: str
    confirmation_time: str
    price: float
    direction: str
    pivot_bar: int
    confirmation_bar: int

    @property
    def confirmation_lag_bars(self) -> int:
        return int(self.confirmation_bar - self.pivot_bar)


# =============================================================================
# load_ohlcv_window(asset_id: str, start: str | None, end: str | None) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Load a bounded OHLCV window through the asset config
#  - Read only the raw OHLCV table
# Parameters:
#  - asset_id: asset id from config/assets.json
#  - start: optional UTC start string
#  - end: optional UTC end string
# =============================================================================
def load_ohlcv_window(
    asset_id: str = "solusdt_fw60",
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    db_cfg      = utils.load_asset_config(asset_id)
    db_path     = db_cfg["database"]["db_path"]
    table_ohlcv = db_cfg["database"]["tables"]["ohlcv"]

    with sqlite_connect(db_path) as conn:
        if end is None:
            row = conn.execute(f"SELECT MAX(open_time) FROM {table_ohlcv}").fetchone()
            end = row[0]
        if start is None:
            end_ts = pd.Timestamp(end)
            start = (end_ts - pd.Timedelta(days=365)).strftime(TIME_FORMAT)

        df = pd.read_sql_query(
            f"""
            SELECT open_time, open, high, low, close, volume
            FROM {table_ohlcv}
            WHERE open_time BETWEEN ? AND ?
            ORDER BY open_time
            """,
            conn,
            params=(start, end),
        )

    if df.empty:
        raise ValueError(f"No OHLCV rows found for {asset_id} between {start} and {end}")

    df["open_time"] = pd.to_datetime(df["open_time"], format=TIME_FORMAT)
    return df


# =============================================================================
# resample_ohlcv(df_1m: pd.DataFrame, timeframe: str) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Convert 1m OHLCV into a configured research timeframe
#  - Keep UTC timestamp strings compatible with project storage rules
# Parameters:
#  - df_1m: raw one-minute OHLCV rows
#  - timeframe: one of 30m, 1h, 2h
# =============================================================================
def resample_ohlcv(df_1m: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if timeframe not in TIMEFRAME_PROFILES:
        raise ValueError(f"Unsupported Elliott timeframe: {timeframe}")

    freq = TIMEFRAME_PROFILES[timeframe]["freq"]
    indexed = df_1m.set_index("open_time")
    df = indexed.resample(freq, label="left", closed="left").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index()
    df["open_time"] = df["open_time"].dt.strftime(TIME_FORMAT)
    return df


# =============================================================================
# detect_confirmed_pivots(df: pd.DataFrame, threshold: float) -> list[Pivot]
# =============================================================================
# Purpose:
#  - Detect swing pivots after a threshold reversal confirms them
#  - Timestamp each pivot at its confirmation bar to avoid right-edge lookahead
# Parameters:
#  - df: resampled OHLCV rows with open_time and close
#  - threshold: fractional reversal required to confirm a pivot
# =============================================================================
def detect_confirmed_pivots(df: pd.DataFrame, threshold: float) -> list[Pivot]:
    closes = df["close"].astype(float).to_numpy()
    times  = df["open_time"].astype(str).to_numpy()
    if len(closes) < 3:
        return []

    pivots: list[Pivot] = []
    trend = 0
    high_price = low_price = float(closes[0])
    high_idx = low_idx = 0

    for idx in range(1, len(closes)):
        price = float(closes[idx])

        if trend == 0:
            if price > high_price:
                high_price = price
                high_idx   = idx
            if price < low_price:
                low_price = price
                low_idx   = idx

            if low_price > 0 and (price / low_price - 1.0) >= threshold:
                pivots.append(
                    Pivot(
                        pivot_time        = str(times[low_idx]),
                        confirmation_time = str(times[idx]),
                        price             = low_price,
                        direction         = "low",
                        pivot_bar         = low_idx,
                        confirmation_bar  = idx,
                    )
                )
                trend      = 1
                high_price = price
                high_idx   = idx
            elif high_price > 0 and (price / high_price - 1.0) <= -threshold:
                pivots.append(
                    Pivot(
                        pivot_time        = str(times[high_idx]),
                        confirmation_time = str(times[idx]),
                        price             = high_price,
                        direction         = "high",
                        pivot_bar         = high_idx,
                        confirmation_bar  = idx,
                    )
                )
                trend     = -1
                low_price = price
                low_idx   = idx
            continue

        if trend == 1:
            if price > high_price:
                high_price = price
                high_idx   = idx
            elif high_price > 0 and (price / high_price - 1.0) <= -threshold:
                pivots.append(
                    Pivot(
                        pivot_time        = str(times[high_idx]),
                        confirmation_time = str(times[idx]),
                        price             = high_price,
                        direction         = "high",
                        pivot_bar         = high_idx,
                        confirmation_bar  = idx,
                    )
                )
                trend     = -1
                low_price = price
                low_idx   = idx
        else:
            if price < low_price:
                low_price = price
                low_idx   = idx
            elif low_price > 0 and (price / low_price - 1.0) >= threshold:
                pivots.append(
                    Pivot(
                        pivot_time        = str(times[low_idx]),
                        confirmation_time = str(times[idx]),
                        price             = low_price,
                        direction         = "low",
                        pivot_bar         = low_idx,
                        confirmation_bar  = idx,
                    )
                )
                trend      = 1
                high_price = price
                high_idx   = idx

    return pivots


def _valid_bullish_12(
    p0: Pivot,
    p1: Pivot,
    p2: Pivot,
    min_retrace: float,
    max_retrace: float,
) -> tuple[bool, float]:
    if [p0.direction, p1.direction, p2.direction] != ["low", "high", "low"]:
        return False, np.nan
    wave1 = p1.price - p0.price
    if wave1 <= 0 or p2.price <= p0.price or p2.price >= p1.price:
        return False, np.nan
    retrace = (p1.price - p2.price) / wave1
    return min_retrace <= retrace <= max_retrace, float(retrace)


def _setup_score(retrace_values: list[float], lag_values: list[int]) -> float:
    retrace_score = 0.0
    for retrace in retrace_values:
        retrace_score += max(0.0, 1.0 - abs(retrace - 0.618) / 0.404)
    lag_score = 1.0 / (1.0 + max(lag_values or [0]) / 8.0)
    score = (retrace_score / max(1, len(retrace_values))) * 0.75 + lag_score * 0.25
    return round(float(max(0.0, min(1.0, score))), 6)


# =============================================================================
# detect_long_1212_setups(...) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Emit long wave-3 setup events from confirmed bullish 1-2 and 1-2-1-2 pivots
#  - Keep event times at the confirming pivot timestamp
# Parameters:
#  - pivots: confirmed pivots from a single timeframe
#  - timeframe: timeframe label for event storage
#  - threshold: pivot profile threshold
# =============================================================================
def detect_long_1212_setups(
    pivots: list[Pivot],
    timeframe: str,
    threshold: float,
    min_retrace: float = 0.382,
    max_retrace: float = 0.786,
) -> pd.DataFrame:
    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()

    for idx in range(2, len(pivots)):
        p0, p1, p2 = pivots[idx - 2], pivots[idx - 1], pivots[idx]
        valid_12, retrace_12 = _valid_bullish_12(p0, p1, p2, min_retrace, max_retrace)
        if valid_12:
            key = ("wave3_setup_12", p2.confirmation_time)
            if key not in seen:
                seen.add(key)
                rows.append(
                    _event_row(
                        event_type     = "wave3_setup_12",
                        pivots         = [p0, p1, p2],
                        timeframe      = timeframe,
                        threshold      = threshold,
                        retraces       = [retrace_12],
                        engine         = ENGINE,
                        engine_version = ENGINE_VERSION,
                        profile_id     = PROFILE_ID,
                    )
                )

    for idx in range(4, len(pivots)):
        p0, p1, p2, p3, p4 = pivots[idx - 4], pivots[idx - 3], pivots[idx - 2], pivots[idx - 1], pivots[idx]
        outer_valid, outer_retrace = _valid_bullish_12(
            p0,
            p1,
            p2,
            min_retrace,
            max_retrace,
        )
        inner_valid, inner_retrace = _valid_bullish_12(
            p2,
            p3,
            p4,
            min_retrace,
            max_retrace,
        )
        if not outer_valid or not inner_valid:
            continue
        if p4.price <= p2.price:
            continue

        key = ("wave3_setup_nested_1212", p4.confirmation_time)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            _event_row(
                event_type     = "wave3_setup_nested_1212",
                pivots         = [p0, p1, p2, p3, p4],
                timeframe      = timeframe,
                threshold      = threshold,
                retraces       = [outer_retrace, inner_retrace],
                engine         = ENGINE,
                engine_version = ENGINE_VERSION,
                profile_id     = PROFILE_ID,
            )
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values(["open_time", "event_type"]).reset_index(drop=True)
    return _assign_event_ids(df)


def _event_row(
    event_type: str,
    pivots: list[Pivot],
    timeframe: str,
    threshold: float,
    retraces: list[float],
    engine: str,
    engine_version: str,
    profile_id: str,
) -> dict:
    last_pivot = pivots[-1]
    lag_values = [pivot.confirmation_lag_bars for pivot in pivots]
    source_pivots = [
        {
            "pivot_time": pivot.pivot_time,
            "confirmation_time": pivot.confirmation_time,
            "price": pivot.price,
            "direction": pivot.direction,
            "confirmation_lag_bars": pivot.confirmation_lag_bars,
        }
        for pivot in pivots
    ]
    diagnostics = {
        "pivot_threshold": threshold,
        "retraces": retraces,
        "pivot_count": len(pivots),
    }
    return {
        "event_id": "",
        "open_time": last_pivot.confirmation_time,
        "asset_id": "solusdt_fw60",
        "timeframe": timeframe,
        "engine": engine,
        "engine_version": engine_version,
        "profile_id": profile_id,
        "event_type": event_type,
        "direction": "long",
        "setup_score": _setup_score(retraces, lag_values),
        "expected_wave": "3",
        "confirmation_lag_bars": max(lag_values),
        "source_pivots_json": json.dumps(source_pivots, separators=(",", ":")),
        "diagnostics_json": json.dumps(diagnostics, separators=(",", ":")),
    }


def _assign_event_ids(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["event_id"] = [
        (
            f"solusdt_{row.engine}_{row.timeframe}_{row.event_type}_"
            f"{row.open_time.replace(' ', 'T')}_{idx}"
        )
        for idx, row in enumerate(result.itertuples(index=False))
    ]
    return result


def _taew_wave3_candidates(close: np.ndarray) -> list[dict]:
    import taew

    with contextlib.redirect_stdout(io.StringIO()):
        candidates = taew.Practical_ElliottWave3_label_upward(close.astype(np.double))
    return candidates


def _taew_wave3_candidates_chunked(close: np.ndarray) -> list[dict]:
    if len(close) <= TAEW_MAX_POINTS:
        return _taew_wave3_candidates(close)

    candidates: list[dict] = []
    step = TAEW_MAX_POINTS - TAEW_OVERLAP_POINTS
    for start in range(0, len(close), step):
        end = min(start + TAEW_MAX_POINTS, len(close))
        if end - start < 10:
            continue
        for cand in _taew_wave3_candidates(close[start:end]):
            adjusted = dict(cand)
            adjusted["z"] = [int(z) + start for z in cand.get("z", [])]
            candidates.append(adjusted)
        if end == len(close):
            break
    return candidates


def _pivot_from_taew_candidate(
    df: pd.DataFrame,
    candidate: dict,
    pos: int,
    confirmation_shift: int = 1,
) -> Pivot:
    z = int(candidate["z"][pos])
    confirm_z = min(z + confirmation_shift, len(df) - 1)
    directions = ["low", "high", "low"]
    return Pivot(
        pivot_time        = str(df["open_time"].iloc[z]),
        confirmation_time = str(df["open_time"].iloc[confirm_z]),
        price             = float(candidate["x"][pos]),
        direction         = directions[pos],
        pivot_bar         = z,
        confirmation_bar  = confirm_z,
    )


def _retrace_from_pivots(p0: Pivot, p1: Pivot, p2: Pivot) -> float:
    wave1 = p1.price - p0.price
    if wave1 <= 0:
        return np.nan
    return float((p1.price - p2.price) / wave1)


# =============================================================================
# detect_taew_long_1212_setups(...) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Use the taew Practical_ElliottWave3_label_upward package output
#  - Convert package wave-1/wave-2 candidates into comparable long setup events
#  - Derive nested 1-2-1-2 when a package candidate starts at a prior wave-2 end
# Parameters:
#  - df: resampled OHLCV rows with open_time and close
#  - timeframe: timeframe label for event storage
# =============================================================================
def detect_taew_long_1212_setups(
    df: pd.DataFrame,
    timeframe: str,
    pivots: list[Pivot] | None = None,
) -> pd.DataFrame:
    confirmation_shift = 1
    source_df = df
    if pivots is not None:
        source_df = pd.DataFrame(
            {
                "open_time": [pivot.confirmation_time for pivot in pivots],
                "close": [pivot.price for pivot in pivots],
            }
        )
        confirmation_shift = 0

    candidates = _taew_wave3_candidates_chunked(
        source_df["close"].astype(float).to_numpy()
    )
    if not candidates:
        return pd.DataFrame(columns=_event_table_columns())

    usable = [
        cand for cand in candidates
        if len(cand.get("x", [])) >= 3 and len(cand.get("z", [])) >= 3
    ]

    records = []
    for cand in usable:
        p0 = _pivot_from_taew_candidate(source_df, cand, 0, confirmation_shift)
        p1 = _pivot_from_taew_candidate(source_df, cand, 1, confirmation_shift)
        p2 = _pivot_from_taew_candidate(source_df, cand, 2, confirmation_shift)
        retrace = _retrace_from_pivots(p0, p1, p2)
        score = _setup_score([retrace], [p0.confirmation_lag_bars, p1.confirmation_lag_bars, p2.confirmation_lag_bars])
        records.append(
            {
                "candidate": cand,
                "pivots": [p0, p1, p2],
                "retrace": retrace,
                "score": score,
                "z0": int(cand["z"][0]),
                "z2": int(cand["z"][2]),
            }
        )

    best_by_end: dict[int, dict] = {}
    best_by_start: dict[int, dict] = {}
    for record in records:
        if record["score"] > best_by_end.get(record["z2"], {}).get("score", -1):
            best_by_end[record["z2"]] = record
        if record["score"] > best_by_start.get(record["z0"], {}).get("score", -1):
            best_by_start[record["z0"]] = record

    rows: list[dict] = []
    for record in best_by_end.values():
        p0, p1, p2 = record["pivots"]
        rows.append(
            _event_row(
                event_type     = "taew_wave3_setup_12",
                pivots         = [p0, p1, p2],
                timeframe      = timeframe,
                threshold      = 0.0,
                retraces       = [record["retrace"]],
                engine         = TAEW_ENGINE,
                engine_version = TAEW_ENGINE_VERSION,
                profile_id     = TAEW_PROFILE_ID,
            )
        )

    for outer in best_by_end.values():
        inner = best_by_start.get(outer["z2"])
        if inner is None:
            continue
        p0, p1, p2 = outer["pivots"]
        _, p3, p4 = inner["pivots"]
        rows.append(
            _event_row(
                event_type     = "taew_wave3_setup_nested_1212",
                pivots         = [p0, p1, p2, p3, p4],
                timeframe      = timeframe,
                threshold      = 0.0,
                retraces       = [outer["retrace"], inner["retrace"]],
                engine         = TAEW_ENGINE,
                engine_version = TAEW_ENGINE_VERSION,
                profile_id     = TAEW_PROFILE_ID,
            )
        )

    if not rows:
        return pd.DataFrame(columns=_event_table_columns())

    result = pd.DataFrame(rows)
    result = result.drop_duplicates(
        subset=["open_time", "timeframe", "event_type", "source_pivots_json"]
    )
    result = result.sort_values(["open_time", "event_type"]).reset_index(drop=True)
    return _assign_event_ids(result)


# =============================================================================
# add_forward_outcomes(events: pd.DataFrame, df_1m: pd.DataFrame) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Measure raw 60-minute upside after each event from OHLCV only
#  - Add binary price-rise and threshold-hit outcome columns
# Parameters:
#  - events: detected Elliott events
#  - df_1m: raw one-minute OHLCV rows covering event window and 60m horizon
# =============================================================================
def add_forward_outcomes(
    events: pd.DataFrame,
    df_1m: pd.DataFrame,
    horizon_minutes: int = 60,
    raw_up_threshold: float = 0.01,
) -> pd.DataFrame:
    if events.empty:
        return events

    market = df_1m.copy()
    market["open_time"] = pd.to_datetime(market["open_time"])
    market = market.set_index("open_time").sort_index()

    rows = []
    for row in events.to_dict("records"):
        event_time = pd.Timestamp(row["open_time"])
        if event_time not in market.index:
            pos = market.index.searchsorted(event_time)
            if pos >= len(market.index):
                continue
            event_time = market.index[pos]

        event_close = float(market.loc[event_time, "close"])
        end_time = event_time + timedelta(minutes=horizon_minutes)
        fwd = market.loc[(market.index > event_time) & (market.index <= end_time)]
        if fwd.empty:
            continue

        forward_max_up = float(fwd["high"].max() / event_close - 1.0)
        forward_close_return = float(fwd["close"].iloc[-1] / event_close - 1.0)
        row["open_time"] = event_time.strftime(TIME_FORMAT)
        row["event_close"] = event_close
        row["forward_max_up_60m"] = forward_max_up
        row["forward_close_return_60m"] = forward_close_return
        row["price_up_60m"] = int(forward_close_return > 0)
        row["raw_up_hit_60m"] = int(forward_max_up >= raw_up_threshold)
        row["raw_up_threshold"] = float(raw_up_threshold)
        rows.append(row)

    return pd.DataFrame(rows)


# =============================================================================
# run_event_study(...) -> tuple[pd.DataFrame, str]
# =============================================================================
# Purpose:
#  - Execute the one-year SOLUSDT long Elliott event study
#  - Write the separate Elliott event table and markdown report
# Parameters:
#  - asset_id: asset id from config/assets.json
#  - start/end: optional UTC window; defaults to latest one year
# =============================================================================
def run_event_study(
    asset_id: str = "solusdt_fw60",
    start: str | None = None,
    end: str | None = None,
    event_table: str = EVENT_TABLE,
    report_path: str = "docs/analysis/solusdt_elliott_event_study_v1.md",
    raw_up_threshold: float = 0.01,
) -> tuple[pd.DataFrame, str]:
    df_1m = load_ohlcv_window(asset_id=asset_id, start=start, end=end)
    window_start = df_1m["open_time"].min().strftime(TIME_FORMAT)
    window_end   = df_1m["open_time"].max().strftime(TIME_FORMAT)

    event_frames = []
    for timeframe, profile in TIMEFRAME_PROFILES.items():
        df_tf = resample_ohlcv(df_1m, timeframe)
        pivots = detect_confirmed_pivots(
            df        = df_tf,
            threshold = float(profile["pivot_threshold"]),
        )
        events = detect_long_1212_setups(
            pivots    = pivots,
            timeframe = timeframe,
            threshold = float(profile["pivot_threshold"]),
        )
        if not events.empty:
            event_frames.append(events)
        if timeframe in TAEW_TIMEFRAMES:
            taew_events = detect_taew_long_1212_setups(df_tf, timeframe, pivots=pivots)
            if not taew_events.empty:
                event_frames.append(taew_events)

    if event_frames:
        events_all = pd.concat(event_frames, ignore_index=True)
        events_all = events_all.sort_values("setup_score", ascending=False)
        events_all = events_all.drop_duplicates(
            subset=["engine", "timeframe", "event_type", "open_time"],
            keep="first",
        )
        events_all = add_forward_outcomes(
            events           = events_all,
            df_1m            = df_1m,
            raw_up_threshold = raw_up_threshold,
        )
        events_all = events_all.sort_values(["open_time", "timeframe", "event_type"])
    else:
        events_all = pd.DataFrame(columns=_event_table_columns())

    write_event_table(asset_id, event_table, events_all)
    report = build_report(
        events           = events_all,
        window_start     = window_start,
        window_end       = window_end,
        raw_up_threshold = raw_up_threshold,
    )
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    return events_all, report_path


def _event_table_columns() -> list[str]:
    return [
        "event_id",
        "open_time",
        "asset_id",
        "timeframe",
        "engine",
        "engine_version",
        "profile_id",
        "event_type",
        "direction",
        "setup_score",
        "expected_wave",
        "confirmation_lag_bars",
        "source_pivots_json",
        "diagnostics_json",
        "event_close",
        "forward_max_up_60m",
        "forward_close_return_60m",
        "price_up_60m",
        "raw_up_hit_60m",
        "raw_up_threshold",
    ]


# =============================================================================
# write_event_table(asset_id: str, event_table: str, events: pd.DataFrame) -> None
# =============================================================================
# Purpose:
#  - Replace the separate Elliott event-study table with current bounded results
#  - Preserve event_id uniqueness
# Parameters:
#  - asset_id: asset id from config/assets.json
#  - event_table: output table name
#  - events: event rows to store
# =============================================================================
def write_event_table(asset_id: str, event_table: str, events: pd.DataFrame) -> None:
    db_cfg  = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]
    columns = _event_table_columns()
    df = events.reindex(columns=columns).copy()

    with sqlite_connect(db_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {event_table}")
        conn.execute(
            f"""
            CREATE TABLE {event_table} (
                event_id TEXT PRIMARY KEY,
                open_time TEXT,
                asset_id TEXT,
                timeframe TEXT,
                engine TEXT,
                engine_version TEXT,
                profile_id TEXT,
                event_type TEXT,
                direction TEXT,
                setup_score REAL,
                expected_wave TEXT,
                confirmation_lag_bars INTEGER,
                source_pivots_json TEXT,
                diagnostics_json TEXT,
                event_close REAL,
                forward_max_up_60m REAL,
                forward_close_return_60m REAL,
                price_up_60m INTEGER,
                raw_up_hit_60m INTEGER,
                raw_up_threshold REAL
            )
            """
        )
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{event_table}_open_time ON {event_table} (open_time)")
        if not df.empty:
            df.to_sql(event_table, conn, index=False, if_exists="append")
        conn.commit()


def _rate(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return float(series.mean())


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


# =============================================================================
# build_report(...) -> str
# =============================================================================
# Purpose:
#  - Create a concise markdown report for the one-year long-only event study
# =============================================================================
def build_report(
    events: pd.DataFrame,
    window_start: str,
    window_end: str,
    raw_up_threshold: float,
) -> str:
    lines = [
        "# SOLUSDT Elliott Event Study V1",
        "",
        "## Scope",
        "",
        f"- Window: `{window_start}` to `{window_end}` UTC",
        f"- Engine: `{ENGINE}` `{ENGINE_VERSION}`",
        f"- Profile: `{PROFILE_ID}`",
        "- Source: raw `solusdt_1m` OHLCV only",
        "- Excluded: feature table, prediction table, wave-5, and C-wave analysis",
        "- Direction: long only",
        "- Setup types: bullish own-detector and package-detector 1-2 / nested 1-2-1-2 wave-3 setups",
        f"- Package detector: `{TAEW_ENGINE}` `{TAEW_ENGINE_VERSION}` on `1h` and `2h` bars",
        f"- Raw upside hit threshold: `{raw_up_threshold:.4f}` over the next 60 minutes",
        "",
        "## Summary",
        "",
    ]

    if events.empty:
        lines.extend(
            [
                "- Detected events: `0`",
                "- Result: no measurable long 1-2 / 1-2-1-2 events in this profile.",
                "",
            ]
        )
        return "\n".join(lines)

    total = len(events)
    nested = events[events["event_type"].str.contains("nested_1212")]
    lines.extend(
        [
            f"- Detected Elliott setup events: `{total}`",
            f"- Detected nested 1-2-1-2 events: `{len(nested)}`",
            f"- Close was higher after 60m: `{int(events['price_up_60m'].sum())}` / `{total}` ({_fmt_pct(_rate(events['price_up_60m']))})",
            f"- Forward high reached +{raw_up_threshold * 100:.2f}% within 60m: `{int(events['raw_up_hit_60m'].sum())}` / `{total}` ({_fmt_pct(_rate(events['raw_up_hit_60m']))})",
            f"- Median 60m close return: `{events['forward_close_return_60m'].median():.5f}`",
            f"- Median forward max-up 60m: `{events['forward_max_up_60m'].median():.5f}`",
            f"- Median confirmation lag: `{events['confirmation_lag_bars'].median():.1f}` bars",
            "",
            "## Timeframe Breakdown",
            "",
            "| Timeframe | Events | Nested 1-2-1-2 | 60m Close Up | +Threshold Hit | Median Close Ret | Median Max Up | Median Lag Bars |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for timeframe in sorted(events["timeframe"].unique()):
        df_tf = events[events["timeframe"] == timeframe]
        nested_tf = df_tf[df_tf["event_type"] == "wave3_setup_nested_1212"]
        lines.append(
            "| "
            f"{timeframe} | "
            f"{len(df_tf)} | "
            f"{len(nested_tf)} | "
            f"{_fmt_pct(_rate(df_tf['price_up_60m']))} | "
            f"{_fmt_pct(_rate(df_tf['raw_up_hit_60m']))} | "
            f"{df_tf['forward_close_return_60m'].median():.5f} | "
            f"{df_tf['forward_max_up_60m'].median():.5f} | "
            f"{df_tf['confirmation_lag_bars'].median():.1f} |"
        )

    lines.extend(
        [
            "",
            "## Engine Breakdown",
            "",
            "| Engine | Events | Nested 1-2-1-2 | 60m Close Up | +Threshold Hit | Median Close Ret | Median Max Up |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for engine in sorted(events["engine"].unique()):
        df_engine = events[events["engine"] == engine]
        nested_engine = df_engine[df_engine["event_type"].str.contains("nested_1212")]
        lines.append(
            "| "
            f"{engine} | "
            f"{len(df_engine)} | "
            f"{len(nested_engine)} | "
            f"{_fmt_pct(_rate(df_engine['price_up_60m']))} | "
            f"{_fmt_pct(_rate(df_engine['raw_up_hit_60m']))} | "
            f"{df_engine['forward_close_return_60m'].median():.5f} | "
            f"{df_engine['forward_max_up_60m'].median():.5f} |"
        )

    lines.extend(
        [
            "",
            "## Event Type Breakdown",
            "",
            "| Engine | Timeframe | Event Type | Events | 60m Close Up | +Threshold Hit | Median Close Ret | Median Max Up |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    grouped = events.groupby(["engine", "timeframe", "event_type"], sort=True)
    for (engine, timeframe, event_type), df_group in grouped:
        lines.append(
            "| "
            f"{engine} | "
            f"{timeframe} | "
            f"{event_type} | "
            f"{len(df_group)} | "
            f"{_fmt_pct(_rate(df_group['price_up_60m']))} | "
            f"{_fmt_pct(_rate(df_group['raw_up_hit_60m']))} | "
            f"{df_group['forward_close_return_60m'].median():.5f} | "
            f"{df_group['forward_max_up_60m'].median():.5f} |"
        )

    lines.extend(["", "## Nested 1-2-1-2 Outcome", ""])
    if nested.empty:
        lines.append("- No nested 1-2-1-2 events were detected with this profile.")
    else:
        lines.extend(
            [
                f"- Nested events: `{len(nested)}`",
                f"- Close was higher after 60m: `{int(nested['price_up_60m'].sum())}` / `{len(nested)}` ({_fmt_pct(_rate(nested['price_up_60m']))})",
                f"- Close was not higher after 60m: `{len(nested) - int(nested['price_up_60m'].sum())}` / `{len(nested)}`",
                f"- Forward high reached +{raw_up_threshold * 100:.2f}% within 60m: `{int(nested['raw_up_hit_60m'].sum())}` / `{len(nested)}` ({_fmt_pct(_rate(nested['raw_up_hit_60m']))})",
                f"- Median 60m close return: `{nested['forward_close_return_60m'].median():.5f}`",
                f"- Median forward max-up 60m: `{nested['forward_max_up_60m'].median():.5f}`",
            ]
        )

    lines.extend(
        [
            "",
            "## Event Examples",
            "",
            "| Open Time | Timeframe | Type | Score | Close Ret 60m | Max Up 60m |",
            "| --- | --- | --- | ---: | ---: | ---: |",
        ]
    )
    examples = events.sort_values("forward_close_return_60m", ascending=False).head(5)
    for row in examples.itertuples(index=False):
        lines.append(
            f"| {row.open_time} | {row.timeframe} | {row.event_type} | "
            f"{row.setup_score:.3f} | {row.forward_close_return_60m:.5f} | "
            f"{row.forward_max_up_60m:.5f} |"
        )

    lines.extend(
        [
            "",
            "## Failure Examples",
            "",
            "| Open Time | Timeframe | Type | Score | Close Ret 60m | Max Up 60m |",
            "| --- | --- | --- | ---: | ---: | ---: |",
        ]
    )
    failures = events.sort_values("forward_close_return_60m", ascending=True).head(5)
    for row in failures.itertuples(index=False):
        lines.append(
            f"| {row.open_time} | {row.timeframe} | {row.event_type} | "
            f"{row.setup_score:.3f} | {row.forward_close_return_60m:.5f} | "
            f"{row.forward_max_up_60m:.5f} |"
        )

    own_30m = events[
        (events["engine"] == ENGINE)
        & (events["timeframe"] == "30m")
    ]
    own_1h_nested = events[
        (events["engine"] == ENGINE)
        & (events["timeframe"] == "1h")
        & (events["event_type"] == "wave3_setup_nested_1212")
    ]
    taew_1h_nested = events[
        (events["engine"] == TAEW_ENGINE)
        & (events["timeframe"] == "1h")
        & (events["event_type"] == "taew_wave3_setup_nested_1212")
    ]

    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            "- Treat this as a first deterministic event-study baseline, not a production Elliott feature module.",
            (
                f"- For 1h prediction research, the strongest practical timeframe is `30m` with the in-house detector: "
                f"`{len(own_30m)}` events, `{_fmt_pct(_rate(own_30m['price_up_60m']))}` 60m close-up rate, "
                f"and `{_fmt_pct(_rate(own_30m['raw_up_hit_60m']))}` +threshold hit rate."
            ),
            (
                f"- The in-house `1h` nested setup is cleaner but sparse: `{len(own_1h_nested)}` events, "
                f"`{_fmt_pct(_rate(own_1h_nested['price_up_60m']))}` 60m close-up rate."
            ),
            (
                f"- The package `taew` `1h` nested setup is broader but weaker: `{len(taew_1h_nested)}` events, "
                f"`{_fmt_pct(_rate(taew_1h_nested['price_up_60m']))}` 60m close-up rate."
            ),
            "- Do not build Elliott LightGBM features until this signal is retested for threshold stability and leakage with synthetic perturbation tests.",
            "",
        ]
    )
    return "\n".join(lines)
