#!/usr/bin/env python3
# =============================================================================
# SOLUSDT Elliott nested 1-2-1-2 comprehensive parameter scan
# Variants use only time-bounded windows (no unlimited).
# Window = how fresh the pattern must be when the signal fires.
# =============================================================================

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from elliott_waves.elliott_event_study import (
    add_forward_outcomes,
    detect_1m_1212_setups,
    load_ohlcv_window,
    resample_ohlcv,
)

REPORT_PATH      = "docs/analysis/solusdt_elliott_scan_v1.md"
RAW_UP_THRESHOLD = 0.01
MIN_RETRACE      = 0.382
MAX_RETRACE      = 0.68
MIN_LEG_BARS     = 2

# bars_per_hour by timeframe
_BPH = {"5m": 12, "10m": 6, "15m": 4, "30m": 2}


def _bars(tf: str, hours: float) -> int:
    return max(1, int(_BPH[tf] * hours))


def _window_label(tf: str, max_bars: int) -> str:
    minutes = max_bars * (60 // _BPH[tf])
    h = minutes / 60
    if h < 24:
        return f"{int(h)}h" if h == int(h) else f"{h:.1f}h"
    d = h / 24
    return f"{int(d)}d" if d == int(d) else f"{d:.1f}d"


# ---------------------------------------------------------------------------
# Variants: (tf_label, resample_freq, pivot_threshold, max_pattern_bars)
# Windows are expressed in hours/days to keep them meaningful:
#   - 4h, 8h, 12h, 24h, 48h, 72h, 5d
# Focus: 15m (best in prev scan), 10m, 30m for comparison
# ---------------------------------------------------------------------------
VARIANTS: list[tuple[str, str, float, int]] = []

# --- 5m ---
for t in [0.008, 0.010, 0.012, 0.015]:
    for h in [4, 8, 12, 24, 48]:
        VARIANTS.append(("5m", "5min", t, _bars("5m", h)))

# --- 10m ---
for t in [0.008, 0.010, 0.012, 0.015, 0.018]:
    for h in [4, 8, 12, 24, 48, 72]:
        VARIANTS.append(("10m", "10min", t, _bars("10m", h)))

# --- 15m (most promising from prev scan) ---
for t in [0.010, 0.012, 0.015, 0.018]:
    for h in [4, 8, 12, 24, 48, 72, 120]:
        VARIANTS.append(("15m", "15min", t, _bars("15m", h)))

# --- 30m ---
for t in [0.010, 0.012, 0.015, 0.018, 0.020]:
    for h in [8, 12, 24, 48, 72, 120]:
        VARIANTS.append(("30m", "30min", t, _bars("30m", h)))


def run_scan(asset_id: str = "solusdt_fw60") -> tuple[pd.DataFrame, pd.DataFrame]:
    print("Loading 1m OHLCV...")
    df_1m = load_ohlcv_window(asset_id=asset_id)
    df_1m["open_time"] = pd.to_datetime(df_1m["open_time"])

    resampled_cache: dict[str, pd.DataFrame] = {}
    rows = []

    for i, (tf_label, freq, threshold, max_bars) in enumerate(VARIANTS, 1):
        win = _window_label(tf_label, max_bars)
        print(
            f"[{i:03d}/{len(VARIANTS)}] {tf_label}  thr={threshold:.3f}  win={win:<6}",
            end="  ",
        )

        if freq not in resampled_cache:
            resampled_cache[freq] = resample_ohlcv(df_1m, freq)
        df_tf = resampled_cache[freq]

        events = detect_1m_1212_setups(
            df_1m            = df_tf,
            pivot_threshold  = threshold,
            max_pattern_bars = max_bars,
            min_leg_bars     = MIN_LEG_BARS,
            min_retrace      = MIN_RETRACE,
            max_retrace      = MAX_RETRACE,
            include_simple   = False,
        )

        if events.empty:
            print("0 events")
            rows.append(_zero_row(tf_label, threshold, max_bars, win))
            continue

        labeled = add_forward_outcomes(
            events           = events,
            df_1m            = df_1m,
            raw_up_threshold = RAW_UP_THRESHOLD,
        )

        n          = len(labeled)
        close_up   = labeled["price_up_60m"].mean() * 100
        thresh_hit = labeled["raw_up_hit_60m"].mean() * 100
        med_ret    = labeled["forward_close_return_60m"].median()
        med_up     = labeled["forward_max_up_60m"].median()
        med_score  = labeled["setup_score"].median()

        print(f"{n:4d} events  close_up={close_up:5.1f}%  +1%={thresh_hit:5.1f}%")
        rows.append({
            "timeframe":         tf_label,
            "threshold":         threshold,
            "max_bars":          max_bars,
            "window":            win,
            "events":            n,
            "close_up_pct":      round(close_up, 1),
            "threshold_hit_pct": round(thresh_hit, 1),
            "med_close_ret":     round(med_ret, 5),
            "med_max_up":        round(med_up, 5),
            "med_score":         round(med_score, 3),
        })

    return pd.DataFrame(rows), df_1m


def _zero_row(tf: str, threshold: float, max_bars: int, window: str) -> dict:
    return {
        "timeframe": tf, "threshold": threshold, "max_bars": max_bars,
        "window": window, "events": 0, "close_up_pct": 0.0,
        "threshold_hit_pct": 0.0, "med_close_ret": None,
        "med_max_up": None, "med_score": None,
    }


def _table_header() -> list[str]:
    return [
        "| TF | Threshold | Window | Events | Close Up % | +1% Hit % | Med Max-Up | Med Score |",
        "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
    ]


def _table_row(r: pd.Series) -> str:
    med_up    = f"{r['med_max_up']:.5f}"  if r["med_max_up"]  is not None else "-"
    med_score = f"{r['med_score']:.3f}"   if r["med_score"]   is not None else "-"
    return (
        f"| {r['timeframe']} | {r['threshold']:.3f} | {r['window']} | "
        f"{int(r['events'])} | {r['close_up_pct']:.1f}% | {r['threshold_hit_pct']:.1f}% | "
        f"{med_up} | {med_score} |"
    )


def build_report(results: pd.DataFrame, df_1m: pd.DataFrame) -> str:
    window_start = df_1m["open_time"].min().strftime("%Y-%m-%d %H:%M:%S")
    window_end   = df_1m["open_time"].max().strftime("%Y-%m-%d %H:%M:%S")
    total        = len(results)
    nonzero      = (results["events"] > 0).sum()

    lines = [
        "# SOLUSDT Elliott Nested 1-2-1-2 Scan",
        "",
        "## Scope",
        "",
        f"- Window: `{window_start}` to `{window_end}` UTC",
        f"- Timeframes: 5m, 10m, 15m, 30m",
        f"- Retracement: `{MIN_RETRACE}` - `{MAX_RETRACE}`  |  Min leg: `{MIN_LEG_BARS}` bars",
        f"- Forward outcome: +`{RAW_UP_THRESHOLD*100:.0f}%` within 60 minutes",
        "- Setup: nested 1-2-1-2 only, time-bounded windows only",
        f"- Variants: `{total}` (`{nonzero}` with events)",
        "",
    ]

    # Per-timeframe tables
    for tf in ["5m", "10m", "15m", "30m"]:
        sub = results[results["timeframe"] == tf].sort_values(["threshold", "max_bars"])
        lines += [f"## {tf} Results", ""]
        lines += _table_header()
        for _, r in sub.iterrows():
            lines.append(_table_row(r))
        lines.append("")

    # Top 15 by close-up (min 20 events)
    lines += ["## Top 15 by Close-Up Rate  (min 20 events)", ""]
    top_cu = (
        results[results["events"] >= 20]
        .sort_values("close_up_pct", ascending=False)
        .head(15)
    )
    if top_cu.empty:
        lines.append("- No variant with 20+ events.\n")
    else:
        lines += _table_header()
        for _, r in top_cu.iterrows():
            lines.append(_table_row(r))
        lines.append("")

    # Top 15 by +1% hit (min 20 events)
    lines += ["## Top 15 by +1% Hit Rate  (min 20 events)", ""]
    top_hit = (
        results[results["events"] >= 20]
        .sort_values("threshold_hit_pct", ascending=False)
        .head(15)
    )
    if top_hit.empty:
        lines.append("- No variant with 20+ events.\n")
    else:
        lines += _table_header()
        for _, r in top_hit.iterrows():
            lines.append(_table_row(r))
        lines.append("")

    # Usable setups
    usable = results[
        (results["events"] >= 30) &
        (results["close_up_pct"] >= 60.0)
    ].sort_values(["close_up_pct", "threshold_hit_pct"], ascending=False)

    lines += ["## Usable Setups  (30+ events, 60%+ close-up)", ""]
    if usable.empty:
        lines.append("- No variant reached both thresholds.\n")
        near = results[
            (results["events"] >= 20) &
            (results["close_up_pct"] >= 60.0)
        ].sort_values(["close_up_pct", "threshold_hit_pct"], ascending=False).head(10)
        if not near.empty:
            lines += ["### Near-misses (20+ events, 60%+ close-up)", ""]
            lines += _table_header()
            for _, r in near.iterrows():
                lines.append(_table_row(r))
            lines.append("")
    else:
        lines += _table_header()
        for _, r in usable.iterrows():
            lines.append(_table_row(r))
        lines.append("")

    # Recommendation
    lines += ["## Recommendation", ""]
    best = (
        results[results["events"] >= 20]
        .sort_values(["close_up_pct", "threshold_hit_pct"], ascending=False)
        .head(1)
    )
    if best.empty:
        lines.append("- Increase window or lower threshold.\n")
    else:
        b = best.iloc[0]
        lines += [
            f"- **Best:** `{b['timeframe']}` thr=`{b['threshold']:.3f}` win=`{b['window']}`",
            f"  - `{int(b['events'])}` events | close-up `{b['close_up_pct']:.1f}%` | +1% `{b['threshold_hit_pct']:.1f}%`",
            "",
        ]

    return "\n".join(lines)


def main() -> None:
    results, df_1m = run_scan()
    report = build_report(results, df_1m)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"\nOK: '{REPORT_PATH}'")
    print("\nTop 10 (20+ events, by close-up %):")
    top = (
        results[results["events"] >= 20]
        .sort_values("close_up_pct", ascending=False)
        .head(10)
    )
    print(top[["timeframe", "threshold", "window", "events",
               "close_up_pct", "threshold_hit_pct"]].to_string(index=False))


if __name__ == "__main__":
    main()
