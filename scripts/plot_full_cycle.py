# =============================================================================
# plot_full_cycle.py — Find and plot the most recent full Elliott cycle
# =============================================================================
# Purpose:
#  - Scan OHLCV for the most recent confirmed 0-1-2-3-4-5-A-B-C cycle
#  - Plot on a candlestick chart with labeled pivot points and colored wave lines
#  - Motive waves (1,3,5) in teal, corrective waves (2,4,A,B,C) in red
# =============================================================================

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import utils
from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import load_ohlcv, resample_ohlcv, PivotKind
from elliott_waves.elliott.pivots.zigzag import detect_zigzag
from elliott_waves.elliott.parser.dynamic_parser import DynamicParser
from elliott_waves.elliott.viz.candle_chart import CandleChart, UP_COLOR, DN_COLOR, TEXT_COLOR, BG_COLOR

_TF_FREQ = {
    "5m":  "5min",
    "15m": "15min",
    "1h":  "1h",
    "4h":  "4h",
}

_LABELS = ["0", "1", "2", "3", "4", "5", "A", "B", "C", "D", "E"]

# Label font sizes per wave significance
_LABEL_SIZES = {
    "1": 11, "2": 9, "3": 13, "4": 9, "5": 11,
    "A": 11, "B": 9, "C": 11, "0": 9,
}


def _load_df(asset_id, tf, bars, start, end):
    asset_cfg   = utils.load_asset_config(asset_id)
    db_cfg      = asset_cfg["database"]
    db_path     = db_cfg["db_path"]
    table_ohlcv = db_cfg["tables"]["ohlcv"]
    print(f"INFO: Loading OHLCV from {table_ohlcv}")
    df_1m = load_ohlcv(db_path, table_ohlcv, start=start, end=end)
    if df_1m.empty:
        print("ERROR: No OHLCV rows found")
        sys.exit(1)
    if tf == "1m":
        df = df_1m.copy()
    else:
        freq = _TF_FREQ.get(tf)
        if not freq:
            print(f"ERROR: Unknown timeframe '{tf}'. Supported: 1m 5m 15m 1h 4h")
            sys.exit(1)
        df = resample_ohlcv(df_1m, freq)
    df = df.tail(bars).reset_index(drop=True)
    print(f"INFO: {len(df)} bars loaded, TF={tf}")
    return df, db_cfg


def _draw_wave_lines(ax, pivots):
    """Draw wave lines with color based on price direction of each segment."""
    for i in range(1, len(pivots)):
        p_prev = pivots[i - 1]
        p_curr = pivots[i]
        color = UP_COLOR if p_curr.price > p_prev.price else DN_COLOR
        ax.plot(
            [p_prev.idx, p_curr.idx],
            [p_prev.price, p_curr.price],
            color     = color,
            linewidth = 2.5,
            linestyle = "-",
            alpha     = 0.90,
            zorder    = 4,
        )


def _draw_pivots(ax, pivots, labels):
    """Draw pivot markers and wave labels with box backgrounds."""
    for k, p in enumerate(pivots):
        label  = labels[k] if k < len(labels) else str(k)
        is_high = p.kind == PivotKind.HIGH
        color   = "#ffd700" if is_high else "#00bfff"
        offset  = 1.004 if is_high else 0.996
        va      = "bottom" if is_high else "top"
        fsize   = _LABEL_SIZES.get(label, 9)

        ax.scatter(p.idx, p.price, color=color, s=80, zorder=6, edgecolors="white", linewidths=0.5)
        ax.annotate(
            label,
            xy         = (p.idx, p.price * offset),
            color      = color,
            fontsize   = fsize,
            fontweight = "bold",
            ha         = "center",
            va         = va,
            zorder     = 7,
            bbox       = dict(
                boxstyle  = "round,pad=0.2",
                facecolor = BG_COLOR,
                edgecolor = color,
                alpha     = 0.85,
                linewidth = 1.0,
            ),
        )


def main():
    parser = argparse.ArgumentParser(description="Plot most recent full Elliott cycle (0-1-2-3-4-5-A-B-C)")
    parser.add_argument("--tf",       default="1h",           help="Timeframe: 1m 5m 15m 1h 4h")
    parser.add_argument("--bars",     type=int, default=1500, help="Recent bars to search in")
    parser.add_argument("--pad",      type=int, default=15,   help="Padding bars around pattern on chart")
    parser.add_argument("--min-score",type=float, default=0,  help="Minimum pattern score (0 = show all hard-pass)")
    parser.add_argument("--asset-id", default="solusdt_fw60", help="Asset config ID")
    parser.add_argument("--start",    default=None,           help="Start time YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end",      default=None,           help="End time YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--out",      default=None,           help="Output PNG path (omit for interactive display)")
    args = parser.parse_args()

    df, db_cfg = _load_df(args.asset_id, args.tf, args.bars, args.start, args.end)
    symbol = db_cfg.get("symbol", args.asset_id.upper())

    cfg = ElliottConfig.for_timeframe(args.tf)
    cfg.min_score = args.min_score

    pivots = detect_zigzag(df, cfg, degree=0)
    print(f"INFO: {len(pivots)} pivots detected")

    if len(pivots) < 9:
        print(f"ERROR: Only {len(pivots)} pivots found — need at least 9 for a full cycle.")
        print("Try --bars 3000 or a higher timeframe like --tf 4h")
        sys.exit(1)

    candidates = DynamicParser(pivots, cfg, degree=0).parse()
    full_cycles = [c for c in candidates if c.hard_pass and c.pattern_type == "FULL_CYCLE"]
    print(f"INFO: {len(full_cycles)} hard-pass full cycle candidates found")

    if not full_cycles:
        print("INFO: No full cycle found. Try --bars 3000 or --tf 4h or --min-score 0")
        sys.exit(1)

    # Most recent: sort by last pivot bar index descending
    full_cycles.sort(key=lambda c: c.pivots[-1].idx, reverse=True)
    best = full_cycles[0]
    diag = best.diagnostics or {}

    # Slice to the meaningful validated pivots only (max len(_LABELS))
    # DynamicParser windows can be larger than what FullCycleValidator uses
    pv = best.pivots[:len(_LABELS)]

    labels_used = _LABELS[:len(pv)]
    pivot_summary = "  ".join(f"{l}={p.price:.2f}" for l, p in zip(labels_used, pv))
    print(f"INFO: Pattern score={best.score:.1f}  direction={'+1' if best.direction == 1 else '-1'}")
    print(f"INFO: Motive={diag.get('motive_type','?')}  Correction={diag.get('corr_type','?')}")
    print(f"INFO: Pivots: {pivot_summary}")

    # Chart window: from before P0 to after C with padding
    x0 = max(0, pv[0].idx - args.pad)
    x1 = min(len(df) - 1, pv[-1].confirmed_idx + args.pad)

    t_start = str(df["open_time"].iloc[x0])[:16]
    t_end   = str(df["open_time"].iloc[x1])[:16]

    motive_label = diag.get("motive_type", "IMPULSE")
    corr_label   = diag.get("corr_type",   "ZIGZAG")
    title = (
        f"{symbol} {args.tf}  —  Elliott Full Cycle  0-1-2-3-4-5-A-B-C"
        f"\n{motive_label} + {corr_label}   score={best.score:.1f}"
        f"   [{t_start} → {t_end}]"
    )

    chart = CandleChart(title=title, figsize=(22, 10))
    chart.add_candles(df)

    # Zoom x-axis to the pattern window
    chart.ax.set_xlim(x0 - 1, x1 + 1)

    # Zoom y-axis to the pattern price range
    window_df = df.iloc[x0 : x1 + 1]
    pv_prices = [p.price for p in pv]
    y_lo = min(window_df["low"].min(), min(pv_prices))
    y_hi = max(window_df["high"].max(), max(pv_prices))
    margin = (y_hi - y_lo) * 0.08
    chart.ax.set_ylim(y_lo - margin, y_hi + margin)

    _draw_wave_lines(chart.ax, pv)
    _draw_pivots(chart.ax, pv, labels_used)

    # Info box bottom-left
    info = (
        f"Motive: {motive_label}   Correction: {corr_label}\n"
        f"Score: {best.score:.1f}   Direction: {'Bullish' if best.direction == 1 else 'Bearish'}\n"
        f"Bars: {pv[-1].idx - pv[0].idx}"
    )
    chart.ax.annotate(
        info,
        xy         = (0.01, 0.03),
        xycoords   = "axes fraction",
        color      = TEXT_COLOR,
        fontsize   = 8,
        va         = "bottom",
        family     = "monospace",
        bbox       = dict(boxstyle="round,pad=0.4", facecolor=BG_COLOR, edgecolor="#444", alpha=0.85),
    )

    if args.out:
        chart.save(args.out)
        print(f"OK: Saved to {args.out}")
    else:
        chart.show()
    chart.close()


if __name__ == "__main__":
    main()
