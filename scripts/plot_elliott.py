# =============================================================================
# Elliott Wave pattern plot script
# =============================================================================
# Purpose:
#  - Load OHLCV from SQLite, detect Elliott patterns, plot top-N candidates
#  - Supports 1212, IMPULSE, WAVE3, WAVE5, ABC, ALL pattern modes
#  - Multi-timeframe: resample 1m base data to any supported TF
#  - Output to file (--out) or interactive display
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import utils
from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import PivotKind, load_ohlcv, resample_ohlcv
from elliott_waves.elliott.parser.dynamic_parser import DynamicParser
from elliott_waves.elliott.pivots.zigzag import detect_zigzag
from elliott_waves.elliott.scanners.abc import ABCScanner
from elliott_waves.elliott.scanners.setup_1212 import detect_1212
from elliott_waves.elliott.scanners.wave3 import Wave3Scanner
from elliott_waves.elliott.scanners.wave5 import Wave5Scanner
from elliott_waves.elliott.viz.candle_chart import DN_COLOR, CandleChart
from elliott_waves.elliott.viz.multi_wave import MultiWavePlot
from elliott_waves.elliott.viz.wave_plot import WavePlot

# -------------------------------------------------------------------------
# Timeframe → pandas resample frequency mapping
# -------------------------------------------------------------------------
_TF_FREQ = {
    "5m":  "5min",
    "15m": "15min",
    "1h":  "1h",
    "4h":  "4h",
}


# =============================================================================
# _load_df(asset_id, tf, bars, start, end) -> (df, db_cfg)
# =============================================================================
# Purpose:
#  - Load 1m OHLCV from SQLite and resample to target timeframe
# Parameters:
#  - asset_id: asset config ID
#  - tf: target timeframe string
#  - bars: number of most recent bars to keep
#  - start / end: optional ISO timestamp bounds
# =============================================================================
def _load_df(
    asset_id: str,
    tf:       str,
    bars:     int,
    start:    str | None,
    end:      str | None,
) -> tuple:
    asset_cfg   = utils.load_asset_config(asset_id)
    db_cfg      = asset_cfg["database"]
    db_path     = db_cfg["db_path"]
    table_ohlcv = db_cfg["tables"]["ohlcv"]

    print(f"INFO: Loading 1m OHLCV from {table_ohlcv}")
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
    print(f"INFO: {len(df)} bars, TF={tf}")
    return df, db_cfg


# =============================================================================
# _plot_1212(df, cfg, top, asset_id, out) -> None
# =============================================================================
# Purpose:
#  - Detect 1212 setups and plot them on a CandleChart
#  - Reconstructs P1-P3 bar indices from zigzag pivots
# =============================================================================
def _plot_1212(
    df:       object,
    cfg:      ElliottConfig,
    top:      int,
    symbol:   str,
    tf:       str,
    out:      str | None,
) -> None:

    setups = detect_1212(df, cfg)
    if setups.empty:
        print("INFO: No 1212 setups found")
        return
    setups = setups.sort_values("score", ascending=False).head(top).reset_index(drop=True)
    print(f"INFO: {len(setups)} 1212 setups (top {top} by score)")

    # -------------------------------------------------------------------------
    # Reconstruct pivot bar indices from zigzag detector
    # -------------------------------------------------------------------------
    pivots = detect_zigzag(df, cfg, degree=0)
    times  = df["open_time"].astype(str).to_list()

    title = f"{symbol} {tf} — 1212 setups (top {top})"
    chart = CandleChart(title=title, figsize=(18, 9))
    chart.add_candles(df)
    wp = WavePlot(chart)

    for _, row in setups.iterrows():
        target_prices = [row["p0"], row["p1"], row["p2"], row["p3"], row["p4"]]
        target_kinds  = [PivotKind.LOW, PivotKind.HIGH, PivotKind.LOW, PivotKind.HIGH, PivotKind.LOW]

        # Find the matching 5-pivot window in the zigzag list
        pv_window = None
        for i in range(4, len(pivots)):
            window = pivots[i - 4: i + 1]
            if (
                abs(window[0].price - target_prices[0]) < 0.05
                and abs(window[4].price - target_prices[4]) < 0.05
                and [p.kind for p in window] == target_kinds
            ):
                pv_window = window
                break

        if pv_window is None:
            continue

        wp.add_pivots(pv_window, df, labels=["P0", "P1", "P2", "P3", "P4"])
        wp.add_wave_lines(pv_window)

        # P3 trigger line
        p3_price  = pv_window[3].price
        p4_conf   = pv_window[4].confirmed_idx
        chart.ax.axhline(
            y         = p3_price,
            xmin      = p4_conf / max(len(df), 1),
            color     = "#ffa726",
            linewidth = 1.0,
            linestyle = "--",
            alpha     = 0.7,
            zorder    = 3,
        )
        wp.add_confirmation_zone(pv_window[4])

        # Score annotation
        chart.ax.annotate(
            f"1212 score={row['score']:.3f}",
            xy        = (pv_window[4].confirmed_idx, pv_window[4].price * 0.998),
            fontsize  = 7,
            color     = "#80cbc4",
            va        = "top",
            zorder    = 6,
        )

    if out:
        chart.save(out)
        print(f"OK: Saved to {out}")
    else:
        chart.show()
    chart.close()


# =============================================================================
# _plot_candidates(df, candidates, top, symbol, tf, pattern, out) -> None
# =============================================================================
# Purpose:
#  - Plot PatternCandidate list using MultiWavePlot
# =============================================================================
def _plot_candidates(
    df:         object,
    candidates: list,
    top:        int,
    symbol:     str,
    tf:         str,
    pattern:    str,
    out:        str | None,
) -> None:
    hard_pass = [c for c in candidates if c.hard_pass]
    hard_pass.sort(key=lambda c: c.score, reverse=True)
    print(f"INFO: {len(hard_pass)} candidates (hard_pass=True), plotting top {top}")

    title = f"{symbol} {tf} — {pattern} top {top}"
    chart = CandleChart(title=title, figsize=(18, 9))
    chart.add_candles(df)

    if hard_pass:
        mwp = MultiWavePlot(chart)
        mwp.add_candidates(hard_pass, df, top_n=top)

        # Best candidate: add target zones and stop
        best = hard_pass[0]
        wp   = WavePlot(chart)
        wp.add_candidate(best, df)
    else:
        chart.ax.annotate(
            "No hard-pass candidates found",
            xy       = (0.5, 0.5),
            xycoords = "axes fraction",
            fontsize = 12,
            color    = DN_COLOR,
            ha       = "center",
        )

    if out:
        chart.save(out)
        print(f"OK: Saved to {out}")
    else:
        chart.show()
    chart.close()


def main() -> None:
    # -------------------------------------------------------------------------
    # Parse arguments
    # -------------------------------------------------------------------------
    parser = argparse.ArgumentParser(description="Plot Elliott Wave patterns")
    parser.add_argument("--tf",        default="5m",           help="Timeframe: 1m 5m 15m 1h 4h")
    parser.add_argument("--pattern",   default="1212",         help="Pattern: 1212 IMPULSE WAVE3 WAVE5 ABC ALL")
    parser.add_argument("--top",       type=int, default=3,    help="Top-N candidates to plot")
    parser.add_argument("--bars",      type=int, default=500,  help="Recent bars to load")
    parser.add_argument("--direction", type=int, default=1,    help="1=bullish  -1=bearish")
    parser.add_argument("--asset-id",  default="solusdt_fw60", help="Asset config ID")
    parser.add_argument("--start",     default=None,           help="Start time YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end",       default=None,           help="End time YYYY-MM-DD HH:MM:SS")
    parser.add_argument(
        "--out",
        default = None,
        help    = "Output PNG file path (omit to display interactively)",
    )
    args = parser.parse_args()

    # -------------------------------------------------------------------------
    # Load data
    # -------------------------------------------------------------------------
    df, db_cfg = _load_df(
        asset_id = args.asset_id,
        tf       = args.tf,
        bars     = args.bars,
        start    = args.start,
        end      = args.end,
    )
    symbol = db_cfg.get("symbol", args.asset_id.upper())
    cfg    = ElliottConfig.for_timeframe(args.tf)

    # -------------------------------------------------------------------------
    # 1212 mode: dedicated scanner + special plot
    # -------------------------------------------------------------------------
    if args.pattern == "1212":
        _plot_1212(df, cfg, args.top, symbol, args.tf, args.out)
        return

    # -------------------------------------------------------------------------
    # Pivot detection
    # -------------------------------------------------------------------------
    pivots = detect_zigzag(df, cfg, degree=0)
    print(f"INFO: {len(pivots)} pivots detected")

    # -------------------------------------------------------------------------
    # Scanner selection
    # -------------------------------------------------------------------------
    pattern_up = args.pattern.upper()
    if pattern_up == "WAVE3":
        candidates = Wave3Scanner().scan(df, cfg, pivots, direction=args.direction)
    elif pattern_up == "WAVE5":
        candidates = Wave5Scanner().scan(df, cfg, pivots, direction=args.direction)
    elif pattern_up == "ABC":
        candidates = ABCScanner().scan(df, cfg, pivots, direction=args.direction)
    else:
        # IMPULSE, ALL, or any other type → DynamicParser
        candidates = DynamicParser(cfg).parse(pivots)
        if pattern_up != "ALL":
            candidates = [c for c in candidates if pattern_up in c.pattern_type.upper()]

    _plot_candidates(df, candidates, args.top, symbol, args.tf, args.pattern, args.out)


if __name__ == "__main__":
    main()
