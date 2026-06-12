# =============================================================================
# Elliott Wave multi-TF backtest and parameter sweep
# =============================================================================
# Purpose:
#  - Run WalkForwardEvaluator or ParamSweep across multiple timeframes
#  - Print TF x threshold x pattern_type x win_rate comparison table
#  - Supports Wave3 setup evaluation with confirmed_idx lookahead barrier
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

import utils
from elliott_waves.elliott.backtest.param_sweep import ParamSweep
from elliott_waves.elliott.backtest.walkforward import WalkForwardEvaluator
from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import load_ohlcv, resample_ohlcv

# -------------------------------------------------------------------------
# Timeframe → pandas resample frequency
# -------------------------------------------------------------------------
_TF_FREQ = {
    "1m":  None,
    "5m":  "5min",
    "15m": "15min",
    "1h":  "1h",
    "4h":  "4h",
}


# =============================================================================
# _run_walkforward(df_1m, tf, cfg, direction, train_bars, test_bars) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Resample to TF, run WalkForwardEvaluator, return per-fold metrics
# Parameters:
#  - df_1m: raw 1m OHLCV DataFrame
#  - tf: target timeframe string
#  - cfg: ElliottConfig preset for this TF
# =============================================================================
def _run_walkforward(
    df_1m:      pd.DataFrame,
    tf:         str,
    cfg:        ElliottConfig,
    direction:  int,
    train_bars: int,
    test_bars:  int,
    step_bars:  int,
) -> pd.DataFrame:
    if tf == "1m":
        df = df_1m.copy()
    else:
        freq = _TF_FREQ[tf]
        df   = resample_ohlcv(df_1m, freq)

    print(f"INFO: TF={tf}  bars={len(df)}")
    if len(df) < train_bars + test_bars:
        print(f"WARN: Not enough bars for TF={tf} — skipping")
        return pd.DataFrame()

    result = WalkForwardEvaluator().run(
        df          = df,
        cfg         = cfg,
        train_bars  = train_bars,
        test_bars   = test_bars,
        step_bars   = step_bars,
        direction   = direction,
    )
    if not result.empty:
        result.insert(0, "tf", tf)
    return result


# =============================================================================
# _run_sweep(df_1m, tf, direction, thresholds, fib_tols, min_scores) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Resample to TF and run ParamSweep grid over threshold x fib_tol x min_score
# =============================================================================
def _run_sweep(
    df_1m:      pd.DataFrame,
    tf:         str,
    direction:  int,
    thresholds: list[float],
    fib_tols:   list[float],
    min_scores: list[float],
) -> pd.DataFrame:
    if tf == "1m":
        df = df_1m.copy()
    else:
        freq = _TF_FREQ[tf]
        df   = resample_ohlcv(df_1m, freq)

    print(f"INFO: TF={tf}  bars={len(df)}")
    if len(df) < 500:
        print(f"WARN: Not enough bars for TF={tf} — skipping")
        return pd.DataFrame()

    result = ParamSweep().run(
        df          = df,
        thresholds  = thresholds,
        fib_tols    = fib_tols,
        min_scores  = min_scores,
        direction   = direction,
    )
    if not result.empty:
        result.insert(0, "tf", tf)
    return result


def main() -> None:
    # -------------------------------------------------------------------------
    # Parse arguments
    # -------------------------------------------------------------------------
    parser = argparse.ArgumentParser(description="Elliott Wave backtest — multi-TF")
    parser.add_argument(
        "--tfs",
        default = "5m,15m",
        help    = "Comma-separated timeframes: 1m,5m,15m,1h",
    )
    parser.add_argument(
        "--mode",
        default = "walkforward",
        choices = ["walkforward", "sweep"],
        help    = "walkforward: rolling out-of-sample  |  sweep: param grid search",
    )
    parser.add_argument("--direction",  type=int,   default=1,      help="1=bullish  -1=bearish")
    parser.add_argument("--train-bars", type=int,   default=3000,   help="Walk-forward train window bars")
    parser.add_argument("--test-bars",  type=int,   default=500,    help="Walk-forward test window bars")
    parser.add_argument("--step-bars",  type=int,   default=250,    help="Walk-forward step size bars")
    parser.add_argument("--asset-id",   default="solusdt_fw60",     help="Asset config ID")
    parser.add_argument("--start",      default=None,               help="Start time YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end",        default=None,               help="End time YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--out",        default=None,               help="Output CSV path (optional)")
    args = parser.parse_args()

    tfs = [t.strip() for t in args.tfs.split(",") if t.strip()]

    for tf in tfs:
        if tf not in _TF_FREQ:
            print(f"ERROR: Unknown timeframe '{tf}'. Supported: {list(_TF_FREQ)}")
            sys.exit(1)

    # -------------------------------------------------------------------------
    # Load 1m OHLCV once — all TFs are resampled from it
    # -------------------------------------------------------------------------
    asset_cfg   = utils.load_asset_config(args.asset_id)
    db_cfg      = asset_cfg["database"]
    db_path     = db_cfg["db_path"]
    table_ohlcv = db_cfg["tables"]["ohlcv"]

    print(f"INFO: Loading 1m OHLCV from {table_ohlcv}")
    df_1m = load_ohlcv(db_path, table_ohlcv, start=args.start, end=args.end)
    if df_1m.empty:
        print("ERROR: No OHLCV rows found")
        sys.exit(1)
    print(f"INFO: {len(df_1m)} 1m bars loaded")

    # -------------------------------------------------------------------------
    # Run evaluations per TF
    # -------------------------------------------------------------------------
    frames: list[pd.DataFrame] = []

    if args.mode == "walkforward":
        print(f"\nINFO: Mode=walkforward  train={args.train_bars}  test={args.test_bars}  step={args.step_bars}")
        for tf in tfs:
            cfg = ElliottConfig.for_timeframe(tf)
            df_tf = _run_walkforward(
                df_1m      = df_1m,
                tf         = tf,
                cfg        = cfg,
                direction  = args.direction,
                train_bars = args.train_bars,
                test_bars  = args.test_bars,
                step_bars  = args.step_bars,
            )
            frames.append(df_tf)

    else:  # sweep
        thresholds = [0.005, 0.010, 0.015, 0.020]
        fib_tols   = [0.08, 0.10, 0.12]
        min_scores = [55.0, 65.0]
        print(f"\nINFO: Mode=sweep  thresholds={thresholds}  fib_tols={fib_tols}  min_scores={min_scores}")
        for tf in tfs:
            df_tf = _run_sweep(
                df_1m      = df_1m,
                tf         = tf,
                direction  = args.direction,
                thresholds = thresholds,
                fib_tols   = fib_tols,
                min_scores = min_scores,
            )
            frames.append(df_tf)

    # -------------------------------------------------------------------------
    # Combine and print results
    # -------------------------------------------------------------------------
    frames = [f for f in frames if not f.empty]
    if not frames:
        print("INFO: No results found")
        return

    combined = pd.concat(frames, ignore_index=True)

    # -------------------------------------------------------------------------
    # Print comparison table
    # -------------------------------------------------------------------------
    if args.mode == "walkforward":
        summary_cols = ["tf", "pattern_type", "n_total", "n_wins", "win_rate", "mean_R"]
        summary_cols = [c for c in summary_cols if c in combined.columns]
        summary = (
            combined[summary_cols]
            .groupby(["tf", "pattern_type"], as_index=False)
            .agg({"n_total": "sum", "n_wins": "sum", "win_rate": "mean", "mean_R": "mean"})
            .sort_values(["tf", "win_rate"], ascending=[True, False])
        )
    else:
        summary_cols = ["tf", "threshold", "pattern_type", "n_total", "win_rate", "mean_R"]
        summary_cols = [c for c in summary_cols if c in combined.columns]
        summary = combined[summary_cols].sort_values(
            ["tf", "win_rate"],
            ascending = [True, False],
        ).head(30)

    print(f"\n{'='*70}")
    print(f"  Elliott Wave Backtest  —  mode={args.mode}  direction={args.direction}")
    print(f"  Asset: {args.asset_id}  TFs: {args.tfs}")
    print(f"{'='*70}")
    print(summary.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print(f"{'='*70}")

    if args.out:
        combined.to_csv(args.out, index=False)
        print(f"\nOK: Full results saved to {args.out}")


if __name__ == "__main__":
    main()
