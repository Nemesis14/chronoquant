"""Rebuild the champion strategy session as a long-only D10 take-profit system.

Strategy rules:
  * enter long when score_pct_long reaches D10 (>= 0.90),
  * target take profit is the median D10 long target log return,
  * stop loss is the symmetric negative of the TP log return,
  * if price reaches TP or SL within 60 bars, exit early,
  * otherwise exit after 60 bars at the current close.
"""

from __future__ import annotations

import json
import math
import shutil
import sys
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from strategy.strategy.build_table import build_scored_table
from strategy.strategy.calibrate import fit_calibration

SESSION_ID = "strat_solusdt_fw60_combo_2101_2605"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _artifact_dir(session_id: str) -> Path:
    return _repo_root() / "artifacts" / session_id


def _load_strategy_cfg(session_id: str) -> dict[str, Any]:
    cfg = json.loads((_repo_root() / "config" / "strategies.json").read_text(encoding="utf-8"))
    return dict(cfg["strategies"][session_id])


def _recreate_artifact_dir(session_id: str) -> Path:
    path = _artifact_dir(session_id)
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _build_cutoffs(calibrated_df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    specs = [
        ("long", "pred_long_raw", "bucket_long", "score_pct_long", "bucket_mean_mfe_long", "bucket_hit_rate_long"),
        ("short", "pred_short_raw", "bucket_short", "score_pct_short", "bucket_mean_mfe_short", "bucket_hit_rate_short"),
    ]
    for direction, raw_col, bucket_col, pct_col, mean_col, hit_col in specs:
        if bucket_col not in calibrated_df.columns:
            continue
        for bucket_id, grp in calibrated_df.groupby(bucket_col, observed=True):
            rows.append({
                "direction": direction,
                "bucket_id": int(bucket_id),
                "score_raw_lower": float(grp[raw_col].min()),
                "score_raw_upper": float(grp[raw_col].max()),
                "score_pct_upper": float(grp[pct_col].max()),
                "bucket_mean_mfe": float(grp[mean_col].iloc[0]),
                "bucket_hit_rate": float(grp[hit_col].iloc[0]),
            })
    return sorted(rows, key=lambda row: (row["direction"], row["bucket_id"]))


def _compute_metrics(trades_df: pd.DataFrame) -> dict[str, Any]:
    if trades_df.empty:
        return {
            "n_trades": 0,
            "avg_expected_log_return": 0.0,
            "avg_fact_log_return": 0.0,
            "avg_fact_1h_max_range_log_return": 0.0,
            "total_fact_log_return": 0.0,
            "compounded_return_pct": 0.0,
            "win_rate": None,
            "max_drawdown": None,
            "tp_hit_rate": None,
            "sufficient_sample": False,
        }

    fact_log = trades_df["fact_log_return"].astype(float)
    cumulative = fact_log.cumsum()
    drawdown = cumulative - cumulative.cummax()
    return {
        "n_trades": int(len(trades_df)),
        "avg_expected_log_return": round(float(trades_df["expected_log_return"].mean()), 6),
        "avg_fact_log_return": round(float(fact_log.mean()), 6),
        "avg_fact_1h_max_range_log_return": round(float(trades_df["fact_1h_max_range_log_return"].mean()), 6),
        "total_fact_log_return": round(float(fact_log.sum()), 6),
        "compounded_return_pct": round(float(math.exp(float(fact_log.sum())) - 1.0), 6),
        "win_rate": round(float((fact_log > 0.0).mean()), 4),
        "max_drawdown": round(float(drawdown.min()), 6),
        "tp_hit_rate": round(float((trades_df["exit_reason"] == "take_profit").mean()), 4),
        "sufficient_sample": bool(len(trades_df) >= 30),
    }


def _simulate_long_d10(
    full_df: pd.DataFrame,
    fit_start: str,
    fit_end: str,
    tp_target_log_return: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    start_ts = pd.Timestamp(f"{fit_start} 00:00:00")
    end_ts = pd.Timestamp(f"{fit_end} 23:59:59")
    end_with_buffer = end_ts + pd.Timedelta(minutes=60)

    df = full_df.sort_values("open_time").copy()
    df["open_time"] = pd.to_datetime(df["open_time"])
    df = df.loc[(df["open_time"] >= start_ts) & (df["open_time"] <= end_with_buffer)].reset_index(drop=True)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    trades: list[dict[str, Any]] = []
    idx = 0
    eligible_entry_idx = df.index[df["open_time"] <= end_ts]
    if len(eligible_entry_idx) == 0:
        return pd.DataFrame(), pd.DataFrame()
    last_entry_idx = min(int(eligible_entry_idx.max()), max(len(df) - 61, 0))

    while idx <= last_entry_idx:
        row = df.iloc[idx]
        if float(row["score_pct_long"]) < 0.9:
            idx += 1
            continue

        entry_price = float(row["close"])
        expected_log_return = tp_target_log_return
        target_price = entry_price * math.exp(expected_log_return)
        stop_price = entry_price * math.exp(-expected_log_return)
        exit_idx = idx + 60
        exit_price = float(df.iloc[exit_idx]["close"])
        exit_reason = "timeout_1h"

        for probe_idx in range(idx + 1, idx + 61):
            probe_low = float(df.iloc[probe_idx]["low"])
            probe_high = float(df.iloc[probe_idx]["high"])
            if probe_low <= stop_price:
                exit_idx = probe_idx
                exit_price = stop_price
                exit_reason = "stop_loss"
                break
            if probe_high >= target_price:
                exit_idx = probe_idx
                exit_price = target_price
                exit_reason = "take_profit"
                break

        exit_row = df.iloc[exit_idx]
        fact_log_return = math.log(exit_price / entry_price)
        profit_pct = (exit_price / entry_price) - 1.0

        trades.append({
            "entry_time": row["open_time"],
            "exit_time": exit_row["open_time"],
            "direction": "long",
            "entry_price": entry_price,
            "exit_price": exit_price,
            "profit_pct": profit_pct,
            "expected_log_return": expected_log_return,
            "fact_log_return": fact_log_return,
            "fact_1h_max_range_log_return": float(row["long_mfe_fw60"]),
            "tp_price": target_price,
            "sl_price": stop_price,
            "hold_minutes": int((exit_row["open_time"] - row["open_time"]).total_seconds() / 60),
            "exit_reason": exit_reason,
            "score_pct_at_entry": float(row["score_pct_long"]),
            "bucket_mean_mfe": expected_log_return,
        })
        idx = exit_idx + 1

    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        equity_df = pd.DataFrame({
            "trade_index": pd.Series([], dtype="int64"),
            "entry_time": pd.Series([], dtype="datetime64[ns]"),
            "bucket_mean_mfe": pd.Series([], dtype="float64"),
            "cumulative_mfe": pd.Series([], dtype="float64"),
            "fact_log_return": pd.Series([], dtype="float64"),
            "cumulative_fact_log_return": pd.Series([], dtype="float64"),
        })
        return trades_df, equity_df

    trades_df["trade_index"] = range(len(trades_df))
    trades_df["cumulative_fact_log_return"] = trades_df["fact_log_return"].cumsum()
    equity_df = pd.DataFrame({
        "trade_index": trades_df["trade_index"],
        "entry_time": trades_df["entry_time"],
        "bucket_mean_mfe": trades_df["bucket_mean_mfe"],
        "cumulative_mfe": trades_df["fact_log_return"].cumsum(),
        "fact_log_return": trades_df["fact_log_return"],
        "cumulative_fact_log_return": trades_df["cumulative_fact_log_return"],
    })
    return trades_df, equity_df


def _summary_frame(trades_df: pd.DataFrame, metrics: dict[str, Any], tp_target_log_return: float) -> pd.DataFrame:
    cols = [
        "exit_reason",
        "n_trades",
        "avg_entry_price",
        "avg_exit_price",
        "avg_profit_pct",
        "avg_expected_log_return",
        "avg_fact_log_return",
        "avg_fact_1h_max_range_log_return",
        "total_fact_log_return",
        "compounded_return_pct",
        "realized_directional_win_rate",
        "avg_hold_minutes",
        "tp_target_log_return",
    ]
    if trades_df.empty:
        return pd.DataFrame(columns=cols)

    grouped = (
        trades_df.groupby("exit_reason", observed=True)
        .agg(
            n_trades=("exit_reason", "size"),
            avg_entry_price=("entry_price", "mean"),
            avg_exit_price=("exit_price", "mean"),
            avg_profit_pct=("profit_pct", "mean"),
            avg_expected_log_return=("expected_log_return", "mean"),
            avg_fact_log_return=("fact_log_return", "mean"),
            avg_fact_1h_max_range_log_return=("fact_1h_max_range_log_return", "mean"),
            total_fact_log_return=("fact_log_return", "sum"),
            realized_directional_win_rate=("fact_log_return", lambda s: float((s > 0.0).mean())),
            avg_hold_minutes=("hold_minutes", "mean"),
        )
        .reset_index()
    )
    grouped["compounded_return_pct"] = grouped["total_fact_log_return"].map(lambda x: float(math.exp(float(x)) - 1.0))
    grouped["tp_target_log_return"] = tp_target_log_return

    total_row = pd.DataFrame([{
        "exit_reason": "ALL",
        "n_trades": int(len(trades_df)),
        "avg_entry_price": float(trades_df["entry_price"].mean()),
        "avg_exit_price": float(trades_df["exit_price"].mean()),
        "avg_profit_pct": float(trades_df["profit_pct"].mean()),
        "avg_expected_log_return": float(trades_df["expected_log_return"].mean()),
        "avg_fact_log_return": float(trades_df["fact_log_return"].mean()),
        "avg_fact_1h_max_range_log_return": float(trades_df["fact_1h_max_range_log_return"].mean()),
        "total_fact_log_return": float(trades_df["fact_log_return"].sum()),
        "compounded_return_pct": float(math.exp(float(trades_df["fact_log_return"].sum())) - 1.0),
        "realized_directional_win_rate": float((trades_df["fact_log_return"] > 0.0).mean()),
        "avg_hold_minutes": float(trades_df["hold_minutes"].mean()),
        "tp_target_log_return": tp_target_log_return,
    }])
    return pd.concat([grouped.loc[:, cols], total_row.loc[:, cols]], ignore_index=True)


def _write_strategy_artifact_json(
    *,
    session_id: str,
    long_model_id: str,
    short_model_id: str,
    fit_period: dict[str, str],
    decision_params: dict[str, Any],
    metrics: dict[str, Any],
) -> Path:
    out_path = _artifact_dir(session_id) / "strategy_artifact.json"
    artifact = {
        "session_id": session_id,
        "long_model": long_model_id,
        "short_model": short_model_id,
        "signal_mode": "rank_first",
        "evaluation_mode": "same_window",
        "fit_period": fit_period,
        "rank_lookup_long_path": "rank_lookup_long.parquet",
        "rank_lookup_short_path": "rank_lookup_short.parquet",
        "isotonic_long_path": "isotonic_long.pkl",
        "isotonic_short_path": "isotonic_short.pkl",
        "decision_params": decision_params,
        "metrics": metrics,
        "trade_csv_path": "trade_ledger.csv",
        "summary_csv_path": "summary.csv",
    }
    out_path.write_text(json.dumps(artifact, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return out_path


def main() -> None:
    cfg = _load_strategy_cfg(SESSION_ID)
    asset_id = str(cfg["asset_id"])
    long_model_id = str(cfg["model_id_long"])
    short_model_id = str(cfg["model_id_short"])
    fit_start = str(cfg["fit_period"]["start"])
    fit_end = str(cfg["fit_period"]["end"])

    _recreate_artifact_dir(SESSION_ID)

    scored_df = build_scored_table(
        long_model_id=long_model_id,
        short_model_id=short_model_id,
        asset_id=asset_id,
    )
    calibrated_df, _, _ = fit_calibration(
        session_id=SESSION_ID,
        scored_df=scored_df,
        start=fit_start,
        end=fit_end,
    )

    cutoffs = _build_cutoffs(calibrated_df)
    cutoffs_df = pd.DataFrame(cutoffs)
    fit_start_ts = pd.Timestamp(f"{fit_start} 00:00:00")
    fit_end_ts = pd.Timestamp(f"{fit_end} 23:59:59")
    fit_mask = calibrated_df["open_time"].between(fit_start_ts, fit_end_ts)
    d10_mask = calibrated_df["score_pct_long"] >= 0.9
    tp_target_log_return = float(calibrated_df.loc[fit_mask & d10_mask, "long_mfe_fw60"].median())

    trades_df, equity_df = _simulate_long_d10(calibrated_df, fit_start, fit_end, tp_target_log_return)
    metrics = _compute_metrics(trades_df)
    summary_df = _summary_frame(trades_df, metrics, tp_target_log_return)
    artifact_dir = _artifact_dir(SESSION_ID)
    trades_df.drop(columns=["trade_index", "cumulative_fact_log_return"], errors="ignore").to_csv(
        artifact_dir / "trade_ledger.csv",
        index=False,
    )
    summary_df.to_csv(artifact_dir / "summary.csv", index=False)
    cutoffs_df.to_csv(artifact_dir / "cutoffs.csv", index=False)
    equity_df.to_csv(artifact_dir / "equity_curve.csv", index=False)

    _write_strategy_artifact_json(
        session_id=SESSION_ID,
        long_model_id=long_model_id,
        short_model_id=short_model_id,
        fit_period={"start": fit_start, "end": fit_end},
        decision_params={
            "strategy_variant": "long_only_d10_tp",
            "trade_direction": "long_only",
            "entry_decile": 10,
            "long_entry_pct": 0.90,
            "short_entry_pct": 1.10,
            "tp_target_log_return": round(tp_target_log_return, 6),
            "tp_target_mode": "median_d10_long_target",
            "stop_loss_log_return": round(-tp_target_log_return, 6),
            "max_hold_minutes": 60,
            "min_hold_minutes": 0,
            "cooldown_minutes": 0,
            "min_edge_gap": 0.0,
            "rearm_pct": 0.0,
            "conflict_rule": "long_only",
        },
        metrics=metrics,
    )

    print(f"[OK] rebuilt {SESSION_ID}")
    print(f"  trades: {len(trades_df)}")
    print(f"  tp_target_log_return: {tp_target_log_return:.6f}")
    print(f"  avg_fact_log_return: {metrics['avg_fact_log_return']}")
    print(f"  total_fact_log_return: {metrics['total_fact_log_return']}")
    print(f"  compounded_return_pct: {metrics['compounded_return_pct']}")
    print(f"  trade_csv: {artifact_dir / 'trade_ledger.csv'}")
    print(f"  summary_csv: {artifact_dir / 'summary.csv'}")


if __name__ == "__main__":
    main()
