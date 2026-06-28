"""Helpers for parameterized strategy session analysis notebooks."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


def artifact_dir(root: Path, session_id: str) -> Path:
    """Return strategy artifact directory."""
    return root / "artifacts" / session_id


def load_strategy_config(root: Path, session_id: str) -> dict:
    """Load strategy config entry if present."""
    cfg = json.loads((root / "config" / "strategies.json").read_text(encoding="utf-8"))
    return dict(cfg.get("strategies", {}).get(session_id, {}))


def load_strategy_artifact(root: Path, session_id: str) -> dict:
    """Load strategy_artifact.json."""
    return json.loads((artifact_dir(root, session_id) / "strategy_artifact.json").read_text(encoding="utf-8"))


def asset_id_for_session(root: Path, session_id: str) -> str:
    """Resolve asset_id from config/strategies.json or fallback from model config."""
    strategy_cfg = load_strategy_config(root, session_id)
    asset_id = strategy_cfg.get("asset_id")
    if asset_id:
        return str(asset_id)

    artifact = load_strategy_artifact(root, session_id)
    model_cfg = json.loads((root / "config" / "models.json").read_text(encoding="utf-8"))
    long_meta = model_cfg.get("models", {}).get(artifact["long_model"], {})
    if long_meta.get("asset_id"):
        return str(long_meta["asset_id"])
    return "solusdt"


def _find_repo_root(start: Path) -> Path:
    cur = start.resolve()
    for candidate in [cur, *cur.parents]:
        if (candidate / "analyst").exists() and (candidate / "config").exists():
            return candidate
    raise RuntimeError("Repo root not found.")


def _read_lab_table(asset_id: str, table_fqn: str) -> pd.DataFrame:
    import sys

    root = _find_repo_root(Path.cwd())
    src_path = root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    import utils  # noqa: WPS433

    conn = utils.open_lab_connection(asset_id)
    try:
        return conn.execute(f"SELECT * FROM {table_fqn}").df()
    finally:
        conn.close()


def load_trades(root: Path, session_id: str) -> pd.DataFrame:
    """Load strat trades table with derived columns."""
    artifact = load_strategy_artifact(root, session_id)
    asset_id = asset_id_for_session(root, session_id)
    df = _read_lab_table(asset_id, artifact["trades_table"]).copy()
    if df.empty:
        return df

    df["entry_time"] = pd.to_datetime(df["entry_time"])
    df["exit_time"] = pd.to_datetime(df["exit_time"])
    df = df.sort_values("entry_time").reset_index(drop=True)
    df["trade_index"] = np.arange(len(df), dtype=int)
    df["entry_date"] = df["entry_time"].dt.floor("D")
    df["entry_month"] = df["entry_time"].dt.to_period("M").dt.to_timestamp()
    df["entry_weekday"] = df["entry_time"].dt.day_name()
    df["entry_hour"] = df["entry_time"].dt.hour
    df["holding_hours"] = df["hold_minutes"].astype(float) / 60.0
    df["mfe_proxy"] = df["bucket_mean_mfe"].astype(float)
    df["mfe_proxy_pct"] = (np.exp(df["mfe_proxy"]) - 1.0) * 100.0
    df["realized_return"] = np.where(
        df["direction"].eq("long"),
        (df["exit_price"].astype(float) / df["entry_price"].astype(float)) - 1.0,
        (df["entry_price"].astype(float) / df["exit_price"].astype(float)) - 1.0,
    )
    df["realized_return_pct"] = df["realized_return"] * 100.0
    df["realized_win_flag"] = df["realized_return"] > 0.0
    df["proxy_win_flag"] = df["mfe_proxy"] > 0.0
    df["cum_mfe_proxy"] = df["mfe_proxy"].cumsum()
    df["cum_mfe_proxy_pct"] = (np.exp(df["cum_mfe_proxy"]) - 1.0) * 100.0
    df["cum_realized_return"] = df["realized_return"].cumsum()
    df["cum_realized_return_pct"] = df["cum_realized_return"] * 100.0
    peak = df["cum_mfe_proxy"].cummax()
    df["drawdown_from_peak"] = df["cum_mfe_proxy"] - peak
    realized_peak = df["cum_realized_return"].cummax()
    df["realized_drawdown_from_peak"] = df["cum_realized_return"] - realized_peak
    return df


def load_equity(root: Path, session_id: str) -> pd.DataFrame:
    """Load strat equity table."""
    artifact = load_strategy_artifact(root, session_id)
    asset_id = asset_id_for_session(root, session_id)
    df = _read_lab_table(asset_id, artifact["equity_table"]).copy()
    if df.empty:
        return df
    df["entry_time"] = pd.to_datetime(df["entry_time"])
    df["cumulative_mfe_pct"] = (np.exp(df["cumulative_mfe"].astype(float)) - 1.0) * 100.0
    return df.sort_values("trade_index").reset_index(drop=True)


def load_cutoffs(root: Path, session_id: str) -> pd.DataFrame:
    """Load strat cutoffs table."""
    artifact = load_strategy_artifact(root, session_id)
    asset_id = asset_id_for_session(root, session_id)
    df = _read_lab_table(asset_id, artifact["cutoffs_table"]).copy()
    if df.empty:
        return df
    return df.sort_values(["direction", "bucket_id"]).reset_index(drop=True)


def entry_decile_summary(
    trades: pd.DataFrame,
    cutoffs: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize executed trades against development-time decile expectations."""
    if cutoffs.empty:
        return pd.DataFrame()

    base = cutoffs.loc[:, ["direction", "bucket_id", "bucket_mean_mfe", "bucket_hit_rate"]].copy()
    base = base.rename(columns={"bucket_id": "entry_decile"})
    base["target_1h_max_log_return_raw"] = base["bucket_mean_mfe"]
    base["target_1h_directional_hit_rate"] = np.where(
        base["direction"].eq("short"),
        1.0 - base["bucket_hit_rate"],
        base["bucket_hit_rate"],
    )
    base["target_1h_max_log_return"] = np.where(
        base["direction"].eq("short"),
        -base["target_1h_max_log_return_raw"],
        base["target_1h_max_log_return_raw"],
    )
    base = base.drop(columns=["bucket_mean_mfe", "bucket_hit_rate"])

    if trades.empty:
        base["executed_trades"] = 0
        base["realized_exit_log_return_avg"] = np.nan
        base["realized_exit_log_return_sum"] = np.nan
        base["realized_directional_win_rate"] = np.nan
        return base.sort_values(["direction", "entry_decile"]).reset_index(drop=True)

    executed = trades.copy()
    executed["entry_decile"] = np.minimum(np.ceil(executed["score_pct_at_entry"] * 10.0).astype(int), 10)
    actual = (
        executed.groupby(["direction", "entry_decile"], observed=True)
        .agg(
            executed_trades=("trade_index", "size"),
            realized_exit_log_return_avg=("realized_return", "mean"),
            realized_exit_log_return_sum=("realized_return", "sum"),
            realized_directional_win_rate=("realized_win_flag", "mean"),
        )
        .reset_index()
    )
    out = base.merge(actual, on=["direction", "entry_decile"], how="left")
    out["executed_trades"] = out["executed_trades"].fillna(0).astype(int)
    cols = [
        "direction",
        "entry_decile",
        "target_1h_max_log_return_raw",
        "target_1h_directional_hit_rate",
        "target_1h_max_log_return",
        "executed_trades",
        "realized_exit_log_return_avg",
        "realized_exit_log_return_sum",
        "realized_directional_win_rate",
    ]
    return out.loc[:, cols].sort_values(["direction", "entry_decile"]).reset_index(drop=True)


def load_sweep_results(root: Path, session_id: str) -> pd.DataFrame:
    """Load Optuna sweep CSV."""
    path = artifact_dir(root, session_id) / "sweep_results.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path).sort_values("value", ascending=False).reset_index(drop=True)


def summary_snapshot(trades: pd.DataFrame, artifact: dict) -> pd.DataFrame:
    """Return compact top-level summary."""
    if trades.empty:
        return pd.DataFrame([{
            "session_id": artifact["session_id"],
            "rows": 0,
        }])

    best_idx = trades["realized_return"].idxmax()
    worst_idx = trades["realized_return"].idxmin()
    return pd.DataFrame([{
        "session_id": artifact["session_id"],
        "long_model": artifact["long_model"],
        "short_model": artifact["short_model"],
        "fit_start": artifact["fit_period"]["start"],
        "fit_end": artifact["fit_period"]["end"],
        "trade_rows": len(trades),
        "first_entry": trades["entry_time"].min(),
        "last_entry": trades["entry_time"].max(),
        "realized_win_rate": trades["realized_win_flag"].mean(),
        "proxy_win_rate": trades["proxy_win_flag"].mean(),
        "avg_realized_return": trades["realized_return"].mean(),
        "total_realized_return": trades["realized_return"].sum(),
        "avg_mfe_proxy": trades["mfe_proxy"].mean(),
        "total_mfe_proxy": trades["mfe_proxy"].sum(),
        "proxy_max_drawdown": trades["drawdown_from_peak"].min(),
        "realized_max_drawdown": trades["realized_drawdown_from_peak"].min(),
        "avg_hold_minutes": trades["hold_minutes"].mean(),
        "median_hold_minutes": trades["hold_minutes"].median(),
        "best_trade_realized_return": trades.loc[best_idx, "realized_return"],
        "worst_trade_realized_return": trades.loc[worst_idx, "realized_return"],
    }])


def direction_summary(trades: pd.DataFrame) -> pd.DataFrame:
    """Summary by long/short direction."""
    if trades.empty:
        return pd.DataFrame()
    grouped = trades.groupby("direction", observed=True)
    rows = grouped.agg(
        trades=("trade_index", "size"),
        realized_win_rate=("realized_win_flag", "mean"),
        proxy_win_rate=("proxy_win_flag", "mean"),
        avg_realized_return=("realized_return", "mean"),
        median_realized_return=("realized_return", "median"),
        total_realized_return=("realized_return", "sum"),
        avg_mfe_proxy=("mfe_proxy", "mean"),
        median_mfe_proxy=("mfe_proxy", "median"),
        total_mfe_proxy=("mfe_proxy", "sum"),
        avg_hold_minutes=("hold_minutes", "mean"),
        median_hold_minutes=("hold_minutes", "median"),
    ).reset_index()
    rows["trade_share"] = rows["trades"] / len(trades)
    return rows.sort_values("direction").reset_index(drop=True)


def exit_reason_summary(trades: pd.DataFrame) -> pd.DataFrame:
    """Summary by exit_reason."""
    if trades.empty:
        return pd.DataFrame()
    grouped = trades.groupby("exit_reason", observed=True)
    rows = grouped.agg(
        trades=("trade_index", "size"),
        realized_win_rate=("realized_win_flag", "mean"),
        proxy_win_rate=("proxy_win_flag", "mean"),
        avg_realized_return=("realized_return", "mean"),
        total_realized_return=("realized_return", "sum"),
        avg_mfe_proxy=("mfe_proxy", "mean"),
        total_mfe_proxy=("mfe_proxy", "sum"),
        avg_hold_minutes=("hold_minutes", "mean"),
    ).reset_index()
    rows["trade_share"] = rows["trades"] / len(trades)
    return rows.sort_values("trades", ascending=False).reset_index(drop=True)


def monthly_summary(trades: pd.DataFrame) -> pd.DataFrame:
    """Monthly trade performance by entry month."""
    if trades.empty:
        return pd.DataFrame()
    grouped = trades.groupby("entry_month", observed=True)
    rows = grouped.agg(
        trades=("trade_index", "size"),
        realized_win_rate=("realized_win_flag", "mean"),
        proxy_win_rate=("proxy_win_flag", "mean"),
        avg_realized_return=("realized_return", "mean"),
        total_realized_return=("realized_return", "sum"),
        avg_mfe_proxy=("mfe_proxy", "mean"),
        total_mfe_proxy=("mfe_proxy", "sum"),
        avg_hold_minutes=("hold_minutes", "mean"),
        long_share=("direction", lambda s: (s == "long").mean()),
    ).reset_index()
    rows["cum_total_realized_return"] = rows["total_realized_return"].cumsum()
    rows["cum_total_realized_return_pct"] = rows["cum_total_realized_return"] * 100.0
    rows["cum_total_mfe_proxy"] = rows["total_mfe_proxy"].cumsum()
    rows["cum_total_mfe_proxy_pct"] = (np.exp(rows["cum_total_mfe_proxy"]) - 1.0) * 100.0
    return rows.sort_values("entry_month").reset_index(drop=True)


def daily_summary(trades: pd.DataFrame) -> pd.DataFrame:
    """Daily trade performance by entry date."""
    if trades.empty:
        return pd.DataFrame()
    grouped = trades.groupby("entry_date", observed=True)
    rows = grouped.agg(
        trades=("trade_index", "size"),
        realized_win_rate=("realized_win_flag", "mean"),
        proxy_win_rate=("proxy_win_flag", "mean"),
        avg_realized_return=("realized_return", "mean"),
        total_realized_return=("realized_return", "sum"),
        avg_mfe_proxy=("mfe_proxy", "mean"),
        total_mfe_proxy=("mfe_proxy", "sum"),
        long_share=("direction", lambda s: (s == "long").mean()),
    ).reset_index()
    rows["cum_total_realized_return"] = rows["total_realized_return"].cumsum()
    rows["cum_total_mfe_proxy"] = rows["total_mfe_proxy"].cumsum()
    return rows.sort_values("entry_date").reset_index(drop=True)


def weekday_summary(trades: pd.DataFrame) -> pd.DataFrame:
    """Weekday summary."""
    if trades.empty:
        return pd.DataFrame()
    order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    grouped = trades.groupby("entry_weekday", observed=True)
    rows = grouped.agg(
        trades=("trade_index", "size"),
        realized_win_rate=("realized_win_flag", "mean"),
        proxy_win_rate=("proxy_win_flag", "mean"),
        avg_realized_return=("realized_return", "mean"),
        total_realized_return=("realized_return", "sum"),
        avg_mfe_proxy=("mfe_proxy", "mean"),
        total_mfe_proxy=("mfe_proxy", "sum"),
    ).reset_index()
    rows["entry_weekday"] = pd.Categorical(rows["entry_weekday"], categories=order, ordered=True)
    return rows.sort_values("entry_weekday").reset_index(drop=True)


def hour_summary(trades: pd.DataFrame) -> pd.DataFrame:
    """Hour-of-day summary."""
    if trades.empty:
        return pd.DataFrame()
    grouped = trades.groupby("entry_hour", observed=True)
    rows = grouped.agg(
        trades=("trade_index", "size"),
        realized_win_rate=("realized_win_flag", "mean"),
        proxy_win_rate=("proxy_win_flag", "mean"),
        avg_realized_return=("realized_return", "mean"),
        total_realized_return=("realized_return", "sum"),
        avg_mfe_proxy=("mfe_proxy", "mean"),
        total_mfe_proxy=("mfe_proxy", "sum"),
    ).reset_index()
    return rows.sort_values("entry_hour").reset_index(drop=True)


def monthly_direction_pivot(trades: pd.DataFrame) -> pd.DataFrame:
    """Monthly trade counts by direction."""
    if trades.empty:
        return pd.DataFrame()
    pivot = trades.pivot_table(
        index="entry_month",
        columns="direction",
        values="trade_index",
        aggfunc="count",
        fill_value=0,
    ).reset_index()
    pivot.columns.name = None
    return pivot.sort_values("entry_month").reset_index(drop=True)


def rolling_trade_summary(trades: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """Rolling stability metrics across trade order."""
    if trades.empty:
        return pd.DataFrame()
    out = trades[["trade_index", "entry_time", "mfe_proxy", "realized_return", "realized_win_flag", "proxy_win_flag", "hold_minutes"]].copy()
    out["rolling_realized_win_rate"] = out["realized_win_flag"].rolling(window, min_periods=window).mean()
    out["rolling_proxy_win_rate"] = out["proxy_win_flag"].rolling(window, min_periods=window).mean()
    out["rolling_avg_realized_return"] = out["realized_return"].rolling(window, min_periods=window).mean()
    out["rolling_total_realized_return"] = out["realized_return"].rolling(window, min_periods=window).sum()
    out["rolling_avg_mfe_proxy"] = out["mfe_proxy"].rolling(window, min_periods=window).mean()
    out["rolling_total_mfe_proxy"] = out["mfe_proxy"].rolling(window, min_periods=window).sum()
    out["rolling_avg_hold_minutes"] = out["hold_minutes"].rolling(window, min_periods=window).mean()
    return out


def drawdown_segments(trades: pd.DataFrame) -> pd.DataFrame:
    """Return worst drawdown rows for inspection."""
    if trades.empty:
        return pd.DataFrame()
    cols = [
        "trade_index", "entry_time", "direction", "realized_return", "cum_realized_return",
        "realized_drawdown_from_peak", "mfe_proxy", "drawdown_from_peak", "exit_reason",
    ]
    return trades.loc[:, cols].sort_values("realized_drawdown_from_peak").head(10).reset_index(drop=True)


def best_worst_trades(trades: pd.DataFrame, n: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return top and bottom trades by realized return."""
    if trades.empty:
        return pd.DataFrame(), pd.DataFrame()
    cols = [
        "trade_index", "entry_time", "exit_time", "direction", "realized_return",
        "realized_return_pct", "mfe_proxy", "mfe_proxy_pct", "score_pct_at_entry", "hold_minutes", "exit_reason",
        "entry_price", "exit_price",
    ]
    best = trades.loc[:, cols].sort_values("realized_return", ascending=False).head(n).reset_index(drop=True)
    worst = trades.loc[:, cols].sort_values("realized_return", ascending=True).head(n).reset_index(drop=True)
    return best, worst


def streak_summary(trades: pd.DataFrame) -> pd.DataFrame:
    """Compute longest win/loss streaks."""
    if trades.empty:
        return pd.DataFrame()
    values = trades["realized_win_flag"].tolist()
    longest_win = 0
    longest_loss = 0
    cur_win = 0
    cur_loss = 0
    for flag in values:
        if flag:
            cur_win += 1
            cur_loss = 0
        else:
            cur_loss += 1
            cur_win = 0
        longest_win = max(longest_win, cur_win)
        longest_loss = max(longest_loss, cur_loss)
    return pd.DataFrame([{
        "longest_win_streak": longest_win,
        "longest_loss_streak": longest_loss,
    }])


def top_bottom_days(daily: pd.DataFrame, n: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return best and worst days by total realized return."""
    if daily.empty:
        return pd.DataFrame(), pd.DataFrame()
    best = daily.sort_values("total_realized_return", ascending=False).head(n).reset_index(drop=True)
    worst = daily.sort_values("total_realized_return", ascending=True).head(n).reset_index(drop=True)
    return best, worst


def commentary_lines(
    artifact: dict,
    trades: pd.DataFrame,
    monthly: pd.DataFrame,
    direction_df: pd.DataFrame,
    exit_df: pd.DataFrame,
) -> list[str]:
    """Generate short interpretation bullets."""
    if trades.empty:
        return ["A session trade ledger üres, ezért nincs értelmezhető backtest-visszanézet."]

    best_month = monthly.sort_values("total_realized_return", ascending=False).iloc[0]
    worst_month = monthly.sort_values("total_realized_return", ascending=True).iloc[0]
    stronger_direction = direction_df.sort_values("avg_realized_return", ascending=False).iloc[0]
    top_exit = exit_df.sort_values("trades", ascending=False).iloc[0]
    return [
        (
            f"A szimuláció {len(trades)} lezárt trade-et adott a {artifact['fit_period']['start']} – "
            f"{artifact['fit_period']['end']} ablakban; az összesített realizált áreredmény "
            f"{trades['realized_return'].sum():.3%}, a realizált win rate {trades['realized_win_flag'].mean():.1%}."
        ),
        (
            f"A legerősebb hónap {best_month['entry_month']:%Y-%m} volt "
            f"({best_month['trades']} trade, összesen {best_month['total_realized_return']:.3%}), "
            f"a leggyengébb {worst_month['entry_month']:%Y-%m} "
            f"({worst_month['total_realized_return']:.3%})."
        ),
        (
            f"Irány szerint a jobb átlagos trade-minőséget a `{stronger_direction['direction']}` oldal hozta "
            f"({stronger_direction['avg_realized_return']:.3%} átlag/trade)."
        ),
        (
            f"A leggyakoribb kilépési mód az `{top_exit['exit_reason']}` volt "
            f"({int(top_exit['trades'])} trade, {top_exit['trade_share']:.1%} részarány)."
        ),
        (
            "A realizált áreredmény most elsődleges metrika; az MFE proxy külön marad, "
            "mint másodlagos döntési minőségjel."
        ),
    ]
