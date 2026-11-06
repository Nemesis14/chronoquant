# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false
# (pandas/plotly stubs are too loose for this numeric code; behaviour is covered by tests)
"""Report ``04_strategy``: how the strategy works, validation-period trades, weekly charts.

Source: ``<artifact>/strategy/{strategy_artifact.json, rank_lookup_<dir>.parquet}``,
``strat."<session>__trades"`` (lab DB) and ``predictions`` + ``ohlcv`` (live DB, read-only).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import utils
from reports import ReportBuilder, theme
from reports.builders._common import (
    artifact_dir_of,
    direction_of,
    fmt,
    read_live,
    source_info,
    themed,
)

logger = logging.getLogger(__name__)

SLUG = "04_strategy"
GENERATOR = "reports.builders.strategy"
TRADE_SIZE = 1000.0
GREEN, YELLOW, RED = "#0ecb81", "#f0b90b", "#f6465d"
_OUTCOME_COLOR = {"green": GREEN, "yellow": YELLOW, "red": RED}


def _iso(series: pd.Series) -> pd.Series:
    return series.dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_trades(session_id: str, valid_start: str) -> pd.DataFrame:
    con = duckdb.connect(utils.lab_db_path(), read_only=True)
    try:
        trades = con.execute(
            f"""
            SELECT entry_time, exit_time, entry_price, exit_price, fact_log_return, exit_reason,
                   hold_minutes
            FROM strat."{session_id}__trades"
            WHERE entry_time >= ?
            ORDER BY entry_time
            """,
            [valid_start],
        ).fetchdf()
    finally:
        con.close()
    trades["entry_time"] = pd.to_datetime(trades["entry_time"])
    trades["exit_time"] = pd.to_datetime(trades["exit_time"])
    trades["outcome"] = np.where(
        trades["exit_reason"] == "tp", "green", np.where(trades["fact_log_return"] > 0, "yellow", "red")
    )
    trades["pnl"] = TRADE_SIZE * (np.exp(trades["fact_log_return"]) - 1)
    trades["week"] = trades["entry_time"].dt.to_period("W").dt.start_time
    return trades


def _weekly_table(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for week, grp in trades.groupby("week"):
        rows.append(_week_row(pd.Timestamp(week).strftime("%Y-%m-%d"), grp))
    rows.append(_week_row("ÖSSZESEN", trades))
    return pd.DataFrame(rows)


def _week_row(label: str, grp: pd.DataFrame) -> dict[str, Any]:
    pnl = {o: float(grp.loc[grp["outcome"] == o, "pnl"].sum()) for o in _OUTCOME_COLOR}
    n = len(grp)
    green = int((grp["outcome"] == "green").sum())
    return {
        "week": label, "trades": n, "green": green,
        "yellow": int((grp["outcome"] == "yellow").sum()), "red": int((grp["outcome"] == "red").sum()),
        "tp_rate": green / n if n else 0.0, "g_gain": pnl["green"], "y_gain": pnl["yellow"],
        "r_gain": pnl["red"], "net": sum(pnl.values()),
    }


def _week_chart(week_start: pd.Timestamp, df_all: pd.DataFrame, trades: pd.DataFrame, pct_col: str,
                threshold: float) -> go.Figure:
    week_end = week_start + pd.Timedelta(days=7)
    win = df_all[(df_all["open_time"] >= week_start) & (df_all["open_time"] < week_end)].iloc[::5]
    in_week = trades[(trades["entry_time"] >= week_start) & (trades["entry_time"] < week_end)]
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.04, row_heights=[0.65, 0.35])
    fig.add_trace(go.Scatter(x=_iso(win["open_time"]), y=win["close"], mode="lines", name="Close (USDT)",
                             line={"color": theme.BLUE, "width": 1}), row=1, col=1)
    for outcome, color in _OUTCOME_COLOR.items():
        part = in_week[in_week["outcome"] == outcome]
        if part.empty:
            continue
        label = {"green": "TP", "yellow": "Timeout +", "red": "Timeout -"}[outcome]
        fig.add_trace(go.Scatter(x=_iso(part["entry_time"]), y=part["entry_price"], mode="markers",
                                 name=label, marker={"color": color, "size": 9,
                                                     "line": {"width": 1, "color": "white"}}),
                      row=1, col=1)
    fig.add_trace(go.Scatter(x=_iso(win["open_time"]), y=win[pct_col], mode="lines", name=pct_col,
                             line={"color": theme.BLUE, "width": 0.9}, showlegend=False), row=2, col=1)
    fig.add_hline(y=threshold, row=2, col=1, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1})
    fig.update_yaxes(title_text="Close (USDT)", row=1, col=1)
    fig.update_yaxes(title_text=pct_col, range=[-0.02, 1.02], row=2, col=1)
    return themed(fig, height=520, x_title="UTC")


def _equity_chart(trades: pd.DataFrame, valid_start: pd.Timestamp) -> go.Figure:
    ordered = trades.sort_values("exit_time")
    fig = go.Figure(go.Scatter(
        x=_iso(ordered["exit_time"]), y=ordered["fact_log_return"].cumsum(), mode="lines",
        name="Kumulált log return", line={"color": theme.LONG, "width": 1.8}, fill="tozeroy",
        fillcolor="rgba(25,135,84,0.10)",
    ))
    return themed(fig, x_title=f"Kilépés (valid kezdet: {valid_start:%Y-%m-%d})",
                  y_title="Kumulált fact_log_return")


def build(model_id: str, artifact_dir: Path | None = None, root: Path | None = None) -> Path:
    """Build and write the ``04_strategy`` report from the model's strategy artifact."""
    artifact_dir = artifact_dir or artifact_dir_of(model_id)
    strategy_dir = artifact_dir / "strategy"
    artifact = json.loads((strategy_dir / "strategy_artifact.json").read_text(encoding="utf-8"))
    direction = direction_of(model_id)
    params = artifact["decision_params"]
    entry_cutoff = float(params["entry_cutoff"])
    session_id = artifact.get("session_id", "")
    fit = artifact.get("fit_period", {})
    valid_start, valid_end = fit.get("start", ""), fit.get("end", "")
    metrics = artifact.get("metrics", {})
    pred_col, pct_col = f"{direction}_pred", f"{direction}_pct"

    rl = pd.read_parquet(strategy_dir / f"rank_lookup_{direction}.parquet")
    if direction == "long":
        tp_log = float(rl[rl["score_pct"] >= entry_cutoff]["bucket_median_mfe"].iloc[0])
        threshold = entry_cutoff
    else:
        tp_log = float(rl[rl["score_pct"] <= 1.0 - entry_cutoff]["bucket_median_mfe"].iloc[-1])
        threshold = 1.0 - entry_cutoff
    max_hold = int(params["max_hold_minutes"])
    sl_spec = params.get("sl_spec", "none")

    trades = _load_trades(session_id, valid_start)
    dir_label = "Long" if direction == "long" else "Short"
    pct_low = direction == "short"

    rb = ReportBuilder(
        scope="model", model_id=model_id, slug=SLUG, order=40, root=root,
        title="Stratégia (validációs periódus)",
        summary=f"{dir_label} stratégia: cutoff {entry_cutoff}, {len(trades)} validációs trade, "
                f"win rate {fmt(metrics.get('win_rate'), 3)}.",
        tags=["strategy", direction], source=source_info("strategy", GENERATOR),
    )
    entry_text = (
        f"egy bár long belépőt ad, ha `long_pct >= {entry_cutoff}` (a kalibrációs eloszlás felső "
        f"{(1 - entry_cutoff) * 100:.1f}%-a)"
        if direction == "long" else
        f"egy bár short belépőt ad, ha `(1 - short_pct) >= {entry_cutoff}`, azaz "
        f"`short_pct <= {1.0 - entry_cutoff:.2f}` (a legszélsőségesebb short jelek alsó "
        f"{(1 - entry_cutoff) * 100:.1f}%-a)"
    )
    tp_side = "fölé emelkedik" if direction == "long" else "alá esik"
    rb.add_markdown(
        "## A stratégia működése\n\n"
        "A stratégia a modell nyers predikciós pontszámát három offline származtatott elemmel "
        "kereskedési döntéssé alakítja:\n\n"
        f"1. **Rank kalibráció** - a nyers `{pred_col}` pontszámot a kalibrációs periódusból épített "
        f"lookup (`rank_lookup_{direction}.parquet`) rank-percentilisre (`{pct_col}`, 0-1) képezi. "
        f"{'Alacsony `short_pct` = a legerősebb short jel.' if pct_low else 'Magas `long_pct` = a legerősebb long jel.'} "
        "Lásd: `_doc_/methodology_doc/6100_strategy_calibration.md`.\n"
        f"2. **Belépési szabály** - {entry_text}. A belépés a jelző bár close árán történik. "
        "Lásd: `_doc_/methodology_doc/6300_strategy_grid_search.md`.\n"
        f"3. **Kilépési szabály** - amelyik előbb teljesül: **Take Profit**, ha a következő bár "
        f"{'high' if direction == 'long' else 'low'} értéke eléri az `entry_close x exp(bucket_median_mfe)` "
        f"szintet (a TP log return {tp_log:+.6f}, azaz {tp_log * 100:+.4f}%, vagyis az ár {tp_side}), "
        f"vagy **Timeout**: {max_hold} bár ({max_hold} perc) után a pozíció a záró close áron zár. "
        f"Stop loss: `{sl_spec}`.\n\n"
        f"A TP szint (`bucket_median_mfe = {tp_log:.6f}`) a felső kalibrációs bucket mintáinak "
        "mediánja. Teljes stratégiatervezés: `_doc_/methodology_doc/6000_strategy.md`."
    )

    summary_rows = [
        {"parameter": "Modell", "value": str(artifact.get(f"{direction}_model", model_id))},
        {"parameter": "Irány", "value": direction},
        {"parameter": "Entry cutoff", "value": str(entry_cutoff)},
        {"parameter": "TP spec", "value": str(params.get("tp_spec"))},
        {"parameter": "SL spec", "value": str(sl_spec)},
        {"parameter": "Max hold (perc)", "value": str(max_hold)},
        {"parameter": "Signal mode", "value": str(artifact.get("signal_mode"))},
        {"parameter": "Trade-ek (valid)", "value": str(metrics.get("n_trades"))},
        {"parameter": "Win rate", "value": f"{metrics['win_rate']:.1%}" if "win_rate" in metrics else "-"},
        {"parameter": "Összetett hozam",
         "value": f"{metrics['compounded_return_pct']:.2f}%" if "compounded_return_pct" in metrics else "-"},
        {"parameter": "Fit periódus", "value": f"{valid_start} - {valid_end}"},
        {"parameter": "TP log return", "value": f"{tp_log:.6f} ({tp_log * 100:.4f}%)"},
        {"parameter": "Session", "value": session_id},
    ]
    rb.add_markdown("## Stratégia összefoglaló")
    rb.add_table("strategy_summary", pd.DataFrame(summary_rows), title="Stratégia paraméterek",
                 columns={"parameter": "Paraméter", "value": {"label": "Érték", "type": "str"}})

    rb.add_markdown("## Heti trade összesítő\n\nZöld = TP találat, sárga = pozitív timeout, piros = "
                    f"negatív timeout. A $ értékek {TRADE_SIZE:,.0f} USDT pozícióméretre vonatkoznak.")
    if trades.empty:
        rb.add_markdown("_A validációs periódusban nincs trade._")
    else:
        weekly = _weekly_table(trades)
        rb.add_table("weekly_summary", weekly, title="Heti összesítő", columns={
            "week": "Hét", "trades": "Trade", "green": "Zöld", "yellow": "Sárga", "red": "Piros",
            "tp_rate": {"label": "TP arány", "type": "pct", "digits": 1},
            "g_gain": {"label": "Zöld $", "type": "float", "digits": 0},
            "y_gain": {"label": "Sárga $", "type": "float", "digits": 0},
            "r_gain": {"label": "Piros $", "type": "float", "digits": 0},
            "net": {"label": "Nettó $", "type": "float", "digits": 0}})
        rb.add_chart("equity", _equity_chart(trades, pd.Timestamp(valid_start)),
                     title="Kumulált hozam a validációs periódusban", height=360)

        rb.add_markdown("## Validációs periódus - heti ábrák\n\nMinden héthez (legalább egy trade) egy "
                        "ábra: felül a close és a belépések, alul a rank-percentilis a belépési küszöbbel.")
        live = read_live(
            """
            SELECT p.open_time, p.long_pred, p.short_pred, o.close
            FROM predictions p JOIN ohlcv o ON p.open_time = o.open_time
            WHERE p.open_time >= ? ORDER BY p.open_time
            """,
            [valid_start],
        )
        if live is None or live.empty:
            rb.add_markdown("_Az élő adatbázis nem érhető el (zárolt) - a heti ábrák kimaradtak._")
        else:
            live["open_time"] = pd.to_datetime(live["open_time"])
            live[pct_col] = np.interp(
                live[pred_col].to_numpy(), rl["score_raw"].to_numpy(), rl["score_pct"].to_numpy()
            ).clip(0, 1)
            for week_start, grp in trades.groupby("week"):
                ws = pd.Timestamp(week_start)
                rb.add_markdown(f"### {ws:%Y-%m-%d} - {(ws + pd.Timedelta(days=6)):%Y-%m-%d} ({len(grp)} trade)")
                rb.add_chart(f"week_{ws:%Y_%m_%d}", _week_chart(ws, live, trades, pct_col, threshold),
                             title=f"Hét {ws:%Y-%m-%d}", height=520)

    n = len(trades)
    green = int((trades["outcome"] == "green").sum()) if n else 0
    net = float(trades["pnl"].sum()) if n else 0.0
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"- A validációs periódusban **{n}** trade volt, ebből {green} TP "
        f"({green / n:.1%} TP arány), a nettó eredmény {net:+,.0f} USDT {TRADE_SIZE:,.0f} USDT "
        "pozícióméreten.\n" if n else
        "## Értelmezés\n\n- A validációs periódusban nem keletkezett trade; a cutoff túl szigorú vagy "
        "a periódus túl rövid.\n"
    )
    if n:
        rb.add_markdown(
            f"- A timeout kilépések ({n - green} db) döntik el az eredményt: a TP arány "
            f"{'magas, a stratégia többnyire eléri a célt' if green / n >= 0.5 else 'mérsékelt, a timeoutok súlya jelentős'}.\n"
            f"- Az artifact összetett hozama {fmt(metrics.get('compounded_return_pct'), 2)}%, win rate "
            f"{fmt(metrics.get('win_rate'), 3)}; a heti táblában a negatív hetek száma mutatja a "
            "hozam egyenletességét."
        )
    return rb.write()
