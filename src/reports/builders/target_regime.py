# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false
"""General report ``target_yearly_regime``: yearly behaviour of the MFE targets (replaces notebook 3200).

Source: live DB ``target`` table (``open_time``, ``long_mfe_fw60``, ``short_mfe_fw60``).
Scope ``general`` -> ``_doc_/reports/target_yearly_regime/``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from reports import ReportBuilder, theme
from reports.builders._common import fmt, read_live, source_info, themed

logger = logging.getLogger(__name__)

SLUG = "target_yearly_regime"
GENERATOR = "reports.builders.target_regime"
TARGETS = {"long_mfe_fw60": theme.BLUE, "short_mfe_fw60": theme.ORANGE}


def _load() -> pd.DataFrame | None:
    cols = ", ".join(TARGETS)
    sql = (
        "SELECT date_trunc('month', open_time) AS month, year(open_time) AS year, "
        "COUNT(*) AS n, " + ", ".join(f"AVG({c}) AS {c}_mean, STDDEV({c}) AS {c}_std" for c in TARGETS)
        + f" FROM target WHERE long_mfe_fw60 IS NOT NULL AND short_mfe_fw60 IS NOT NULL GROUP BY 1, 2 ORDER BY 1 -- {cols}"
    )
    return read_live(sql)


def build(root: Path | None = None) -> Path | None:
    """Build and write the report; ``None`` when the live DB is unavailable."""
    monthly = _load()
    if monthly is None or monthly.empty:
        logger.warning("target_yearly_regime skipped: no data")
        return None
    monthly["month"] = pd.to_datetime(monthly["month"])
    rows = []
    for year, g in monthly.groupby("year"):
        n = int(g["n"].sum())
        row: dict[str, object] = {"year": int(year), "n_obs": n, "first_month": g["month"].min().strftime("%Y-%m"),
                                  "last_month": g["month"].max().strftime("%Y-%m")}
        for c in TARGETS:
            row[f"{c}_mean"] = float((g[f"{c}_mean"] * g["n"]).sum() / n)
        rows.append(row)
    yearly = pd.DataFrame(rows)

    rb = ReportBuilder(
        scope="general", slug=SLUG, order=10, root=root,
        title="Target éves rezsim (long/short MFE fw60)",
        summary="A long_mfe_fw60 és short_mfe_fw60 target havi/éves átlaga: mennyire homogén a history.",
        tags=["target", "regime"], source=source_info("target", GENERATOR),
    )
    rb.add_markdown(
        "## Cél\n\nA `target` tábla (`long_mfe_fw60`, `short_mfe_fw60`) éves viselkedése: használható-e a teljes "
        "history egyben tanításra / kalibrációra, vagy a target szintje rezsimenként eltér.\n\n"
        "**Kapcsolódó dokumentáció:** módszertan `_doc_/methodology_doc/`, kód `_doc_/database_and_code_doc/3100_sync_targets.md`. "
        "Az összefoglaló régi notebook-konklúziója: `_doc_/models_doc/target_yearly_regime.md`."
    )
    fig = go.Figure()
    for c, color in TARGETS.items():
        fig.add_trace(go.Scatter(x=monthly["month"], y=monthly[f"{c}_mean"], name=c, mode="lines",
                                 line={"color": color, "width": 1.5}))
    fig.update_yaxes(title="Havi átlag", tickformat=".2%")
    rb.add_markdown("## 1. Havi átlag az időben\n\nA havi átlagos MFE a teljes időszakon; a long és a short (negatív) target együtt.")
    rb.add_chart("monthly_mean", themed(fig, height=420), title="Havi átlagos target")

    rb.add_markdown("## 2. Éves összesítő\n\nMegfigyelésszám és súlyozott éves átlag; a csonka évek (első/utolsó hónap) jelölve a dátumoknál.")
    rb.add_table("yearly_summary", yearly, title="Éves összesítő", columns={
        "year": "Év", "n_obs": "Megfigyelések", "first_month": "Első hónap", "last_month": "Utolsó hónap",
        "long_mfe_fw60_mean": "Átlag long_mfe_fw60", "short_mfe_fw60_mean": "Átlag short_mfe_fw60",
    })
    fig2 = go.Figure()
    for c, color in TARGETS.items():
        fig2.add_trace(go.Bar(x=yearly["year"].astype(str), y=yearly[f"{c}_mean"], name=c, marker_color=color))
    fig2.update_layout(barmode="group")
    fig2.update_yaxes(title="Éves átlag", tickformat=".2%")
    rb.add_chart("yearly_mean", themed(fig2, height=360), title="Éves átlagos target")

    first, last = yearly.iloc[0], yearly.iloc[-1]
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"A long target éves átlaga {fmt(first['long_mfe_fw60_mean'], 4)} ({int(first['year'])}) értékről "
        f"{fmt(last['long_mfe_fw60_mean'], 4)} ({int(last['year'])}) értékre változott; a short oldal "
        f"{fmt(first['short_mfe_fw60_mean'], 4)} -> {fmt(last['short_mfe_fw60_mean'], 4)}. "
        "A korai (2021-2022) évek magasabb opportunity-szintet jelentenek; a teljes history nem kizárandó, de nem "
        "kezelhető egyetlen rezsimként: expanding-window + recency-aware validáció indokolt, a kalibrációhoz "
        "a legutóbbi teljes évek az elsődleges referencia."
    )
    return rb.write()


if __name__ == "__main__":
    print(build())
