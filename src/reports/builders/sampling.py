# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false
# (pandas/plotly stubs are too loose for groupby/.dt/add_hline typing; behaviour is covered by tests)
"""Report ``01_sampling``: train/valid period of the model sample (replaces the 01_sampling notebook).

Source: ``model."<model_id>__sample"`` (open_time, <target>, split) + hourly closes of the live DB.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.stats import gaussian_kde

import utils
from reports import ReportBuilder, theme
from reports.builders._common import (
    direction_of,
    fmt,
    read_live,
    source_info,
    target_of,
    themed,
    vrect,
)

logger = logging.getLogger(__name__)

SLUG = "01_sampling"
GENERATOR = "reports.builders.sampling"
_COLORS = {"Train": theme.BLUE, "Valid": theme.ORANGE}


def _load_sample(model_id: str, target: str) -> pd.DataFrame:
    con = duckdb.connect(utils.lab_db_path(), read_only=True)
    try:
        df = con.execute(
            f'SELECT open_time, {target} AS target, split FROM model."{model_id}__sample"'
        ).fetchdf()
    finally:
        con.close()
    df["open_time"] = pd.to_datetime(df["open_time"])
    df["segment"] = df["split"].map({0: "Train", 1: "Valid"})
    return df.dropna(subset=["segment"]).sort_values("open_time").reset_index(drop=True)


def _iso(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _vline(fig: go.Figure, x: pd.Timestamp, label: str) -> None:
    fig.add_shape(
        type="line", x0=_iso(x), x1=_iso(x), y0=0, y1=1, yref="paper",
        line={"color": theme.ORANGE, "width": 1.3, "dash": "dash"},
    )
    fig.add_annotation(
        x=_iso(x), y=1, yref="paper", text=label, showarrow=False, xanchor="left", yanchor="bottom",
        font={"size": 11, "color": theme.ORANGE},
    )


def _tail_mask(values: pd.Series, direction: str, threshold: float) -> pd.Series:
    """Top decile: long = highest 10% of the target, short = lowest (most negative) 10%."""
    return values >= threshold if direction == "long" else values <= threshold


def _tail_quantile(values: pd.Series, direction: str) -> float:
    return float(np.percentile(values.dropna(), 90 if direction == "long" else 10))


def _summary_table(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby("segment")
    out = grouped.agg(
        n_obs=("open_time", "count"),
        first_date=("open_time", "min"),
        last_date=("open_time", "max"),
        avg_target=("target", "mean"),
        negative_rate=("target", lambda x: float((x < 0).mean())),
    ).reset_index()
    out["first_date"] = out["first_date"].dt.strftime("%Y-%m-%d")
    out["last_date"] = out["last_date"].dt.strftime("%Y-%m-%d")
    return out


def _monthly_chart(df: pd.DataFrame, target: str, valid_start: pd.Timestamp) -> go.Figure:
    monthly = (
        df.assign(month=df["open_time"].dt.to_period("M").dt.to_timestamp())
        .groupby("month")["target"]
        .agg(["mean", "std"])
        .reset_index()
    )
    upper = monthly["mean"] + monthly["std"]
    lower = monthly["mean"] - monthly["std"]
    x = monthly["month"].dt.strftime("%Y-%m-%d")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=upper, mode="lines", line={"width": 0}, showlegend=False,
                             hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=x, y=lower, mode="lines", line={"width": 0}, fill="tonexty",
                             fillcolor="rgba(13,110,253,0.18)", name="±1σ sáv", hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=x, y=monthly["mean"], mode="lines", name=f"Átlag {target}",
                             line={"color": theme.BLUE, "width": 1.8}))
    vrect(fig, _iso(valid_start.replace(day=1)), _iso(monthly["month"].max() + pd.DateOffset(months=1)))
    _vline(fig, valid_start.replace(day=1), f"Valid kezdet {valid_start:%Y-%m}")
    fig.add_hline(y=0, line={"color": theme.NEUTRAL, "dash": "dot", "width": 1})
    return themed(fig, x_title="Hónap", y_title=f"{target} (log return)")


def _kde_chart(train: pd.Series, valid: pd.Series, target: str) -> go.Figure:
    both = pd.concat([train, valid]).dropna()
    lo, hi = float(both.quantile(0.001)), float(both.quantile(0.999))
    grid = np.linspace(lo, hi, 300)
    fig = go.Figure()
    for name, series in (("Train", train), ("Valid", valid)):
        data = series.dropna().to_numpy()
        if len(data) > 20_000:
            data = np.random.default_rng(42).choice(data, 20_000, replace=False)
        density = gaussian_kde(data)(grid)
        color = _COLORS[name]
        fig.add_trace(go.Scatter(x=grid, y=density, mode="lines", name=name, fill="tozeroy",
                                 line={"color": color, "width": 1.8}, opacity=0.75))
    fig.add_vline(x=0, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1})
    return themed(fig, x_title=f"{target} (log return)", y_title="Sűrűség", hovermode="x")


def _box_chart(train: pd.Series, valid: pd.Series, target: str) -> go.Figure:
    fig = go.Figure()
    for name, series in (("Train", train), ("Valid", valid)):
        s = series.dropna()
        q1, med, q3 = (float(s.quantile(q)) for q in (0.25, 0.5, 0.75))
        iqr = q3 - q1
        lo_fence = float(s[s >= q1 - 1.5 * iqr].min())
        hi_fence = float(s[s <= q3 + 1.5 * iqr].max())
        fig.add_trace(go.Box(
            name=name, q1=[q1], median=[med], q3=[q3], lowerfence=[lo_fence], upperfence=[hi_fence],
            mean=[float(s.mean())], sd=[float(s.std())], marker={"color": _COLORS[name]},
            boxmean=True,
        ))
    fig.add_hline(y=0, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1})
    return themed(fig, y_title=f"{target} (log return)", height=380, hovermode="closest")


def _metrics_table(train: pd.Series, valid: pd.Series) -> pd.DataFrame:
    rows = []
    for seg, s in (("Train", train), ("Valid", valid)):
        rows.append({
            "segment": seg,
            "median": s.median(),
            "mean": s.mean(),
            "std": s.std(),
            "iqr": s.quantile(0.75) - s.quantile(0.25),
            "negative_rate": float((s < 0).mean()),
        })
    return pd.DataFrame(rows)


def _top_tables(
    df: pd.DataFrame, direction: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, float]]:
    """(reference table, monthly+yearly table, monthly trend frame, p-thresholds per segment)."""
    thresholds: dict[str, float] = {}
    top_avg: dict[str, float] = {}
    seg_avg: dict[str, float] = {}
    for seg in ("Train", "Valid"):
        part = df[df["segment"] == seg]
        thr = _tail_quantile(part["target"], direction)
        thresholds[seg] = thr
        top_avg[seg] = float(part.loc[_tail_mask(part["target"], direction, thr), "target"].mean())
        seg_avg[seg] = float(part["target"].mean())

    def lift(seg: str) -> float:
        return abs(top_avg[seg]) / abs(seg_avg[seg]) if seg_avg[seg] else float("nan")

    ref = pd.DataFrame([
        {"metric": "Top10 küszöb (szegmensenkénti P" + ("90" if direction == "long" else "10") + ")",
         "train": thresholds["Train"], "valid": thresholds["Valid"]},
        {"metric": "Top10 átlag target", "train": top_avg["Train"], "valid": top_avg["Valid"]},
        {"metric": "Teljes szegmens átlag target", "train": seg_avg["Train"], "valid": seg_avg["Valid"]},
        {"metric": "Lift (|top10 átlag| / |szegmens átlag|)", "train": lift("Train"),
         "valid": lift("Valid")},
    ])

    week_starts = pd.date_range(df["open_time"].min().normalize(),
                                df["open_time"].max().normalize() + pd.Timedelta(days=1), freq="7D")
    weeks_per_month = pd.Series(week_starts.strftime("%Y-%m")).value_counts().to_dict()

    rows: list[dict[str, Any]] = []
    trend: list[dict[str, Any]] = []
    for seg in ("Train", "Valid"):
        part = df[df["segment"] == seg].copy()
        part["top"] = _tail_mask(part["target"], direction, thresholds[seg])
        part["ym"] = part["open_time"].dt.to_period("M")
        part["year"] = part["open_time"].dt.year
        for year, ydf in part.groupby("year"):
            for ym, mdf in ydf.groupby("ym"):
                n_t = int(mdf["top"].sum())
                n_w = weeks_per_month.get(str(ym), 0)
                rows.append({
                    "segment": seg, "period": str(ym), "n_sample": len(mdf), "n_top10": n_t,
                    "top10_avg": float(mdf.loc[mdf["top"], "target"].mean()) if n_t else None,
                    "top10_per_week": round(n_t / n_w, 1) if n_w else None,
                    "top10_per_day": round(n_t / ym.days_in_month, 2),
                })
                trend.append({"segment": seg, "date": ym.to_timestamp(),
                              "per_day": n_t / ym.days_in_month})
            n_ty = int(ydf["top"].sum())
            n_days = (ydf["open_time"].dt.date.max() - ydf["open_time"].dt.date.min()).days + 1
            rows.append({
                "segment": f"{seg} {year} ▶", "period": f"{year} összesen", "n_sample": len(ydf),
                "n_top10": n_ty,
                "top10_avg": float(ydf.loc[ydf["top"], "target"].mean()) if n_ty else None,
                "top10_per_week": None, "top10_per_day": round(n_ty / n_days, 2),
            })
    return ref, pd.DataFrame(rows), pd.DataFrame(trend), thresholds


def _trend_chart(trend: pd.DataFrame, thresholds: dict[str, float], valid_start: pd.Timestamp,
                 direction: str) -> go.Figure:
    fig = go.Figure()
    q = "P90" if direction == "long" else "P10"
    for seg in ("Train", "Valid"):
        part = trend[trend["segment"] == seg]
        fig.add_trace(go.Scatter(
            x=part["date"].dt.strftime("%Y-%m-%d"), y=part["per_day"], mode="lines+markers",
            name=f"{seg} top10/nap ({q}={thresholds[seg]:.5f})",
            line={"color": _COLORS[seg], "width": 1.5}, marker={"size": 4},
        ))
    vrect(fig, _iso(valid_start.replace(day=1)), _iso(trend["date"].max() + pd.DateOffset(months=1)))
    _vline(fig, valid_start.replace(day=1), f"Valid kezdet {valid_start:%Y-%m}")
    return themed(fig, x_title="Hónap", y_title="Top10 decilis / nap (db)")


def _hourly_close(df: pd.DataFrame) -> pd.DataFrame | None:
    prices = read_live(
        "SELECT open_time, close FROM ohlcv WHERE open_time >= ? AND open_time <= ? "
        "AND minute(open_time) = 0 ORDER BY open_time",
        [df["open_time"].min().to_pydatetime(), df["open_time"].max().to_pydatetime()],
    )
    if prices is None or prices.empty:
        return None
    prices["open_time"] = pd.to_datetime(prices["open_time"])
    return prices


def _lift_chart(part: pd.DataFrame, prices: pd.DataFrame | None, threshold: float,
                direction: str, label: str, target: str) -> go.Figure:
    top = part[part["top"]]
    fig = make_subplots(rows=2 if prices is not None else 1, cols=1, shared_xaxes=True,
                        vertical_spacing=0.04, row_heights=[0.6, 0.4] if prices is not None else [1])
    target_row = 2 if prices is not None else 1
    if prices is not None:
        win = prices[(prices["open_time"] >= part["open_time"].min())
                     & (prices["open_time"] <= part["open_time"].max())]
        fig.add_trace(go.Scatter(x=win["open_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"), y=win["close"],
                                 mode="lines", name="Close (USDT)",
                                 line={"color": theme.BLUE, "width": 1}), row=1, col=1)
        marks = pd.merge_asof(top[["open_time"]].sort_values("open_time"), win.sort_values("open_time"),
                              on="open_time", direction="nearest",
                              tolerance=pd.Timedelta(hours=1)).dropna()
        fig.add_trace(go.Scatter(x=marks["open_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                 y=marks["close"], mode="markers", name="Top10 decilis",
                                 marker={"color": theme.SHORT, "size": 5}), row=1, col=1)
        fig.update_yaxes(title_text="Close (USDT)", row=1, col=1)
    fig.add_trace(go.Scatter(x=part["open_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"), y=part["target"],
                             mode="lines", name=target, line={"color": theme.ACCENT, "width": 0.8},
                             showlegend=prices is None), row=target_row, col=1)
    fig.add_trace(go.Scatter(x=top["open_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"), y=top["target"],
                             mode="markers", name="Top10 target", marker={"color": theme.SHORT, "size": 5},
                             showlegend=prices is None), row=target_row, col=1)
    fig.add_hline(y=threshold, row=target_row, col=1,
                  line={"color": theme.NEUTRAL, "dash": "dot", "width": 1},
                  annotation_text=f"{'P90' if direction == 'long' else 'P10'} {label} = {threshold:.5f}",
                  annotation_position="top left")
    fig.update_yaxes(title_text=target, row=target_row, col=1)
    return themed(fig, height=520 if prices is not None else 360, x_title="Dátum (UTC)")


def build(model_id: str, artifact_dir: Path | None = None, root: Path | None = None) -> Path:
    """Build and write the ``01_sampling`` report; returns the report directory."""
    direction = direction_of(model_id)
    target = target_of(direction)
    df = _load_sample(model_id, target)
    train = df.loc[df["segment"] == "Train", "target"]
    valid = df.loc[df["segment"] == "Valid", "target"]
    if train.empty or valid.empty:
        raise ValueError(f"{model_id}: sample has no train/valid split rows")
    valid_start = df.loc[df["segment"] == "Valid", "open_time"].min()
    tail = "legmagasabb" if direction == "long" else "leg-negatívabb"
    q = "P90" if direction == "long" else "P10"
    dir_label = "Long" if direction == "long" else "Short"

    rb = ReportBuilder(
        scope="model", model_id=model_id, slug=SLUG, order=10, root=root,
        title="Mintavételezés (train/valid)",
        summary=f"{dir_label} modell train/valid periódusa, a {target} target stabilitása.",
        tags=["sampling", direction], source=source_info("sample", GENERATOR),
    )
    rb.add_markdown(
        f"## Cél\n\nA `{model_id}` ({dir_label}) modell **sampling periódusát** vizsgáljuk: "
        "train/valid szétválasztás, megfigyelésszámok, dátumtartományok, a "
        f"`{target}` átlagos időbeli alakulása és a target-eloszlás stabilitása train vs valid között.\n\n"
        "**Kapcsolódó dokumentáció:** módszertan `_doc_/methodology_doc/3000_sampling.md`, "
        "kód `_doc_/database_and_code_doc/5300_create_sample.md`.\n\n"
        "**Asset:** SOLUSDT | **Forrás:** "
        f"`model.\"{model_id}__sample\"` (`open_time`, `{target}`, `split`)."
    )

    rb.add_markdown(
        "## 1. Train / Valid összesítő\n\n"
        "A sample-sorok `split` szerint (0 = train, 1 = valid): megfigyelések száma, első és utolsó "
        f"időpont, átlagos `{target}` és a negatív arány (target < 0 hányada)."
    )
    rb.add_table("train_valid_summary", _summary_table(df), title="Train és Valid összesítő", columns={
        "segment": "Szegmens", "n_obs": "Megfigyelések (db)", "first_date": "Első adat",
        "last_date": "Utolsó adat", "avg_target": f"Átlag {target}",
        "negative_rate": "Negatív arány",
    })

    rb.add_markdown(
        "## 2. Havi target alakulása\n\n"
        f"A `{target}` havi átlaga és ±1σ sávja a teljes periódusra; a narancs terület a valid "
        "időszak. Megmutatja, stabil-e a target szintje, van-e rezsimváltás train és valid között."
    )
    rb.add_chart("monthly_target", _monthly_chart(df, target, valid_start),
                 title=f"Havi átlagos {target} és ±1σ sáv")

    rb.add_markdown(
        "## 3. Target eloszlása (Train vs Valid)\n\n"
        "(a) KDE sűrűségbecslés a train és valid eloszlásra; (b) boxplot (medián, IQR, bajusz, "
        "átlag). Az eloszlások hasonlósága jelzi, hogy a valid reprezentatív-e; jelentős eltolódás "
        "distribution shiftet jelez."
    )
    rb.add_chart("distribution_kde", _kde_chart(train, valid, target), title=f"{target} eloszlása (KDE)")
    rb.add_chart("distribution_box", _box_chart(train, valid, target), title=f"{target} boxplot",
                 height=380)

    metrics = _metrics_table(train, valid)
    rb.add_markdown("### Összehasonlító metrikák")
    rb.add_table("interp_metrics", metrics, title="Train vs Valid metrikák", columns={
        "segment": "Szegmens", "median": "Medián", "mean": "Átlag", "std": "Szórás (σ)", "iqr": "IQR",
        "negative_rate": "Negatív arány",
    })

    ref, combined, trend, thresholds = _top_tables(df, direction)
    rb.add_markdown(
        "## 4. Top10 decilis havi bontásban\n\n"
        f"A top10 = a {tail} 10% ({q} küszöb), **szegmensenként külön** számolva. "
        "A táblában a hónapok és az éves összesítő sorok (▶); a trend-ábra a havi 'top10 / nap' "
        "értéket mutatja."
    )
    rb.add_table("top10_reference", ref, title="Top10 küszöb és lift", columns={
        "metric": "Metrika", "train": {"label": "Train", "type": "float", "digits": 5},
        "valid": {"label": "Valid", "type": "float", "digits": 5},
    })
    rb.add_table("top10_monthly", combined, title="Top10 decilis havi/éves bontás", columns={
        "segment": "Szegmens", "period": "Időszak", "n_sample": "N sample", "n_top10": "N top10",
        "top10_avg": {"label": f"Top10 átlag target ({tail})", "digits": 5},
        "top10_per_week": {"label": "Top10 / hét", "type": "float", "digits": 1},
        "top10_per_day": {"label": "Top10 / nap", "type": "float", "digits": 2},
    })
    rb.add_chart("top10_monthly_trend", _trend_chart(trend, thresholds, valid_start, direction),
                 title="Havi top10 decilis / nap")

    rb.add_markdown(
        "## 5. Top10 lift ábrák\n\n"
        "Mikor és milyen close árfolyam mellett jelennek meg a top10 decilisbe eső megfigyelések. "
        "Szegmensenként és naptári évenként egy-egy ábra: felül a close, alul a target, piros pontok "
        f"= top10 (küszöb: szegmens {q})."
    )
    prices = _hourly_close(df)
    if prices is None:
        rb.add_markdown("_Az élő adatbázis nem érhető el (zárolt) - az árfolyam panel kimaradt._")
    for seg in ("Train", "Valid"):
        part_seg = df[df["segment"] == seg].copy()
        part_seg["top"] = _tail_mask(part_seg["target"], direction, thresholds[seg])
        seg_avg = float(part_seg["target"].mean())
        for year, part in part_seg.groupby(part_seg["open_time"].dt.year):
            n_top = int(part["top"].sum())
            top_avg = float(part.loc[part["top"], "target"].mean()) if n_top else float("nan")
            year_lift = abs(top_avg) / abs(seg_avg) if n_top and seg_avg else float("nan")
            rb.add_markdown(
                f"### {seg} - {year}\n\nn = {len(part):,} | top10 = {n_top} | lift = "
                f"{fmt(year_lift, 2)}x | {q} = {thresholds[seg]:.5f}"
            )
            rb.add_chart(f"top10_{seg.lower()}_{year}",
                         _lift_chart(part, prices, thresholds[seg], direction, seg.lower(), target),
                         title=f"{seg} {year}: close és {target}", height=520 if prices is not None else 360)

    train_mean, valid_mean = float(train.mean()), float(valid.mean())
    shift = (valid_mean - train_mean) / float(train.std()) if float(train.std()) else float("nan")
    std_ratio = float(valid.std()) / float(train.std()) if float(train.std()) else float("nan")
    train_pd = float(trend.loc[trend["segment"] == "Train", "per_day"].mean())
    valid_pd = float(trend.loc[trend["segment"] == "Valid", "per_day"].mean())
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"- **Szint:** a valid átlag {valid_mean:.5f} vs train {train_mean:.5f} "
        f"(eltolódás {shift:+.2f} train-σ); a szórás aránya valid/train = {fmt(std_ratio, 2)}.\n"
        f"- **Arány:** train {len(train):,} / valid {len(valid):,} megfigyelés "
        f"({len(valid) / (len(train) + len(valid)):.1%} valid).\n"
        f"- **Top10 sűrűség:** átlag {train_pd:.2f} (train) vs {valid_pd:.2f} (valid) top10 / nap; "
        "a két érték közeli eloszlást és hasonló jelgyakoriságot jelez, nagy eltérés esetén a valid "
        "mutatók óvatosan értelmezendők.\n"
        f"- **Használhatóság:** {'a valid periódus reprezentatívnak tekinthető' if abs(shift) < 0.5 and 0.7 < std_ratio < 1.4 else 'a valid periódus jelentősen eltér a trainétől - rezsimhatást érdemes vizsgálni'} "
        "(küszöb: |eltolódás| < 0,5σ és szórásarány 0,7-1,4)."
    )
    return rb.write()
