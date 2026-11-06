# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false
# (pandas/plotly stubs are too loose for this numeric code; behaviour is covered by tests)
"""Report ``03_hyperparameter_search``: Optuna search progress, best trial, feature importance.

Source: ``<artifact>/search/{search_trials.jsonl, search_best.json, pruning_report.json?,
trial_curves/trial_NNNN.json?}``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

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
    source_info,
    themed,
)

logger = logging.getLogger(__name__)

SLUG = "03_hyperparameter_search"
GENERATOR = "reports.builders.hyperparameter_search"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _progress_chart(df: pd.DataFrame, ratio_key: str, train_key: str, best_trial: Any) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["trial_no"], y=df[ratio_key], mode="lines", name=f"Valid {ratio_key}",
                             line={"color": theme.BLUE, "width": 1}, opacity=0.8))
    fig.add_trace(go.Scatter(x=df["trial_no"], y=df[train_key], mode="lines", name=f"Train {train_key}",
                             line={"color": theme.NEUTRAL, "width": 1, "dash": "dash"}, opacity=0.6))
    fig.add_trace(go.Scatter(x=df["trial_no"], y=df[ratio_key].cummax(), mode="lines",
                             name="Futó maximum (valid)", line={"color": theme.LONG, "width": 2.2}))
    brow = df[df["trial_no"] == best_trial]
    if len(brow):
        fig.add_trace(go.Scatter(x=brow["trial_no"], y=brow[ratio_key], mode="markers",
                                 name=f"Kiválasztott (#{best_trial})",
                                 marker={"color": theme.SHORT, "size": 12, "symbol": "star"}))
    return themed(fig, x_title="Trial", y_title="Valid ratio (top10 lift)")


def _overfit_chart(df: pd.DataFrame, ratio_key: str, train_key: str, best_trial: Any) -> go.Figure:
    lo = float(min(df[train_key].min(), df[ratio_key].min())) * 0.99
    hi = float(max(df[train_key].max(), df[ratio_key].max())) * 1.01
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[lo, hi], y=[lo, hi], mode="lines", name="Nincs overfitting",
                             line={"color": theme.NEUTRAL, "dash": "dash", "width": 1}))
    mono = df["decile_monotonicity"].clip(0.5, 1.0) if "decile_monotonicity" in df else None
    fig.add_trace(go.Scatter(
        x=df[train_key], y=df[ratio_key], mode="markers", name="Trial",
        text=[f"trial #{t}" for t in df["trial_no"]],
        marker={"size": 7, "opacity": 0.75, "color": mono, "colorscale": "RdYlGn", "cmin": 0.5,
                "cmax": 1.0, "colorbar": {"title": "Decile monotonicity"}} if mono is not None
        else {"size": 7, "color": theme.BLUE},
    ))
    brow = df[df["trial_no"] == best_trial]
    if len(brow):
        fig.add_trace(go.Scatter(x=brow[train_key], y=brow[ratio_key], mode="markers",
                                 name=f"Kiválasztott (#{best_trial})",
                                 marker={"color": theme.SHORT, "size": 14, "symbol": "star"}))
    return themed(fig, x_title="Train top10 lift", y_title="Valid top10 lift", hovermode="closest")


def _param_effect_chart(df: pd.DataFrame, ratio_key: str, best_params: dict[str, Any],
                        best_ratio: float) -> go.Figure | None:
    params = df["params"].dropna()
    if params.empty:
        return None
    first = params.iloc[0]
    numeric = [k for k, v in first.items() if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if not numeric:
        return None
    ncols = 3
    nrows = (len(numeric) + ncols - 1) // ncols
    fig = make_subplots(rows=nrows, cols=ncols, subplot_titles=numeric, vertical_spacing=0.12 / nrows * 3)
    for i, name in enumerate(numeric):
        row, col = i // ncols + 1, i % ncols + 1
        vals = pd.to_numeric(df["params"].apply(lambda d, n=name: d.get(n) if isinstance(d, dict) else None),
                             errors="coerce")
        fig.add_trace(go.Scatter(x=vals, y=df[ratio_key], mode="markers", showlegend=False,
                                 marker={"size": 5, "color": theme.BLUE, "opacity": 0.6}),
                      row=row, col=col)
        if best_params.get(name) is not None:
            fig.add_trace(go.Scatter(x=[best_params[name]], y=[best_ratio], mode="markers",
                                     showlegend=False, marker={"size": 10, "color": theme.SHORT}),
                          row=row, col=col)
        arr = vals.dropna().to_numpy(dtype=float)
        if len(arr) and (arr > 0).all() and arr.max() / arr.min() > 10:
            fig.update_xaxes(type="log", row=row, col=col)
    return themed(fig, height=300 * nrows, hovermode="closest")


def _importance_chart(prune: dict[str, Any]) -> go.Figure:
    fi = pd.DataFrame(prune["all_feature_importance"])
    used = fi[fi["split"] > 0].head(30).iloc[::-1]
    fig = go.Figure(go.Bar(x=used["gain"], y=used["feature"], orientation="h",
                           marker={"color": theme.LONG}, opacity=0.85))
    return themed(fig, height=max(380, 22 * len(used) + 120), x_title="Gain importance",
                  hovermode="closest")


def _curve_chart(curve: dict[str, Any], best_iter: Any) -> go.Figure | None:
    train = curve.get("train", {}).get("quantile", [])
    valid = curve.get("valid", {}).get("quantile", [])
    if not (train and valid):
        return None
    iters = list(range(1, len(train) + 1))
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=iters, y=train, mode="lines", name="Train quantile loss",
                             line={"color": theme.BLUE, "width": 1.4}))
    fig.add_trace(go.Scatter(x=iters, y=valid, mode="lines", name="Valid quantile loss",
                             line={"color": theme.SHORT, "width": 1.4}))
    if best_iter:
        fig.add_shape(type="line", x0=best_iter, x1=best_iter, y0=0, y1=1, yref="paper",
                      line={"color": theme.LONG, "dash": "dash", "width": 1.3})
        fig.add_annotation(x=best_iter, y=1, yref="paper", text=f"Best iter = {best_iter}",
                           showarrow=False, xanchor="left", font={"color": theme.LONG})
    return themed(fig, x_title="Iteráció", y_title="Quantile loss")


def build(model_id: str, artifact_dir: Path | None = None, root: Path | None = None) -> Path:
    """Build and write the ``03_hyperparameter_search`` report."""
    artifact_dir = artifact_dir or artifact_dir_of(model_id)
    search_dir = artifact_dir / "search"
    meta = utils.load_models_config()["models"][model_id]
    direction = direction_of(model_id)
    trials = [
        json.loads(line)
        for line in (search_dir / "search_trials.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    best = _read_json(search_dir / "search_best.json")
    prune_path = search_dir / "pruning_report.json"
    prune = _read_json(prune_path) if prune_path.exists() else None

    df = pd.DataFrame(trials)
    df = df[df["objective_score"].notna()].reset_index(drop=True)
    if direction == "long":
        ratio_key, train_key, quantile = "valid_ratio_p925", "train_ratio_p925", "p92.5"
    else:
        ratio_key, train_key, quantile = "valid_ratio_p075", "train_ratio_p075", "p07.5"
    best_trial = best.get("trial_no", "?")
    best_ratio = float(best.get(ratio_key, float("nan")))
    best_iter = best.get("best_iteration", "?")
    best_params = best.get("params", {})
    dir_label = "Long" if direction == "long" else "Short"

    rb = ReportBuilder(
        scope="model", model_id=model_id, slug=SLUG, order=30, root=root,
        title="Hyperparaméter keresés",
        summary=f"{dir_label} modell Optuna keresése: {len(df)} trial, legjobb #{best_trial} "
                f"({ratio_key} = {fmt(best_ratio)}).",
        tags=["search", direction], source=source_info("search", GENERATOR),
    )
    rb.add_markdown(
        f"## Áttekintés\n\n**Modell:** `{model_id}` ({dir_label}) | **Trial-ek:** {len(df)} | "
        f"**Loss:** ratio@{quantile} (maximalizálandó) | **Target:** `{meta.get('target_name', '')}`"
    )

    rows = [
        {"metric": "Best trial №", "value": str(best_trial)},
        {"metric": ratio_key, "value": fmt(best_ratio)},
        {"metric": train_key, "value": fmt(best.get(train_key))},
        {"metric": "Train-valid gap", "value": fmt(best.get("train_valid_gap"))},
        {"metric": "Spearman ρ (valid)", "value": fmt(best.get("spearman_rho"))},
        {"metric": "Decile monotonicity", "value": fmt(best.get("decile_monotonicity"), 3)},
        {"metric": "Best iteration", "value": str(best_iter)},
    ]
    if prune:
        rows += [
            {"metric": "Features (selected)", "value": str(prune["n_selected"])},
            {"metric": "Zero-split removed", "value": str(prune["n_zero_split"])},
            {"metric": "Features (pruned)", "value": str(prune["n_pruned_selected"])},
        ]
    rb.add_markdown("## Összefoglalás")
    rb.add_table("best_summary", pd.DataFrame(rows), title="Legjobb trial összefoglaló",
                 columns={"metric": "Metrika", "value": {"label": "Érték", "type": "str"}})

    rb.add_markdown("## Trial progressz\n\nBal: a valid ratio trialonként a futó maximummal; "
                    "jobb: overfitting nézet (a átlóhoz közeli pontok jobban általánosítanak).")
    rb.add_chart("trial_progress", _progress_chart(df, ratio_key, train_key, best_trial),
                 title="Valid ratio - trial progressz")
    rb.add_chart("overfitting_view", _overfit_chart(df, ratio_key, train_key, best_trial),
                 title="Overfitting nézet (train vs valid lift)")

    cols = ["trial_no", ratio_key, train_key, "train_valid_gap", "spearman_rho",
            "decile_monotonicity", "valid_rmse", "best_iteration", "elapsed_s"]
    top10 = df.nlargest(10, ratio_key)[[c for c in cols if c in df.columns]].reset_index(drop=True)
    rb.add_markdown("## Top 10 trial")
    rb.add_table("top10_trials", top10, title="Top 10 trial",
                 columns={"trial_no": "Trial", "best_iteration": "Best iter", "elapsed_s": "Idő (s)"})

    rb.add_markdown("## Best hyperparaméterek")
    rb.add_table(
        "best_params",
        pd.DataFrame([{"param": k, "value": str(v)} for k, v in sorted(best_params.items())]),
        title="Legjobb hyperparaméterek",
        columns={"param": "Paraméter", "value": {"label": "Érték", "type": "str"}},
    )

    rb.add_markdown(f"## Paraméter hatás ({ratio_key})\n\nPiros pont = a kiválasztott trial.")
    effect = _param_effect_chart(df, ratio_key, best_params, best_ratio)
    if effect is not None:
        rows_n = int(np.ceil(len(effect.layout.annotations) / 3))
        rb.add_chart("param_effect", effect, title=f"Paraméter vs {ratio_key}", height=300 * rows_n)
    else:
        rb.add_markdown("_Nincs numerikus paraméter a trialokban._")

    rb.add_markdown("## Feature importance (post-prune)")
    if prune:
        rb.add_chart("feature_importance", _importance_chart(prune),
                     title="Top 30 feature (split > 0), gain", height=720)
        if prune.get("zero_split_features"):
            names = ", ".join(f"`{f}`" for f in sorted(prune["zero_split_features"]))
            rb.add_markdown(f"**Zero-split (eltávolítva, {prune['n_zero_split']} db):** {names}")
        else:
            rb.add_markdown(
                f"**Nincs zero-split feature** - mind a {prune['n_selected']} selected feature részt "
                "vett legalább egy vágásban a best params modellben."
            )
    else:
        rb.add_markdown("_Pruning report hiányzik (`search/pruning_report.json`); futtasd a "
                        "`run_prune()`-t a keresés után._")

    rb.add_markdown("## Learning curve (best trial)")
    curve_path = search_dir / "trial_curves" / f"trial_{int(best_trial):04d}.json" \
        if isinstance(best_trial, int) else None
    curve = _curve_chart(_read_json(curve_path), best.get("best_iteration")) \
        if curve_path and curve_path.exists() else None
    if curve is not None:
        rb.add_chart("learning_curve", curve, title=f"Learning curve - best trial #{best_trial}")
    else:
        rb.add_markdown(f"_Learning curve nem található a #{best_trial} trialhoz "
                        "(`search/trial_curves/`)._")

    gap = best.get("train_valid_gap")
    mono = best.get("decile_monotonicity")
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"- A legjobb trial (#{best_trial}) valid lift-je **{fmt(best_ratio, 3)}**, a train lift "
        f"{fmt(best.get(train_key), 3)} (gap {fmt(gap, 3)}): "
        f"{'mérsékelt overfitting' if isinstance(gap, (int, float)) and gap < 0.5 else 'jelentős train-valid különbség, a lift óvatosan értelmezendő'}.\n"
        f"- Decilis-monotonitás {fmt(mono, 3)}, Spearman ρ {fmt(best.get('spearman_rho'), 3)}: "
        "minél közelebb az 1-hez, annál következetesebb a rangsorolás a decilisekben.\n"
        f"- {len(df)} trialból a futó maximum alapján "
        f"{'a keresés konvergált (a legjobb a vége előtt megvolt)' if isinstance(best_trial, int) and best_trial < len(df) * 0.9 else 'a keresés a végéig javult - érdemes több trialt futtatni'}."
    )
    return rb.write()
