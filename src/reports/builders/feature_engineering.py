# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false
# (pandas/plotly stubs are too loose for this numeric code; behaviour is covered by tests)
"""Report ``02_feature_engineering``: IV / MI / quality / redundancy based feature selection.

Replaces ``src/modeling/01_feature_engineering.ipynb`` (pipeline FE step, writes
``feature_engineering/feature_set.json``) and the former Quarto 02_feature_engineering notebook
(analyze step, report only).

Selection logic (unchanged from the notebook):
  1. quality filter on the train rows (null rate > 1%, inf rate > 0.1%, variance < 1e-8),
  2. MI threshold per feature (``mutual_info_regression``, MI <= 0.001 -> dropped),
  3. per group correlation de-duplication (|r| >= 0.98: the lower-MI feature is dropped).
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go

import utils
from reports import ReportBuilder, theme
from reports.builders._common import (
    artifact_dir_of,
    direction_of,
    source_info,
    target_of,
    themed,
)
from reports.builders.feature_groups import (
    GROUP_DOCS,
    GROUP_ORDER,
    assign_group,
    feature_description,
)

logger = logging.getLogger(__name__)

SLUG = "02_feature_engineering"
GENERATOR = "reports.builders.feature_engineering"

MI_THRESHOLD = 0.001
CORR_THRESHOLD = 0.98
MAX_NULL_RATE = 0.01
MAX_INF_RATE = 0.001
MIN_VARIANCE = 1e-8
MAX_BINS = 10


@dataclass
class FeatureAnalysis:
    """Everything the FE step computes (selection + per-feature binning)."""

    model_id: str
    snapshot_id: str
    target: str
    n_train_rows: int
    min_open_time: str
    max_open_time: str
    feat_cols: list[str]
    quality_dropped: list[str]
    iv: dict[str, dict[str, Any]]  # feat -> {iv, mi, n_bins, bins(DataFrame|None)}
    corr: dict[str, pd.DataFrame]  # group -> correlation matrix of the passing features
    selected: list[str] = field(default_factory=list)
    dropped_mi: dict[str, str] = field(default_factory=dict)
    dropped_corr: dict[str, str] = field(default_factory=dict)
    dropped_qual: dict[str, str] = field(default_factory=dict)
    dedup: dict[str, dict[str, str]] = field(default_factory=dict)  # group -> {dropped: kept}
    passing: dict[str, list[str]] = field(default_factory=dict)  # group -> features that passed
    groups: dict[str, list[str]] = field(default_factory=dict)  # group -> all its features
    summary: pd.DataFrame = field(default_factory=pd.DataFrame)
    top10: list[str] = field(default_factory=list)
    top1_per_group: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- computation


def _load_train(model_id: str, snapshot_id: str, asset_id: str) -> pd.DataFrame:
    conn = utils.open_lab_connection(asset_id)
    try:
        df = conn.execute(
            f"""
            SELECT s.*, m.split
            FROM snap."{snapshot_id}" AS s
            INNER JOIN model."{model_id}__sample" AS m ON s.open_time = m.open_time
            ORDER BY s.open_time
            """
        ).fetchdf()
    finally:
        conn.close()
    return df


def _resolve_snapshot(model_id: str, artifact_dir: Path, snapshot_id: str | None) -> str:
    if snapshot_id:
        return snapshot_id
    manifest_path = artifact_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        found = (
            manifest.get("snapshot_id")
            or manifest.get("sampling", {}).get("snapshot_id")
            or manifest.get("provenance", {}).get("snapshot_id")
        )
        if found:
            return str(found)
    meta = utils.load_models_config()["models"][model_id]
    found = meta.get("sampling", {}).get("snapshot_id")
    if not found:
        raise ValueError(f"snapshot_id could not be resolved for {model_id}")
    return str(found)


def bin_feature(x: np.ndarray, y: np.ndarray) -> tuple[pd.DataFrame, float]:
    """Supervised binning of one feature against a continuous target.

    A single-feature regression tree (<= ``MAX_BINS`` leaves, each >= 5% of the rows) chooses the
    cut points (replaces ``optbinning.ContinuousOptimalBinning``, which is not a project
    dependency).  Returns the bin table (``Bin``, ``Count``, ``Count (%)``, ``Mean``, ``Std``,
    ``Min``, ``Max``) and the information value
    ``IV = sum_i |mean_i - mean_total| * n_i / n`` (target units).
    """
    from sklearn.tree import DecisionTreeRegressor

    n = len(x)
    tree = DecisionTreeRegressor(
        max_leaf_nodes=MAX_BINS, min_samples_leaf=max(int(0.05 * n), 20), random_state=42
    )
    tree.fit(x.reshape(-1, 1), y)
    cuts = np.sort(tree.tree_.threshold[tree.tree_.feature >= 0])
    idx = np.searchsorted(cuts, x, side="left")
    frame = pd.DataFrame({"bin": idx, "x": x, "y": y})
    grouped = frame.groupby("bin")
    table = grouped.agg(
        Count=("y", "size"), Mean=("y", "mean"), Std=("y", "std"), Min=("x", "min"), Max=("x", "max")
    ).reset_index()
    edges = [-np.inf, *cuts.tolist(), np.inf]
    table["Bin"] = [
        f"({edges[int(i)]:.4g}, {edges[int(i) + 1]:.4g}]" for i in table["bin"]
    ]
    table["Count (%)"] = table["Count"] / n
    total_mean = float(np.mean(y))
    iv_value = float((np.abs(table["Mean"] - total_mean) * table["Count"] / n).sum())
    return pd.DataFrame(table[["Bin", "Count", "Count (%)", "Mean", "Std", "Min", "Max"]]), iv_value


def compute(model_id: str, artifact_dir: Path | None = None,
            snapshot_id: str | None = None) -> FeatureAnalysis:
    """Run quality filter, IV/MI binning and the group-wise selection."""
    from sklearn.feature_selection import mutual_info_regression

    artifact_dir = artifact_dir or artifact_dir_of(model_id)
    snapshot_id = _resolve_snapshot(model_id, artifact_dir, snapshot_id)
    target = target_of(direction_of(model_id))
    asset_id = utils.model_asset_id(model_id)

    df_all = _load_train(model_id, snapshot_id, asset_id)
    all_feats = [c for c in df_all.columns if c.startswith("feat_")]
    df_train = df_all[df_all["split"] == 0].copy()
    del df_all

    null_rates = pd.Series(df_train[all_feats].isnull().mean())
    inf_rates = pd.Series(np.isinf(df_train[all_feats]).mean())
    variances = pd.Series(df_train[all_feats].var())
    bad = set(
        null_rates[null_rates > MAX_NULL_RATE].index.tolist()
        + inf_rates[inf_rates > MAX_INF_RATE].index.tolist()
        + variances[variances < MIN_VARIANCE].index.tolist()
    )
    quality_dropped = sorted(bad)
    feat_cols = [f for f in all_feats if f not in bad]
    logger.info("quality filter: %d dropped, %d kept", len(quality_dropped), len(feat_cols))

    iv: dict[str, dict[str, Any]] = {}
    feats_clean = df_train[feat_cols].replace([np.inf, -np.inf], np.nan)
    for feat in feat_cols:
        x = feats_clean[feat].dropna()
        y = df_train.loc[x.index, target]
        if len(x) < 50:
            iv[feat] = {"iv": float("nan"), "n_bins": 0, "bins": None}
            continue
        try:
            bins, iv_value = bin_feature(x.to_numpy(), y.to_numpy())
            iv[feat] = {"iv": iv_value, "n_bins": len(bins), "bins": bins}
        except Exception as exc:  # noqa: BLE001 - never let one feature kill the report
            logger.warning("binning failed for %s: %s", feat, exc)
            iv[feat] = {"iv": float("nan"), "n_bins": 0, "bins": None}
    n_ok = sum(1 for v in iv.values() if v["bins"] is not None)
    logger.info("IV computed: %d/%d", n_ok, len(feat_cols))

    x_mi = feats_clean.fillna(feats_clean.median())
    valid_mi = df_train[target].notna()
    mi_values = mutual_info_regression(
        x_mi[valid_mi], df_train.loc[valid_mi, target], discrete_features=False, random_state=42
    )
    for feat, mi in zip(feat_cols, mi_values, strict=True):
        iv[feat]["mi"] = round(float(mi), 5)

    groups: dict[str, list[str]] = {}
    for feat in sorted(feat_cols):
        groups.setdefault(assign_group(feat), []).append(feat)
    ordered = [g for g in GROUP_ORDER if g in groups] + sorted(g for g in groups if g not in GROUP_ORDER)

    result = FeatureAnalysis(
        model_id=model_id, snapshot_id=snapshot_id, target=target, n_train_rows=len(df_train),
        min_open_time=str(df_train["open_time"].min()), max_open_time=str(df_train["open_time"].max()),
        feat_cols=feat_cols, quality_dropped=quality_dropped, iv=iv, corr={},
        dropped_qual={f: "quality: null/inf/variance" for f in quality_dropped},
        groups={g: groups[g] for g in ordered},
    )

    for group in ordered:
        in_group = groups[group]
        mi_ok = [f for f in in_group if iv[f].get("mi", 0) > MI_THRESHOLD]
        dedup: dict[str, str] = {}
        if len(mi_ok) >= 2:
            cm = df_train[mi_ok].corr().abs()
            seen: set[str] = set()
            for i, fa in enumerate(mi_ok):
                if fa in seen:
                    continue
                for fb in mi_ok[i + 1:]:
                    if fb in seen:
                        continue
                    if cm.loc[fa, fb] >= CORR_THRESHOLD:
                        drop, keep = (fb, fa) if iv[fa]["mi"] >= iv[fb]["mi"] else (fa, fb)
                        dedup[drop] = keep
                        seen.add(drop)
        passing = [f for f in mi_ok if f not in dedup]
        result.dedup[group] = dedup
        result.passing[group] = passing
        for f in in_group:
            mi_val = iv[f].get("mi", 0)
            if mi_val != mi_val or mi_val <= MI_THRESHOLD:
                result.dropped_mi[f] = f"mi <= {MI_THRESHOLD} (mi={mi_val:.5f})"
            elif f in dedup:
                result.dropped_corr[f] = f"duplikatum -> {dedup[f]}"
            else:
                result.selected.append(f)
        if len(passing) >= 2:
            result.corr[group] = df_train[passing].corr()

    _fill_summary(result)
    return result


def _decision(result: FeatureAnalysis, feat: str) -> str:
    if feat in result.dropped_qual:
        return "quality: null/inf/variance"
    if feat in result.dropped_corr:
        return "duplikátum → " + result.dropped_corr[feat].split("-> ")[-1].replace("feat_", "")
    if feat in result.dropped_mi:
        return f"MI ≤ {MI_THRESHOLD}"
    return "✓ selected"


def _fill_summary(result: FeatureAnalysis) -> None:
    rows = []
    for feat in result.feat_cols + result.quality_dropped:
        info = result.iv.get(feat, {})
        rows.append({
            "feature": feat.replace("feat_", ""), "group": assign_group(feat),
            "mi": info.get("mi", float("nan")), "iv": info.get("iv", float("nan")),
            "decision": _decision(result, feat), "full_name": feat,
        })
    summary = pd.DataFrame(rows).sort_values("mi", ascending=False, na_position="last")
    result.summary = summary.reset_index(drop=True)
    selected = result.summary[result.summary["decision"] == "✓ selected"]
    result.top10 = selected["full_name"].head(10).tolist()
    best: dict[str, str] = {}
    for _, row in selected.iterrows():
        best.setdefault(row["group"], row["full_name"])
    result.top1_per_group = list(best.values())


# --------------------------------------------------------------------------- feature_set.json


def feature_set_dict(result: FeatureAnalysis) -> dict[str, Any]:
    """The ``feature_set.json`` payload (same schema as the notebook produced)."""
    now = datetime.now()
    dropped: list[dict[str, str]] = [{"col": f, "reason": r} for f, r in result.dropped_qual.items()]
    dropped += [{"col": f, "reason": r} for f, r in result.dropped_mi.items()
                if f not in result.dropped_qual]
    dropped += [{"col": f, "reason": r} for f, r in result.dropped_corr.items()]
    return {
        "run_id": now.strftime("run_%Y%m%d_%H%M%S"),
        "asset_id": utils.model_asset_id(result.model_id),
        "model_id": result.model_id,
        "target_col": result.target,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "target_cols": [result.target],
        "selected": sorted(result.selected),
        "top10": result.top10,
        "top1_per_group": result.top1_per_group,
        "dropped": dropped,
        "review": [],
        "provenance": {
            "snapshot_id": result.snapshot_id, "sample_rows": result.n_train_rows,
            "min_open_time": result.min_open_time, "max_open_time": result.max_open_time,
        },
        "thresholds": {
            "mi_threshold": MI_THRESHOLD, "corr_threshold": CORR_THRESHOLD,
            "max_null_rate": MAX_NULL_RATE, "max_inf_rate": MAX_INF_RATE,
            "min_variance": MIN_VARIANCE,
        },
    }


def write_feature_set(result: FeatureAnalysis, artifact_dir: Path) -> Path:
    """Write ``<artifact_dir>/feature_engineering/feature_set.json`` (downstream pipeline input)."""
    out_dir = artifact_dir / "feature_engineering"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "feature_set.json"
    path.write_text(
        json.dumps(feature_set_dict(result), indent=4, ensure_ascii=False), encoding="utf-8"
    )
    return path


# --------------------------------------------------------------------------- report


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "_", text.lower()).strip("_")[:48] or "x"


def _corr_chart(corr: pd.DataFrame) -> go.Figure:
    labels = [c.replace("feat_", "") for c in corr.columns]
    n = len(labels)
    fig = go.Figure(go.Heatmap(
        z=corr.values.round(3), x=labels, y=labels, zmin=-1, zmax=1, zmid=0,
        colorscale="RdBu", reversescale=True, colorbar={"title": "r"},
        text=corr.values.round(2) if n <= 20 else None, texttemplate="%{text}" if n <= 20 else None,
    ))
    fig.update_yaxes(autorange="reversed")
    fig.update_xaxes(tickangle=-45)
    return themed(fig, height=max(380, 24 * n + 160), hovermode="closest")


def _bin_chart(feat: str, bins: pd.DataFrame, target: str) -> go.Figure:
    labels = [str(b) for b in bins["Bin"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=labels, y=bins["Count (%)"] * 100, name="Megfigyelések (%)",
                         marker={"color": theme.NEUTRAL}, opacity=0.45, yaxis="y"))
    fig.add_trace(go.Scatter(x=labels, y=bins["Mean"], name=f"Átlag {target}", mode="lines+markers",
                             line={"color": theme.BLUE, "width": 2}, yaxis="y2"))
    fig.update_layout(
        yaxis={"title": {"text": "Megfigyelések (%)"}},
        yaxis2={"title": {"text": f"Átlag {target}"}, "overlaying": "y", "side": "right",
                "showgrid": False},
        xaxis={"title": {"text": feat.replace("feat_", "")}, "tickangle": -30},
    )
    return themed(fig, height=360, hovermode="x")


def _group_table(result: FeatureAnalysis, group: str) -> pd.DataFrame:
    rows = []
    for feat in result.groups[group]:
        info = result.iv.get(feat, {})
        mi = info.get("mi", float("nan"))
        if mi != mi or mi <= MI_THRESHOLD:
            decision = "✗ kizár (MI)"
        elif feat in result.dedup.get(group, {}):
            decision = "✗ duplikátum → " + result.dedup[group][feat].replace("feat_", "")
        else:
            decision = ""
        rows.append({"feature": feat, "iv": info.get("iv", float("nan")), "mi": mi,
                     "n_bins": info.get("n_bins", 0), "decision": decision})
    return pd.DataFrame(rows).sort_values("mi", ascending=False).reset_index(drop=True)


def build_report(result: FeatureAnalysis, root: Path | None = None) -> Path:
    """Write the ``02_feature_engineering`` report from a :class:`FeatureAnalysis`."""
    target = result.target
    direction = direction_of(result.model_id)
    dir_label = "long" if direction == "long" else "short"
    negative_note = (
        "Negatív értékek = profitábilis short mozgás." if direction == "short"
        else "Pozitív értékek = profitábilis long mozgás."
    )
    rb = ReportBuilder(
        scope="model", model_id=result.model_id, slug=SLUG, order=20, root=root,
        title="Feature engineering (IV, MI, szelekció)",
        summary=(f"{len(result.selected)} kiválasztott feature / {len(result.feat_cols)} vizsgált "
                 f"({dir_label}), MI + korreláció alapú szelekció."),
        tags=["feature_engineering", direction], source=source_info("feature_engineering", GENERATOR),
    )
    rb.add_markdown(
        f"## Cél\n\nA `{result.model_id}` ({dir_label}) modell összes feature-jének prediktív erejét "
        "vizsgáljuk felügyelt (döntési fa alapú) binninggel (max 10 bin / feature). Minden feature-re:\n\n"
        "- optimális binekre osztjuk az értékeket, és bin-enként a target átlagot mutatjuk,\n"
        "- IV (Information Value) és MI (Mutual Information) értéket számolunk az összehasonlíthatóságért.\n\n"
        f"**Target:** `{target}` ({dir_label} MFE 60 perces ablakban). {negative_note}\n\n"
        "**Szelekció:** (1) quality szűrő (null > 1%, inf > 0,1%, variancia < 1e-8), (2) MI > "
        f"{MI_THRESHOLD}, (3) csoportonként |r| ≥ {CORR_THRESHOLD} duplikátumok közül a magasabb MI marad.\n\n"
        "**Struktúra:** egy szekció = egy feature-csoport (leírás, IV/MI tábla, korrelációs mátrix, "
        "bin-diagramok), a végén az összefoglaló."
    )
    n_other = len(result.groups.get("Other", []))
    rb.add_markdown(
        f"**Quality szűrő:** {len(result.quality_dropped)} feature kizárva | maradék: "
        f"{len(result.feat_cols)}\n\n**Adatbetöltés:** train sorok: {result.n_train_rows:,} | "
        f"feature-ök: {len(result.feat_cols)} | csoportok: {len(result.groups)} | Other (nem mappelt): "
        f"{n_other}\n\nSnapshot: `{result.snapshot_id}`"
    )

    chart_ids: set[str] = set()

    def unique_id(base: str) -> str:
        candidate, i = base, 2
        while candidate in chart_ids:
            candidate = f"{base[:58]}_{i}"
            i += 1
        chart_ids.add(candidate)
        return candidate

    for group in result.groups:
        rb.add_markdown(f"## {group}")
        if group in GROUP_DOCS:
            rb.add_markdown(GROUP_DOCS[group].replace("\n- ", "\n\n- "))
        feats = result.groups[group]
        desc_rows = [
            f"| `{f.replace('feat_', '')}` | {feature_description(f).replace(' | ', ' - ').replace('|', '/')} |"
            for f in feats if feature_description(f)
        ]
        if desc_rows:
            rb.add_markdown("| Feature | Leírás & Képlet |\n|:--------|:----------------|\n" + "\n".join(desc_rows))
        rb.add_table(
            unique_id(f"iv_{_slug(group)}"), _group_table(result, group), title=f"{group}: IV és MI",
            columns={"feature": "Feature", "iv": {"label": "IV", "digits": 5},
                     "mi": {"label": "MI", "digits": 5}, "n_bins": "Bin-ek", "decision": "Döntés"},
        )
        corr = result.corr.get(group)
        if corr is not None:
            cid = unique_id(f"corr_{_slug(group)}")
            rb.add_markdown("### Korrelációs mátrix (átment feature-ök)")
            rb.add_chart(cid, _corr_chart(corr), title=f"{group}: korrelációs mátrix",
                         height=max(380, 24 * len(corr) + 160))
        passing = result.passing.get(group, [])
        if not passing:
            rb.add_markdown("*Minden feature kizárva (MI küszöb vagy duplikátum) - nincs megjeleníthető diagram.*")
            continue
        for feat in passing:
            bins = result.iv[feat].get("bins")
            rb.add_markdown(f"### {feat}")
            if bins is None or bins.empty:
                rb.add_markdown("*Nem elégséges adat a binninghez.*")
                continue
            rb.add_chart(unique_id(f"bin_{_slug(feat.replace('feat_', ''))}"),
                         _bin_chart(feat, bins, target), title=f"{feat}: bin-enkénti átlag target",
                         height=360)

    summary = result.summary
    rb.add_markdown("## Összefoglaló - feature szelekció eredménye")
    selected = summary[summary["decision"] == "✓ selected"]
    champs = selected.drop_duplicates("group")[["group", "feature", "mi"]]
    rb.add_markdown(f"### Csoport bajnokok ({len(champs)} csoport)")
    rb.add_table("group_champions", champs, title="Csoport bajnokok (legjobb MI)", columns={
        "group": "Csoport", "feature": "Feature", "mi": {"label": "MI", "digits": 4}})
    rb.add_markdown("### Top 10 feature (MI szerint)")
    rb.add_table("top10_features", selected.head(10)[["feature", "group", "mi", "iv"]],
                 title="Top 10 feature", columns={
                     "feature": "Feature", "group": "Csoport", "mi": {"label": "MI", "digits": 4},
                     "iv": {"label": "IV", "digits": 5}})
    rb.add_markdown(f"### Összes feature MI szerint rendezve ({len(summary)} db)")
    rb.add_table("all_features", summary[["feature", "group", "mi", "iv", "decision"]],
                 title="Összes feature", columns={
                     "feature": "Feature", "group": "Csoport", "mi": {"label": "MI", "digits": 4},
                     "iv": {"label": "IV", "digits": 5}, "decision": "Döntés"})
    counts = pd.DataFrame([
        {"category": "selected", "count": len(result.selected)},
        {"category": "top10", "count": len(result.top10)},
        {"category": "top1_per_group", "count": len(result.top1_per_group)},
        {"category": "dropped (quality)", "count": len(result.dropped_qual)},
        {"category": "dropped (MI)", "count": len(result.dropped_mi)},
        {"category": "dropped (duplikátum)", "count": len(result.dropped_corr)},
    ])
    rb.add_markdown("### Feature szelekció eredménye")
    rb.add_table("selection_counts", counts, title="Szelekció darabszámok",
                 columns={"category": "Kategória", "count": "Darab"})
    top_names = ", ".join(f"`{f.replace('feat_', '')}`" for f in result.top10[:5])
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"- A {len(result.feat_cols) + len(result.quality_dropped)} feature-ből **{len(result.selected)}** "
        f"került a végső halmazba ({len(result.dropped_mi)} MI, {len(result.dropped_corr)} duplikátum, "
        f"{len(result.dropped_qual)} quality kizárás).\n"
        f"- Legerősebb jelek (MI): {top_names}.\n"
        "- Az IV/MI értékek **train** soron készültek, így a valid periódusról nem hordoznak "
        "információt; a szelekció stabilitását a hyperparaméter-keresés riportja (`03_hyperparameter_search`) "
        "valid mutatói ellenőrzik."
    )
    return rb.write()


def build(
    model_id: str,
    artifact_dir: Path | None = None,
    snapshot_id: str | None = None,
    *,
    write_selection: bool = True,
    root: Path | None = None,
) -> Path:
    """Compute the selection, optionally write ``feature_set.json``, write the report.

    ``write_selection=True`` is the pipeline FE step; the analyze step passes ``False`` so that a
    report refresh never rewrites the model input ``feature_set.json``.
    """
    artifact_dir = artifact_dir or artifact_dir_of(model_id)
    result = compute(model_id, artifact_dir, snapshot_id)
    if write_selection:
        path = write_feature_set(result, artifact_dir)
        logger.info("feature_set.json written -> %s", path)
    return build_report(result, root=root)
