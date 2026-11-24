# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Impulse Scout model 1 (epic_073 / t932, 5300 family P1, hypothesis H1, round 1): LightGBM on hand-crafted prefix
features + order flow, pre-registered fixed configuration (no tuning on the CV).

Features (every one known at ``t_c``: only 1m bars closed at or before ``t_c``, trailing baselines; no label, no
``tau``, no other asset's event / label):

* **S0 + C** (the baseline's six scalars) + ``drop_depth_sigma``;
* **rebound shape** ``(t0, t_c]`` in ``sigma_W`` units of ``log(price / low[t0])``: peak, pull-back from the peak, peak
  position, path efficiency, linear-fit R^2, half-window share, last-15m move, late re-test depth, rebound / decline
  realised-volatility ratio;
* **drawdown leg** ``[t0 - B, t0]``: position of the leg top, R^2, last-hour drop, largest 1m drop;
* **order flow**: the 20 columns of ``research."scout_flow_v1"`` (key ``asset|t0``, t930);
* **S/R distance** (cheap): 3d high above ``close[t_c]`` relative to the target distance ``A``, 7d high, ``low[t0]`` vs
  the prior 7d low;
* **time of day** of ``t_c`` (sin / cos) + weekend flag;
* **BTC context** up to ``t_c`` (1h / 4h / 24h log return in units of BTC's own 24h 1m volatility, 24h / 7d volatility
  ratio). For BTC events this is the asset's own history.

Holdout-2 hygiene: the event frame is the train split only (``sb.load_scout_events``), the 1m bars and the flow rows are
cut at the OOS start (2026-02-01) in the SQL / before any feature is built, and :func:`sb.guard_no_holdout` runs on
the frame. ``python -m reports.builders.scout_models_lgbm`` runs the benchmark (baseline + this model, N = 1000 full
refit permutations), persists ONLY this model's leaderboard row and writes ``_doc_/reports/scout_lgbm/``.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl

from reports import ReportBuilder, theme
from reports.builders import scout_bench as sb
from reports.builders import wave_events as we
from reports.builders._common import fmt, source_info, themed
from reports.builders.scout_bench.frame import y_class

logger = logging.getLogger(__name__)

MODEL_NAME = "scout_lgbm_r1"
SLUG = "scout_lgbm"
GENERATOR = "reports.builders.scout_models_lgbm"
OOS_START = datetime.fromisoformat(we.params_v2().oos_start)
CONTEXT_ASSET = "btcusdt"

S0C = list(sb.S0C_COLS)
EXTRA_SCALAR = ["drop_depth_sigma"]
REBOUND = ["reb_peak_sigma", "reb_pullback_sigma", "reb_peak_pos", "reb_eff", "reb_r2", "reb_half_share",
           "reb_ret15_sigma", "reb_retest_sigma", "reb_rv_ratio"]
DECLINE = ["dec_top_pos", "dec_r2", "dec_last1h_sigma", "dec_max_drop_sigma"]
SR = ["sr_hi3d_over_a", "sr_hi7d_sigma", "sr_low_vs_7d_sigma"]
TIME = ["tod_sin", "tod_cos", "weekend"]
BTC = ["btc_ret1h_z", "btc_ret4h_z", "btc_ret24h_z", "btc_rv_ratio"]
FLOW = ["tb_ratio_prefix", "tb_ratio_decline", "tb_ratio_rebound", "tb_ratio_diff", "tb_ratio_slope_1h",
        "tb_ratio_last1h", "tb_ratio_low15", "delta_decline_norm", "delta_rebound_norm",
        "trades_rel_prefix", "trades_rel_decline", "trades_rel_rebound", "trade_size_rel",
        "vol_decline_rel", "vol_rebound_rel", "vol_rebound_over_decline", "vol_last1h_rel", "vol_peak_rel", "vol_peak_pos",
        "qv_logz_prefix"]
GROUPS: dict[str, list[str]] = {"S0 + C": S0C + EXTRA_SCALAR, "visszapattanás-alak": REBOUND, "esés-szakasz": DECLINE,
                                "order flow": FLOW, "S/R távolság": SR, "napszak": TIME, "BTC-kontextus": BTC}
FEATURES: list[str] = [c for g in GROUPS.values() for c in g]
COMPUTED = REBOUND + DECLINE + SR + TIME + BTC
N_THREADS = 1                       # runtime only (not part of the hashed config): small data, 1 thread is fastest

#: pre-registered round-1 configuration (fixed before the first evaluation; hashed into the leaderboard row)
CONFIG: dict[str, Any] = {
    "objective": "multiclass", "n_estimators": 300, "learning_rate": 0.03, "num_leaves": 7, "max_depth": 3,
    "min_child_samples": 40, "subsample": 0.8, "subsample_freq": 1, "colsample_bytree": 0.7, "reg_lambda": 5.0,
    "min_split_gain": 0.0, "features": FEATURES, "event_variant": "a15", "context_asset": CONTEXT_ASSET,
}


# --------------------------------------------------------------------------- model


class ScoutLGBM(sb.ScoutModel):
    """3-class LightGBM (impulse / false_start / sideways) on :data:`FEATURES` (columns of the event frame)."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = dict(config or CONFIG)
        self.features = list(self.config["features"])

    def _x(self, df: pl.DataFrame) -> np.ndarray:
        return df.select(self.features).to_numpy().astype(float)

    def fit(self, train: pl.DataFrame, prefix_data: Any, *, seed: int) -> None:
        import lightgbm as lgb

        c = self.config
        params = {"objective": "multiclass", "num_class": len(sb.CLASSES), "learning_rate": c["learning_rate"],
                  "num_leaves": c["num_leaves"], "max_depth": c["max_depth"], "min_data_in_leaf": c["min_child_samples"],
                  "bagging_fraction": c["subsample"], "bagging_freq": c["subsample_freq"],
                  "feature_fraction": c["colsample_bytree"], "lambda_l2": c["reg_lambda"],
                  "min_gain_to_split": c["min_split_gain"], "seed": seed % (2**31), "num_threads": N_THREADS,
                  "deterministic": True, "force_col_wise": True, "verbose": -1}
        ds = lgb.Dataset(self._x(train), label=y_class(train), feature_name=self.features, free_raw_data=True)
        self.booster = lgb.train(params, ds, num_boost_round=c["n_estimators"])

    def predict_proba(self, df: pl.DataFrame, prefix_data: Any) -> np.ndarray:
        return np.asarray(self.booster.predict(self._x(df)), dtype=float).reshape(df.height, len(sb.CLASSES))

    def importance(self) -> pd.Series:
        return pd.Series(self.booster.feature_importance(importance_type="gain"), index=self.features)


SPEC = sb.ModelSpec(MODEL_NAME, ScoutLGBM, family="P1", hypothesis="H1", round=1, config=CONFIG,
                    description="LightGBM 3 osztály, kézi prefix-jellemzők (alak, esés, S/R, napszak, BTC) + order flow")


def register(registry: sb.ModelRegistry | None = None) -> sb.ModelSpec:
    """Register :data:`SPEC` (idempotent) in ``registry`` (default: the shared ``sb.REGISTRY``)."""
    reg = sb.REGISTRY if registry is None else registry
    if MODEL_NAME not in reg:
        reg.register(SPEC)
    return SPEC


register()


# --------------------------------------------------------------------------- features


def _r2(y: np.ndarray) -> float:
    ok = np.isfinite(y)
    if ok.sum() < 3:
        return float("nan")
    x = np.arange(y.size)[ok]
    yy = y[ok]
    c = np.corrcoef(x, yy)[0, 1]
    return float(c * c) if np.isfinite(c) else float("nan")


def _nanstd(a: np.ndarray) -> float:
    a = a[np.isfinite(a)]
    return float(a.std()) if a.size >= 3 else float("nan")


def event_features(m: we.MinuteData, t0: Any, w: int, b: int, low0: float, close_tc: float, sigma_w: float, a_log: float
                   ) -> dict[str, float]:
    """Own-asset rebound / decline / S/R features of one event; bars ``i0 - b .. i0 + w - 1`` (closed <= t_c)."""
    nan = float("nan")
    out = dict.fromkeys(REBOUND + DECLINE + SR, nan)
    i0 = we._epoch_min(t0) - m.m0
    tc = i0 + w - 1
    if i0 - b < 0 or tc >= m.n or not (sigma_w > 0 and low0 > 0):
        return out
    lc = np.log(m.close[i0 - b:tc + 1])
    lh = np.log(m.high[i0 - b:tc + 1])
    ll = np.log(m.low[i0 - b:tc + 1])
    L = np.log(low0)
    x = (lc - L) / sigma_w                                         # path in sigma_W units, index b = t0
    reb, dec = x[b:], x[:b + 1]
    reb_hi = (lh[b + 1:] - L) / sigma_w
    if np.isfinite(reb_hi).any():
        k = int(np.nanargmax(reb_hi))
        out["reb_peak_sigma"] = float(reb_hi[k])
        out["reb_peak_pos"] = (k + 1) / w
        out["reb_pullback_sigma"] = float(reb_hi[k] - reb[-1]) if np.isfinite(reb[-1]) else nan
    dr = np.diff(reb)
    tot = np.nansum(np.abs(dr))
    out["reb_eff"] = float((reb[-1] - reb[0]) / tot) if tot > 0 and np.isfinite(reb[-1]) and np.isfinite(reb[0]) else nan
    out["reb_r2"] = _r2(reb)
    if np.isfinite(reb[-1]) and reb[-1] != 0:
        out["reb_half_share"] = float(reb[w // 2] / reb[-1]) if np.isfinite(reb[w // 2]) else nan
    out["reb_ret15_sigma"] = float(reb[-1] - reb[-16]) if np.isfinite(reb[-1]) and np.isfinite(reb[-16]) else nan
    late_lo = ll[b + w // 2:]
    if np.isfinite(late_lo).any():
        out["reb_retest_sigma"] = float((np.nanmin(late_lo) - L) / sigma_w)
    rv_r, rv_d = _nanstd(np.diff(lc[b:])), _nanstd(np.diff(lc[:b + 1]))
    out["reb_rv_ratio"] = rv_r / rv_d if rv_d and rv_d > 0 else nan
    dec_hi = lh[:b + 1]
    if np.isfinite(dec_hi).any():
        out["dec_top_pos"] = float((b - int(np.nanargmax(dec_hi))) / b)
    out["dec_r2"] = _r2(dec)
    out["dec_last1h_sigma"] = float(dec[b - 60] - dec[b]) if np.isfinite(dec[b - 60]) and np.isfinite(dec[b]) else nan
    dd = np.diff(lc[:b + 1])
    out["dec_max_drop_sigma"] = float(-np.nanmin(dd) / sigma_w) if np.isfinite(dd).any() else nan
    # S/R distance: trailing windows ending at t_c (3d / 7d highs) and before the prefix (7d low)
    lc_tc = np.log(close_tc)
    hi3 = m.high[max(tc - 3 * 1440 + 1, 0):tc + 1]
    hi7 = m.high[max(tc - 7 * 1440 + 1, 0):tc + 1]
    lo7 = m.low[max(i0 - b - 7 * 1440, 0):i0 - b]
    if np.isfinite(hi3).any() and a_log > 0:
        out["sr_hi3d_over_a"] = float((np.log(np.nanmax(hi3)) - lc_tc) / a_log)
    if np.isfinite(hi7).any():
        out["sr_hi7d_sigma"] = float((np.log(np.nanmax(hi7)) - lc_tc) / sigma_w)
    if lo7.size and np.isfinite(lo7).any():
        out["sr_low_vs_7d_sigma"] = float((L - np.log(np.nanmin(lo7))) / sigma_w)
    return out


def context_features(ctx: we.MinuteData, t_c: Any) -> dict[str, float]:
    """BTC context from bars closed at or before ``t_c`` (last bar index = open ``t_c - 1m``)."""
    nan = float("nan")
    out = dict.fromkeys(BTC, nan)
    j = we._epoch_min(t_c) - 1 - ctx.m0
    if j - 7 * 1440 < 0 or j >= ctx.n:
        return out
    lc = np.log(ctx.close)
    r24 = np.diff(lc[j - 1440:j + 1])
    r7 = np.diff(lc[j - 7 * 1440:j + 1])
    sd24, sd7 = _nanstd(r24), _nanstd(r7)
    if not (sd24 and sd24 > 0) or not np.isfinite(lc[j]):
        return out
    for k, n in (("btc_ret1h_z", 60), ("btc_ret4h_z", 240), ("btc_ret24h_z", 1440)):
        out[k] = float((lc[j] - lc[j - n]) / (sd24 * np.sqrt(n))) if np.isfinite(lc[j - n]) else nan
    out["btc_rv_ratio"] = sd24 / sd7 if sd7 and sd7 > 0 else nan
    return out


def time_features(t_c: Any) -> dict[str, float]:
    ts = pd.Timestamp(t_c)
    h = ts.hour + ts.minute / 60
    return {"tod_sin": float(np.sin(2 * np.pi * h / 24)), "tod_cos": float(np.cos(2 * np.pi * h / 24)),
            "weekend": float(ts.dayofweek >= 5)}


def build_features(fr: pl.DataFrame, grids: dict[str, we.MinuteData], flow: pl.DataFrame) -> pl.DataFrame:
    """Append :data:`COMPUTED` + :data:`FLOW` columns to a prepared frame (row order kept)."""
    rows = []
    cols = ["asset_id", "t0", "t_c", "w_min", "b_min", "low_t0", "close_tc", "sigma_w", "a_log"]
    for asset, t0, t_c, w, b, low0, ctc, sw, al in fr.select(cols).iter_rows():
        r = event_features(grids[asset], t0, int(w), int(b), float(low0), float(ctc), float(sw), float(al))
        r |= context_features(grids[CONTEXT_ASSET], t_c) | time_features(t_c)
        rows.append(r)
    feat = pl.DataFrame(rows, schema=dict.fromkeys(COMPUTED, pl.Float64), orient="row").with_columns(
        pl.col(c).fill_nan(None) for c in COMPUTED)
    out = pl.concat([fr, feat], how="horizontal")
    fl = flow.select("flow_key", *FLOW)
    out = out.join(fl, on="flow_key", how="left", maintain_order="left")
    if out.height != fr.height or not (out["row_id"] == fr["row_id"]).all():
        raise RuntimeError("feature join changed the row order")
    return out


# --------------------------------------------------------------------------- data (train period only)


def load_flow_train(table: str = "scout_flow_v1") -> pl.DataFrame:
    """Order-flow rows with ``t_c`` before the OOS start (holdout-2 rows never enter memory)."""
    import duckdb

    import utils

    con = duckdb.connect(utils.lab_db_path("solusdt"), read_only=True)
    try:
        return con.execute(f'SELECT * FROM research."{table}" WHERE t_c < ?', [OOS_START]).pl()
    finally:
        con.close()


def load_grids(assets: list[str]) -> dict[str, we.MinuteData]:
    """1m grids of the assets (+ the context asset), cut at the OOS start before anything else touches them."""
    from reports.builders.sr_detection import load_ohlcv_1m
    from reports.builders.wave_oos_eval import assert_no_holdout

    grids = {}
    for a in sorted(set(assets) | {CONTEXT_ASSET}):
        df = load_ohlcv_1m(a)
        if df is None:
            raise RuntimeError(f"live db unavailable for {a}")
        df = df.filter(pl.col("open_time") < OOS_START)
        assert_no_holdout(df, sb.frame.HOLDOUT2_START)
        grids[a] = we.to_minute_grid(df)
    return grids


def load_inputs() -> tuple[pl.DataFrame, dict[str, we.MinuteData]]:
    """Prepared a15 train frame with the model's feature columns + the 1m grids (for the trade simulation)."""
    frame = sb.load_scout_events()
    sb.guard_no_holdout(frame)
    fr = sb.prepare_frame(frame)
    grids = load_grids(sorted(set(fr["asset_id"].to_list())))
    fr = build_features(fr, grids, load_flow_train())
    if fr["t_c"].max() >= OOS_START:
        raise ValueError("an event after the train period reached the feature frame")
    return fr, grids


# --------------------------------------------------------------------------- run


def fold_importance(fr: pl.DataFrame, folds: sb.FoldSet, seed: int = sb.runner.SEED) -> pd.DataFrame:
    """Gain importance of the fold models (same folds / seed as the benchmark): mean, sd and share per feature."""
    imps = []
    for f in folds:
        m = ScoutLGBM()
        m.fit(fr[f.train_idx], None, seed=seed)
        imps.append(m.importance())
    tab = pd.concat(imps, axis=1)
    share = tab / tab.sum(axis=0)
    out = pd.DataFrame({"feature": tab.index, "gain_mean": tab.mean(axis=1).to_numpy(), "share_mean": share.mean(axis=1).to_numpy(),
                        "share_sd": share.std(axis=1).to_numpy()})
    grp = {c: g for g, cs in GROUPS.items() for c in cs}
    out["group"] = out["feature"].map(grp)
    return out.sort_values("share_mean", ascending=False).reset_index(drop=True)


def run(*, n_perm: int = 1000, n_boot: int = 2000, spa_boot: int = 5000, persist_lab: bool = True, note: str = ""
        ) -> tuple[sb.BenchResult, pd.DataFrame]:
    fr, grids = load_inputs()
    reg = sb.new_registry()
    register(reg)
    res = sb.run_benchmark(fr, reg, grids, n_perm=n_perm, n_boot=n_boot, spa_boot=spa_boot, prepared=True,
                           note=note or f"t932 {MODEL_NAME} (baseline + P1)")
    imp = fold_importance(res.frame, res.folds)
    if persist_lab:
        logger.info("persisted %s", sb.persist(res, models=[MODEL_NAME]))
    return res, imp


# --------------------------------------------------------------------------- report


def _imp_fig(imp: pd.DataFrame, top: int = 25) -> go.Figure:
    t = imp.head(top).iloc[::-1]
    fig = go.Figure(go.Bar(x=t["share_mean"], y=t["feature"], orientation="h", marker={"color": theme.ACCENT},
                           error_x={"type": "data", "array": t["share_sd"], "color": theme.NEUTRAL},
                           customdata=t["group"], hovertemplate="%{y} (%{customdata}): %{x:.3f}<extra></extra>"))
    fig.update_xaxes(title_text="gain-részesedés (6 fold átlaga, ± szórás)")
    return themed(fig, height=max(360, 18 * len(t)), showlegend=False)


def _group_fig(imp: pd.DataFrame) -> go.Figure:
    g = imp.groupby("group")["share_mean"].sum().sort_values()
    fig = go.Figure(go.Bar(x=g.to_numpy(), y=g.index, orientation="h", marker={"color": theme.BLUE},
                           hovertemplate="%{y}: %{x:.3f}<extra></extra>"))
    fig.update_xaxes(title_text="gain-részesedés összesen")
    return themed(fig, height=300, showlegend=False)


def _blocks_fig(res: sb.BenchResult) -> go.Figure:
    t = res.tables[f"blocks:{MODEL_NAME}"]
    x = [f"B{b}" for b in t["block"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x, y=t["auc_model"], name=MODEL_NAME, marker={"color": theme.ORANGE}))
    fig.add_trace(go.Bar(x=x, y=t["auc_base"], name="baseline S0 + C", marker={"color": theme.BLUE}))
    fig.add_hline(y=0.5, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1})
    fig.update_yaxes(title_text="AUC (impulzus vs többi)", range=[0.3, 0.9])
    fig.update_layout(barmode="group")
    return themed(fig, height=340)


def _null_fig(res: sb.BenchResult, obs: float) -> go.Figure | None:
    null = res.perm_null.get(MODEL_NAME)
    if null is None or not null.size:
        return None
    fig = go.Figure(go.Histogram(x=null, nbinsx=50, marker={"color": theme.NEUTRAL}))
    fig.add_vline(x=obs, line={"color": theme.ACCENT, "width": 2}, annotation_text=f"megfigyelt ΔAUC {obs:+.4f}",
                  annotation_position="top right")
    fig.update_xaxes(title_text="ΔAUC (modell − baseline) nap-csomagosan permutált címkéken, teljes refit")
    fig.update_yaxes(title_text="permutációk száma")
    return themed(fig, height=320, showlegend=False)


def _cmp_table(lb: pd.DataFrame) -> pd.DataFrame:
    keys = ["auc", "delta_auc", "ci_lo", "ci_hi", "logloss3", "delta_logloss3", "n_top20", "prec_top20", "lift_top20",
            "n_top10", "prec_top10", "lift_top10", "n_top20_trthr", "prec_top20_trthr", "blocks_pos", "blocks_prec_better",
            "dauc_wo_best_block_mean", "assets_nonneg", "asset_min_dauc", "cal_slope", "perm_p", "holm_p", "spa_p",
            "trade20_n", "trade20_hit_rate", "trade20_break_even_hit", "trade20_mean_net_bps", "trade20_ci_lo_bps",
            "trade20_ci_hi_bps", "tradeall_mean_net_bps"]
    rows = []
    for k in keys:
        if k not in lb.columns:
            continue
        rows.append({"metric": k, **{str(r["model"]): r[k] for _, r in lb.iterrows()}})
    return pd.DataFrame(rows).astype(object).where(lambda d: d.notna(), None)


def build_report(res: sb.BenchResult, imp: pd.DataFrame, root: Path | None = None) -> Path:
    lb = res.leaderboard
    m = lb[lb["model"] == MODEL_NAME].iloc[0]
    b = lb[lb["is_baseline"]].iloc[0]
    run = res.run
    fr = res.frame
    n, n_pos = fr.height, int(run["n_impulse"])
    rb = ReportBuilder(
        scope="general", slug=SLUG, order=34, root=root,
        title="Impulse Scout P1 — LightGBM kézi prefix-jellemzőkön + order flow (H1, 1. forduló)",
        summary=f"t932: {MODEL_NAME} vs baseline S0 + C a közös purged CV-n ({n} esemény, {run['assets']}): AUC "
                f"{m['auc']:.4f} vs {b['auc']:.4f} (ΔAUC {m['delta_auc']:+.4f}, perm. p {fmt(m['perm_p'])}); go/no-go: {m['go_status']}.",
        tags=["scout", "epic_073", "lightgbm", "H1"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    rb.add_markdown(
        "## Cél\n\n"
        "**t932 (epic_073):** az 5300 módszertan **H1** elsődleges hipotézise (P1 család): egy LightGBM a kézi, `t_c`-ig ismert "
        "prefix-jellemzőkön és az order flow-n jobban rangsorolja-e az impulzust, mint a skalár logit (S0 + C). A konfiguráció "
        "**előre rögzített** (1. forduló, nincs hangolás a CV-n), a közös `scout_bench` keretben fut (azonos foldok, metrikák, "
        "permutáció, SPA, szimuláció; `_doc_/methodology_doc/5300_impulse_scout.md`, kód: `src/reports/builders/scout_models_lgbm.py`).\n\n"
        f"Adat: `research.\"scout_events_v1\"` (a15, W4h_H8h, train split) **{n} esemény**, **{n_pos} impulzus** "
        f"({100 * n_pos / n:.1f}%), assetek: {run['assets']}. Fold-hash `{run['fold_hash']}`, run `{run['run_id']}`.\n\n"
        "> **Holdout-védelem:** az eseménykeret csak a train split; az 1m árak és az order-flow sorok az OOS kezdete "
        "(2026-02-01) előtt levágva, mielőtt bármely jellemző készül; a holdout-2 sorai és címkéi nem kerültek a memóriába.")
    cfg = {k: v for k, v in CONFIG.items() if k != "features"}
    rb.add_markdown(
        "## Modell és jellemzők\n\n"
        f"- **Tanuló:** LightGBM, 3 osztályos `multiclass` cél (impulzus / hamis indulás / oldalazás); score = P(impulzus). "
        f"Rögzített hiperparaméterek: `{cfg}`; config-hash `{m['config_hash']}`.\n"
        f"- **{len(FEATURES)} jellemző** hét csoportban (mind a `t_c`-ig zárt 1m bar-okból; címke, `τ`, más asset eseménye / "
        "címkéje nincs köztük):\n"
        + "".join(f"  - *{g}* ({len(cs)}): {', '.join(f'`{c}`' for c in cs)}\n" for g, cs in GROUPS.items())
        + "- Az alak- és távolság-jellemzők `log(ár / low[t0])` `σ_W` egységben (az amplitúdó megmarad); a volumen / flow a t930 "
        "arány-jellemzői (asset-saját 7 napos bázis). A BTC-kontextus 1h / 4h / 24h log-hozam a BTC saját 24 órás 1m "
        "volatilitásával normálva (BTC-eseménynél a saját múlt).\n"
        "- **Megjegyzés (5300):** a módszertan az asset-közi jellemzőt (BTC-kontextus) kísérletinek nevezi; a ticket a P1 "
        "előre rögzített készletébe sorolta. A hozzájárulását a jellemző-fontosság mutatja (lent).")

    rb.add_markdown(
        "## 1. Eredmény a baseline-hoz képest\n\n"
        "A ΔAUC CI nap-klaszter bootstrap; a `perm_p` nap-csomagos permutáció **N = "
        f"{run['n_perm']}**, teljes refit (a baseline és a modell is újratanul minden permutációban); a Holm-korrekció a "
        "futásban regisztrált elsődleges hipotézis(ek)re vonatkozik (itt csak H1; a teljes H1-H3 Holm a t936 leaderboardon). "
        "A felső 20% / 10% az **elsődleges nézet: teszt-foldon belüli rangsor**; a `_trthr` sorok a másodlagos, train-küszöbös nézet.")
    rb.add_table("comparison", _cmp_table(lb), title="Modell vs baseline: fő metrikák",
                 columns={"metric": "Metrika", MODEL_NAME: {"label": MODEL_NAME, "digits": 4},
                          str(b["model"]): {"label": "baseline S0 + C", "digits": 4}})
    gates = {k: m[k] for k in sb.runner.GATE_KEYS}
    gtab = pd.DataFrame([{"feltétel": k.removeprefix("gate_"), "teljesül": ("n/a" if v is None or (isinstance(v, float) and np.isnan(v))
                                                                            else ("igen" if bool(v) else "nem"))}
                         for k, v in gates.items()])
    rb.add_table("gates", gtab, title=f"Go / no-go feltételek (5300 A1–A7 + B) — {m['go_status']}",
                 columns={"feltétel": "Feltétel", "teljesül": "Teljesül"})
    nul = _null_fig(res, float(m["delta_auc"]))
    if nul is not None:
        rb.add_chart("null_dauc", nul, title="ΔAUC permutációs null (H1)", height=320)

    rb.add_markdown("## 2. Blokk- és asset-bontás\n\nA blokkonkénti ΔAUC és a felső 20% precizitás (elsődleges nézet) "
                    "a go-feltételek 5. és 6. pontjához.")
    rb.add_chart("blocks", _blocks_fig(res), title="Blokkonkénti AUC: modell vs baseline", height=340)
    blk = res.tables[f"blocks:{MODEL_NAME}"].merge(res.tables[f"topq_blocks:{MODEL_NAME}"].drop(columns="n"), on="block")
    rb.add_table("blocks_table", blk, title="Blokk-táblázat",
                 columns={"block": "Blokk", "n": "Esemény", "n_impulse": "Impulzus", "auc_model": {"label": "AUC", "digits": 4},
                          "auc_base": {"label": "AUC baseline", "digits": 4}, "delta_auc": {"label": "ΔAUC", "digits": 4},
                          "n_top20": "Top20 n", "prec_top20": {"label": "Prec. top20", "digits": 3},
                          "base_prec_top20": {"label": "Prec. top20 baseline", "digits": 3},
                          "base_rate": {"label": "Alapráta", "digits": 3}})
    rb.add_table("assets_table", res.tables[f"assets:{MODEL_NAME}"], title="Asset-bontás",
                 columns={"asset_id": "Asset", "n": "Esemény", "n_impulse": "Impulzus", "auc_model": {"label": "AUC", "digits": 4},
                          "auc_base": {"label": "AUC baseline", "digits": 4}, "delta_auc": {"label": "ΔAUC", "digits": 4}})
    rb.add_table("calibration_asset", res.tables[f"calibration_asset:{MODEL_NAME}"], title="Kalibráció assetenként (modell)",
                 columns={"asset_id": "Asset", "n": "Esemény", "slope": {"label": "Meredekség", "digits": 2},
                          "intercept": {"label": "Tengelymetszet", "digits": 2}, "mean_pred": {"label": "Átl. becslés", "digits": 3},
                          "obs_rate": {"label": "Megfigyelt", "digits": 3}})

    rb.add_markdown("## 3. Jellemző-fontosság\n\nLightGBM gain, a 6 fold-modell (azonos foldok és seed) átlagos részesedése ± "
                    "szórás. A fontosság a modell belső használatát mutatja, nem oksági vagy marginális hatást.")
    rb.add_chart("importance", _imp_fig(imp), title="Jellemző-fontosság (felső 25)", height=max(360, 18 * 25))
    rb.add_chart("importance_groups", _group_fig(imp), title="Fontosság csoportonként", height=300)
    rb.add_table("importance_table", imp, title="Jellemző-fontosság (mind)",
                 columns={"feature": "Jellemző", "group": "Csoport", "gain_mean": {"label": "Gain átlag", "digits": 1},
                          "share_mean": {"label": "Részesedés", "digits": 4}, "share_sd": {"label": "Szórás", "digits": 4}})

    trows = []
    for q, label in (("20", "felső 20% (fold-rang)"), ("10", "felső 10% (fold-rang)"), ("20t", "felső 20% (train-küszöb)"),
                     ("all", "minden esemény")):
        for _, r in lb.iterrows():
            if f"trade{q}_n" in r:
                trows.append({"model": r["model"], "signal": label, "n": r[f"trade{q}_n"], "hit": r[f"trade{q}_hit_rate"],
                              "be_hit": r[f"trade{q}_break_even_hit"], "net_bps": r[f"trade{q}_mean_net_bps"],
                              "ci_lo": r[f"trade{q}_ci_lo_bps"], "ci_hi": r[f"trade{q}_ci_hi_bps"], "pf": r[f"trade{q}_profit_factor"]})
    rb.add_markdown("## 4. Triple-barrier szimuláció (díjak után)\n\nBelépés a `t_c` utáni első 1m bar nyitóján, cél `P*`, stop "
                    "`low[t0]`, időkorlát `t_c + H`; költségek a `config/strategies.json` `costs` szekciójából "
                    f"(`{run['costs_json']}`). CI: nap-klaszter bootstrap.")
    rb.add_table("trades", pd.DataFrame(trows), title="Szimuláció: modell vs baseline",
                 columns={"model": "Modell", "signal": "Jelzés", "n": "Esemény", "hit": {"label": "Cél-arány", "digits": 3},
                          "be_hit": {"label": "Break-even", "digits": 3}, "net_bps": {"label": "Nettó bp / esemény", "digits": 1},
                          "ci_lo": {"label": "CI alsó", "digits": 1}, "ci_hi": {"label": "CI felső", "digits": 1},
                          "pf": {"label": "Profit faktor", "digits": 2}})

    top_groups = imp.groupby("group")["share_mean"].sum().sort_values(ascending=False)
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"- **Rangsorolás:** AUC {m['auc']:.4f} vs baseline {b['auc']:.4f}, ΔAUC **{m['delta_auc']:+.4f}** "
        f"(95% CI [{m['ci_lo']:+.4f}; {m['ci_hi']:+.4f}]), permutációs p = {fmt(m['perm_p'])}. "
        f"Blokkonként ΔAUC > 0: {int(m['blocks_pos'])} / {int(m['n_blocks'])}; a legjobb blokk nélkül átlag {m['dauc_wo_best_block_mean']:+.4f}.\n"
        f"- **Valószínűség-minőség:** 3 osztályos log-loss {m['logloss3']:.4f} vs {b['logloss3']:.4f} (ΔLL {m['delta_logloss3']:+.4f}); "
        f"kalibrációs meredekség {fmt(m['cal_slope'], 2)}.\n"
        f"- **Szelekció (elsődleges nézet):** felső 20% precizitás {fmt(m['prec_top20'], 3)} vs {fmt(b['prec_top20'], 3)} "
        f"(lift {fmt(m['lift_top20'], 2)}; jobb blokk {int(m['blocks_prec_better'])} / 6); felső 10% {fmt(m['prec_top10'], 3)} vs "
        f"{fmt(b['prec_top10'], 3)}.\n"
        f"- **Gazdasági előszűrő:** felső 20% nettó {fmt(m.get('trade20_mean_net_bps'), 1)} bp / esemény "
        f"(CI [{fmt(m.get('trade20_ci_lo_bps'), 1)}; {fmt(m.get('trade20_ci_hi_bps'), 1)}]) vs baseline {fmt(b.get('trade20_mean_net_bps'), 1)}.\n"
        f"- **Jellemzők:** a gain legnagyobb része: {', '.join(f'{g} {v:.2f}' for g, v in top_groups.head(3).items())}.\n"
        f"- **Döntés (5300):** **{m['go_status']}**" + (f" — bukott feltételek: `{m['go_failed']}`." if m["go_failed"] else ".")
        + " A Holm-korrekció a három elsődleges (H1-H3) p-értéken a t936 leaderboardon véglegesül; az 1. forduló konfigurációja "
          "rögzített, utólag nem módosítható (5300 iterációs szabályok).")
    return rb.write()


def build_report_safe(res: sb.BenchResult, imp: pd.DataFrame, root: Path | None = None) -> Path:
    """Windows folder-replace fallback (``WinError 5``): build in a temp dir and copy over."""
    try:
        return build_report(res, imp, root)
    except PermissionError:
        tmp = Path(tempfile.mkdtemp(prefix="scout_lgbm_"))
        out = build_report(res, imp, tmp)
        target = (root or Path(__file__).resolve().parents[3]) / "_doc_" / "reports" / SLUG
        out = tmp / "_doc_" / "reports" / SLUG
        target.mkdir(parents=True, exist_ok=True)
        shutil.copytree(out, target, dirs_exist_ok=True)
        return target


def main() -> None:
    ap = argparse.ArgumentParser(description="t932 Impulse Scout LightGBM (H1, round 1)")
    ap.add_argument("--n-perm", type=int, default=1000)
    ap.add_argument("--no-persist", action="store_true")
    ap.add_argument("--root", default=None)
    a = ap.parse_args()
    res, imp = run(n_perm=a.n_perm, persist_lab=not a.no_persist)
    print(build_report_safe(res, imp, Path(a.root) if a.root else None))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
