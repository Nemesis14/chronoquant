"""``reports.builders.sr_placebo`` on synthetic 1m data (no real database)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pytest

from data_handling.sync_tables import _levels_polars as det
from reports import store
from reports.builders import sr_placebo as sp
from reports.schema import Manifest
from reports.tests.test_sr_detection import PARAMS, _synthetic_1m

TRAIN_START = datetime(2025, 1, 1)
OOS_START = datetime(2025, 2, 15)


@pytest.fixture(scope="module")
def df_1m() -> Any:
    return _synthetic_1m()


@pytest.fixture(scope="module")
def results(df_1m: Any) -> dict[str, det.ScaleResult]:
    return det.detect_all_scales(df_1m, params=PARAMS)


def _feature_sample(df_1m: Any) -> pd.DataFrame:
    rng = np.random.default_rng(3)
    t = pd.date_range("2025-01-01", periods=70 * 24, freq="h")
    n = len(t)
    long_t = rng.normal(0.004, 0.002, n)
    out = pd.DataFrame({"open_time": t, "long_mfe_fw60": long_t, "short_mfe_fw60": rng.normal(0.004, 0.002, n)})
    for c in sp.level_feature_columns()[:6]:
        x = rng.normal(size=n)
        x[rng.random(n) < 0.2] = np.nan
        out[c] = x
    out["feat_sr_dist_res_atr_s"] = long_t * 100 + rng.normal(0, 0.1, n)  # one informative column
    out["feat_champ_a"] = rng.normal(size=n)
    out["feat_champ_b"] = out["feat_sr_dist_res_atr_s"] * 0.9 + rng.normal(0, 0.05, n)  # redundant twin
    return out


def test_batch_scan_equals_detector(results: dict[str, det.ScaleResult]) -> None:
    """The vectorised scan + labels reproduce the detector exactly on several placebo sets of every scale."""
    n_checked = 0
    for res in results.values():
        if res.levels.height + res.trendlines.height == 0:
            continue
        pb = sp.make_placebo(res, PARAMS, n_sets=4, seed=11)
        t0 = int(TRAIN_START.replace(tzinfo=UTC).timestamp())
        acc = sp.Accum(n_days=80, n_sets=4)
        clock = sp.Clock(close_s=sp._epoch_s(res.bars["close_time"]), t0=t0, oos_day=45)  # noqa: SLF001
        for k in range(4):
            got = sp.accumulate_placebo(acc, res, pb, PARAMS, clock, collect_set=k)
            assert got is not None
            n_checked += sp.verify_against_detector(res, pb, PARAMS, got, k)
    assert n_checked > 0  # the comparison is not vacuous


def test_batch_scan_equals_detector_on_real_objects(results: dict[str, det.ScaleResult]) -> None:
    """With the real prices (K = 1) the batch scan gives the detector's own events."""
    n_ev = 0
    for res in results.values():
        arrays = sp._bar_arrays(res)  # noqa: SLF001
        cl = arrays[2]
        stops = sp._stops(res, res.levels["expired_at"])  # noqa: SLF001
        for i, r in enumerate(res.levels.iter_rows(named=True)):
            ci = r["confirm_idx"]
            if ci + 1 >= res.bars.height:
                continue
            kk, jj, tt, _, _ = sp.scan_batch(arrays, ci + 1, int(stops[i]), np.array([r["price"]]), 0, 0.0,
                                             np.array([1.0 if cl[ci] >= r["price"] else -1.0]), 0.25, 0.5, 12, False)
            ref = res.events.filter(pl.col("obj_id") == r["level_id"]).sort("bar_idx")
            assert jj.tolist() == ref["bar_idx"].to_list()
            assert [sp.EVENT_TYPES[t] for t in tt] == ref["event_type"].to_list()
            n_ev += kk.size
    assert n_ev > 0


def test_holm() -> None:
    p = np.array([0.01, 0.04, 0.03, 0.005, np.nan])
    adj = sp.holm(p)
    assert np.isnan(adj[4])
    np.testing.assert_allclose(adj[:4], [0.03, 0.06, 0.06, 0.02])


def test_delta_stats_no_effect_and_effect() -> None:
    rng = np.random.default_rng(0)
    days = 300
    rn = rng.poisson(20, days).astype(float)
    pn = rn * 50
    w = sp.bootstrap_weights(days, 500, 1)
    same = sp.delta_stats(rn, rng.binomial(rn.astype(int), 0.6).astype(float), pn,
                          rng.binomial(pn.astype(int), 0.6).astype(float), w)
    assert abs(same["delta"]) < 0.03 and same["p_boot"] > 0.001
    eff = sp.delta_stats(rn, rng.binomial(rn.astype(int), 0.7).astype(float), pn,
                         rng.binomial(pn.astype(int), 0.6).astype(float), w)
    assert eff["delta"] > 0.07 and eff["p_boot"] < 1e-6


def test_feature_group() -> None:
    assert sp.feature_group("feat_sr_dist_res_atr_s") == ("sr_dist", "s")
    assert sp.feature_group("feat_sr_n_active_m") == ("sr_n_active", "m")
    assert sp.feature_group("feat_tl_break_age_m") == ("tl_break_age", "m")
    assert sp.feature_group("feat_sr_is_round_sup_s") == ("sr_is_round", "s")


@pytest.fixture(scope="module")
def report(tmp_path_factory: pytest.TempPathFactory, df_1m: Any) -> Path:
    root = tmp_path_factory.mktemp("repo")
    out = sp.build(root=root, df_1m=df_1m, params=PARAMS, n_sets=20, n_boot=200, feature_df=_feature_sample(df_1m),
                   train_start=TRAIN_START, oos_start=OOS_START)
    assert out is not None
    return out


def test_report_manifest_and_content(report: Path) -> None:
    manifest = Manifest.model_validate_json((report / "manifest.json").read_text(encoding="utf-8"))
    assert manifest.scope == "general" and manifest.id == sp.SLUG
    root = report.parents[2]
    assert [r["id"] for r in store.list_reports("general", None, root)] == [sp.SLUG]
    _, md = store.read_report("general", sp.SLUG, root)
    charts = {c.id for c in manifest.content.charts}
    tables = {t.id for t in manifest.content.tables}
    assert {"delta_forest", "train_vs_oos", "memory_curve", "feat_rho", "feat_redundancy", "feat_deciles"} <= charts
    assert {"verification", "placebo_check", "go_nogo", "memory", "feat_diag", "champion_benchmark"} <= tables
    for cid in charts:
        assert "{{chart:" + cid + "}}" in md
    for tid in tables:
        assert "{{table:" + tid + "}}" in md
    assert "vizuálisan látott" in md and "Holm" in md
    go = json.loads((report / "tables" / "go_nogo.json").read_text(encoding="utf-8"))
    assert go["total_rows"] == sp.N_CELLS
    keys = [c["key"] for c in go["columns"]]
    assert "decision" in keys and "tr_p_holm" in keys


def test_charts_have_data(report: Path) -> None:
    for path in (report / "charts").glob("*.json"):
        fig = json.loads(path.read_text(encoding="utf-8"))
        assert "template" not in fig["layout"]
        assert fig["data"], f"no traces in {path.name}"


def test_feature_diagnostics_detects_signal_and_redundancy(df_1m: Any) -> None:
    fdf = _feature_sample(df_1m)
    lc = [c for c in sp.level_feature_columns() if c in fdf.columns]
    fd, bench = sp.feature_diagnostics(fdf, lc, ["feat_champ_a", "feat_champ_b"], OOS_START)
    row = fd.set_index("feature").loc["feat_sr_dist_res_atr_s"]
    assert row["rho_long"] > 0.8 and row["max_rho_champion"] > 0.9
    assert row["max_rho_champion_feature"] == "feat_champ_b"
    assert len(bench) == 2
    assert "feat_sr_dist_res_atr_s" not in sp.feature_candidates(fd, bench)["feature"].tolist()
