# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false
"""``reports.builders.scout_bench`` (epic_073 / t931) on synthetic multi-asset data (no database).

Covers: cross-asset fold leakage, identical folds, determinism, metric correctness on toy data, day-bundle permutation
structure, Holm / SPA, cost reading from the config, the triple-barrier simulation on hand-built bars (same-bar double touch
included), the holdout guard, the registry rules and an end-to-end run with a primary model.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

pytest.importorskip("sklearn")

from reports.builders import scout_bench as sb  # noqa: E402
from reports.builders import wave_events as we  # noqa: E402
from reports.builders.scout_bench import metrics as mt  # noqa: E402
from reports.builders.scout_bench import stats as st  # noqa: E402
from reports.builders.scout_bench import trades as tr  # noqa: E402

START = datetime(2022, 1, 1)
END = datetime(2022, 7, 1)
PARAMS = we.params_v2(train_start="2022-01-01", oos_start="2022-07-01")
BOUNDS = we.cv_block_bounds(PARAMS)
ASSETS = ("aaausdt", "bbbusdt", "cccusdt")
H = 480
COSTS = tr.Costs(2.0, 5.0, 1.5, {"tp": "maker", "sl": "taker", "timeout": "taker"})


def make_frame(n_per_asset: int = 180, seed: int = 0, signal: float = 1.0) -> pl.DataFrame:
    """Random multi-asset events over six months; ``a_sigma`` carries signal for the impulse label."""
    rng = np.random.default_rng(seed)
    rows = []
    span = (END - START).total_seconds() / 60 - 2 * (240 + H)
    for a in ASSETS:
        for _ in range(n_per_asset):
            t0 = START + timedelta(minutes=240 + float(rng.uniform(0, span)))
            t0 = t0.replace(second=0, microsecond=0)
            x = rng.normal(size=6)
            p = 1 / (1 + np.exp(-(-1.6 + signal * x[0])))
            u = rng.random()
            cls = "impulse" if u < p else ("false_start" if rng.random() < 0.5 else "sideways")
            tau = float(rng.integers(5, H)) if cls != "sideways" else float(H)
            t_c = t0 + timedelta(minutes=240)
            rows.append({
                "asset_id": a, "cell_id": "W4h_H8h", "t0": t0, "t_c": t_c, "event_start": t0 - timedelta(minutes=240),
                "label_end": t_c + timedelta(minutes=H), "split": "train", "h_min": H, "label_class": cls, "tau_min": tau,
                "p_star": 110.0, "low_t0": 95.0, "close_tc": 100.0, "sigma_w": 0.02,
                **dict(zip(sb.S0C_COLS, x, strict=True)),
            })
    return pl.DataFrame(rows)


def prepared(**kw) -> pl.DataFrame:
    return sb.prepare_frame(make_frame(**kw), BOUNDS)


# --------------------------------------------------------------------------- folds


def test_fold_no_cross_asset_leakage_and_same_folds():
    fr = prepared()
    folds = sb.make_folds(fr, BOUNDS)
    assert len(folds) == 6
    es = fr["event_start"].to_numpy().astype("datetime64[us]")
    le = fr["label_end"].to_numpy().astype("datetime64[us]")
    emb = np.timedelta64(timedelta(minutes=H))
    assets = fr["asset_id"].to_numpy()
    for f in folds:
        lo, hi = np.datetime64(BOUNDS[f.block], "us"), np.datetime64(BOUNDS[f.block + 1], "us")
        assert not set(f.train_idx) & set(f.test_idx)
        assert (fr["cv_block"].to_numpy()[f.test_idx] == f.block).all()
        # no train event of ANY asset overlaps the test block widened by the embargo (purge incl. embargo)
        tr_es, tr_le = es[f.train_idx], le[f.train_idx]
        assert not ((tr_es < hi + emb) & (tr_le > lo - emb)).any()
        # the test block holds several assets, and the train set also (cross-asset pooling is real)
        assert len(set(assets[f.test_idx])) == 3 and len(set(assets[f.train_idx])) == 3
    assert sb.make_folds(fr, BOUNDS).hash == folds.hash                       # hash-fixed
    # every usable event is tested exactly once
    all_test = np.concatenate([f.test_idx for f in folds])
    assert sorted(all_test.tolist()) == list(range(fr.height))


def test_purge_and_embargo_remove_boundary_events():
    base = make_frame(n_per_asset=1)
    lo = BOUNDS[3]
    # event A (asset a) ends 1h after the block-3 start but starts in block 2 -> straddles (dropped from all folds);
    # event B (asset b) is inside block 2 but its interval ends within the H embargo of block 3 -> embargoed
    # event C (asset c) is well inside block 2 -> stays in the block-3 train set
    def ev(asset, t0):
        r = base.row(0, named=True)
        r.update(asset_id=asset, t0=t0, t_c=t0 + timedelta(minutes=240), event_start=t0 - timedelta(minutes=240),
                 label_end=t0 + timedelta(minutes=240 + H))
        return r
    rows = [ev("aaausdt", lo - timedelta(minutes=240 + H - 60)),
            ev("bbbusdt", lo - timedelta(minutes=240 + H + 240 + 30)),
            ev("cccusdt", lo - timedelta(days=20)),
            ev("aaausdt", lo + timedelta(days=3))]
    fr = sb.prepare_frame(pl.DataFrame(rows), BOUNDS)
    assert fr.height == 3                                                     # the straddler is gone
    f3 = sb.make_folds(fr, BOUNDS).folds[3]
    train_assets = set(fr["asset_id"].to_numpy()[f3.train_idx])
    assert "cccusdt" in train_assets and "bbbusdt" not in train_assets
    assert f3.n_embargoed == 1


# --------------------------------------------------------------------------- determinism + end-to-end


class NoisyLogit(sb.ScoutModel):
    """Seeded model: logistic on the first feature + a seed-dependent jitter (determinism check)."""

    def fit(self, train, prefix_data, *, seed):
        self.base = sb.ScalarLogit(cols=("a_sigma",))
        self.base.fit(train, prefix_data, seed=seed)
        self.rng = np.random.default_rng(seed)

    def predict_proba(self, df, prefix_data):
        p = self.base.predict_proba(df, prefix_data)
        j = np.random.default_rng(1234).normal(0, 1e-3, df.height)
        p[:, 0] = np.clip(p[:, 0] + j, 1e-3, 0.99)
        return p / p.sum(axis=1, keepdims=True)


def _registry():
    reg = sb.new_registry()
    reg.register(sb.ModelSpec("toy_h1", lambda: sb.ScalarLogit(c=1.0, cols=("a_sigma", "velocity_1h")), "P1", hypothesis="H1",
                              config={"c": 1.0}))
    reg.register(sb.ModelSpec("toy_noisy", NoisyLogit, "X0", config={"jitter": 1e-3}))
    return reg


def _bars_for(fr: pl.DataFrame):
    return None


def test_end_to_end_determinism_and_primary_permutation():
    fr = make_frame(n_per_asset=120)
    kw = {"n_perm": 6, "n_boot": 60, "spa_boot": 200, "bounds": BOUNDS, "costs": COSTS}
    r1 = sb.run_benchmark(fr, _registry(), **kw)
    r2 = sb.run_benchmark(fr, _registry(), **kw)
    drop = ["created_at", "fit_seconds"]
    pd1, pd2 = r1.leaderboard.drop(columns=drop), r2.leaderboard.drop(columns=drop)
    assert pd1.equals(pd2)                                                    # bit-identical, incl. permutation / bootstrap
    assert r1.run["run_hash"] == r2.run["run_hash"] and r1.folds.hash == r2.folds.hash
    lb = r1.leaderboard.set_index("model")
    assert lb.loc["S0C_logit", "delta_auc"] == 0
    assert 0 < lb.loc["toy_h1", "perm_p"] <= 1 and lb.loc["toy_h1", "holm_p"] >= lb.loc["toy_h1", "perm_p"]
    assert np.isnan(lb.loc["toy_noisy", "perm_p"])                            # exploratory: no primary test
    assert lb.loc["S0C_logit", "perm_p"] >= 1 / 7                             # baseline AUC permutation p exists
    assert r1.spa is not None and 0 <= r1.spa["p_consistent"] <= 1 and r1.run["spa_best"] in ("toy_h1", "toy_noisy")
    assert lb.loc["toy_noisy", "primary"] == False and lb.loc["toy_h1", "primary"] == True  # noqa: E712
    # identical folds: every model's OOF covers every event exactly once
    for o in r1.outputs.values():
        assert np.isfinite(o.proba).all() and np.allclose(o.proba.sum(axis=1), 1)
    # baseline AUC on the toy signal is clearly above chance
    assert lb.loc["S0C_logit", "auc"] > 0.6


def test_baseline_binary_score_equals_plain_logit_auc():
    fr = prepared(seed=3)
    folds = sb.make_folds(fr, BOUNDS)
    out = sb.cross_val(sb.baseline_spec(), fr, folds, seed=1)
    from sklearn.linear_model import LogisticRegression

    y = (fr["label_class"] == "impulse").to_numpy().astype(float)
    ref = np.full(fr.height, np.nan)
    for f in folds:
        x = fr.select(list(sb.S0C_COLS)).to_numpy()
        mu, sd = x[f.train_idx].mean(0), x[f.train_idx].std(0)
        m = LogisticRegression(C=0.1, max_iter=2000).fit((x[f.train_idx] - mu) / sd, y[f.train_idx])
        ref[f.test_idx] = m.predict_proba((x[f.test_idx] - mu) / sd)[:, 1]
    assert np.allclose(out.score, ref)


# --------------------------------------------------------------------------- metrics on toy data


def test_auc_toy_values():
    y = np.array([0, 0, 1, 1])
    assert mt.auc(y, np.array([0.1, 0.2, 0.3, 0.4])) == 1.0
    assert mt.auc(y, np.array([0.4, 0.3, 0.2, 0.1])) == 0.0
    assert mt.auc(y, np.array([0.5, 0.5, 0.5, 0.5])) == 0.5                   # ties
    assert mt.auc(y, np.array([0.1, 0.35, 0.3, 0.4])) == 0.75
    assert np.isnan(mt.auc(np.zeros(4), np.arange(4.0)))


def test_logloss_topq_calibration_blocks():
    cls = np.array([0, 1, 2])
    proba = np.array([[0.7, 0.2, 0.1], [0.1, 0.8, 0.1], [0.2, 0.2, 0.6]])
    assert mt.logloss_3class(cls, proba) == pytest.approx(-(np.log(0.7) + np.log(0.8) + np.log(0.6)) / 3)
    y = np.array([1, 0, 1, 0, 0, 0, 1, 0, 0, 0], float)
    s = np.arange(10.0)
    t = mt.top_q(y, s, 6.0)                                                   # scores 6..9 -> y = 1, 0, 0, 0
    assert t["n"] == 4 and t["precision"] == pytest.approx(0.25) and t["lift"] == pytest.approx(0.25 / 0.3)
    assert mt.top_q(y, s, np.full(10, 6.0))["n"] == 4                         # per-event thresholds
    rng = np.random.default_rng(0)
    p = rng.uniform(0.05, 0.95, 4000)
    yy = (rng.random(4000) < p).astype(float)
    tab, st_ = mt.calibration(yy, p)
    assert abs(st_["slope"] - 1) < 0.15 and abs(st_["intercept"]) < 0.15 and len(tab) == 5
    # per-block deltas and best-block removal
    blocks = np.repeat(np.arange(4), 50)
    yb = np.tile(np.array([0, 1] * 25), 4).astype(float)
    base = rng.normal(size=200)
    model = base.copy()
    model[blocks == 2] = yb[blocks == 2] * 3 + rng.normal(size=50) * 0.1           # one block carries all the lift
    bb = mt.best_block_removed(yb, model, base, blocks)
    assert bb["best_block"] == 2 and abs(bb["mean_wo_best"]) < 0.2
    assert mt.breakdown_auc(yb, model, base, blocks).query("group == 2")["delta_auc"].iloc[0] > 0.25


# --------------------------------------------------------------------------- permutation, bootstrap, Holm, SPA


def test_day_bundle_permutation_preserves_day_structure():
    fr = prepared(n_per_asset=300, seed=5)
    t0, asset = fr["t0"].to_numpy(), fr["asset_id"].to_numpy()
    bundles = st.day_bundles(t0, asset)
    assert bundles.n_structure_groups >= 1
    rng = np.random.default_rng(0)
    src = st.permutation_source(fr.height, bundles, rng)
    assert sorted(src.tolist()) == list(range(fr.height))                     # a true permutation
    fp = st.permute_frame_labels(fr, src)
    cls, cls_p = fr["label_class"].to_numpy(), fp["label_class"].to_numpy()
    assert sorted(cls) == sorted(cls_p)                                       # overall counts kept
    # tau and impulse flag move together with the class
    assert ((fp["label_class"] == "impulse").to_numpy() == fp["label_impulse"].to_numpy()).all()
    assert ((fp["label_class"] == "sideways").to_numpy() == (fp["tau_min"].to_numpy() == H)).all()
    day = t0.astype("datetime64[D]")
    changed = 0
    for d in np.unique(day):
        pos = np.flatnonzero(day == d)
        pos = pos[np.lexsort((t0[pos], asset[pos]))]
        key = tuple(int((asset[pos] == a).sum()) for a in sorted(set(asset)))
        # the permuted day vector equals the label vector of some original day with the same per-asset structure
        vec = tuple(cls_p[pos])
        found = False
        for g in bundles.groups:
            if pos[0] in g[:, 0] and any(tuple(cls[row]) == vec for row in g):
                found = True
                for row in g:
                    assert tuple(int((asset[row] == a).sum()) for a in sorted(set(asset))) in (key,) or g.shape[1] == len(pos)
        fixed = vec == tuple(cls[pos])
        assert found or fixed
        changed += vec != tuple(cls[pos])
    assert changed > 0
    # features, keys, times untouched
    assert fp["a_sigma"].to_list() == fr["a_sigma"].to_list() and fp["t0"].to_list() == fr["t0"].to_list()


def test_permutation_fallback_groups_and_fixed_days():
    t0 = np.array(["2022-01-01T01:00", "2022-01-02T01:00", "2022-01-02T02:00", "2022-01-03T01:00", "2022-01-03T02:00",
                   "2022-01-04T01:00", "2022-01-04T02:00", "2022-01-04T03:00"], dtype="datetime64[us]")
    asset = np.array(["a", "a", "b", "a", "a", "a", "a", "b"])
    b = st.day_bundles(t0, asset)
    # day1 {a:1} alone; day2 {a:1,b:1} ; day3 {a:2} ; day4 {a:2,b:1} -> no equal structure; totals: 1, 2, 2, 3
    assert b.n_structure_groups == 0 and b.n_total_groups == 1 and b.n_fixed_days == 2
    src = st.permutation_source(8, b, np.random.default_rng(1))
    assert src[0] == 0 and (src[5:] == np.arange(5, 8)).all()                  # the unpaired days keep their labels


def test_perm_p_holm_and_cluster_bootstrap():
    assert st.perm_p(0.5, np.array([0.1, 0.2, 0.5, 0.6])) == pytest.approx(3 / 5)
    assert st.perm_p(9.0, np.zeros(99)) == pytest.approx(1 / 100)
    adj = st.holm({"H1": 0.001, "H2": 0.02, "H3": 0.04})
    assert adj == {"H1": pytest.approx(0.003), "H2": pytest.approx(0.04), "H3": pytest.approx(0.04)}
    t0 = (np.datetime64("2022-01-01") + np.repeat(np.arange(100), 3)).astype("datetime64[us]")
    cl = st.day_clusters(t0)
    assert cl.n_days == 100 and all(len(m) == 3 for m in cl.members)
    vals = np.random.default_rng(0).normal(1.0, 1.0, 300)
    lo, hi, draws = st.cluster_bootstrap(lambda idx: float(vals[idx].mean()), cl, n_boot=300, seed=1)
    assert lo < vals.mean() < hi and lo > 0.8 and hi < 1.25 and draws.size == 300


def test_spa_detects_a_superior_model_and_not_noise():
    rng = np.random.default_rng(0)
    t = 600
    noise = rng.normal(0, 1, (t, 6))
    p_noise = st.spa_test(noise, n_boot=1500, seed=3)
    assert p_noise["p_consistent"] > 0.1
    good = noise.copy()
    good[:, 2] += 0.35
    p_good = st.spa_test(good, n_boot=1500, seed=3)
    assert p_good["best"] == 2 and p_good["p_consistent"] < 0.02
    assert p_good["p_lower"] <= p_good["p_consistent"] <= p_good["p_upper"] + 1e-12
    idx = st.stationary_indices(50, 40, 5.0, np.random.default_rng(0))
    assert idx.shape == (40, 50) and idx.min() >= 0 and idx.max() < 50


def test_spa_all_models_worse_than_baseline_gives_p_one():
    """Regression (t932 validation): T = max(0, .) is 0 when no model beats the baseline; p must be 1, not the share of
    bootstrap draws > 0 (the strict ``>`` gave 0.01-0.02 for clearly worse models)."""
    rng = np.random.default_rng(1)
    t = 400
    worse = rng.normal(-0.3, 1.0, (t, 2))                       # loss_baseline - loss_model < 0: both models worse
    r = st.spa_test(worse, n_boot=1000, seed=3)
    assert r["t_stat"] == 0.0
    assert r["p_consistent"] == pytest.approx(1.0) and r["p_lower"] == pytest.approx(1.0) and r["p_upper"] == pytest.approx(1.0)


# --------------------------------------------------------------------------- costs + trade simulation


def test_costs_read_from_config_not_hardcoded(tmp_path: Path):
    real = json.loads((Path(__file__).resolve().parents[3] / "config" / "strategies.json").read_text(encoding="utf-8"))["costs"]
    c = tr.load_costs()
    assert (c.maker_bps, c.taker_bps, c.slippage_bps) == (real["maker_bps"], real["taker_bps"], real["slippage_bps"])
    assert dict(c.exit_order_type) == real["exit_order_type"]
    cfg = tmp_path / "s.json"
    cfg.write_text(json.dumps({"costs": {"maker_bps": 1.0, "taker_bps": 7.0, "slippage_bps": 3.0,
                                         "exit_order_type": {"tp": "taker", "sl": "taker", "timeout": "maker"}}}))
    c2 = tr.load_costs(cfg)
    assert (c2.maker_bps, c2.taker_bps, c2.slippage_bps) == (1.0, 7.0, 3.0) and c2.exit_order_type["tp"] == "taker"
    cfg.write_text(json.dumps({"costs": {"maker_bps": 1, "taker_bps": 7, "slippage_bps": 3,
                                         "exit_order_type": {"tp": "limit", "sl": "taker", "timeout": "maker"}}}))
    with pytest.raises(ValueError):
        tr.load_costs(cfg)


def grid(rows: list[tuple[float, float, float, float]], t0: datetime = datetime(2022, 1, 1)) -> we.MinuteData:
    a = np.array(rows, float)
    m0 = we._epoch_min(t0)
    return we.MinuteData(m0=m0, open=a[:, 0], high=a[:, 1], low=a[:, 2], close=a[:, 3], volume=np.ones(len(a)))


def _sim(rows, h, p_star=110.0, stop=95.0, costs=COSTS):
    return tr.simulate_event(grid(rows), we._epoch_min(datetime(2022, 1, 1)), h, p_star, stop, costs)


def test_trade_sim_target_stop_timeout_and_same_bar():
    flat = (100.0, 101.0, 99.0, 100.0)
    slip, fee_t, fee_m = 1.5e-4, 5e-4, 2e-4
    # target: second bar's high pierces P* -> exit at P* (maker, no slippage)
    r = _sim([flat, (100, 111, 99, 105), flat, flat], 4)
    assert r["exit_type"] == "tp" and r["exit"] == 110.0 and r["bars_held"] == 2 and not r["same_bar"]
    entry_eff = 100 * (1 + slip)
    assert r["net"] == pytest.approx(110 / entry_eff - 1 - (fee_t + fee_m))
    # stop: price drops below low[t0]
    r = _sim([flat, (100, 101, 94, 96), flat, flat], 4)
    assert r["exit_type"] == "sl" and r["exit"] == 95.0
    assert r["net"] == pytest.approx(95 * (1 - slip) / entry_eff - 1 - 2 * fee_t)
    # time-out: neither level; exit at the close of the last window bar (taker + slippage)
    r = _sim([flat, flat, flat, (100, 101, 99, 102)], 4)
    assert r["exit_type"] == "timeout" and r["bars_held"] == 4 and r["exit"] == 102.0
    assert r["net"] == pytest.approx(102 * (1 - slip) / entry_eff - 1 - 2 * fee_t)
    # same bar touches target AND stop -> STOP (conservative)
    r = _sim([flat, (100, 112, 90, 100), flat, flat], 4)
    assert r["exit_type"] == "sl" and r["same_bar"] is True
    # a later target after an earlier stop bar: first touch decides
    r = _sim([flat, (100, 101, 94, 96), (96, 120, 95, 110), flat], 4)
    assert r["exit_type"] == "sl"
    # target touched first, stop later -> target
    r = _sim([flat, (100, 111, 99, 105), (105, 106, 90, 91), flat], 4)
    assert r["exit_type"] == "tp"
    # gap through the stop fills at the open
    r = _sim([flat, (92, 93, 90, 91), flat, flat], 4)
    assert r["exit_type"] == "sl" and r["exit"] == 92.0 and r["gap"]
    # entry bar open = first bar after t_c; window past the data / NaN bar -> invalid
    assert _sim([flat, flat], 4)["exit_type"] == "invalid"
    assert _sim([flat, (np.nan, np.nan, np.nan, np.nan), flat, flat], 4)["exit_type"] == "invalid"


def test_trade_sim_costs_change_with_config():
    rows = [(100, 101, 99, 100), (100, 111, 99, 105), (100, 101, 99, 100), (100, 101, 99, 100)]
    cheap = tr.Costs(0.0, 0.0, 0.0, {"tp": "maker", "sl": "taker", "timeout": "taker"})
    assert _sim(rows, 4, costs=cheap)["net"] == pytest.approx(0.10)
    assert _sim(rows, 4)["net"] < _sim(rows, 4, costs=cheap)["net"]


def test_simulate_and_summarise_table():
    fr = prepared(n_per_asset=12, seed=1)
    bars = {}
    base_min = we._epoch_min(START) - 1000
    n = int((END - START).total_seconds() / 60) + 3000
    rng = np.random.default_rng(0)
    close = 100 + np.cumsum(rng.normal(0, 0.02, n))
    for a in ASSETS:
        bars[a] = we.MinuteData(m0=base_min, open=close, high=close + 0.3, low=close - 0.3, close=close, volume=np.ones(n))
    fr = fr.with_columns(pl.col("close_tc").alias("low_t0") * 0.999, (pl.col("close_tc") * 1.002).alias("p_star"))
    tt = tr.simulate(fr, bars, COSTS)
    assert len(tt) == fr.height and set(tt["exit_type"]) <= {"tp", "sl", "timeout"}
    s = tr.summarise(tt, fr["t0"].to_numpy()[tt["row_id"].to_numpy()], n_boot=50)
    assert s["n"] == len(tt) and s["hit_rate"] + s["stop_rate"] + s["timeout_rate"] == pytest.approx(1)
    assert s["ci_lo_bps"] <= s["mean_net_bps"] <= s["ci_hi_bps"]
    assert tr.summarise(tt.iloc[0:0])["n"] == 0


# --------------------------------------------------------------------------- holdout guard, registry, adapters


def test_holdout_guard():
    fr = make_frame(n_per_asset=3)
    sb.guard_no_holdout(fr)
    with pytest.raises(ValueError):
        sb.guard_no_holdout(fr.with_columns(pl.lit("holdout2").alias("split")))
    late = fr.with_columns((pl.col("t0") + timedelta(days=4000)).alias("t0"), (pl.col("t_c") + timedelta(days=4000)).alias("t_c"),
                           (pl.col("label_end") + timedelta(days=4000)).alias("label_end"))
    with pytest.raises(ValueError):
        sb.prepare_frame(late, BOUNDS)
    reach = fr.with_columns(pl.lit(datetime(2026, 8, 1, 6)).alias("label_end"))
    with pytest.raises(ValueError):
        sb.guard_no_holdout(reach)
    # a non-train split never enters the usable frame (OOS rows are dropped)
    mixed = pl.concat([fr, fr.with_columns(pl.lit("oos").alias("split"))])
    assert sb.prepare_frame(mixed, BOUNDS).height <= fr.height


def test_registry_rules():
    reg = sb.new_registry()
    mk = lambda **kw: sb.ModelSpec(factory=sb.ScalarLogit, family="P1", **kw)  # noqa: E731
    reg.register(mk(name="m1", hypothesis="H1"))
    with pytest.raises(ValueError):
        reg.register(mk(name="m1b", hypothesis="H1"))                          # one model per hypothesis
    with pytest.raises(ValueError):
        reg.register(mk(name="m2", hypothesis="H2", round=2))                  # primaries: round-1 fixed config only
    with pytest.raises(ValueError):
        reg.register(mk(name="m1"))                                            # duplicate name
    with pytest.raises(ValueError):
        reg.register(mk(name="m3", round=3))
    reg.register(mk(name="m4", round=2))                                       # round-2 exploratory is fine (SPA only)
    assert [s.name for s in reg.specs(primary=True)] == ["m1"]
    assert reg.baseline.name == sb.BASELINE_NAME and len(reg) == 3
    assert reg.get("m1").config_hash == mk(name="m1", hypothesis="H1").config_hash


def test_adapter_and_scout_contract():
    ev = pl.DataFrame({
        "cell_id": ["W4h_H8h"] * 3, "t0": [START] * 3, "t_c": [START] * 3, "event_start": [START] * 3, "label_end": [START] * 3,
        "split": ["train"] * 3, "h_min": [480] * 3, "label_class": ["impulse", "false_start", "sideways"],
        "mins_to_target": [30, None, None], "mins_to_t0_break": [None, 12, None], "p_star": [1.0] * 3, "low_t0": [1.0] * 3,
        "close_tc": [1.0] * 3, "sigma_w": [1.0] * 3, **{c: [0.0] * 3 for c in sb.S0C_COLS},
    }, schema_overrides={"mins_to_target": pl.Int32, "mins_to_t0_break": pl.Int32})
    f = sb.from_wave_events_v2(ev, "solusdt")
    assert f["asset_id"].to_list() == ["solusdt"] * 3 and f["tau_min"].to_list() == [30.0, 12.0, 480.0]
    assert sb.from_scout_events(f).height == 3
    with pytest.raises(KeyError):
        sb.from_scout_events(f.drop("asset_id"))
    with pytest.raises(LookupError):                                          # t930 table not delivered yet -> clear error
        import utils

        if not Path(utils.lab_db_path(None)).exists():
            raise LookupError("no lab db")
        sb.load_scout_events("scout_events_v1_does_not_exist")


# --------------------------------------------------------------------------- t932: primary fold-rank top-q, gates, persist


def test_fold_rank_select_is_per_fold_and_label_free():
    groups = np.repeat([0, 1, 2], [10, 20, 7])
    rng = np.random.default_rng(5)
    s = rng.normal(size=groups.size)
    s[groups == 1] += 100                                     # a global threshold would pick only fold 1
    sel = mt.fold_rank_select(s, groups, 0.2)
    assert [int(sel[groups == g].sum()) for g in (0, 1, 2)] == [2, 4, 2]          # ceil(0.2 * n_g) per fold
    for g in (0, 1, 2):                                       # the selected ones are the fold's highest scores
        m = groups == g
        assert s[m][sel[m]].min() >= s[m][~sel[m]].max()
    ties = mt.fold_rank_select(np.zeros(10), np.zeros(10, int), 0.3)
    assert ties.tolist() == [True] * 3 + [False] * 7          # deterministic tie-break: earliest rows
    y = np.zeros(groups.size)
    y[np.flatnonzero(sel)[:3]] = 1
    t = mt.top_q_mask(y, sel)
    assert t["n"] == 8 and t["precision"] == pytest.approx(3 / 8) and t["lift"] == pytest.approx((3 / 8) / (3 / groups.size))


def test_leaderboard_primary_and_secondary_topq_columns():
    fr = make_frame(n_per_asset=120)
    res = sb.run_benchmark(fr, _registry(), n_perm=0, n_boot=40, spa_boot=100, bounds=BOUNDS, costs=COSTS)
    lb = res.leaderboard.set_index("model")
    blocks = res.frame["cv_block"].to_numpy()
    y = (res.frame["label_class"] == "impulse").to_numpy().astype(float)
    for name, o in res.outputs.items():
        sel = mt.fold_rank_select(o.score, blocks, 0.2)
        assert lb.loc[name, "n_top20"] == sum(int(np.ceil(0.2 * (blocks == b).sum() - 1e-9)) for b in set(blocks.tolist()))
        assert lb.loc[name, "prec_top20"] == pytest.approx(y[sel].mean())
        assert lb.loc[name, "n_top20_trthr"] == int((o.score >= o.thr20).sum())         # secondary view kept
        assert lb.loc[name, "prec_top20_trthr"] == pytest.approx(y[o.score >= o.thr20].mean())
        assert lb.loc[name, "topq_view"] == "fold_rank"
        tq = res.tables[f"topq_blocks:{name}"]
        assert len(tq) == len(set(blocks.tolist())) and tq["n_top20"].sum() == lb.loc[name, "n_top20"]
    assert lb.loc["S0C_logit", "go_status"] == "baseline"
    assert lb.loc["toy_h1", "go_status"] in ("NO-GO", "GO-jelölt")
    assert lb.loc["toy_h1", "gate_min_events"] == False      # noqa: E712  (toy frame is far below 1500 events)
    assert lb.loc["toy_noisy", "gate_holm"] is None           # exploratory: Holm not applicable


def test_go_no_go_uses_primary_view():
    good = {"primary": True, "delta_auc": 0.03, "ci_lo": 0.005, "holm_p": 0.003, "delta_logloss3": -0.01,
            "prec_top20": 0.30, "base_prec_top20": 0.25, "prec_top20_trthr": 0.10, "blocks_prec_better": 5,
            "lift_top20": 2.1, "blocks_pos": 5, "dauc_wo_best_block_mean": 0.02, "assets_nonneg": 2, "n_assets": 3,
            "asset_min_dauc": -0.01, "spa_p": 0.01, "spa_best": True, "trade20_mean_net_bps": 10.0,
            "trade20_ci_lo_bps": 1.0, "trade20_hit_rate": 0.4, "trade20_break_even_hit": 0.3}
    g = sb.go_no_go(good, min_events=True)
    assert g["go_status"] == "GO-jelölt" and g["go_failed"] == ""        # the poor secondary view does not matter
    g = sb.go_no_go(good | {"prec_top20": 0.2}, min_events=True)
    assert g["go_status"] == "NO-GO" and "top20" in g["go_failed"]
    g = sb.go_no_go(good | {"primary": False, "holm_p": None}, min_events=True)
    assert g["go_status"] == "kísérleti jelölt" and g["gate_holm"] is None
    assert sb.go_no_go(good, min_events=False)["go_status"] == "NO-GO"


def test_go_no_go_spa_gate_only_for_selected_best():
    """t936: the SPA p belongs to the selected best model of the universe; any other model is not applicable and
    can never be a candidate, even with a small universe p."""
    row = {"primary": True, "delta_auc": 0.03, "ci_lo": 0.005, "holm_p": 0.003, "delta_logloss3": -0.01,
           "prec_top20": 0.30, "base_prec_top20": 0.25, "blocks_prec_better": 5, "lift_top20": 2.1, "blocks_pos": 5,
           "dauc_wo_best_block_mean": 0.02, "assets_nonneg": 3, "n_assets": 3, "asset_min_dauc": 0.0, "spa_p": 0.01,
           "trade20_mean_net_bps": 10.0, "trade20_ci_lo_bps": 1.0, "trade20_hit_rate": 0.4,
           "trade20_break_even_hit": 0.3}
    g = sb.go_no_go(row | {"spa_best": False}, min_events=True)
    assert g["gate_spa"] is None and g["go_status"] == "NO-GO" and "not_spa_best" in g["go_failed"]
    assert "spa" not in g["go_failed"].split(",")
    g = sb.go_no_go(row | {"spa_best": True, "spa_p": 0.2}, min_events=True)
    assert g["gate_spa"] is False and "spa" in g["go_failed"].split(",")


def test_go_no_go_econ_needs_top10_too():
    row = {"primary": False, "delta_auc": 0.03, "ci_lo": 0.005, "delta_logloss3": -0.01, "prec_top20": 0.30,
           "base_prec_top20": 0.25, "blocks_prec_better": 5, "lift_top20": 2.1, "blocks_pos": 5,
           "dauc_wo_best_block_mean": 0.02, "assets_nonneg": 3, "n_assets": 3, "asset_min_dauc": 0.0, "spa_p": 0.01,
           "spa_best": True, "trade20_mean_net_bps": 10.0, "trade20_ci_lo_bps": 1.0, "trade20_hit_rate": 0.4,
           "trade20_break_even_hit": 0.3}
    ok10 = {"trade10_mean_net_bps": 12.0, "trade10_ci_lo_bps": 0.5, "trade10_hit_rate": 0.45, "trade10_break_even_hit": 0.3}
    assert sb.go_no_go(row | ok10, min_events=True)["go_status"] == "kísérleti jelölt"
    g = sb.go_no_go(row | ok10 | {"trade10_ci_lo_bps": -3.0}, min_events=True)
    assert g["gate_econ"] is False and g["go_status"] == "NO-GO" and "econ" in g["go_failed"]


def test_persist_by_model_does_not_clobber(tmp_path: Path):
    import duckdb

    fr = make_frame(n_per_asset=90)
    kw = {"n_perm": 0, "n_boot": 20, "spa_boot": 50, "bounds": BOUNDS, "costs": COSTS}
    lab = str(tmp_path / "lab.duckdb")
    r1 = sb.run_benchmark(fr, _registry(), **kw)
    sb.persist(r1, lab_path=lab)                                                   # legacy: whole run
    reg2 = sb.new_registry()
    reg2.register(sb.ModelSpec("agent_b_model", lambda: sb.ScalarLogit(c=0.5), "P2", config={"c": 0.5}))
    r2 = sb.run_benchmark(fr, reg2, **kw)
    r2.leaderboard["brand_new_col"] = 1.0                                          # schema evolution
    sb.persist(r2, lab_path=lab, models=["agent_b_model"])
    sb.persist(r2, lab_path=lab, models=["agent_b_model"])                         # re-run replaces its own rows
    with pytest.raises(KeyError):
        sb.persist(r2, lab_path=lab, models=["not_in_run"])
    con = duckdb.connect(lab, read_only=True)
    try:
        rows = con.execute('SELECT model, count(*) FROM research."scout_leaderboard_v1" GROUP BY model ORDER BY model').fetchall()
        newc = con.execute('SELECT count(brand_new_col) FROM research."scout_leaderboard_v1"').fetchone()
    finally:
        con.close()
    assert rows == [("S0C_logit", 1), ("agent_b_model", 1), ("toy_h1", 1), ("toy_noisy", 1)]
    assert newc == (1,)


def test_persist_runs_spa_p_is_double_and_widened(tmp_path: Path):
    import duckdb

    fr = make_frame(n_per_asset=90)
    kw = {"n_perm": 0, "n_boot": 20, "spa_boot": 50, "bounds": BOUNDS, "costs": COSTS}
    lab = str(tmp_path / "lab.duckdb")
    r0 = sb.run_benchmark(fr, sb.new_registry(), **kw)                            # baseline only: spa_p NaN -> NULL
    sb.persist(r0, lab_path=lab)
    con = duckdb.connect(lab)
    con.execute('ALTER TABLE research."scout_leaderboard_v1__runs" ALTER COLUMN spa_p TYPE INTEGER')   # legacy table
    con.close()
    r1 = sb.run_benchmark(fr, _registry(), **kw)
    sb.persist(r1, lab_path=lab, models=["toy_h1"])
    con = duckdb.connect(lab, read_only=True)
    try:
        typ = con.execute("SELECT data_type FROM information_schema.columns WHERE table_name = "
                          "'scout_leaderboard_v1__runs' AND column_name = 'spa_p'").fetchone()
        val = con.execute('SELECT spa_p FROM research."scout_leaderboard_v1__runs" WHERE run_id = ?',
                          [r1.run["run_id"]]).fetchone()
    finally:
        con.close()
    assert typ == ("DOUBLE",) and val is not None and val[0] == pytest.approx(r1.run["spa_p"])
