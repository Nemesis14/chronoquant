# pyright: reportArgumentType=false, reportCallIssue=false, reportOptionalSubscript=false, reportAttributeAccessIssue=false
"""t940: Impulse Scout runtime (``scout.runtime``) + the frozen ``scout_s0c_r1`` artifact.

Synthetic 1m data (small test parameters, no DB): no look-ahead (truncation at ``t_c``), determinism (repeat runs,
extra history before the anchor), resolution (open -> resolved, consistency with the research label and
``rules.simulate_rule_event`` / ``trades.simulate_event``, gap -> invalid). Artifact tests run when the artifact exists;
the live SOL reproduction (t938 event 2026-11-21, P(impulse) 0.096) skips when the live DB is busy / absent.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from reports.builders import wave_events as we
from reports.builders.scout_bench import auc
from reports.builders.scout_bench.rules import simulate_rule_event
from reports.builders.scout_bench.trades import Costs, simulate_event
from scout import runtime as rt

START = datetime(2025, 1, 1)
DAYS = 30
CELL = ("W2h_H2h", 120, 120)
PARAMS = we.WaveParams(cells=(CELL,), version="v2", sigma_window=1440, trend_bars=720, velocity_bars=30,
                       train_start="2024-01-01", oos_start="2024-06-01", holdout2_start="2024-12-01", min_a_sigma=1.0)
COSTS = Costs(2.0, 5.0, 1.5, {"tp": "maker", "sl": "taker", "timeout": "taker"})
FEATS = tuple(we.FEATURE_COLS)
PRED_COLS = ("p_impulse", "p_false_start", "p_sideways", "oof_percentile", "top20", "top10", "scorable",
             "velocity_1h", "pretrend_3d", "log_sigma_1m", "drop_depth", "volume_ratio", "a_sigma", "p_star", "low_t0")


def _synthetic(seed: int = 3, days: int = DAYS, start: datetime = START) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    n = days * 1440
    vol = 0.0008 * (1 + 0.8 * np.sin(np.arange(n) / 2900.0 + seed) ** 2)
    close = 100 * np.exp(np.cumsum(rng.normal(0, vol)))
    open_ = np.concatenate(([close[0]], close[:-1]))
    wick = np.abs(rng.normal(0, 0.0004, n)) * close
    return pl.DataFrame({
        "open_time": [start + timedelta(minutes=i) for i in range(n)],
        "open": open_, "high": np.maximum(open_, close) + wick, "low": np.minimum(open_, close) - wick,
        "close": close, "volume": rng.uniform(5, 15, n),
    })


def _artifact(anchor: datetime = START, live_start: datetime | None = None) -> rt.ScoutArtifact:
    oof = np.linspace(0.02, 0.4, 200)
    return rt.ScoutArtifact(
        model_id="scout_test", root=Path("."), manifest={}, features=FEATS,
        mu=np.array([2.3, 0.6, -1.9, -7.0, 0.01, 1.5]), sd=np.array([1.0, 1.2, 4.0, 0.5, 0.01, 0.8]),
        coef=np.array([-0.8, 0.2, 0.15, -0.08, -0.04, 0.15]), intercept=-1.9, q_false_start=0.6,
        oof_sorted=oof, thr20=float(np.quantile(oof, 0.8)), thr10=float(np.quantile(oof, 0.9)),
        params=PARAMS, cell_id=CELL[0], h_min=CELL[2],
        rules=(("default", rt.DEFAULT_RULE), ("r2_03", rt.R2_03)), costs=COSTS,
        detect_anchor=anchor, live_start=live_start or anchor + timedelta(days=2),
    )


@pytest.fixture(scope="module")
def df() -> pl.DataFrame:
    return _synthetic()


@pytest.fixture(scope="module")
def art() -> rt.ScoutArtifact:
    return _artifact()


@pytest.fixture(scope="module")
def out(df: pl.DataFrame, art: rt.ScoutArtifact) -> pl.DataFrame:
    o = rt.run("testusdt", df, art)
    assert o.height >= 10, "synthetic data produced too few events"
    return o


# --------------------------------------------------------------------------- contract


def test_output_schema_and_ids(out: pl.DataFrame, art: rt.ScoutArtifact) -> None:
    assert out.columns == list(rt.OUTPUT_COLUMNS)
    assert dict(zip(out.columns, out.dtypes, strict=True)) == rt.OUTPUT_SCHEMA
    assert out["event_id"].is_unique().all()
    assert out["event_id"].str.starts_with(f"testusdt|{CELL[0]}|").all()
    assert (out["t0"] >= art.live_start).all() and out["t0"].is_sorted()
    assert (out["t_c"] == out["t0"] + timedelta(minutes=CELL[1])).all()
    assert (out["label_end"] == out["t_c"] + timedelta(minutes=CELL[2])).all()
    assert (out["stop"] == out["low_t0"]).all() and (out["default_tp"] == out["p_star"]).all()
    assert set(out["status"].unique()) <= set(rt.STATUSES)
    s = out.filter(pl.col("scorable"))
    assert s.height and np.allclose(s.select("p_impulse", "p_false_start", "p_sideways").to_numpy().sum(axis=1), 1.0)
    assert (s["top20"] == (s["p_impulse"] >= art.thr20)).all()


def test_predict_proba_matches_sklearn_logit() -> None:
    from reports.builders.scout_bench import ScalarLogit

    rng = np.random.default_rng(0)
    n = 300
    tr = pl.DataFrame({c: rng.normal(k, 1 + k / 3, n) for k, c in enumerate(FEATS)}).with_columns(
        pl.Series("label_class", rng.choice(["impulse", "false_start", "sideways"], n, p=[0.2, 0.5, 0.3])))
    m = ScalarLogit()
    m.fit(tr, None, seed=1)
    rest = tr.filter(pl.col("label_class") != "impulse")
    a = rt.ScoutArtifact(**{**_artifact().__dict__, "mu": m.mu, "sd": m.sd, "coef": m.clf.coef_[0],
                            "intercept": float(m.clf.intercept_[0]),
                            "q_false_start": float((rest["label_class"] == "false_start").mean())})
    np.testing.assert_allclose(a.predict_proba(tr.select(FEATS).to_numpy()), m.predict_proba(tr, None), rtol=0, atol=1e-12)


def test_refuses_since_before_live_start_and_short_history(df: pl.DataFrame, art: rt.ScoutArtifact) -> None:
    with pytest.raises(ValueError, match="live start"):
        rt.run("testusdt", df, art, since=art.live_start - timedelta(minutes=1))
    with pytest.raises(ValueError, match="anchor"):
        rt.run("testusdt", df.filter(pl.col("open_time") >= START + timedelta(days=1)), art)
    with pytest.raises(KeyError):
        rt.run("testusdt", df.drop("volume"), art)


# --------------------------------------------------------------------------- no look-ahead


def test_no_lookahead_truncation_at_t_c(df: pl.DataFrame, art: rt.ScoutArtifact, out: pl.DataFrame) -> None:
    """Data up to t_c (bars opening < t_c) give the same event and prediction; one bar less gives no event."""
    for r in out.filter(pl.col("scorable")).head(6).iter_rows(named=True):
        cut = df.filter(pl.col("open_time") < r["t_c"])
        o = rt.run("testusdt", cut, art)
        row = o.filter(pl.col("event_id") == r["event_id"])
        assert row.height == 1, r["event_id"]
        for c in PRED_COLS:
            assert row[c][0] == r[c], (r["event_id"], c)
        assert row["status"][0] == "open" and row["entry"][0] is None and row["r2_03_tp"][0] is None
        assert row["default_outcome"][0] is None and row["label_class"][0] is None
        early = rt.run("testusdt", df.filter(pl.col("open_time") < r["t_c"] - timedelta(minutes=1)), art)
        assert r["event_id"] not in early["event_id"].to_list()


def test_future_prices_do_not_change_predictions(df: pl.DataFrame, art: rt.ScoutArtifact, out: pl.DataFrame) -> None:
    """Scrambling every price after an event's t_c leaves its prediction columns untouched."""
    r = out.filter(pl.col("scorable")).row(out.height // 3, named=True)
    rng = np.random.default_rng(9)
    after = pl.col("open_time") >= r["t_c"]
    k = float(rng.uniform(0.7, 1.3))
    mod = df.with_columns(*(pl.when(after).then(pl.col(c) * k).otherwise(pl.col(c)).alias(c)
                            for c in ("open", "high", "low", "close")))
    row = rt.run("testusdt", mod, art).filter(pl.col("event_id") == r["event_id"])
    for c in rt.IMMUTABLE_COLUMNS:
        if c not in ("label_end",):
            assert row[c][0] == r[c], c


# --------------------------------------------------------------------------- determinism


def test_determinism_repeat_and_extra_history(df: pl.DataFrame, art: rt.ScoutArtifact, out: pl.DataFrame) -> None:
    assert rt.run("testusdt", df, art).equals(out)
    older = _synthetic(seed=99, days=5, start=START - timedelta(days=5))           # history before the anchor
    assert rt.run("testusdt", pl.concat([older, df]), art).equals(out)
    shuffled = df.sample(fraction=1.0, shuffle=True, seed=1)
    assert rt.run("testusdt", shuffled, art).equals(out)


def test_incremental_runs_agree(df: pl.DataFrame, art: rt.ScoutArtifact, out: pl.DataFrame) -> None:
    """A run at an earlier 'now' yields a prefix of the final events with identical immutable columns."""
    now = START + timedelta(days=DAYS // 2)
    part = rt.run("testusdt", df.filter(pl.col("open_time") < now), art)
    assert part.height
    full = out.filter(pl.col("event_id").is_in(part["event_id"].implode()))
    assert full.height == part.height
    assert part.select(rt.IMMUTABLE_COLUMNS).equals(full.select(rt.IMMUTABLE_COLUMNS))
    done = part.filter(pl.col("status") != "open")
    assert done.equals(full.filter(pl.col("event_id").is_in(done["event_id"].implode())).with_columns(
        pl.lit(part["as_of"][0]).alias("as_of")))


# --------------------------------------------------------------------------- resolution


def test_resolution_matches_label_and_simulators(df: pl.DataFrame, art: rt.ScoutArtifact, out: pl.DataFrame) -> None:
    m = we.to_minute_grid(df)
    res = out.filter(pl.col("status") == "resolved")
    assert res.height >= 5
    to_label = {"tp": "impulse", "sl": "false_start", "timeout": "sideways"}
    for r in res.iter_rows(named=True):
        assert to_label[r["default_outcome"]] == r["label_class"]
        t_c_min = we._epoch_min(r["t_c"])
        ref = simulate_event(m, t_c_min, CELL[2], r["p_star"], r["low_t0"], COSTS)
        assert ref["exit_type"] == r["default_outcome"]
        assert r["default_net_bps"] == pytest.approx(ref["net"] * 1e4, abs=1e-9)
        ev = {**r, "atr14_15m": float("nan")}
        r2 = simulate_rule_event(m, t_c_min, CELL[2], ev, rt.R2_03, COSTS)
        assert r2["exit_type"] == r["r2_03_outcome"] and r["r2_03_net_bps"] == pytest.approx(r2["net"] * 1e4)
        assert r["entry"] == m.open[t_c_min - m.m0]
        assert r["t_c"] < r["default_outcome_at"] <= r["label_end"]
        assert r["r2_03_tp"] == pytest.approx(r["entry"] * np.exp(3 * r["sigma_w"]))
    last = out.row(out.height - 1, named=True)
    as_of = last["as_of"]
    for r in out.iter_rows(named=True):
        assert (r["status"] == "open") == (r["label_end"] > as_of)


def test_open_event_resolves_when_window_closes(df: pl.DataFrame, art: rt.ScoutArtifact, out: pl.DataFrame) -> None:
    r = out.filter(pl.col("status") == "resolved").row(0, named=True)
    before = rt.run("testusdt", df.filter(pl.col("open_time") < r["label_end"] - timedelta(minutes=1)), art)
    b = before.filter(pl.col("event_id") == r["event_id"]).row(0, named=True)
    assert b["status"] == "open" and b["default_outcome"] is None and b["entry"] == r["entry"]
    at = rt.run("testusdt", df.filter(pl.col("open_time") < r["label_end"]), art)
    a = at.filter(pl.col("event_id") == r["event_id"]).row(0, named=True)
    assert a["status"] == "resolved"
    assert {k: a[k] for k in a if k != "as_of"} == {k: r[k] for k in r if k != "as_of"}


def test_gap_in_window_is_invalid_like_rules(df: pl.DataFrame, art: rt.ScoutArtifact, out: pl.DataFrame) -> None:
    r = out.filter(pl.col("status") == "resolved").row(1, named=True)
    hole = r["t_c"] + timedelta(minutes=CELL[2] - 5)
    o = rt.run("testusdt", df.filter(pl.col("open_time") != hole), art).filter(pl.col("event_id") == r["event_id"])
    row = o.row(0, named=True)
    assert row["status"] == "invalid" and row["label_class"] is None
    assert row["default_outcome"] == "invalid" and row["r2_03_outcome"] == "invalid" and row["default_net_bps"] is None
    assert row["p_impulse"] == r["p_impulse"]                                   # prediction untouched


def test_prefix_gap_suppresses_event_and_nan_feature_not_scored(df: pl.DataFrame, art: rt.ScoutArtifact,
                                                               out: pl.DataFrame) -> None:
    r = out.filter(pl.col("scorable")).row(2, named=True)
    hole = r["t0"] - timedelta(minutes=30)                                       # inside [t0 - B, t0)
    o = rt.run("testusdt", df.filter(pl.col("open_time") != hole), art)
    assert r["event_id"] not in o["event_id"].to_list()                          # candidate needs a complete prefix
    hole = r["t0"] - timedelta(minutes=PARAMS.trend_bars)                        # close[t0 - 3d] -> pretrend NaN
    row = rt.run("testusdt", df.filter(pl.col("open_time") != hole), art).filter(pl.col("event_id") == r["event_id"])
    assert row.height == 1 and row["pretrend_3d"][0] is None
    assert not row["scorable"][0] and row["p_impulse"][0] is None and row["top20"][0] is None
    assert row["oof_percentile"][0] is None and row["default_tp"][0] == r["default_tp"]


# --------------------------------------------------------------------------- frozen artifact

needs_artifact = pytest.mark.skipif(not (rt.ARTIFACT_DIR / rt.MANIFEST).exists(), reason="artifact not frozen yet")


@needs_artifact
def test_artifact_manifest_and_oof_auc() -> None:
    a = rt.load_artifact()
    man = a.manifest
    assert a.model_id == "scout_s0c_r1" and a.cell_id == "W4h_H8h" and a.h_min == 480
    assert man["train"]["oof_auc"] == pytest.approx(0.636, abs=5e-4)
    oof = pl.read_parquet(a.root / "train_oof.parquet")
    assert oof.height == man["train"]["n_events"] == 2170
    assert auc((oof["label_class"] == "impulse").to_numpy().astype(float), oof["oof_score"].to_numpy()) == pytest.approx(
        man["train"]["oof_auc"], abs=1e-12)
    assert set(man["train"]["assets"]) == {"solusdt", "btcusdt", "ethusdt"}
    assert datetime.fromisoformat(man["train"]["label_end_max"]) <= datetime.fromisoformat(man["runtime"]["holdout2_start"])
    assert a.thr20 == pytest.approx(np.quantile(oof["oof_score"].to_numpy(), 0.8))
    assert [k for k, _ in a.rules] == list(rt.RULE_KEYS)
    assert a.rules[1][1] == rt.R2_03


@needs_artifact
def test_artifact_pickle_equals_manifest_coefficients() -> None:
    import pickle

    a = rt.load_artifact()
    with open(a.root / "model.pkl", "rb") as fh:
        model = pickle.load(fh)
    rng = np.random.default_rng(4)
    x = a.mu + rng.normal(0, 1, (200, len(a.features))) * a.sd
    frame = pl.DataFrame({c: x[:, k] for k, c in enumerate(a.features)})
    np.testing.assert_allclose(a.predict_proba(x), model.predict_proba(frame, None), rtol=0, atol=1e-12)


@needs_artifact
def test_artifact_tamper_detected(tmp_path: Path) -> None:
    dst = tmp_path / "art"
    shutil.copytree(rt.ARTIFACT_DIR, dst)
    man = json.loads((dst / rt.MANIFEST).read_text(encoding="utf-8"))
    man["thresholds"]["top20"] = 0.0
    (dst / rt.MANIFEST).write_text(json.dumps(man), encoding="utf-8")
    with pytest.raises(ValueError, match="freeze_hash"):
        rt.load_artifact(dst)


@needs_artifact
@pytest.mark.integration
def test_live_sol_event_2026_11_21_reproduces_t938() -> None:
    """t938: SOL event t0 2026-11-21 -> P(impulse) 0.096 (needs the SOL live DB; skipped when busy / absent)."""
    from reports.builders.scout_live_score import load_window

    a = rt.load_artifact()
    try:
        df = load_window("solusdt", a.detect_anchor, tries=3, delay=2.0)
    except Exception as exc:                                   # live DB locked by the worker / absent
        pytest.skip(f"SOL live DB unavailable: {exc}")
    out = rt.run("solusdt", df, a, since=datetime(2026, 11, 21))
    ev = out.filter(pl.col("t0").dt.date() == datetime(2026, 11, 21).date())
    if ev.height == 0:
        pytest.skip("live data does not reach the 2026-11-21 event")
    assert ev["p_impulse"][0] == pytest.approx(0.096, abs=5e-4)
