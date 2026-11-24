# pyright: reportArgumentType=false, reportCallIssue=false, reportOptionalSubscript=false, reportAttributeAccessIssue=false
"""``reports.builders.scout_events`` (+ report tables) on synthetic multi-asset 1m data (no real database for the core tests).

Covers the t930 checklist: per-asset look-ahead (truncation), order-flow features use only bars closed <= t_c,
holdout-2 labels absent (and holdout-2 prices cannot change any non-masked row), regression against the single-asset
``wave_events`` v2 logic (synthetic always; real SOL lab table when present), CV blocks shared across assets, tau,
persistence round-trip and the pure report tables.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import pytest

from reports.builders import scout_events as se
from reports.builders import scout_events_report as sr
from reports.builders import wave_events as we

START = datetime(2025, 1, 1)
DAYS = 70
PARAMS = we.WaveParams(
    cells=(("W2h_H2h", 120, 120),), version="v2",
    sigma_window=1440, trend_bars=720, velocity_bars=30,
    train_start=str((START + timedelta(days=3)).date()), oos_start=str((START + timedelta(days=40)).date()),
    holdout2_start=str((START + timedelta(days=55)).date()), min_a_sigma=1.0,
)
ASSETS = ("aaausdt", "bbbusdt")
FLOW_TOL = dict(rtol=1e-9, atol=1e-12, equal_nan=True)


def _synthetic(seed: int, days: int = DAYS) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    n = days * 1440
    vol = 0.0008 * (1 + 0.8 * np.sin(np.arange(n) / 2900.0 + seed) ** 2)
    close = 100 * np.exp(np.cumsum(rng.normal(0, vol)))
    open_ = np.concatenate(([close[0]], close[:-1]))
    wick = np.abs(rng.normal(0, 0.0004, n)) * close
    volume = rng.uniform(5, 15, n)
    return pl.DataFrame({
        "open_time": [START + timedelta(minutes=i) for i in range(n)],
        "open": open_, "high": np.maximum(open_, close) + wick, "low": np.minimum(open_, close) - wick,
        "close": close, "volume": volume, "quote_volume": volume * close,
        "trades": rng.integers(50, 400, n).astype(float), "taker_buy_base": volume * rng.uniform(0.3, 0.7, n),
        "taker_buy_quote": volume * close * rng.uniform(0.3, 0.7, n),
    })


@pytest.fixture(scope="module")
def dfs() -> dict[str, pl.DataFrame]:
    return {a: _synthetic(7 + k) for k, a in enumerate(ASSETS)}


@pytest.fixture(scope="module")
def built(dfs: dict[str, pl.DataFrame]) -> dict[str, tuple[pl.DataFrame, pl.DataFrame, we.WaveParams]]:
    return {a: se.build_asset(d, a, params=PARAMS) for a, d in dfs.items()}


@pytest.fixture(scope="module")
def flow(dfs: dict[str, pl.DataFrame], built: dict) -> dict[str, pl.DataFrame]:
    return {a: se.build_flow(dfs[a], built[a][0], PARAMS, a) for a in dfs}


def _labelled(ev: pl.DataFrame, k: int = 5) -> list[dict]:
    e = ev.filter((pl.col("split") == "train") & pl.col("label_class").is_not_null())
    assert e.height >= k, "synthetic data produced too few events"
    idx = np.linspace(e.height // 4, e.height - 2, k).astype(int)
    return [e.row(int(i), named=True) for i in idx]


def test_events_have_all_classes_per_asset(built: dict) -> None:
    for a, (ev, fn, _) in built.items():
        assert (ev["asset_id"] == a).all()
        assert set(ev["label_class"].drop_nulls().unique()) == set(we.CLASS_ORDER_V2), a
        assert fn["asset_id"][0] == a


@pytest.mark.parametrize("asset", ASSETS)
def test_per_asset_lookahead_truncation(asset: str, dfs: dict, built: dict, flow: dict) -> None:
    """Data cut right after t_c reproduces the event, its S0 / C features AND its order-flow features."""
    ev, _, _ = built[asset]
    cols = (*we.FEATURE_COLS, *we.EVENT_COLS, "t_c", "event_start", "event_id")
    for row in _labelled(ev):
        trunc = dfs[asset].filter(pl.col("open_time") < row["t_c"])
        ev2, _, _ = se.build_asset(trunc, asset, params=PARAMS)
        hit = ev2.filter(pl.col("t0") == row["t0"])
        assert hit.height == 1
        r2 = hit.row(0, named=True)
        for c in cols:
            if isinstance(row[c], float):
                assert np.isclose(row[c], r2[c], **FLOW_TOL), c
            else:
                assert row[c] == r2[c], c
        assert r2["label_class"] is None and r2["tau_min"] is None
        f_full = flow[asset].filter(pl.col("t0") == row["t0"])
        f_cut = se.build_flow(trunc, hit, PARAMS, asset)
        assert f_cut.height == 1 and f_full.height == 1
        for c in se.FLOW_COLS:
            a_, b_ = f_full[c][0], f_cut[c][0]
            assert (a_ is None and b_ is None) or np.isclose(a_, b_, **FLOW_TOL), c


@pytest.mark.parametrize("asset", ASSETS)
def test_flow_uses_only_bars_up_to_tc(asset: str, dfs: dict, built: dict, flow: dict) -> None:
    """Scrambling every bar (price and flow) from t_c on changes no flow feature of that event."""
    ev, _, _ = built[asset]
    for row in _labelled(ev, 3):
        mod = dfs[asset].with_columns(
            pl.when(pl.col("open_time") >= row["t_c"]).then(pl.col(c) * 5).otherwise(pl.col(c)).alias(c)
            for c in ("open", "high", "low", "close", "volume", "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"))
        f2 = se.build_flow(mod, ev.filter(pl.col("t0") == row["t0"]), PARAMS, asset)
        f1 = flow[asset].filter(pl.col("t0") == row["t0"])
        for c in se.FLOW_COLS:
            a_, b_ = f1[c][0], f2[c][0]
            assert (a_ is None and b_ is None) or np.isclose(a_, b_, **FLOW_TOL), c


def test_flow_values_hand_computed(dfs: dict, built: dict, flow: dict) -> None:
    asset = ASSETS[0]
    ev = built[asset][0]
    row = _labelled(ev, 1)[0]
    d = dfs[asset]
    t0, tc = row["t0"], row["t_c"]
    decl = d.filter((pl.col("open_time") >= row["event_start"]) & (pl.col("open_time") <= t0))
    reb = d.filter((pl.col("open_time") > t0) & (pl.col("open_time") < tc))
    pre = d.filter((pl.col("open_time") >= row["event_start"]) & (pl.col("open_time") < tc))
    f = flow[asset].filter(pl.col("t0") == t0).row(0, named=True)
    assert np.isclose(f["tb_ratio_decline"], decl["taker_buy_base"].sum() / decl["volume"].sum())
    assert np.isclose(f["tb_ratio_rebound"], reb["taker_buy_base"].sum() / reb["volume"].sum())
    assert np.isclose(f["tb_ratio_prefix"], pre["taker_buy_base"].sum() / pre["volume"].sum())
    assert np.isclose(f["tb_ratio_diff"], f["tb_ratio_rebound"] - f["tb_ratio_decline"])
    past = d.filter((pl.col("open_time") >= t0 - timedelta(minutes=PARAMS.sigma_window)) & (pl.col("open_time") < t0))
    assert np.isclose(f["trades_rel_prefix"], pre["trades"].mean() / past["trades"].mean())
    assert pre.height == 2 * 120


@pytest.mark.parametrize("asset", ASSETS)
def test_holdout2_labels_absent(asset: str, built: dict) -> None:
    ev, _, p = built[asset]
    ho0 = datetime.fromisoformat(p.holdout2_start)
    masked = ev.filter(pl.col("label_end") > ho0)
    assert masked.height > 0 and masked["label_masked"].all()
    assert (ev["t0"] >= ho0).sum() > 0
    for c in se.MASKED_COLS:
        if c in se.MASKED_KEEP:
            continue
        assert masked[c].null_count() == masked.height, c
    h2 = ev.filter(pl.col("split") == "holdout2")
    assert h2.height > 0 and h2["is_holdout2"].all() and h2["label_masked"].all()
    # every labelled row is fully before holdout-2
    assert (ev.filter(pl.col("label_class").is_not_null())["label_end"] <= ho0).all()


def test_holdout2_prices_cannot_change_labelled_rows(dfs: dict, built: dict) -> None:
    """Rewriting all bars from holdout-2 on changes no row whose label window ends before it (labels incl.)."""
    asset = ASSETS[0]
    ev, _, p = built[asset]
    ho0 = datetime.fromisoformat(p.holdout2_start)
    mod = dfs[asset].with_columns(
        pl.when(pl.col("open_time") >= ho0).then(pl.col(c) * 3).otherwise(pl.col(c)).alias(c)
        for c in ("open", "high", "low", "close"))
    ev2, _, _ = se.build_asset(mod, asset, params=PARAMS)
    a = ev.filter(pl.col("label_end") <= ho0).sort("t0")
    b = ev2.filter(pl.col("label_end") <= ho0).sort("t0")
    cols = [c for c in a.columns if c not in ("event_id",)]
    assert a.height == b.height
    for c in cols:
        x, y = a[c], b[c]
        if x.dtype == pl.Float64:
            assert np.allclose(x.to_numpy(), y.to_numpy(), equal_nan=True, rtol=1e-9), c
        else:
            assert x.to_list() == y.to_list(), c


@pytest.mark.parametrize("asset", ASSETS)
def test_matches_wave_events_v2_logic(asset: str, dfs: dict, built: dict) -> None:
    """Synthetic regression: scout rows == wave_events v2 rows for every column of the old table (non-masked rows)."""
    ev, _, _ = built[asset]
    ref = we.build_events(dfs[asset], PARAMS).events
    ho0 = datetime.fromisoformat(PARAMS.holdout2_start)
    assert ev.height == ref.height
    a, b = ev.sort("t0"), ref.sort("t0")
    ok = (a["label_end"] <= ho0).to_numpy()
    for c in ref.columns:
        x, y = a[c].filter(pl.Series(ok)), b[c].filter(pl.Series(ok))
        if x.dtype == pl.Float64:
            assert np.allclose(x.to_numpy(), y.to_numpy(), equal_nan=True, rtol=1e-12), c
        else:
            assert x.to_list() == y.to_list(), c
    # the unmasked columns (identification, features) match for ALL rows
    for c in ("t0", "t_c", "split", "cv_block", "cv_straddle", *we.FEATURE_COLS):
        x, y = a[c], b[c]
        if x.dtype == pl.Float64:
            assert np.allclose(x.to_numpy(), y.to_numpy(), equal_nan=True), c
        else:
            assert x.to_list() == y.to_list(), c


def test_default_variant_is_5200_v2_params() -> None:
    p, q = se.scout_params("a15"), we.params_v2()
    assert p.to_json() == q.to_json()
    assert se.scout_params("a10").min_a_sigma == 1.0
    assert se.table_name("a15") == "scout_events_v1" and se.table_name("a10") == "scout_events_v1_a10"


@pytest.mark.skipif(not Path("database/solusdt/solusdt_lab.duckdb").exists(), reason="no SOL lab DB")
def test_sol_subset_matches_wave_events_v2_real_table() -> None:
    """SOL rows of research.scout_events_v1 (W4h_H8h, train + OOS) == research.wave_events_v2 (needs t930 table)."""
    import utils

    try:
        con = duckdb.connect(utils.lab_db_path("solusdt"), read_only=True)
    except duckdb.Error:
        pytest.skip("lab db locked")
    try:
        names = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='research'").fetchall()}
        if not {"scout_events_v1", "wave_events_v2"} <= names:
            pytest.skip("tables missing")
        new = con.execute("SELECT * FROM research.scout_events_v1 WHERE asset_id='solusdt' AND cell_id='W4h_H8h' "
                          "AND split IN ('train','oos') ORDER BY t0").pl()
        old = con.execute("SELECT * FROM research.wave_events_v2 WHERE cell_id='W4h_H8h' AND split IN ('train','oos') "
                          "ORDER BY t0").pl()
    finally:
        con.close()
    assert new.height == old.height > 600
    for c in ("t0", "t_c", "label_class", "cv_block", "cv_straddle", "mins_to_target", "mins_to_t0_break", "split"):
        assert new[c].to_list() == old[c].to_list(), c
    for c in (*we.FEATURE_COLS, "p_star", "low_t0", "close_tc"):
        assert np.allclose(new[c].to_numpy(), old[c].to_numpy(), equal_nan=True, rtol=1e-9), c


def test_cv_blocks_shared_across_assets(built: dict) -> None:
    bounds = {a: we.cv_block_bounds(p) for a, (_, _, p) in built.items()}
    assert len({tuple(b) for b in bounds.values()}) == 1
    b = bounds[ASSETS[0]]
    pooled = pl.concat([ev for ev, _, _ in built.values()], how="vertical_relaxed")
    tr = pooled.filter(pl.col("split") == "train")
    for r in tr.iter_rows(named=True):
        k = r["cv_block"]
        assert b[k] <= r["t0"] < b[k + 1]
    # one calendar day maps to a single block regardless of the asset
    per_day = tr.group_by(pl.col("t0").dt.date()).agg(pl.col("cv_block").n_unique().alias("n"))
    assert (per_day["n"] == 1).all()
    assert set(tr["cv_block"].unique()) == set(range(PARAMS.cv_blocks))
    # whole-day boundaries
    assert all(x.hour == 0 and x.minute == 0 for x in b)
    for var in se.VARIANTS:
        assert [x.isoformat() for x in we.cv_block_bounds(se.scout_params(var))] == [x.isoformat() for x in we.cv_block_bounds(se.scout_params())]


def test_tau(built: dict) -> None:
    ev = pl.concat([e for e, _, _ in built.values()], how="vertical_relaxed")
    lab = ev.filter(pl.col("label_class").is_not_null())
    imp = lab.filter(pl.col("label_class") == "impulse")
    fs = lab.filter(pl.col("label_class") == "false_start")
    sw = lab.filter(pl.col("label_class") == "sideways")
    assert imp.height and fs.height and sw.height
    assert (imp["tau_min"] == imp["mins_to_target"].cast(pl.Float64)).all()
    assert (fs["tau_min"] == fs["mins_to_t0_break"].cast(pl.Float64)).all()
    assert (sw["tau_min"] == sw["h_min"].cast(pl.Float64)).all() and sw["first_touch_min"].null_count() == sw.height
    assert (lab.filter(pl.col("label_class") != "sideways")["first_touch_min"] == lab.filter(pl.col("label_class") != "sideways")["tau_min"]).all()
    assert (lab["tau_min"] > 0).all() and (lab["tau_min"] <= lab["h_min"]).all()


def test_dataset_persist_roundtrip(dfs: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(se, "VARIANTS", {"a15": 1.5, "a10": 1.0})
    monkeypatch.setattr(se, "scout_params", lambda variant="a15", **kw: we.WaveParams(
        **{**PARAMS.__dict__, "min_a_sigma": se.VARIANTS[variant]}))
    ds = se.build_dataset(dfs, variants=("a15", "a10"))
    path = str(tmp_path / "lab.duckdb")
    out = se.persist(ds, lab_path=path)
    assert out["tables"] == ["scout_events_v1", "scout_events_v1_a10"]
    con = duckdb.connect(path, read_only=True)
    try:
        n = con.execute("SELECT count(*) FROM research.scout_events_v1").fetchone()[0]
        assert n == ds.events.height and n > 0
        assert con.execute("SELECT count(DISTINCT asset_id) FROM research.scout_events_v1").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM research.scout_events_v1 WHERE label_masked AND "
                           "(label_class IS NOT NULL OR tau_min IS NOT NULL OR fwd_max_high IS NOT NULL)").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM research.scout_events_v1 e LEFT JOIN research.scout_flow_v1 f USING (flow_key) "
                           "WHERE f.flow_key IS NULL").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM research.scout_events_v1__meta").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM research.scout_events_v1__funnel").fetchone()[0] == 2 * 2 * len(PARAMS.cells)
    finally:
        con.close()
    # looser A threshold admits more events
    assert ds.variants["a10"].height >= ds.variants["a15"].height


def test_report_tables(built: dict, flow: dict) -> None:
    ev = pl.concat([e for e, _, _ in built.values()], how="vertical_relaxed")
    spl = sr.split_counts(ev.with_columns(pl.lit("W4h_H8h").alias("cell_id")))
    assert set(spl["asset_id"]) == set(ASSETS)
    assert (spl["holdout2"] > 0).all()
    assert auc(np.array([1.0, 2, 3, 4]), np.array([0, 0, 1, 1])) == 1.0
    assert auc(np.array([1.0, 1, 1, 1]), np.array([0, 0, 1, 1])) == 0.5
    assert auc(np.array([1.0, 2]), np.array([1, 1])) is None


def auc(s: np.ndarray, y: np.ndarray) -> float | None:
    return sr.auc(s, y)
