# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false
"""``reports.builders.scout_models_hazard`` (epic_073 / t934) on synthetic data (no production database).

Covers: time binning edges, person-period expansion (hand-checked rows, censoring, row count), CIF from hazards
(hand computation, sum <= 1, monotone), 3-class probabilities, model determinism / fold-local expansion, no post-``t_c``
covariate, the registry spec (H3, round 1) and the model-only lab persistence on a temporary DuckDB file.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import polars as pl
import pytest

pytest.importorskip("sklearn")

from reports.builders import scout_bench as sb  # noqa: E402
from reports.builders import scout_models_hazard as hz  # noqa: E402

H = hz.H_MIN


def make_frame(n: int = 600, seed: int = 0) -> pl.DataFrame:
    """Synthetic events: ``a_sigma`` raises the impulse hazard, flow columns are noise (with a few NaNs)."""
    rng = np.random.default_rng(seed)
    x = rng.normal(size=(n, len(sb.S0C_COLS)))
    p_imp = 1 / (1 + np.exp(-(-1.7 + 1.2 * x[:, 0])))
    u = rng.random(n)
    cls = np.where(u < p_imp, "impulse", np.where(rng.random(n) < 0.3, "false_start", "sideways"))
    tau = np.where(cls == "sideways", float(H), rng.integers(1, H + 1, size=n).astype(float))
    t0 = [datetime(2022, 1, 1) + timedelta(hours=9 * i) for i in range(n)]
    d = {"label_class": cls, "tau_min": tau, "t0": t0, **{c: x[:, k] for k, c in enumerate(sb.S0C_COLS)}}
    d["vol_decline_rel"] = np.exp(rng.normal(size=n))
    d["trades_rel_decline"] = np.exp(rng.normal(size=n))
    q = rng.normal(size=n)
    q[:5] = np.nan
    d["qv_logz_prefix"] = q
    d["tb_ratio_rebound"] = rng.uniform(0.4, 0.6, size=n)
    return pl.DataFrame(d)


# --------------------------------------------------------------------------- binning / expansion


def test_time_bin_edges():
    tau = np.array([0, 1, 30, 31, 60, 61, 479, 480, 900])
    assert hz.time_bin(tau).tolist() == [0, 0, 0, 1, 1, 2, 15, 15, 15]


def test_expansion_hand_checked():
    cls = np.array(["impulse", "false_start", "sideways", "impulse"])
    tau = np.array([45.0, 30.0, 480.0, 480.0])
    pos, b, out = hz.expand_person_period(cls, tau)
    # impulse at bin 1 -> 2 rows; false start at bin 0 -> 1 row; sideways -> 16 rows; impulse at 480 -> 16 rows
    assert pos.tolist() == [0, 0, 1] + [2] * 16 + [3] * 16
    assert b.tolist() == [0, 1, 0] + list(range(16)) + list(range(16))
    assert out.tolist() == [0, hz.OUT_IMPULSE, hz.OUT_FALSE] + [0] * 16 + [0] * 15 + [hz.OUT_IMPULSE]


def test_expansion_properties():
    fr = make_frame(400, seed=3)
    cls, tau = fr["label_class"].to_numpy(), fr["tau_min"].to_numpy()
    pos, b, out = hz.expand_person_period(cls, tau)
    j = hz.time_bin(tau)
    expected_rows = np.where(cls == "sideways", hz.N_BINS, j + 1)
    assert pos.size == expected_rows.sum()
    assert np.bincount(pos, minlength=cls.size).tolist() == expected_rows.tolist()
    # exactly one event row per non-censored event, at its own bin; none for sideways
    ev = out > 0
    assert ev.sum() == (cls != "sideways").sum()
    assert np.array_equal(b[ev], j[pos[ev]])
    assert set(cls[pos[ev]]) <= {"impulse", "false_start"}
    assert np.all((out[ev] == hz.OUT_IMPULSE) == (cls[pos[ev]] == "impulse"))
    # bins of one event are consecutive from 0 (rows of one event are contiguous)
    starts = np.r_[0, np.flatnonzero(np.diff(pos)) + 1]
    assert np.all(b[starts] == 0)


# --------------------------------------------------------------------------- CIF


def test_cif_hand_computed():
    h = np.zeros((1, 3, 3))
    h[0, 0] = [0.7, 0.2, 0.1]
    h[0, 1] = [0.5, 0.3, 0.2]
    h[0, 2] = [1.0, 0.0, 0.0]
    cif = hz.cif_from_hazards(h)
    assert np.allclose(cif[0, :, 0], [0.2, 0.2 + 0.7 * 0.3, 0.41])
    assert np.allclose(cif[0, :, 1], [0.1, 0.1 + 0.7 * 0.2, 0.24])
    p = hz.three_class_from_cif(cif)
    assert np.allclose(p, [[0.41, 0.24, 0.35]])


def test_cif_sums_le_one_and_monotone():
    rng = np.random.default_rng(1)
    raw = rng.random((200, hz.N_BINS, 3))
    h = raw / raw.sum(axis=2, keepdims=True)
    cif = hz.cif_from_hazards(h)
    tot = cif.sum(axis=2)
    assert np.all(tot <= 1 + 1e-12)
    assert np.all(np.diff(cif, axis=1) >= -1e-15)
    # 1 - sum = survival at the bin end
    assert np.allclose(1 - tot, np.cumprod(h[:, :, 0], axis=1))
    p = hz.three_class_from_cif(cif)
    assert np.allclose(p.sum(axis=1), 1.0) and np.all(p >= 0)


# --------------------------------------------------------------------------- model


def test_model_fit_predict_deterministic_and_valid():
    fr = make_frame(600, seed=0)
    tr, te = fr[:450], fr[450:]
    m1, m2 = hz.HazardModel(), hz.HazardModel()
    m1.fit(tr, None, seed=1)
    m2.fit(tr, None, seed=99)                 # no randomness: the seed does not matter
    p1, p2 = m1.predict_proba(te, None), m2.predict_proba(te, None)
    assert p1.shape == (150, 3) and np.isfinite(p1).all()
    assert np.array_equal(p1, p2)
    assert np.allclose(p1.sum(axis=1), 1.0)
    cif = m1.cif(te)
    assert np.all(cif.sum(axis=2) <= 1 + 1e-12)
    # expansion is built from the training events only
    pos, _, _ = hz.expand_person_period(tr["label_class"].to_numpy(), tr["tau_min"].to_numpy())
    assert m1.n_pp_rows == pos.size
    # the signal covariate ranks impulses (synthetic a_sigma effect)
    y = (te["label_class"] == "impulse").to_numpy().astype(float)
    assert sb.auc(y, p1[:, 0]) > 0.6


def test_predictions_ignore_labels_of_scored_rows():
    """predict_proba must not read the scored rows' label / tau (only covariates known at t_c)."""
    fr = make_frame(500, seed=5)
    m = hz.HazardModel()
    m.fit(fr[:400], None, seed=0)
    te = fr[400:]
    shuffled = te.with_columns(pl.Series("label_class", np.full(te.height, "sideways")), pl.lit(float(H)).alias("tau_min"))
    assert np.array_equal(m.predict_proba(te, None), m.predict_proba(shuffled, None))


def test_covariates_are_known_at_tc():
    forbidden = {"label_class", "label_impulse", "tau_min", "first_touch_min", "mins_to_target", "mins_to_t0_break",
                 "fwd_max_high", "fwd_min_low", "fwd_mfe_log", "fwd_mae_log", "first_hit", "same_bar", "label_sub", "p_star"}
    assert not forbidden & set(hz.COVARIATES)
    assert set(sb.S0C_COLS) <= set(hz.COVARIATES)


def test_spec_is_h3_round1():
    s = hz.spec()
    assert (s.name, s.family, s.hypothesis, s.round) == ("scout_hazard_r1", "P3", "H3", 1)
    r = hz.registry()
    assert [x.name for x in r.specs()] == [sb.BASELINE_NAME, "scout_hazard_r1"]
    assert s.config_hash == hz.spec().config_hash


# --------------------------------------------------------------------------- persistence


class _Res:
    def __init__(self, run_id: str, models: list[str], extra: bool = False) -> None:
        d = {"run_id": [run_id] * len(models), "model": models, "auc": [0.6 + 0.01 * i for i in range(len(models))]}
        if extra:
            d["new_col"] = [1.0] * len(models)
        self.leaderboard = pd.DataFrame(d)
        self.run = {"run_id": run_id, "note": "t", "spa_p": float("nan")}


def test_persist_writes_only_own_rows(tmp_path):
    import duckdb

    path = str(tmp_path / "lab.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE SCHEMA research")
    con.execute('CREATE TABLE research."scout_leaderboard_v1" (run_id VARCHAR, model VARCHAR, auc DOUBLE)')
    con.execute("INSERT INTO research.\"scout_leaderboard_v1\" VALUES ('old', 'scout_lgbm_r1', 0.7), ('old', 'S0C_logit', 0.66)")
    con.close()
    res = _Res("r1", ["S0C_logit", hz.MODEL_NAME], extra=True)
    hz.persist_model_rows(res, lab_path=path)
    hz.persist_model_rows(res, lab_path=path)          # idempotent re-write
    con = duckdb.connect(path, read_only=True)
    rows = con.execute('SELECT run_id, model FROM research."scout_leaderboard_v1" ORDER BY run_id, model').fetchall()
    runs = con.execute('SELECT run_id FROM research."scout_leaderboard_v1__runs"').fetchall()
    cols = [r[0] for r in con.execute("DESCRIBE research.\"scout_leaderboard_v1\"").fetchall()]
    con.close()
    assert rows == [("old", "S0C_logit"), ("old", "scout_lgbm_r1"), ("r1", hz.MODEL_NAME)]
    assert runs == [("r1",)]
    assert "new_col" in cols
