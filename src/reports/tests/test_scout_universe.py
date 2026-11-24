# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false
"""t936: ``scout_bench.rules`` (generalised trade rules) and ``scout_bench.universe`` (universe SPA, joint Holm,
universe gates, strategy SPA) on synthetic data (no database)."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import polars as pl
import pytest

pytest.importorskip("sklearn")

from reports.builders import scout_bench as sb  # noqa: E402
from reports.builders import wave_events as we  # noqa: E402
from reports.builders.scout_bench import rules as rl  # noqa: E402
from reports.builders.scout_bench import trades as tr  # noqa: E402
from reports.builders.scout_bench import universe as uv  # noqa: E402

COSTS = tr.Costs(2.0, 5.0, 1.5, {"tp": "maker", "sl": "taker", "timeout": "taker"})
T0 = datetime(2022, 1, 1)
SLIP, FEE_T, FEE_M = 1.5e-4, 5e-4, 2e-4
EV = {"p_star": 110.0, "low_t0": 95.0, "close_tc": 100.0, "a_log": float(np.log(110 / 100)), "sigma_w": 0.02,
      "atr14_15m": 1.0}


def grid(rows) -> we.MinuteData:
    a = np.array(rows, float)
    return we.MinuteData(m0=we._epoch_min(T0), open=a[:, 0], high=a[:, 1], low=a[:, 2], close=a[:, 3], volume=np.ones(len(a)))


def sim(rows, rule, h=None, ev=EV):
    return rl.simulate_rule_event(grid(rows), we._epoch_min(T0), h or len(rows), ev, rule, COSTS)


FLAT = (100.0, 100.5, 99.5, 100.0)


# --------------------------------------------------------------------------- rules


def test_default_rule_equals_harness_simulation_on_random_paths():
    rng = np.random.default_rng(7)
    for k in range(300):
        n = 40
        c = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
        o = np.r_[100.0, c[:-1]] * np.exp(rng.normal(0, 0.002, n))
        hi = np.maximum(o, c) * np.exp(np.abs(rng.normal(0, 0.006, n)))
        lo = np.minimum(o, c) * np.exp(-np.abs(rng.normal(0, 0.006, n)))
        m = we.MinuteData(m0=0, open=o, high=hi, low=lo, close=c, volume=np.ones(n))
        p_star, low0 = 100 * np.exp(rng.uniform(0.01, 0.06)), 100 * np.exp(-rng.uniform(0.01, 0.06))
        ev = EV | {"p_star": p_star, "low_t0": low0}
        a = tr.simulate_event(m, 0, n, p_star, low0, COSTS)
        b = rl.simulate_rule_event(m, 0, n, ev, rl.DEFAULT_RULE, COSTS)
        assert a["exit_type"] == b["exit_type"], k
        assert a["net"] == pytest.approx(b["net"], abs=1e-12) and a["bars_held"] == b["bars_held"]


def test_sigma_and_atr_levels_from_entry():
    entry_eff = 100 * (1 + SLIP)
    r = sim([FLAT, (100, 102.5, 99.6, 102), FLAT], rl.TradeRule("s", tp=("sigma", 1.0), sl=("sigma", 1.0)))
    tp = 100 * np.exp(0.02)
    assert r["exit_type"] == "tp" and r["net"] == pytest.approx(tp / entry_eff - 1 - FEE_T - FEE_M)
    r = sim([FLAT, (100, 100.4, 97.9, 98), FLAT], rl.TradeRule("s", tp=("sigma", 1.0), sl=("sigma", 1.0)))
    sl = 100 * np.exp(-0.02)
    assert r["exit_type"] == "sl" and r["net"] == pytest.approx(sl * (1 - SLIP) / entry_eff - 1 - 2 * FEE_T)
    r = sim([FLAT, (100, 102.1, 99.6, 102), FLAT], rl.TradeRule("a", tp=("atr", 2.0), sl=("atr", 1.0)))
    assert r["exit_type"] == "tp" and r["net"] == pytest.approx(102 / entry_eff - 1 - FEE_T - FEE_M)


def test_time_stop_and_same_validity_as_harness():
    rows = [FLAT, FLAT, (100, 100.5, 99.5, 101.0), FLAT, (100, 100.5, 99.5, 99.0)]
    r = sim(rows, rl.TradeRule("t", time_min=3), h=5)
    assert r["exit_type"] == "timeout" and r["bars_held"] == 3
    assert r["net"] == pytest.approx(101 * (1 - SLIP) / (100 * (1 + SLIP)) - 1 - 2 * FEE_T)
    gap = [FLAT, FLAT, FLAT, (np.nan,) * 4, FLAT]
    assert sim(gap, rl.TradeRule("t", time_min=2), h=5)["exit_type"] == "invalid"     # full-H validity, like the harness


def test_partial_exit_then_break_even_stop():
    rule = rl.TradeRule("p", partial_frac=0.5, tp1=("half", 0.0), be_after_tp1=True)
    half = 100 * np.exp(EV["a_log"] / 2)
    entry_eff = 100 * (1 + SLIP)
    # bar 1 reaches the half level, bar 2 falls back below the entry -> half at tp1 (maker), half at entry (taker)
    r = sim([FLAT, (100, half + 0.1, 100.2, 104), (104, 104, 99.0, 99.5), FLAT], rule)
    leg1 = 0.5 * (half / entry_eff - 1 - FEE_T - FEE_M)
    leg2 = 0.5 * (100 * (1 - SLIP) / entry_eff - 1 - 2 * FEE_T)
    assert r["n_legs"] == 2 and r["tp_any"] and r["exit_type"] == "sl" and r["net"] == pytest.approx(leg1 + leg2)
    # half level and P* in the same bar -> both legs are targets
    r = sim([FLAT, (100, 111, 100.2, 110), FLAT], rule)
    assert r["exit_type"] == "tp" and r["net"] == pytest.approx(0.5 * (half / entry_eff - 1 - FEE_T - FEE_M)
                                                                 + 0.5 * (110 / entry_eff - 1 - FEE_T - FEE_M))
    # a bar touching the original stop first -> whole position stopped (no partial)
    r = sim([FLAT, (100, half + 0.1, 94, 96), FLAT], rule)
    assert r["n_legs"] == 1 and r["exit_type"] == "sl"


def test_break_even_trigger_is_conservative_in_the_trigger_bar():
    rule = rl.TradeRule("be", be_trigger_sigma=1.0)
    trig = 100 * np.exp(0.02)
    r = sim([FLAT, (100, trig + 0.01, 100.2, 102), (102, 102, 99.0, 99.5), FLAT], rule)
    assert r["exit_type"] == "sl" and r["bars_held"] == 3
    assert r["net"] == pytest.approx(100 * (1 - SLIP) / (100 * (1 + SLIP)) - 1 - 2 * FEE_T)
    r = sim([FLAT, (100, trig + 0.01, 99.9, 102), FLAT, FLAT], rule)                       # trigger bar dips below entry
    assert r["exit_type"] == "sl" and r["bars_held"] == 2


def test_rule_validation():
    with pytest.raises(ValueError):
        rl.TradeRule("x", tp=("foo", 1.0))
    with pytest.raises(ValueError):
        rl.TradeRule("x", partial_frac=0.5)


def test_simulate_rule_frame_matches_harness_default():
    rng = np.random.default_rng(3)
    n = 3000
    c = 100 + np.cumsum(rng.normal(0, 0.05, n))
    bars = {"aaausdt": we.MinuteData(m0=we._epoch_min(T0), open=c, high=c + 0.2, low=c - 0.2, close=c, volume=np.ones(n))}
    rows = []
    for k in range(20):
        t_c = T0 + timedelta(minutes=60 + 100 * k)
        i = 60 + 100 * k
        rows.append({"row_id": k, "asset_id": "aaausdt", "t_c": t_c, "h_min": 120, "p_star": c[i] * 1.004,
                     "low_t0": c[i] * 0.996, "close_tc": c[i], "a_log": float(np.log(1.004)), "sigma_w": 0.003,
                     "atr14_15m": 0.3})
    ev = pl.DataFrame(rows)
    a = tr.simulate(ev, bars, COSTS).set_index("row_id")
    b = rl.simulate_rule(ev, bars, rl.DEFAULT_RULE, COSTS).set_index("row_id")
    assert list(a.index) == list(b.index)
    assert np.allclose(a["net_bps"], b["net_bps"]) and (a["exit_type"] == b["exit_type"]).all()
    sub = rl.simulate_rule(ev, bars, rl.DEFAULT_RULE, COSTS, rows=np.array([2, 5]))
    assert sorted(sub["row_id"]) == [2, 5]


# --------------------------------------------------------------------------- universe


def _days(n_days: int, per_day: int = 3) -> np.ndarray:
    return np.array([np.datetime64(T0 + timedelta(days=d, hours=h)) for d in range(n_days) for h in range(per_day)])


def test_logloss_spa_universe_detects_best_and_noise():
    rng = np.random.default_rng(0)
    t0 = _days(200)
    n = t0.size
    x = rng.normal(size=n)
    y = (rng.random(n) < 1 / (1 + np.exp(-(-1.5 + 1.2 * x)))).astype(float)
    base = np.full(n, y.mean())
    good = 1 / (1 + np.exp(-(-1.5 + 1.2 * x)))
    noise = np.clip(base + rng.normal(0, 0.05, n), 0.01, 0.99)
    res = uv.logloss_spa(y, base, {"noise": noise, "good": good}, t0, n_boot=500)
    assert res["best_name"] == "good" and res["p_consistent"] < 0.05
    res = uv.logloss_spa(y, base, {"noise": noise}, t0, n_boot=500)
    assert res["p_consistent"] > 0.2


def test_joint_holm_counts_missing_as_one():
    h = uv.joint_holm({"H1": 0.72, "H2": 0.3057, "H3": 0.269})
    assert h["H3"] == pytest.approx(0.807) and h["H2"] == pytest.approx(0.807) and h["H1"] == pytest.approx(0.807)
    h = uv.joint_holm({"H1": 0.004, "H2": float("nan")})
    assert h["H1"] == pytest.approx(0.008) and h["H2"] == 1.0


def test_apply_universe_sets_best_only_and_regates():
    lb = pd.DataFrame([
        {"model": "B", "is_baseline": True, "primary": False, "spa_p": np.nan, "holm_p": np.nan},
        {"model": "m1", "is_baseline": False, "primary": True, "spa_p": 1.0, "holm_p": 0.2, "delta_auc": 0.0, "ci_lo": -0.1},
        {"model": "m2", "is_baseline": False, "primary": False, "spa_p": 1.0, "holm_p": np.nan, "delta_auc": 0.0,
         "ci_lo": -0.1},
    ])
    out = uv.apply_universe(lb, spa_p=0.3, spa_best="m2", holm_p={"m1": 0.6}, min_events=True)
    o = out.set_index("model")
    assert o.loc["m1", "spa_p"] == 0.3 and o.loc["m2", "spa_p"] == 0.3 and np.isnan(o.loc["B", "spa_p"])
    assert bool(o.loc["m2", "spa_best"]) and not bool(o.loc["m1", "spa_best"])
    assert o.loc["m1", "holm_p"] == 0.6 and o.loc["m1", "gate_spa"] is None and o.loc["m2", "gate_spa"] is False
    assert "not_spa_best" in o.loc["m1", "go_failed"] and o.loc["m2", "go_status"] == "NO-GO"
    assert lb.loc[1, "spa_p"] == 1.0                                         # input untouched


def test_event_pnl_and_pnl_spa():
    rng = np.random.default_rng(1)
    t0 = _days(250, 2)
    n = t0.size
    trades = pd.DataFrame({"row_id": np.arange(n), "net_bps": rng.normal(0, 50, n)})
    sel = np.zeros(n, bool)
    sel[::3] = True
    v = uv.event_pnl(n, trades, sel)
    assert (v[~sel] == 0).all() and np.allclose(v[sel], trades["net_bps"].to_numpy()[sel])
    edge = uv.event_pnl(n, trades.assign(net_bps=trades["net_bps"] + 40.0))
    res = uv.pnl_spa({"noise": v, "edge": edge}, t0, n_boot=500)
    assert res["best_name"] == "edge" and res["p_consistent"] < 0.05
    res = uv.pnl_spa({"noise": v}, t0, n_boot=500)
    assert res["p_consistent"] > 0.05
    # against itself as the benchmark: no strategy is better -> p = 1
    assert uv.pnl_spa({"edge": edge}, t0, benchmark=edge, n_boot=200)["p_consistent"] == 1.0
    assert uv.studentized_best({"noise": v, "edge": edge}, t0, ["noise", "edge"], n_boot=200) == "edge"


def test_universe_api_exported():
    assert sb.universe.logloss_spa is uv.logloss_spa and sb.rules.TradeRule is rl.TradeRule


def test_patch_table_three_shapes():
    from reports.builders import scout_leaderboard as sl

    lb = pd.DataFrame([
        {"model": "S0C_logit", "spa_p": np.nan, "holm_p": np.nan, "go_status": "baseline", "go_failed": "",
         "gate_spa": None, "gate_holm": None},
        {"model": "m1", "spa_p": 0.42, "holm_p": 0.81, "go_status": "NO-GO", "go_failed": "dauc,not_spa_best",
         "gate_spa": None, "gate_holm": False},
    ])
    t1 = {"columns": [{"key": "metric"}, {"key": "S0C_logit"}, {"key": "m1"}],
          "rows": [["auc", 0.6, 0.61], ["spa_p", None, 1.0], ["holm_p", None, 0.7]]}
    assert sl.patch_table(t1, lb)
    assert t1["rows"][0] == ["auc", 0.6, 0.61] and t1["rows"][1][2] == 0.42 and t1["rows"][2][2] == 0.81
    assert t1["rows"][1][1] is None                                           # baseline column untouched
    t2 = {"columns": [{"key": "model"}, {"key": "auc"}, {"key": "spa_p"}, {"key": "go_failed"}],
          "rows": [["S0C_logit", 0.6, None, ""], ["m1", 0.61, 1.0, "dauc,spa"]]}
    assert sl.patch_table(t2, lb)
    assert t2["rows"][1] == ["m1", 0.61, 0.42, "dauc,not_spa_best"] and t2["rows"][0][2] is None
    t3 = {"columns": [{"key": "feltétel"}, {"key": "teljesül"}], "rows": [["spa", "igen"], ["holm", "igen"], ["dauc", "nem"]],
          "_model": "m1"}
    assert sl.patch_table(t3, lb)
    assert t3["rows"] == [["spa", "n/a"], ["holm", "nem"], ["dauc", "nem"]]
    assert not sl.patch_table({"columns": [{"key": "x"}], "rows": [[1]]}, lb)
