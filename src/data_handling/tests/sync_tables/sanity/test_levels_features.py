"""feat_levels bar-level feature tests (epic_071 / t912).

Pins: (1) the vectorised features equal a straightforward reference built
from ``levels_as_of`` / ``trendlines_as_of`` and the events known at T,
(2) leakage: a run on OHLCV truncated at T reproduces every feature row
<= T of the full run, on all three scales (s = 1h, m = 4h, l = 1d),
(3) t-1 lag: the row at t does not depend on bar t, (4) NULL when no object.
"""

import math
from datetime import datetime, timedelta

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import utils
from data_handling.store.duckdb_store import _feat_levels_v1_columns
from data_handling.sync_tables import _levels_features as lf
from data_handling.sync_tables import _levels_polars as lv

pytestmark = pytest.mark.sanity

T0 = datetime(2024, 1, 1)
DAYS = 200


def _random_walk_1m(days: int, seed: int) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    n = days * 1440
    e = rng.normal(0, 0.0012, n)
    x = np.zeros(n)
    for i in range(1, n):
        x[i] = x[i - 1] * 0.99995 + e[i]
    close = 100.0 * np.exp(x + 0.15 * np.sin(np.arange(n) / 5000.0))
    open_ = np.concatenate(([close[0]], close[:-1]))
    wig = np.abs(rng.normal(0, 0.0006, n)) * close
    return pl.DataFrame({
        "open_time": pl.datetime_range(T0, T0 + timedelta(minutes=n - 1), "1m", eager=True),
        "open": open_, "high": np.maximum(open_, close) + wig,
        "low": np.minimum(open_, close) - wig, "close": close, "volume": np.ones(n),
    })


@pytest.fixture(scope="module")
def data() -> pl.DataFrame:
    return _random_walk_1m(DAYS, 7)


@pytest.fixture(scope="module")
def params() -> dict:
    return utils.load_levels_detector_config()


@pytest.fixture(scope="module")
def full(data: pl.DataFrame, params: dict) -> tuple[dict, pl.DataFrame]:
    res = lv.detect_all_scales(data, params=params)
    return res, lf.level_features_from_results(data, res, params)


# %% Schema


def test_columns_match_migration_and_no_labels(full: tuple[dict, pl.DataFrame]) -> None:
    _, f = full
    assert lf.feature_columns() == _feat_levels_v1_columns()
    assert f.columns == ["open_time", "available_ts", *_feat_levels_v1_columns()]
    assert not any("forward" in c for c in f.columns)
    assert f["open_time"].equals(f["available_ts"].alias("open_time"))


def test_every_scale_produces_values(full: tuple[dict, pl.DataFrame]) -> None:
    res, f = full
    for x in ("s", "m", "l"):
        assert res[x].levels.height > 0
        assert f[f"feat_sr_dist_sup_atr_{x}"].drop_nulls().len() > 0
        assert f[f"feat_sr_dist_res_atr_{x}"].drop_nulls().len() > 0
    for x in ("s", "m"):
        assert f[f"feat_tl_dist_sup_atr_{x}"].drop_nulls().len() > 0
        assert f[f"feat_tl_break_age_{x}"].drop_nulls().len() > 0


# %% Reference (as_of) equality


def _close(a: float | None, b: float | None) -> bool:
    a_null = a is None or (isinstance(a, float) and math.isnan(a))
    b_null = b is None or (isinstance(b, float) and math.isnan(b))
    if a_null or b_null:
        return a_null and b_null
    assert a is not None and b is not None
    return abs(a - b) <= 1e-9 * max(1.0, abs(b))


def _reference_row(res: dict, params: dict, T: datetime, ref: float) -> dict:
    """Features of one row from the *_as_of snapshots (the specification)."""
    exp: dict = {}
    for x, r in res.items():
        _, bar_secs, lookback = lv._scale_cfg(params, x)
        bars = r.bars.filter(pl.col("close_time") <= T)
        atr = bars["atr"][-1] if bars.height else None
        act = lv.levels_as_of(r, T, params=params)
        exp[f"feat_sr_n_active_{x}"] = float(act.height)
        sides = (("res", act.filter(pl.col("price") > ref).sort("price").head(1)),
                 ("sup", act.filter(pl.col("price") <= ref).sort("price", descending=True).head(1)))
        for side, sub in sides:
            has = sub.height > 0
            pr = sub["price"][0] if has else None
            dist = None
            if has and atr is not None:
                assert pr is not None
                dist = (pr - ref) / atr if side == "res" else (ref - pr) / atr
            exp[f"feat_sr_dist_{side}_atr_{x}"] = dist
            exp[f"feat_sr_strength_{side}_{x}"] = sub["strength"][0] if has else None
            exp[f"feat_sr_touches_{side}_{x}"] = float(sub["n_touches"][0]) if has else None
            exp[f"feat_sr_flips_{side}_{x}"] = float(sub["n_role_flips"][0]) if has else None
            if x in lf.ROUND_SCALES:
                exp[f"feat_sr_is_round_{side}_{x}"] = float(sub["is_round"][0]) if has else None
        groups = [("level", "sr")] + ([("line", "tl")] if x in lf.LINE_SCALES else [])
        for obj_type, pre in groups:
            ev = r.events.filter((pl.col("obj_type") == obj_type) & (pl.col("event_time") <= T))
            brk = ev.filter(pl.col("event_type") == "break").sort("event_time", "obj_id")
            age = dirn = flag = None
            if brk.height:
                b = brk.row(brk.height - 1, named=True)
                dt = (T - b["event_time"]).total_seconds()
                if dt <= lookback:
                    age = dt / bar_secs
                    dirn = 1.0 if b["side"] == "resistance" else -1.0
                    rt = ev.filter((pl.col("event_type") == "retest")
                                   & (pl.col("obj_id") == b["obj_id"])
                                   & (pl.col("event_time") > b["event_time"]))
                    flag = 1.0 if rt.height else 0.0
            exp[f"feat_{pre}_break_age_{x}"] = age
            exp[f"feat_{pre}_break_dir_{x}"] = dirn
            exp[f"feat_{pre}_retest_flag_{x}"] = flag
        if x in lf.LINE_SCALES:
            tl = lv.trendlines_as_of(r, T)
            for kind, side in (("support", "sup"), ("resistance", "res")):
                sub = tl.filter(pl.col("kind") == kind)
                val = None
                if sub.height and atr is not None:
                    sg = (ref - sub["price_at"]) if side == "sup" else (sub["price_at"] - ref)
                    j = int(np.argmin(np.abs(sg.to_numpy())))
                    val = sg[j] / atr
                exp[f"feat_tl_dist_{side}_atr_{x}"] = val
    return exp


def test_features_equal_as_of_reference(
    data: pl.DataFrame, params: dict, full: tuple[dict, pl.DataFrame]
) -> None:
    res, f = full
    rng = np.random.default_rng(3)
    rows = np.sort(rng.integers(1, data.height, 120))
    times = data["open_time"].to_list()
    closes = data["close"].to_numpy()
    checked = 0
    for i in rows:
        exp = _reference_row(res, params, times[i], float(closes[i - 1]))
        got = f.row(int(i), named=True)
        for k, v in exp.items():
            assert _close(got[k], v), (times[i], k, got[k], v)
            checked += v is not None
    assert checked > 1000           # the sample really exercises non-null states


# %% Leakage: truncated == full on all scales


@pytest.mark.parametrize("cut_day", [61.3, 123.71, 187.02])
def test_truncated_run_equals_full_run(
    data: pl.DataFrame, params: dict, full: tuple[dict, pl.DataFrame], cut_day: float
) -> None:
    _, f_full = full
    T = T0 + timedelta(minutes=int(cut_day * 1440))
    trunc = data.filter(pl.col("open_time") <= T)
    f_trunc = lf.compute_level_features(trunc, params=params)
    expected = f_full.filter(pl.col("open_time") <= T)
    assert_frame_equal(f_trunc, expected, check_exact=False, rel_tol=1e-12, abs_tol=1e-12)
    # the compared window carries real values on every scale (incl. l = 1d)
    late = f_trunc.filter(pl.col("open_time") > T - timedelta(days=20))
    for x in ("s", "m", "l"):
        assert late[f"feat_sr_n_active_{x}"].max() > 0, x  # type: ignore[operator]


def test_row_t_does_not_use_bar_t(data: pl.DataFrame, params: dict,
                                  full: tuple[dict, pl.DataFrame]) -> None:
    """t-1 lag: perturbing bar t (and later) leaves row t unchanged."""
    _, f_full = full
    i = 150 * 1440 + 37
    T = data["open_time"][i]
    bump = pl.col("open_time") >= T
    shocked = data.filter(pl.col("open_time") <= T + timedelta(hours=2)).with_columns(
        pl.when(bump).then(pl.col("high") * 1.3).otherwise(pl.col("high")).alias("high"),
        pl.when(bump).then(pl.col("low") * 0.7).otherwise(pl.col("low")).alias("low"),
        pl.when(bump).then(pl.col("close") * 1.2).otherwise(pl.col("close")).alias("close"))
    f_s = lf.compute_level_features(shocked, params=params)
    assert_frame_equal(f_s.filter(pl.col("open_time") <= T),
                       f_full.filter(pl.col("open_time") <= T),
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-12)
    nxt = f_s.filter(pl.col("open_time") == T + timedelta(minutes=1))
    old = f_full.filter(pl.col("open_time") == T + timedelta(minutes=1))
    assert not nxt.equals(old)          # bar t does feed row t+1


# %% NULL semantics


def test_null_before_any_object(params: dict) -> None:
    df = _random_walk_1m(3, 1)          # 3 days: no 1d ATR, no l-scale level possible
    f = lf.compute_level_features(df, params=params)
    assert f["feat_sr_n_active_l"][0] is None           # row 0: no previous close
    assert f["feat_sr_n_active_l"].drop_nulls().max() == 0
    for c in ("feat_sr_dist_sup_atr_l", "feat_sr_strength_res_l", "feat_sr_break_age_l"):
        assert f[c].null_count() == f.height
