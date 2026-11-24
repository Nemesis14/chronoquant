"""Feature table of epic_079 / t971: the epic_078 variables + dynamics, zigzag sequence, 4T context, Wyckoff-like, regime.

Everything is a function of the bars ``<= conf_idx`` of a candidate (confirmed low pivot).  Prefix = group:
``z_ v_ w_ f_ q_ qd_ s_`` (epic_078: snapshot values), plus the new groups

* ``d_``  dynamics — change of RSI_T, VWAP distance, taker imbalance, volume, trades, realised vol, momentum over the last
  0.25T / 0.5T / 1T; shape of the rebound (efficiency, first-half share, drawdown, volume tilt)
* ``g_``  zigzag sequence — the last three up- and down-legs (size in sigma, duration in T, ratios), higher-low / higher-high
  runs, room to the previous swing highs
* ``m_``  next larger scale (4T) — state of the 4T zigzag as confirmed at ``conf_idx``, position in rolling 1T..30T ranges
* ``k_``  Wyckoff-like — spring depth, retest volume vs the previous low, effort / result, selling-climax volume
* ``r_``  regime — 7d / 30d trend, efficiency ratio, vol-regime ratio, distance to the 30d high, time of day / week, time and
  count of big moves at this scale, funding change

All distances are in sigma_T (at the confirmation bar), all durations in T, so pooled scales are comparable.

``uv run python -m reports.builders.cand_v2_features``
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

import numpy as np
import pandas as pd
import polars as pl

from reports.builders import cand_pred_features as cf
from reports.builders import cand_v2_sample as sm
from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an

logger = logging.getLogger(__name__)
TABLE_FEATURES = "cand_v2_features"
D30 = 30 * 1440
D7 = 7 * 1440
EPS = 1e-12
NEW_GROUPS = ("d", "g", "m", "k", "r")
ALL_GROUPS = ("z", "v", "w", "f", "q", "s", *NEW_GROUPS)


def _log(x: np.ndarray) -> np.ndarray:
    return np.log(np.maximum(x, EPS))


class Aux:
    """Series-level helper arrays (rolling extremes, cumulative sigma, funding sums) built once per (cut) series."""

    def __init__(self, s: cf.Series) -> None:
        lg = pd.Series(s.logp)
        self.rmax30 = lg.rolling(D30, min_periods=1).max().to_numpy()
        self.rmin30 = lg.rolling(D30, min_periods=1).min().to_numpy()
        self.cfund = np.concatenate(([0.0], np.cumsum(s.fund_r)))


def pivot_context6(piv: np.ndarray, low_idx: np.ndarray) -> dict[str, np.ndarray]:
    """The six pivots before each candidate low: hp, lp, hpp, lpp, h3, l3 (alternating, most recent first) and a validity mask."""
    pos = np.searchsorted(piv, low_idx)
    if not np.array_equal(piv[pos], low_idx):
        raise ValueError("candidate low is not a pivot of the zigzag")
    ok = pos >= 6
    q = np.where(ok, pos, 6)
    return {"hp": piv[q - 1], "lp": piv[q - 2], "hpp": piv[q - 3], "lpp": piv[q - 4], "h3": piv[q - 5], "l3": piv[q - 6], "ok": ok}


def _slice_stat(x: np.ndarray, a: np.ndarray, b: np.ndarray, fn: Any) -> np.ndarray:
    return np.array([fn(x[max(int(i), 0):int(j) + 1]) for i, j in zip(a, b, strict=True)])


def v2_features(s: cf.Series, aux: Aux, T: int, sigma: np.ndarray, piv4: np.ndarray, typ4: np.ndarray, cnf4: np.ndarray,
                sigma4: np.ndarray, row: dict[str, np.ndarray], big_end: np.ndarray) -> dict[str, np.ndarray]:
    """The new groups for the candidates in ``row`` (low, conf, hp, lp, hpp, lpp, h3, l3 as bar indices)."""
    lo, cfi = row["low"].astype(np.int64), row["conf"].astype(np.int64)
    hp, lp, hpp, lpp, h3, l3 = (row[k].astype(np.int64) for k in ("hp", "lp", "hpp", "lpp", "h3", "l3"))
    lg = s.logp
    sig = sigma[cfi]
    x0, L = lg[cfi], lg[lo]
    Hp, Lp, Hpp, Lpp, H3, L3 = lg[hp], lg[lp], lg[hpp], lg[lpp], lg[h3], lg[l3]
    f: dict[str, np.ndarray] = {}
    fr = {"q1": 0.25, "q2": 0.5, "q4": 1.0}
    win = {k: np.maximum((v * T).astype(int) if False else int(round(v * T)), 2) for k, v in fr.items()}

    def rsi_at(idx: np.ndarray) -> np.ndarray:
        up = cf._wsum(s.cup, idx - T + 1, idx)
        dn = cf._wsum(s.cdn, idx - T + 1, idx)
        return up / np.maximum(up + dn, EPS)

    def vwd_at(idx: np.ndarray) -> np.ndarray:
        return (lg[idx] - _log(cf._vwap(s, idx - 1440 + 1, idx))) / sig

    def imb(a: np.ndarray, b: np.ndarray) -> np.ndarray:
        return 2 * cf._wsum(s.ctb, a, b) / np.maximum(cf._wsum(s.cv, a, b), EPS) - 1

    # --- d: dynamics ---------------------------------------------------------------------------------------------
    rsi0, vwd0 = rsi_at(cfi), vwd_at(cfi)
    base7 = cf._wmean(s.cv, cfi - D7 + 1, cfi)
    for name, w in win.items():
        f[f"d_rsi_{name}"] = rsi0 - rsi_at(cfi - w)
        f[f"d_vwap_{name}"] = vwd0 - vwd_at(cfi - w)
        f[f"d_imb_{name}"] = imb(cfi - w + 1, cfi)
        f[f"d_ret_{name}"] = (x0 - lg[np.maximum(cfi - w, 0)]) / sig
    f["d_imb_trend"] = f["d_imb_q1"] - f["d_imb_q4"]
    w1, w2 = win["q1"], win["q2"]
    f["d_ret_accel"] = f["d_ret_q1"] - (lg[np.maximum(cfi - w1, 0)] - lg[np.maximum(cfi - 2 * w1, 0)]) / sig
    f["d_vol_accel"] = _log(cf._wmean(s.cv, cfi - w1 + 1, cfi) / np.maximum(cf._wmean(s.cv, cfi - T + 1, cfi - w1), EPS))
    f["d_vol_half"] = _log(cf._wmean(s.cv, cfi - w2 + 1, cfi) / np.maximum(cf._wmean(s.cv, cfi - 2 * w2 + 1, cfi - w2), EPS))
    f["d_trades_accel"] = _log(cf._wmean(s.ctr, cfi - w1 + 1, cfi) / np.maximum(cf._wmean(s.ctr, cfi - T + 1, cfi - w1), EPS))
    rv_a = cf._wsum(s.csq, cfi - w1 + 1, cfi) / w1
    rv_b = cf._wsum(s.csq, cfi - T + 1, cfi - w1) / np.maximum(T - w1, 1)
    f["d_rv_accel"] = 0.5 * _log(rv_a / np.maximum(rv_b, EPS))
    path = cf._wsum(s.car, lo + 1, cfi)
    f["d_reb_er"] = (x0 - L) / np.maximum(path, EPS)
    mid = (lo + cfi) // 2
    f["d_reb_first_half"] = (lg[mid] - L) / np.maximum(x0 - L, EPS)
    f["d_reb_vol_tilt"] = _log(cf._wmean(s.cv, mid + 1, cfi) / np.maximum(cf._wmean(s.cv, lo, mid), EPS))
    f["d_reb_mdd"] = np.array([float(np.max(np.maximum.accumulate(lg[a:b + 1]) - lg[a:b + 1])) for a, b in zip(lo, cfi, strict=True)]) / sig

    # --- g: zigzag sequence --------------------------------------------------------------------------------------
    u1, u2, u3 = Hp - Lp, Hpp - Lpp, H3 - L3
    d1, d2, d3 = Hp - L, Hpp - Lp, H3 - Lpp
    for n, v in (("u1", u1), ("u2", u2), ("u3", u3), ("d1", d1), ("d2", d2), ("d3", d3)):
        f[f"g_{n}"] = v / sig
    f["g_dd1_ratio"] = _log(d1 / np.maximum(d2, EPS))
    f["g_dd2_ratio"] = _log(d2 / np.maximum(d3, EPS))
    f["g_uu1_ratio"] = _log(u1 / np.maximum(u2, EPS))
    f["g_uu2_ratio"] = _log(u2 / np.maximum(u3, EPS))
    f["g_du_ratio"] = _log(d1 / np.maximum(u1, EPS))
    f["g_dur_u1"] = (hp - lp) / T
    f["g_dur_u2"] = (hpp - lpp) / T
    f["g_dur_d1"] = (lo - hp) / T
    f["g_dur_d2"] = (lp - hpp) / T
    f["g_hl_run"] = (Lp < L).astype(float) + (Lp > Lpp) + (Lpp > L3)
    f["g_hh_run"] = (Hp > Hpp).astype(float) + (Hpp > H3)
    f["g_room_hp"] = (Hp - x0) / sig
    f["g_room_hpp"] = (Hpp - x0) / sig
    f["g_room_max3"] = (np.maximum.reduce([Hp, Hpp, H3]) - x0) / sig
    f["g_span_ll"] = (L - L3) / sig

    # --- m: next larger scale (4T), state confirmed at conf ------------------------------------------------------
    sig4 = sigma4[cfi]
    kk = np.searchsorted(cnf4, cfi, side="right") - 1
    okk = kk >= 2
    k0 = np.where(okk, kk, 2)
    last, prev = piv4[k0], piv4[k0 - 1]
    last_typ = typ4[k0]
    is_low = last_typ == -1
    xl, xp = lg[last], lg[prev]
    f["m_up_phase"] = np.where(okk, is_low.astype(float), np.nan)
    f["m_since"] = np.where(okk, (cfi - last) / (4 * T), np.nan)
    f["m_leg_size"] = np.where(okk, np.abs(xl - xp) / sig4, np.nan)
    # fraction of the last confirmed 4T leg already retraced (down-leg after a high) / recovered (up-leg after a low)
    f["m_state_frac"] = np.where(okk, np.where(is_low, (x0 - xl) / np.maximum(np.abs(xp - xl), EPS), (xl - x0) / np.maximum(np.abs(xl - xp), EPS)), np.nan)
    f["m_from_last"] = np.where(okk, (x0 - xl) / sig4, np.nan)
    for name, mult in (("1T", 1), ("2T", 2), ("4T", 4)):
        a = cfi - mult * T
        hi = _slice_stat(lg, a, cfi, np.max)
        lw = _slice_stat(lg, a, cfi, np.min)
        f[f"m_pos_{name}"] = (x0 - lw) / np.maximum(hi - lw, EPS)

    # --- y: Wyckoff-like -----------------------------------------------------------------------------------------
    f["k_spring"] = np.maximum(0.0, (Lp - L) / sig)
    q4 = max(T // 4, 1)
    a_lo, b_lo = lo - q4, np.minimum(cfi, lo + q4)
    a_lp, b_lp = lp - q4, lp + q4
    vol_lo = cf._wmean(s.cv, a_lo, b_lo)
    f["k_retest_vol"] = _log(vol_lo / np.maximum(cf._wmean(s.cv, a_lp, b_lp), EPS))
    rng = _slice_stat(lg, a_lo, b_lo, np.max) - _slice_stat(lg, a_lo, b_lo, np.min)
    f["k_effort_result"] = _log(vol_lo / np.maximum(base7, EPS)) - _log(rng / sig)
    f["k_climax"] = _log(_slice_stat(s.vol, a_lo, lo, np.max) / np.maximum(base7, EPS))

    # --- r: regime ------------------------------------------------------------------------------------------------
    sd30 = sig * np.sqrt(D30 / T)
    f["r_ret30d"] = (x0 - lg[np.maximum(cfi - D30, 0)]) / sd30
    f["r_ret7d"] = (x0 - lg[np.maximum(cfi - D7, 0)]) / (sig * np.sqrt(D7 / T))
    a10 = np.maximum(cfi - 10 * T, 0)
    f["r_er10T"] = np.abs(x0 - lg[a10]) / np.maximum(cf._wsum(s.car, a10 + 1, cfi), EPS)
    sg_clean = np.where(np.isfinite(sigma), sigma, 0.0)
    cs = np.concatenate(([0.0], np.cumsum(sg_clean)))
    cc = np.concatenate(([0.0], np.cumsum((sg_clean > 0).astype(float))))
    a90 = np.maximum(cfi - 90 * 1440, 0)
    m90 = (cs[cfi + 1] - cs[a90]) / np.maximum(cc[cfi + 1] - cc[a90], 1)
    f["r_sig_ratio"] = _log(sig / np.maximum(m90, EPS))
    f["r_dist_hi30d"] = (aux.rmax30[cfi] - x0) / sd30
    f["r_dist_lo30d"] = (x0 - aux.rmin30[cfi]) / sd30
    tm = s.m0 + cfi
    mod = (tm % 1440) / 1440.0
    dow = ((tm // 1440) + 3) % 7
    f["r_tod_sin"], f["r_tod_cos"] = np.sin(2 * np.pi * mod), np.cos(2 * np.pi * mod)
    f["r_dow_sin"], f["r_dow_cos"] = np.sin(2 * np.pi * dow / 7), np.cos(2 * np.pi * dow / 7)
    f["r_weekend"] = (dow >= 5).astype(float)
    j = np.searchsorted(big_end, cfi, side="left") - 1                       # last big move whose end was confirmed before conf
    f["r_since_big"] = np.where(j >= 0, np.log1p(np.maximum(cfi - big_end[np.maximum(j, 0)], 0) / T), np.nan)
    j30 = np.searchsorted(big_end, cfi - D30, side="left")
    f["r_n_big_30d"] = (j + 1 - j30).astype(float)
    jf = np.searchsorted(s.fund_t, s.m0 + cfi, side="right") - 1
    jj = np.clip(jf, 21, None)
    f["r_fund_chg"] = np.where(jf >= 21, s.fund_r[jj] - s.fund_r[jj - 1], np.nan)
    f["r_fund_7d"] = np.where(jf >= 21, (aux.cfund[jj + 1] - aux.cfund[jj - 20]) / 21, np.nan)
    return f


def scale_inputs(s: cf.Series, p: mc.CensusParams, T: int, i0: int, i1: int) -> dict[str, Any]:
    sigma = mc.trailing_sigma(s.logp, T, p.sigma_win)
    sigma4 = mc.trailing_sigma(s.logp, 4 * T, p.sigma_win)
    piv, _, _ = mc._zigzag(s.logp, sigma, sm.K, p.rho, i0, i1)
    piv4, typ4, cnf4 = mc._zigzag(s.logp, sigma4, sm.K, p.rho, i0, i1)
    return {"sigma": sigma, "sigma4": sigma4, "piv": piv, "piv4": piv4, "typ4": typ4, "cnf4": cnf4}


def features_for(s: cf.Series, aux: Aux, T: int, inp: dict[str, Any], c: dict[str, np.ndarray], big_end: np.ndarray) -> tuple[dict[str, np.ndarray], np.ndarray]:
    """All features (epic_078 groups + new groups) of the candidates ``c`` (low, conf, chain, retr, risk_z, prev_up_z); the
    second return is the validity mask (>= 6 earlier pivots)."""
    ctx = pivot_context6(inp["piv"], c["low"])
    row = {**c, **{k: ctx[k] for k in ("hp", "lp", "hpp", "lpp", "h3", "l3")}}
    v1 = cf.compute_features(s, T, inp["sigma"], row)
    v2 = v2_features(s, aux, T, inp["sigma"], inp["piv4"], inp["typ4"], inp["cnf4"], inp["sigma4"], row, big_end)
    return {**v1, **v2}, ctx["ok"]


def cand_arrays(d: pl.DataFrame) -> dict[str, np.ndarray]:
    return {"low": d["low_idx"].to_numpy(), "conf": d["conf_idx"].to_numpy(),
            **{k: d[k].to_numpy().astype(float) for k in ("retr", "risk_z", "prev_up_z", "chain")}}


def build_features(s: cf.Series, p: mc.CensusParams, i0: int, i1: int, sample: pl.DataFrame) -> pl.DataFrame:
    aux = Aux(s)
    parts = []
    for T in sm.SCALES:
        d = sample.filter(pl.col("scale_min") == T).sort("low_idx")
        inp = scale_inputs(s, p, T, i0, i1)
        big_end = np.sort(d.filter(pl.col("y_big") == 1)["end_conf_idx"].to_numpy())
        feats, ok = features_for(s, aux, T, inp, cand_arrays(d), big_end)
        t = pl.DataFrame(feats).with_columns(*[d[c] for c in d.columns], pl.Series("valid", ok))
        parts.append(t.filter(pl.col("valid")).drop("valid"))
        logger.info("T=%d candidates=%d valid=%d", T, d.height, int(ok.sum()))
    return pl.concat(parts)


def feature_names(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if c[:2] in {f"{g}_" for g in ALL_GROUPS} or c.startswith("qd_")]


def lookahead_check(s: cf.Series, p: mc.CensusParams, i0: int, tab: pl.DataFrame, n_sample: int = 12, seed: int = 3) -> int:
    """Recompute the features of random candidates on the series cut at the confirmation bar (zigzags, 4T state, big-move list
    included); they must equal the table values."""
    from reports.builders import move_census_candidates as cd

    rng = np.random.default_rng(seed)
    names = feature_names(tab)
    n_ok = 0
    for T in sm.SCALES:
        t = tab.filter(pl.col("scale_min") == T)
        for q in rng.choice(t.height, size=min(n_sample, t.height), replace=False):
            r = t[int(q)]
            lo, cfi = int(r["low_idx"][0]), int(r["conf_idx"][0])
            sc = s.cut(cfi + 1)
            aux = Aux(sc)
            inp = scale_inputs(sc, p, T, i0, cfi)
            csum = np.concatenate(([0.0], np.cumsum(sc.vol)))
            cc = cd.candidates_cell(sc.logp, csum, inp["sigma"], T, sm.K, p, i0, cfi)
            big_end = np.sort(cc.filter(pl.col("big"))["end_conf_idx"].to_numpy())
            big_end = big_end[big_end < cfi]
            c = {"low": np.array([lo]), "conf": np.array([cfi]),
                 **{k: np.array([float(r[k][0])]) for k in ("retr", "risk_z", "prev_up_z", "chain")}}
            f, ok = features_for(sc, aux, T, inp, c, big_end)
            if not ok[0]:
                raise AssertionError("pivot context differs on the cut series")
            for nme in names:
                a, b = float(f[nme][0]), float(r[nme][0])
                if not (np.isclose(a, b, rtol=1e-8, atol=1e-10) or (np.isnan(a) and np.isnan(b))):
                    raise AssertionError(f"look-ahead: {nme} {a} vs {b} (T={T}, low={lo})")
            n_ok += 1
    return n_ok


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    ap.add_argument("--n-check", type=int, default=12)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    p = mc.CensusParams()
    s = cf.load_series(a.asset_id)
    i0, i1 = mc.window_bounds(mc.load_minutes(a.asset_id), p)
    sample = mc.load_table(a.asset_id, sm.TABLE_SAMPLE)
    tab = build_features(s, p, i0, i1, sample)
    logger.info("features table rows=%d, features=%d", tab.height, len(feature_names(tab)))
    n = lookahead_check(s, p, i0, tab, a.n_check)
    logger.info("look-ahead check passed on %d candidates", n)
    an._write(a.asset_id, {TABLE_FEATURES: tab})
    names = feature_names(tab)
    nan_share = tab.select([pl.col(c).is_nan().mean().alias(c) for c in names] + [pl.col(c).is_null().mean().alias(c + "__null") for c in names[:0]])
    bad = [(c, float(nan_share[c][0])) for c in names if float(nan_share[c][0]) > 0]
    inf = [c for c in names if int(tab[c].is_infinite().sum()) > 0]
    print("groups:", {g: sum(1 for c in names if c[:2] == f"{g}_" or (g == "q" and c.startswith("qd_"))) for g in ALL_GROUPS})
    print("rows", tab.height, "features", len(names), "nan-share>0:", bad, "inf:", inf)


if __name__ == "__main__":
    main()
