"""Candidate feature table of epic_078 / t961: six variable groups, every value known at the confirmation bar.

A *candidate* (epic_077 / t953) is a confirmed low pivot of the sigma-normalised zigzag at scale ``T`` / size ``k``; it is
known at ``conf_idx`` (the bar whose close shows the rebound).  Everything below uses bars ``<= conf_idx`` only.  Prefix =
group: ``z_`` zigzag geometry, ``v_`` volume, ``w_`` VWAP, ``f_`` trading data (taker flow, trade count, funding),
``q_`` quant / classic (multi-horizon context, RSI_T, vol state, 1m quant features), ``s_`` shapes (Elliott-like ratios).

Distances in sigma units use ``sigma_T`` at the confirmation bar (trailing, past-only); durations are in units of ``T``.

``uv run python -m reports.builders.cand_pred_features`` (builds the table + runs the look-ahead check).
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import polars as pl

from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an
from reports.builders import move_census_candidates as cd

logger = logging.getLogger(__name__)

SCALES = (60, 240, 960)
K = 4.0
TABLE_FEATURES = "cand_pred_v1_features"
GROUPS: dict[str, str] = {"z": "zigzag", "v": "volume", "w": "vwap", "f": "trading", "q": "quant", "s": "shapes"}
D7 = 7 * 1440
D1 = 1440
D30 = 30 * 1440
EPS = 1e-12


def _log(x: np.ndarray) -> np.ndarray:
    return np.log(np.maximum(x, EPS))


@dataclass
class Series:
    """1m grid arrays + the cumulative sums the features are built from. ``cut(n)`` rebuilds it from the first ``n`` bars."""

    m0: int
    logp: np.ndarray
    vol: np.ndarray
    quote: np.ndarray
    tbuy: np.ndarray
    trades: np.ndarray
    fund_t: np.ndarray                       # epoch minutes of the funding settlements
    fund_r: np.ndarray
    cv: np.ndarray = field(init=False)
    cq: np.ndarray = field(init=False)
    ctb: np.ndarray = field(init=False)
    ctr: np.ndarray = field(init=False)
    cup: np.ndarray = field(init=False)
    cdn: np.ndarray = field(init=False)
    csq: np.ndarray = field(init=False)
    cp: np.ndarray = field(init=False)
    cp2: np.ndarray = field(init=False)
    car: np.ndarray = field(init=False)

    def __post_init__(self) -> None:
        z = np.zeros(1)
        r = np.diff(self.logp, prepend=self.logp[0])
        self.cv = np.concatenate((z, np.cumsum(self.vol)))
        self.cq = np.concatenate((z, np.cumsum(self.quote)))
        self.ctb = np.concatenate((z, np.cumsum(self.tbuy)))
        self.ctr = np.concatenate((z, np.cumsum(self.trades)))
        self.cup = np.concatenate((z, np.cumsum(np.maximum(r, 0.0))))
        self.cdn = np.concatenate((z, np.cumsum(np.maximum(-r, 0.0))))
        self.csq = np.concatenate((z, np.cumsum(r * r)))
        self.cp = np.concatenate((z, np.cumsum(self.logp - self.logp[0])))
        self.cp2 = np.concatenate((z, np.cumsum((self.logp - self.logp[0]) ** 2)))
        self.car = np.concatenate((z, np.cumsum(np.abs(r))))

    @property
    def n(self) -> int:
        return int(self.logp.size)

    def cut(self, n: int) -> Series:
        return Series(self.m0, self.logp[:n], self.vol[:n], self.quote[:n], self.tbuy[:n], self.trades[:n],
                      self.fund_t[self.fund_t <= self.m0 + n - 1], self.fund_r[self.fund_t <= self.m0 + n - 1])


def _wsum(c: np.ndarray, a: np.ndarray, b: np.ndarray) -> np.ndarray:
    a = np.maximum(a, 0)
    return c[b + 1] - c[a]


def _wmean(c: np.ndarray, a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return _wsum(c, a, b) / np.maximum(b - np.maximum(a, 0) + 1, 1)


def _vwap(s: Series, a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return _wsum(s.cq, a, b) / np.maximum(_wsum(s.cv, a, b), EPS)


def _wext(x: np.ndarray, a: np.ndarray, b: np.ndarray, fn: Any) -> np.ndarray:
    return np.array([fn(x[max(int(i), 0):int(j) + 1]) for i, j in zip(a, b, strict=True)])


# --------------------------------------------------------------------------------------------------------------
# data
# --------------------------------------------------------------------------------------------------------------

def _read(sql: str, params: list[Any], asset_id: str) -> Any:
    from reports.builders._common import read_live

    for attempt in range(20):
        df = read_live(sql, params, asset_id=asset_id)
        if df is not None:
            return df
        logger.warning("live db of %s locked (attempt %d/20)", asset_id, attempt + 1)
        time.sleep(3.0)
    raise RuntimeError(f"live db of {asset_id} is not readable")


def _epoch_min(col: Any) -> np.ndarray:
    return np.asarray(col).astype("datetime64[m]").astype("int64")


def load_series(asset_id: str, end: str = mc.WINDOW_END) -> Series:
    """Train-window grid with the extra columns; OOS bars never enter memory."""
    m = mc.load_minutes(asset_id, end)
    n, m0 = m.n, m.m0
    ex = _read("SELECT open_time, quote_volume, trades, taker_buy_base FROM ohlcv WHERE open_time <= ? ORDER BY open_time",
               [end], asset_id)
    pos = _epoch_min(ex["open_time"]) - m0
    ok = (pos >= 0) & (pos < n)
    arrs: dict[str, np.ndarray] = {}
    for col in ("quote_volume", "trades", "taker_buy_base"):
        a = np.zeros(n)
        a[pos[ok]] = np.nan_to_num(ex[col].to_numpy(dtype=float)[ok])
        arrs[col] = a
    fr = _read("SELECT funding_ts, funding_rate FROM funding_rate WHERE funding_ts <= ? ORDER BY funding_ts", [end], asset_id)
    return Series(m0=m0, logp=np.log(m.close), vol=np.nan_to_num(m.volume), quote=arrs["quote_volume"],
                  tbuy=arrs["taker_buy_base"], trades=arrs["trades"], fund_t=_epoch_min(fr["funding_ts"]),
                  fund_r=fr["funding_rate"].to_numpy(dtype=float))


# --------------------------------------------------------------------------------------------------------------
# features
# --------------------------------------------------------------------------------------------------------------

def pivot_context(piv: np.ndarray, low_idx: np.ndarray) -> dict[str, np.ndarray]:
    """The four pivots before each candidate low (``hp`` high, ``lp`` low, ``hpp`` high, ``lpp`` low) and a validity mask."""
    pos = np.searchsorted(piv, low_idx)
    if not np.array_equal(piv[pos], low_idx):
        raise ValueError("candidate low is not a pivot of the zigzag")
    ok = pos >= 4
    q = np.where(ok, pos, 4)
    return {"hp": piv[q - 1], "lp": piv[q - 2], "hpp": piv[q - 3], "lpp": piv[q - 4], "ok": ok}


def compute_features(s: Series, T: int, sigma: np.ndarray, row: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """All features of the candidates in ``row`` (arrays: low, conf, hp, lp, hpp, lpp, retr, risk_z, prev_up_z, chain)."""
    lo, cf, hp, lp, hpp, lpp = (row[k].astype(np.int64) for k in ("low", "conf", "hp", "lp", "hpp", "lpp"))
    lg = s.logp
    sig = sigma[cf]
    x0, L, Hp, Lp, Hpp, Lpp = lg[cf], lg[lo], lg[hp], lg[lp], lg[hpp], lg[lpp]
    f: dict[str, np.ndarray] = {}

    # --- z: zigzag geometry -------------------------------------------------------------------------------------
    f["z_risk"] = row["risk_z"]
    f["z_prev_up"] = row["prev_up_z"]
    f["z_retr"] = row["retr"]
    f["z_chain"] = row["chain"].astype(float)
    f["z_conf_dur"] = (cf - lo) / T
    f["z_down_dur"] = (lo - hp) / T
    f["z_up_dur"] = (hp - lp) / T
    f["z_reb_vs_down_dur"] = _log((cf - lo) / np.maximum(lo - hp, 1))
    reb_speed = (x0 - L) / np.maximum(cf - lo, 1)
    up_speed = (Hp - Lp) / np.maximum(hp - lp, 1)
    f["z_speed_ratio"] = _log(reb_speed / np.maximum(up_speed, EPS))
    f["z_low_vs_prev_low"] = (L - Lp) / sig
    f["z_down_z"] = (Hp - L) / sig

    # --- v: volume (log ratio to the trailing 7-day mean volume) ------------------------------------------------
    a7 = cf - D7 + 1
    base = _wmean(s.cv, a7, cf)
    v_reb = _log(_wmean(s.cv, lo, cf) / np.maximum(base, EPS))
    v_down = _log(_wmean(s.cv, hp, lo) / np.maximum(base, EPS))
    f["v_reb"] = v_reb
    f["v_down"] = v_down
    f["v_up"] = _log(_wmean(s.cv, lp, hp) / np.maximum(base, EPS))
    f["v_reb_vs_down"] = v_reb - v_down
    f["v_last_T"] = _log(_wmean(s.cv, cf - T + 1, cf) / np.maximum(base, EPS))
    f["v_at_low"] = _log(_wmean(s.cv, lo - T // 4, np.minimum(cf, lo + T // 4)) / np.maximum(base, EPS))

    # --- w: VWAP distances in sigma units -----------------------------------------------------------------------
    f["w_1d"] = (x0 - _log(_vwap(s, cf - D1 + 1, cf))) / sig
    f["w_7d"] = (x0 - _log(_vwap(s, a7, cf))) / sig
    f["w_anch_hp"] = (x0 - _log(_vwap(s, hp, cf))) / sig
    f["w_anch_low"] = (x0 - _log(_vwap(s, lo, cf))) / sig
    f["w_anch_lp"] = (x0 - _log(_vwap(s, lp, cf))) / sig
    f["w_low_vs_1d"] = (L - _log(_vwap(s, cf - D1 + 1, cf))) / sig

    # --- f: trading data ----------------------------------------------------------------------------------------
    def tb(a: np.ndarray, b: np.ndarray) -> np.ndarray:
        return _wsum(s.ctb, a, b) / np.maximum(_wsum(s.cv, a, b), EPS)

    tb7 = tb(a7, cf)
    f["f_tb_reb"] = tb(lo, cf) - tb7
    f["f_tb_down"] = tb(hp, lo) - tb7
    f["f_tb_last_T"] = tb(cf - T + 1, cf) - tb7
    size7 = _wsum(s.cv, a7, cf) / np.maximum(_wsum(s.ctr, a7, cf), EPS)
    f["f_size_reb"] = _log(_wsum(s.cv, lo, cf) / np.maximum(_wsum(s.ctr, lo, cf), EPS) / np.maximum(size7, EPS))
    f["f_trades_reb"] = _log(_wmean(s.ctr, lo, cf) / np.maximum(_wmean(s.ctr, a7, cf), EPS))
    j = np.searchsorted(s.fund_t, s.m0 + cf, side="right") - 1
    jj = np.clip(j, 2, None)
    f["f_fund"] = np.where(j >= 2, s.fund_r[jj], np.nan)
    f["f_fund_3"] = np.where(j >= 2, (s.fund_r[jj] + s.fund_r[jj - 1] + s.fund_r[jj - 2]) / 3, np.nan)

    # --- q: quant / classic -------------------------------------------------------------------------------------
    for h in (1, 3, 10, 30):
        f[f"q_ret_{h}T"] = (x0 - lg[np.maximum(cf - h * T, 0)]) / sig
    for h in (10, 30):
        a = cf - h * T
        hi = _wext(lg, a, cf, np.max)
        lw = _wext(lg, a, cf, np.min)
        f[f"q_pos_{h}T"] = (x0 - lw) / np.maximum(hi - lw, EPS)
    up = _wsum(s.cup, cf - T + 1, cf)
    dn = _wsum(s.cdn, cf - T + 1, cf)
    f["q_rsi_T"] = up / np.maximum(up + dn, EPS)
    rv_T = _wsum(s.csq, cf - T + 1, cf) / T
    rv_30 = _wsum(s.csq, cf - D30 + 1, cf) / D30
    f["q_rv_ratio"] = 0.5 * _log(rv_T / np.maximum(rv_30, EPS))
    # 1m quant analogues, from closes only (so they can be rebuilt on a synthetic price path)
    u14 = _wsum(s.cup, cf - 13, cf)
    d14 = _wsum(s.cdn, cf - 13, cf)
    f["qd_rsi14"] = u14 / np.maximum(u14 + d14, EPS)
    a140 = cf - 139
    m1 = _wsum(s.cp, a140, cf) / 140
    var = np.maximum(_wsum(s.cp2, a140, cf) / 140 - m1 * m1, 0.0)
    off = lg[0]
    f["qd_bbpos140"] = 0.5 + ((x0 - off) - m1) / np.maximum(4 * np.sqrt(var), EPS)
    f["qd_absret14"] = _log(_wsum(s.car, cf - 13, cf) / 14)

    # --- s: shapes (Elliott-like ratios) ------------------------------------------------------------------------
    retr = row["retr"]
    f["s_fib382"] = np.abs(retr - 0.382)
    f["s_fib500"] = np.abs(retr - 0.5)
    f["s_fib618"] = np.abs(retr - 0.618)
    f["s_reb_over_down"] = (x0 - L) / np.maximum(Hp - L, EPS)
    f["s_up_vs_pp"] = _log((Hp - Lp) / np.maximum(Hpp - Lpp, EPS))
    f["s_alt"] = _log((Hp - L) / np.maximum(Hpp - Lp, EPS))
    f["s_up_longest"] = ((Hp - Lp) > (Hpp - Lpp)).astype(float)
    return f


def build_features(s: Series, p: mc.CensusParams, i0: int, i1: int, cand: pl.DataFrame) -> pl.DataFrame:
    """Feature + target table of the (T in SCALES, k = K) candidates."""
    parts = []
    for T in SCALES:
        c = cand.filter((pl.col("scale_min") == T) & (pl.col("k") == K)).sort("low_idx")
        sigma = mc.trailing_sigma(s.logp, T, p.sigma_win)
        piv, _, _ = mc._zigzag(s.logp, sigma, K, p.rho, i0, i1)
        ctx = pivot_context(piv, c["low_idx"].to_numpy())
        ok = ctx["ok"]
        row = {"low": c["low_idx"].to_numpy(), "conf": c["conf_idx"].to_numpy(), **{k: ctx[k] for k in ("hp", "lp", "hpp", "lpp")},
               **{k: c[k].to_numpy().astype(float) for k in ("retr", "risk_z", "prev_up_z", "chain")}}
        feats = compute_features(s, T, sigma, row)
        d = pl.DataFrame({**feats}).with_columns(
            c["scale_min"], c["k"], c["low_idx"], c["conf_idx"], c["end_conf_idx"], c["conf_time"], c["low_time"], c["big"],
            c["res1"], c["res2"], c["res3"], c["resolve_bars"], c["cost_r"], pl.Series("valid", ok))
        parts.append(d.filter(pl.col("valid")).drop("valid"))
        logger.info("T=%d candidates=%d with 4 prior pivots=%d", T, c.height, int(ok.sum()))
    return pl.concat(parts)


# --------------------------------------------------------------------------------------------------------------
# look-ahead check
# --------------------------------------------------------------------------------------------------------------

def lookahead_check(s: Series, p: mc.CensusParams, i0: int, cand: pl.DataFrame, table: pl.DataFrame, n_sample: int = 25,
                    seed: int = 11) -> int:
    """Recompute the features of random candidates on data cut at the confirmation bar; they must be identical.
    Also checks that the zigzag run on the cut series reproduces the pivots up to the candidate low."""
    rng = np.random.default_rng(seed)
    n_checked = 0
    feat_cols = [c for c in table.columns if c[:2] in {f"{g}_" for g in GROUPS} or c[:3] == "qd_"]
    for T in SCALES:
        t = table.filter(pl.col("scale_min") == T)
        pick = rng.choice(t.height, size=min(n_sample, t.height), replace=False)
        sigma_full = mc.trailing_sigma(s.logp, T, p.sigma_win)
        piv_full, _, _ = mc._zigzag(s.logp, sigma_full, K, p.rho, i0, s.n - 1)
        for q in pick:
            r = t[int(q)]
            lo, cf = int(r["low_idx"][0]), int(r["conf_idx"][0])
            sc = s.cut(cf + 1)
            sg = mc.trailing_sigma(sc.logp, T, p.sigma_win)
            piv, typ, _ = mc._zigzag(sc.logp, sg, K, p.rho, i0, cf)
            ctx = pivot_context(piv, np.array([lo]))
            full_ctx = pivot_context(piv_full, np.array([lo]))
            for k in ("hp", "lp", "hpp", "lpp"):
                if ctx[k][0] != full_ctx[k][0]:
                    raise AssertionError(f"pivot {k} differs on cut series (T={T}, low={lo})")
            c = cand.filter((pl.col("scale_min") == T) & (pl.col("k") == K) & (pl.col("low_idx") == lo))
            row = {"low": np.array([lo]), "conf": np.array([cf]), **{k: ctx[k] for k in ("hp", "lp", "hpp", "lpp")},
                   **{k: c[k].to_numpy().astype(float) for k in ("retr", "risk_z", "prev_up_z", "chain")}}
            f = compute_features(sc, T, sg, row)
            for name in feat_cols:
                a, b = float(f[name][0]), float(r[name][0])
                if not (np.isclose(a, b, rtol=1e-9, atol=1e-12) or (np.isnan(a) and np.isnan(b))):
                    raise AssertionError(f"look-ahead: {name} differs ({a} vs {b}) T={T} low={lo}")
            n_checked += 1
    return n_checked


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    ap.add_argument("--n-check", type=int, default=25)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    p = mc.CensusParams()
    s = load_series(a.asset_id)
    m = mc.load_minutes(a.asset_id)
    i0, i1 = mc.window_bounds(m, p)
    cand = mc.load_table(a.asset_id, cd.TABLE_CAND)
    tab = build_features(s, p, i0, i1, cand)
    n = lookahead_check(s, p, i0, cand, tab, a.n_check)
    logger.info("look-ahead check passed on %d candidates", n)
    an._write(a.asset_id, {TABLE_FEATURES: tab})
    feat_cols = [c for c in tab.columns if c[:2] in {f"{g}_" for g in GROUPS} or c[:3] == "qd_"]
    miss = tab.select([pl.col(c).is_null().mean().alias(c) for c in feat_cols]).transpose(include_header=True, column_names=["null_share"])
    with pl.Config(tbl_rows=-1, tbl_width_chars=200):
        print(tab.group_by("scale_min").len().sort("scale_min"))
        print(miss.filter(pl.col("null_share") > 0))


if __name__ == "__main__":
    main()
