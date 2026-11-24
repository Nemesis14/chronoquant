"""Time-frame table of the candidate / big-move anatomy (epic_078 / t960).

For the three scales of epic_078 (T = 60, 240, 960 min, k = 4) answers "what time frame do the models live on":
how long until a candidate is known, how long the first impulse of a big move lasts, how big it is, how long the whole
move lasts, and what the typical range of a T-minute window is.  Everything is read from the epic_077 tables plus the
1m grid (train window only).

``uv run python -m reports.builders.cand_pred_timescale``
"""

from __future__ import annotations

import argparse
import logging

import numpy as np
import polars as pl

from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an
from reports.builders import move_census_candidates as cd

logger = logging.getLogger(__name__)

SCALES = (60, 240, 960)
K = 4.0
C_FIRST = 0.33                      # first-impulse level (post-hoc level of epic_077, flagged there)
TABLE = "cand_pred_v1_timescale"


def _q(x: np.ndarray, q: float) -> float:
    return float(np.nanpercentile(x, q)) if x.size else float("nan")


def timescale_table(asset_id: str = "btcusdt") -> pl.DataFrame:
    p = mc.CensusParams()
    m = mc.load_minutes(asset_id)
    i0, i1 = mc.window_bounds(m, p)
    logp = np.log(m.close)
    moves = mc.load_table(asset_id, mc.TABLE_MOVES).filter(pl.col("k") == K)
    cand = mc.load_table(asset_id, cd.TABLE_CAND).filter(pl.col("k") == K)
    rows = []
    for T in SCALES:
        sig = mc.trailing_sigma(logp, T, p.sigma_win)[i0:i1 + 1]
        sig = sig[np.isfinite(sig)]
        # mean range of a T-minute window, sampled every T bars (non-overlapping), in % of the window's first close
        hi = m.high[i0:i1 + 1]
        lo = m.low[i0:i1 + 1]
        cl = m.close[i0:i1 + 1]
        nw = hi.size // T
        wh = np.nanmax(hi[:nw * T].reshape(nw, T), axis=1)
        wl = np.nanmin(lo[:nw * T].reshape(nw, T), axis=1)
        wc = cl[:nw * T].reshape(nw, T)[:, 0]
        rng = (wh - wl) / wc
        c = cand.filter(pl.col("scale_min") == T)
        t_conf = (c["conf_idx"] - c["low_idx"]).to_numpy().astype(float)
        leg_dur = (c["end_idx"] - c["low_idx"]).to_numpy().astype(float)
        big = c["big"].to_numpy()
        mv = moves.filter(pl.col("scale_min") == T)
        legs = an.leg_features(an.decompose(mv, logp, m.volume, m.m0, C_FIRST))
        first = legs.filter(pl.col("j") == 1)
        rows.append({
            "scale_min": T,
            "sigma_T_pct": 100 * float(np.median(sig)),
            "range_T_pct_mean": 100 * float(np.nanmean(rng)),
            "n_candidates": c.height, "p_big": float(big.mean()),
            "conf_after_low_min_med": _q(t_conf, 50), "conf_after_low_min_p25": _q(t_conf, 25), "conf_after_low_min_p75": _q(t_conf, 75),
            "leg_dur_min_med": _q(leg_dur, 50), "leg_dur_big_min_med": _q(leg_dur[big], 50),
            "n_moves": mv.height,
            "move_dur_min_med": _q(mv["dur_min"].to_numpy(), 50), "move_dur_min_p25": _q(mv["dur_min"].to_numpy(), 25),
            "move_dur_min_p75": _q(mv["dur_min"].to_numpy(), 75),
            "move_net_pct_med": 100 * float(np.expm1(np.nanmedian(mv["net_log"].to_numpy()))),
            "first_dur_min_med": _q(first["dur_j"].to_numpy().astype(float), 50),
            "first_dur_min_p25": _q(first["dur_j"].to_numpy().astype(float), 25),
            "first_dur_min_p75": _q(first["dur_j"].to_numpy().astype(float), 75),
            "first_net_pct_med": 100 * float(np.expm1(np.nanmedian(first["net_j"].to_numpy()))),
            "first_share_net_med": float(np.nanmedian((first["net_j"] / first["net_move"]).to_numpy())),
            "first_share_dur_med": float(np.nanmedian((first["dur_j"] / first["dur_move"]).to_numpy())),
        })
    return pl.DataFrame(rows)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    t = timescale_table(a.asset_id)
    an._write(a.asset_id, {TABLE: t})
    with pl.Config(tbl_cols=-1, tbl_rows=-1, tbl_width_chars=250):
        print(t.transpose(include_header=True, column_names=[f"T{s}" for s in t["scale_min"]]))


if __name__ == "__main__":
    main()
