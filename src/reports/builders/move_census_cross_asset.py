"""Cross-asset replication and Scout placement of the move census (epic_077 / t954).

* ``--stage run``    : census + anatomy + candidates + sign placebo for one asset (``--asset-id solusdt|ethusdt``); tables go to
  the asset's own lab DB (``research."move_census_v1_*"``, same names as the BTC tables).
* ``--stage scout``  : matches the Scout events (``research."scout_events_v1"``, cell ``W4h_H8h``, train split) to the
  (T = 240 min, k = 3) census candidates / moves of the three assets; result table ``move_census_v1_scout_map`` in the SOL lab DB.

A Scout event is, by construction, almost a (T = 240, k = 3) candidate: ``A >= 1.5 sigma_W`` is a rebound of
``rho * k * sigma_T = 1.5 sigma_T``; and its impulse label (``P* = close[t_c] * exp(A)`` before ``low[t0]``, within H = 8 h) is a
``+1R`` first touch with ``R = close[t_c] - low[t0]`` and a time limit.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime

import duckdb
import numpy as np
import polars as pl

import utils
from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an
from reports.builders import move_census_candidates as cd
from reports.builders import move_census_placebo as pc

logger = logging.getLogger(__name__)
TABLE_SCOUT_MAP = f"move_census_{mc.VERSION}_scout_map"
ASSETS = ("btcusdt", "solusdt", "ethusdt")


def run_asset(asset_id: str, n_plc: int, workers: int) -> None:
    import numpy as np  # noqa: F811

    p = mc.CensusParams()
    m = mc.load_minutes(asset_id)
    i0, i1 = mc.window_bounds(m, p)
    logp = np.log(m.close)
    moves, cells, meta = mc.build_census(m, p, log=False)
    mc.persist(moves, cells, meta, asset_id)
    logger.info("%s census moves=%d", asset_id, moves.height)
    stats = []
    for c in (0.10, 0.33):
        legs = an.leg_features(an.decompose(moves, logp, m.volume, m.m0, c))
        stats.append(an.anatomy_stats(an.move_summary(legs), legs, c))
    an._write(asset_id, {an.TABLE_STATS: pl.concat(stats)})
    cand = cd.candidates_grid(logp, m.volume, p, i0, i1)
    cand_t = cand.with_columns(
        *[pl.from_epoch((pl.col(c) + m.m0) * 60, time_unit="s").cast(pl.Datetime("ms")).alias(n)
          for c, n in (("conf_idx", "conf_time"), ("low_idx", "low_time"), ("end_idx", "end_time"))])
    summ = cd.summarize(cand).with_columns(pl.lit("real").alias("variant"))
    an._write(asset_id, {cd.TABLE_CAND: cand_t, cd.TABLE_CAND_SUM: summ})
    logger.info("%s candidates=%d", asset_id, cand.height)
    plc = pc.run_placebo(asset_id, n_plc, p, kinds=("sign",), workers=workers)
    pc.persist(plc, asset_id)
    cplc = cd.run_placebo_candidates(asset_id, n_plc, p, kinds=("sign",), workers=workers)
    an._write(asset_id, {cd.TABLE_CAND_PLC: cplc})
    logger.info("%s placebos done", asset_id)


def scout_events() -> pl.DataFrame:
    con = duckdb.connect(utils.lab_db_path("solusdt"), read_only=True)
    try:
        return con.execute(
            "SELECT asset_id, t0, t_c, low_t0, close_tc, a_log, a_sigma, p_star, label_class, label_impulse, split "
            "FROM research.scout_events_v1 WHERE cell_id = 'W4h_H8h' AND split = 'train'").pl()
    finally:
        con.close()


def scout_map(tol_min: int = 15) -> pl.DataFrame:
    """For every train Scout event: the (240, 3) candidate whose low pivot is within ``tol_min`` of ``t0`` and the largest
    census scale (k = 3) that has a big move starting at that low."""
    ev = scout_events()
    ws = datetime.fromisoformat(mc.WINDOW_START)
    rows = []
    for asset in ASSETS:
        e = ev.filter((pl.col("asset_id") == asset) & (pl.col("t_c") >= ws))
        if e.height == 0:
            continue
        cand = mc.load_table(asset, cd.TABLE_CAND).filter((pl.col("scale_min") == 240) & (pl.col("k") == 3.0))
        moves = mc.load_table(asset, mc.TABLE_MOVES).filter(pl.col("k") == 3.0)
        lows = cand["low_time"].dt.epoch("ms").to_numpy() // 60_000
        order = np.argsort(lows)
        lows_s = lows[order]
        t0 = e["t0"].dt.epoch("ms").to_numpy() // 60_000
        j = np.clip(np.searchsorted(lows_s, t0), 1, lows_s.size - 1)
        left, right = lows_s[j - 1], lows_s[j]
        pick = np.where(np.abs(t0 - left) <= np.abs(right - t0), j - 1, j)
        dist = np.abs(t0 - lows_s[pick])
        ok = dist <= tol_min
        cm = cand[order[pick]]
        starts = moves["start_time"].dt.epoch("ms").to_numpy() // 60_000
        sc = moves["scale_min"].to_numpy()
        max_scale = np.zeros(e.height, np.int64)
        for q in range(e.height):
            hit = np.abs(starts - t0[q]) <= tol_min
            max_scale[q] = int(sc[hit].max()) if hit.any() else 0
        rows.append(e.with_columns(
            pl.Series("matched", ok), pl.Series("dist_min", dist),
            pl.Series("cand_res1", cm["res1"].to_numpy()), pl.Series("cand_big", cm["big"].to_numpy()),
            pl.Series("cand_final_z", cm["final_z"].to_numpy()), pl.Series("cand_risk_z", cm["risk_z"].to_numpy()),
            pl.Series("cand_cbucket", cm["cbucket"].to_numpy()), pl.Series("max_big_scale_k3", max_scale)))
    return pl.concat(rows)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stage", choices=("run", "scout"), default="run")
    ap.add_argument("--asset-id", default="solusdt")
    ap.add_argument("--n", type=int, default=40)
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    if a.stage == "run":
        run_asset(a.asset_id, a.n, a.workers)
    else:
        sm = scout_map()
        an._write("solusdt", {TABLE_SCOUT_MAP: sm})
        logger.info("scout map rows=%d matched=%d", sm.height, int(sm["matched"].sum()))


if __name__ == "__main__":
    main()
