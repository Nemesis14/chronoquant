"""Anatomy of the large moves (epic_077 / t952) — leg decomposition, cycle structure, scale invariance. Pure library.

Every big up-move ``[s, e]`` of :mod:`reports.builders.move_census` (low pivot -> high pivot) is split into alternating
*up legs* (impulses) and *down legs* (corrections) by a zigzag **relative to the move's own size**: a leg reverses when the
price retraces ``C_FRAC * net`` from its extreme. Pre-registered: primary ``C_FRAC = 0.10``, sensitivity 0.05 / 0.20.

Vocabulary (no Elliott counting): up leg ``j`` (1-based) is entered after ``j - 1`` complete impulse-correction cycles
(*cycle number* ``c = j - 1``; ``c = 0`` = the first impulse, ``c = 1`` = the impulse after one cycle, ``c >= 2`` = later).
The decomposition is hindsight (the move is complete); it describes shapes, it does not forecast.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl
from numba import njit

from reports.builders import move_census as mc


def _f(x: Any) -> float:
    return float(x)


C_PRIMARY = 0.10
C_SENS = (0.05, 0.20)


@njit(cache=True)
def _decomp(logp: np.ndarray, s: int, e: int, theta: float) -> np.ndarray:
    """Alternating pivots ``low(s), high, low, ..., high(e)`` of the zigzag with fixed reversal ``theta`` inside ``[s, e]``."""
    piv = np.empty(e - s + 2, np.int64)
    n = 1
    piv[0] = s
    mode = 1
    ext = logp[s]
    ext_i = s
    for i in range(s + 1, e + 1):
        x = logp[i]
        if mode == 1:
            if x > ext:
                ext = x
                ext_i = i
            elif ext - x >= theta:
                piv[n] = ext_i
                n += 1
                mode = -1
                ext = x
                ext_i = i
        else:
            if x < ext:
                ext = x
                ext_i = i
            elif x - ext >= theta:
                piv[n] = ext_i
                n += 1
                mode = 1
                ext = x
                ext_i = i
    if mode == 1 and piv[n - 1] != ext_i:
        piv[n] = ext_i
        n += 1
    return piv[:n]


def _idx(col: pl.Series, m0: int) -> np.ndarray:
    return col.dt.epoch("ms").to_numpy() // 60_000 - m0


def decompose(moves: pl.DataFrame, logp: np.ndarray, volume: np.ndarray, m0: int, c_frac: float = C_PRIMARY) -> pl.DataFrame:
    """One row per up leg of every move (``move_id`` = row number of ``moves``)."""
    s_all, e_all = _idx(moves["start_time"], m0), _idx(moves["end_time"], m0)
    sc, kk = moves["scale_min"].to_numpy(), moves["k"].to_numpy()
    csum = np.concatenate(([0.0], np.cumsum(np.nan_to_num(volume))))
    cols: dict[str, list[float]] = {c: [] for c in (
        "move_id", "scale_min", "k", "j", "n_up", "net_j", "dur_j", "net_move", "dur_move", "prev_down_net",
        "prev_down_dur", "vol_j", "vol_prev_down", "start_off", "end_off")}
    for mid in range(s_all.size):
        s, e = int(s_all[mid]), int(e_all[mid])
        net = float(logp[e] - logp[s])
        piv = _decomp(logp, s, e, c_frac * net)
        n_up = piv.size // 2
        for j in range(n_up):
            a, b = int(piv[2 * j]), int(piv[2 * j + 1])
            cols["move_id"].append(mid)
            cols["scale_min"].append(sc[mid])
            cols["k"].append(kk[mid])
            cols["j"].append(j + 1)
            cols["n_up"].append(n_up)
            cols["net_j"].append(float(logp[b] - logp[a]))
            cols["dur_j"].append(b - a)
            cols["net_move"].append(net)
            cols["dur_move"].append(e - s)
            cols["start_off"].append(a - s)
            cols["end_off"].append(b - s)
            cols["vol_j"].append(float((csum[b + 1] - csum[a]) / (b - a + 1)))
            if j > 0:
                pa = int(piv[2 * j - 1])
                cols["prev_down_net"].append(float(logp[pa] - logp[a]))
                cols["prev_down_dur"].append(a - pa)
                cols["vol_prev_down"].append(float((csum[a + 1] - csum[pa]) / (a - pa + 1)))
            else:
                cols["prev_down_net"].append(float("nan"))
                cols["prev_down_dur"].append(float("nan"))
                cols["vol_prev_down"].append(float("nan"))
    return pl.DataFrame(cols).with_columns(
        pl.col("move_id").cast(pl.Int64), pl.col("scale_min").cast(pl.Int32), pl.col("j").cast(pl.Int32),
        pl.col("n_up").cast(pl.Int32),
    )


def leg_features(legs: pl.DataFrame) -> pl.DataFrame:
    """Scale-free ratios per up leg (``*_vs_first`` relative to leg 1 of the same move)."""
    first = legs.filter(pl.col("j") == 1).select(
        "move_id", pl.col("net_j").alias("net1"), pl.col("dur_j").alias("dur1"), pl.col("vol_j").alias("vol1"))
    prev = legs.select("move_id", (pl.col("j") + 1).alias("j"), pl.col("net_j").alias("net_prev"),
                       pl.col("dur_j").alias("dur_prev"))
    out = legs.join(first, on="move_id").join(prev, on=["move_id", "j"], how="left")
    return out.with_columns(
        (pl.col("net_j") / pl.col("net_move")).alias("share_net"),
        (pl.col("dur_j") / pl.col("dur_move")).alias("share_dur"),
        (pl.col("prev_down_net") / pl.col("net_prev")).alias("retr_before"),          # correction depth vs the impulse before
        (pl.col("prev_down_dur") / pl.col("dur_prev")).alias("corr_dur_ratio"),
        ((pl.col("net_j") / pl.col("dur_j").clip(lower_bound=1)) /
         (pl.col("net1") / pl.col("dur1").clip(lower_bound=1))).alias("speed_vs_first"),
        (pl.col("net_j") / pl.col("net1")).alias("net_vs_first"),
        (pl.col("vol_prev_down") / pl.col("vol_j")).alias("vol_corr_vs_impulse"),
        (pl.col("vol_j") / pl.col("vol1")).alias("vol_vs_first"),
        pl.col("j").clip(upper_bound=3).alias("cycle_bucket"),                         # 1 = c0, 2 = c1, 3 = c>=2
    )


def move_summary(legs: pl.DataFrame) -> pl.DataFrame:
    """One row per move: cycle count, first-leg shares, first correction depth."""
    g = legs.group_by("move_id").agg(
        pl.col("scale_min").first(), pl.col("k").first(), pl.col("n_up").first(), pl.col("net_move").first(),
        pl.col("dur_move").first(),
        pl.col("net_j").filter(pl.col("j") == 1).first().alias("net1"),
        pl.col("dur_j").filter(pl.col("j") == 1).first().alias("dur1"),
        pl.col("prev_down_net").filter(pl.col("j") == 2).first().alias("down1_net"),
        pl.col("net_j").max().alias("net_max"),
        pl.col("j").sort_by("net_j", descending=True).first().alias("j_of_max"),
    )
    return g.with_columns(
        (pl.col("net1") / pl.col("net_move")).alias("first_share_net"),
        (pl.col("dur1") / pl.col("dur_move")).alias("first_share_dur"),
        (pl.col("down1_net") / pl.col("net1")).alias("retr1"),
        (pl.col("n_up") - 1).alias("cycles"),
        (pl.col("net_max") / pl.col("net_move")).alias("max_share_net"),
    ).sort("move_id")


def ks_to_pooled(values: pl.DataFrame, col: str, by: str = "scale_min", min_n: int = 30) -> pl.DataFrame:
    """KS distance of each group's distribution to the pooled one (a distance, **not** a p-value: nested moves are not independent)."""
    x_all = np.sort(values[col].drop_nulls().drop_nans().to_numpy())
    rows = []
    for key, g in values.group_by(by, maintain_order=True):
        x = np.sort(g[col].drop_nulls().drop_nans().to_numpy())
        if x.size < min_n:
            continue
        grid = np.unique(np.concatenate([x, x_all]))
        d = float(np.max(np.abs(np.searchsorted(x, grid, side="right") / x.size
                                - np.searchsorted(x_all, grid, side="right") / x_all.size)))
        rows.append({by: key[0], "n": int(x.size), "median": float(np.median(x)), "ks_to_pooled": d})
    return pl.DataFrame(rows).sort(by)


def loglog_slope(cells: pl.DataFrame, k: float, col: str = "n_big", min_count: int = 10) -> float:
    """OLS slope of ``log(count)`` on ``log(T)`` over the scales of one k (cells with fewer than ``min_count`` skipped)."""
    d = cells.filter((pl.col("k") == k) & (pl.col(col) >= min_count)).sort("scale_min")
    if d.height < 3:
        return float("nan")
    x, y = np.log(d["scale_min"].to_numpy()), np.log(d[col].to_numpy())
    return float(np.polyfit(x, y, 1)[0])


def run_on_series(logp: np.ndarray, volume: np.ndarray, m0: int, p: mc.CensusParams, i0: int, i1: int,
                  c_frac: float = C_PRIMARY) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Census + decomposition on one series -> ``(cells, legs, move_summary)`` (used for the real series and the placebos)."""
    rows, frames, scale_rows = mc.run_grid(logp, m0, p, i0, i1, with_moves=True)
    moves = pl.concat(frames)
    cells = pl.DataFrame(rows).join(pl.DataFrame(scale_rows), on="scale_min")
    legs = leg_features(decompose(moves, logp, volume, m0, c_frac))
    return cells, legs, move_summary(legs)


# --------------------------------------------------------------------------------------------------------------
# long-format statistics + placebo comparison
# --------------------------------------------------------------------------------------------------------------

#: pre-registered 0.10 (primary), 0.05 / 0.20 (sensitivity); 0.33 added *after* seeing that 0.10 splits the median move
#: into ~4 impulses (documented in the ticket) — the coarse level that is closest to a "1-2-1-2" reading
C_LEVELS = (0.05, 0.10, 0.20, 0.33)


def anatomy_stats(ms: pl.DataFrame, legs: pl.DataFrame, c_frac: float) -> pl.DataFrame:
    """Long-format statistics ``(scale_min, k, c_frac, stat, value)``; ``scale_min = 0`` pools all scales of a k."""
    out: list[dict[str, Any]] = []

    def add(sc: int, k: float, d_ms: pl.DataFrame, d_lg: pl.DataFrame) -> None:
        n = d_ms.height
        if n == 0:
            return
        cyc = d_ms["cycles"].to_numpy()
        jm = d_ms["j_of_max"].to_numpy()
        stats = {
            "n_moves": float(n), "cyc0": float(np.mean(cyc == 0)), "cyc1": float(np.mean(cyc == 1)),
            "cyc2": float(np.mean(cyc == 2)), "cyc3p": float(np.mean(cyc >= 3)), "cycles_mean": float(np.mean(cyc)),
            "first_share_net_med": _f(d_ms["first_share_net"].median()),
            "first_share_dur_med": _f(d_ms["first_share_dur"].median()),
            "retr1_med": _f(d_ms["retr1"].drop_nulls().median()) if d_ms["retr1"].drop_nulls().len() else float("nan"),
            "max_share_net_med": _f(d_ms["max_share_net"].median()),
            "jmax1": float(np.mean(jm == 1)), "jmax2": float(np.mean(jm == 2)), "jmax3p": float(np.mean(jm >= 3)),
        }
        for b, name in ((2, "2"), (3, "3p")):
            g = d_lg.filter(pl.col("cycle_bucket") == b)
            if g.height:
                stats[f"speed{name}_med"] = _f(g["speed_vs_first"].median())
                stats[f"netvs{name}_med"] = _f(g["net_vs_first"].median())
                stats[f"retr_before{name}_med"] = _f(g["retr_before"].drop_nulls().median())
                stats[f"volcorr{name}_med"] = _f(g["vol_corr_vs_impulse"].drop_nulls().drop_nans().median())
        for st, v in stats.items():
            out.append({"scale_min": sc, "k": k, "c_frac": c_frac, "stat": st, "value": v})

    for k in sorted(ms["k"].unique().to_list()):
        mk, lk = ms.filter(pl.col("k") == k), legs.filter(pl.col("k") == k)
        add(0, k, mk, lk)
        for sc in sorted(mk["scale_min"].unique().to_list()):
            add(int(sc), k, mk.filter(pl.col("scale_min") == sc), lk.filter(pl.col("scale_min") == sc))
    return pl.DataFrame(out)


def compare(real: pl.DataFrame, plc: pl.DataFrame) -> pl.DataFrame:
    """Real vs placebo per (kind, scale_min, k, c_frac, stat): placebo mean / 2.5 / 97.5 % and two-sided empirical p."""
    keys = ["scale_min", "k", "c_frac", "stat"]
    rows = []
    for kind in plc["kind"].unique().sort():
        pk = plc.filter(pl.col("kind") == kind)
        for key, g in pk.group_by(keys, maintain_order=True):
            r = real.filter((pl.col("scale_min") == key[0]) & (pl.col("k") == key[1]) & (pl.col("c_frac") == key[2])
                            & (pl.col("stat") == key[3]))
            x = g["value"].drop_nulls().drop_nans().to_numpy()
            if r.height == 0 or x.size < 5 or not np.isfinite(r["value"][0]):
                continue
            obs, mu = float(r["value"][0]), float(x.mean())
            rows.append({"kind": kind, "scale_min": key[0], "k": key[1], "c_frac": key[2], "stat": key[3], "real": obs,
                         "plc_mean": mu, "plc_p025": float(np.percentile(x, 2.5)), "plc_p975": float(np.percentile(x, 97.5)),
                         "p_two": (1 + float(np.sum(np.abs(x - mu) >= abs(obs - mu)))) / (x.size + 1), "n_rep": int(x.size)})
    return pl.DataFrame(rows)


_A: dict = {}


def _a_init(asset_id: str, p_json: str) -> None:

    m = mc.load_minutes(asset_id)
    _A.update(logp=np.log(m.close), vol=m.volume, m0=m.m0)
    _A["p"] = mc.CensusParams.from_json(p_json)
    _A["win"] = mc.window_bounds(m, _A["p"])


def _a_one(job: tuple[str, int, int, tuple[float, ...]]) -> pl.DataFrame:
    from reports.builders.move_census_placebo import placebo_logp

    kind, rep, seed, cs = job
    lp = placebo_logp(_A["logp"], kind, np.random.default_rng(seed))
    i0, i1 = _A["win"]
    p = _A["p"]
    rows, frames, _ = mc.run_grid(lp, _A["m0"], p, i0, i1, with_moves=True)
    moves = pl.concat(frames).filter(pl.col("k") == 4.0)          # placebo anatomy: k = 4 only (cost)
    parts = []
    for c in cs:
        legs = leg_features(decompose(moves, lp, _A["vol"], _A["m0"], c))
        parts.append(anatomy_stats(move_summary(legs), legs, c))
    return pl.concat(parts).with_columns(pl.lit(kind).alias("kind"), pl.lit(rep).alias("rep"))


def run_placebo_anatomy(asset_id: str, n: int, p: mc.CensusParams | None = None, *, cs: tuple[float, ...] = C_LEVELS,
                        kinds: tuple[str, ...] = ("sign", "block"), seed0: int = 20261125, workers: int = 4) -> pl.DataFrame:
    import logging
    import time
    from concurrent.futures import ProcessPoolExecutor

    log = logging.getLogger(__name__)
    p = p or mc.CensusParams()
    jobs = [(kind, rep, seed0 + 1000 * ki + rep, cs) for ki, kind in enumerate(kinds) for rep in range(n)]
    parts = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=workers, initializer=_a_init, initargs=(asset_id, p.to_json())) as ex:
        for i, df in enumerate(ex.map(_a_one, jobs, chunksize=1)):
            parts.append(df)
            if (i + 1) % 10 == 0:
                log.info("anatomy placebo %d / %d (%.0fs)", i + 1, len(jobs), time.time() - t0)
    return pl.concat(parts)


# --------------------------------------------------------------------------------------------------------------
# runner
# --------------------------------------------------------------------------------------------------------------

TABLE_LEGS = f"move_census_{mc.VERSION}_legs"
TABLE_STATS = f"move_census_{mc.VERSION}_anat_stats"
TABLE_PLC = f"move_census_{mc.VERSION}_anat_placebo"


def _write(asset_id: str, tables: dict[str, pl.DataFrame], tries: int = 20, delay: float = 3.0) -> None:
    import logging
    import time

    import duckdb

    import utils

    log = logging.getLogger(__name__)
    path = utils.lab_db_path(asset_id)
    last: Exception | None = None
    for attempt in range(tries):
        try:
            con = duckdb.connect(path)
        except duckdb.Error as exc:
            last = exc
            log.warning("lab db locked (attempt %d/%d): %s", attempt + 1, tries, exc)
            time.sleep(delay)
            continue
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {mc.SCHEMA}")
            for name, df in tables.items():
                arr = df.to_arrow()  # noqa: F841
                con.execute(f'CREATE OR REPLACE TABLE {mc.SCHEMA}."{name}" AS SELECT * FROM arr')
            return
        finally:
            con.close()
    raise RuntimeError(f"lab db write failed: {last}")


def main(argv: list[str] | None = None) -> None:
    import argparse
    import logging

    ap = argparse.ArgumentParser(description="t952 anatomy: --stage real | placebo")
    ap.add_argument("--asset-id", default="btcusdt")
    ap.add_argument("--stage", choices=("real", "placebo"), default="real")
    ap.add_argument("--n", type=int, default=30)
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    p = mc.CensusParams()
    if a.stage == "real":
        m = mc.load_minutes(a.asset_id)
        i0, i1 = mc.window_bounds(m, p)
        logp = np.log(m.close)
        rows, frames, _ = mc.run_grid(logp, m.m0, p, i0, i1, with_moves=True)
        moves = pl.concat(frames)
        stats, legs_primary = [], None
        for c in C_LEVELS:
            legs = leg_features(decompose(moves, logp, m.volume, m.m0, c))
            stats.append(anatomy_stats(move_summary(legs), legs, c))
            if c == C_PRIMARY:
                legs_primary = legs
            log.info("c=%.2f legs=%d", c, legs.height)
        assert legs_primary is not None
        _write(a.asset_id, {TABLE_LEGS: legs_primary, TABLE_STATS: pl.concat(stats)})
    else:
        plc = run_placebo_anatomy(a.asset_id, a.n, p, workers=a.workers)
        _write(a.asset_id, {TABLE_PLC: plc})
        log.info("placebo anatomy rows=%d", plc.height)


if __name__ == "__main__":
    main()
