"""Multi-scale large-move census (epic_077 / t950) — pure library, no report.

Question: how many *large up-moves* (long only) does one asset have on 1m data, on which scales, how long do they last?
The definition is scale-free: a move is a swing leg of a sigma-normalised zigzag.

Definition (all log prices, closes of the 1m grid, naive UTC):

* ``sigma_T[t]``  : std of the overlapping ``T``-minute log returns ``logp[u] - logp[u - T]`` for ``u`` in the trailing
  ``SIGMA_WIN`` bars ending at ``t`` (30 days; only data ``<= t``). Empirical scaling, *not* ``sigma_1m * sqrt(T)``.
* zigzag(T, k)    : online, close based. A leg started at pivot ``p`` reverses when the price falls (rises) from its
  running extreme by ``RHO * k * sigma_T[p]``. RHO = 0.5, so the internal pull-back of a leg is always < 0.5 k sigma.
* big up-move     : up-leg (low pivot -> high pivot) with ``net >= k * sigma_T[low pivot]``.
* the pivot is *confirmed* at the first bar that shows the reversal (``confirm_idx``); a move is "known" only from there.

The catalogue is hindsight by construction (an up-leg is only recognised after it ended). It is a descriptive
frame, not a forecast (see ``_jira_/epic_077_multiscale_move_census/epic.md``).
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any

import duckdb
import numpy as np
import polars as pl
from numba import njit

from reports.builders.wave_events import MinuteData, to_minute_grid

logger = logging.getLogger(__name__)

VERSION = "v1"
SCHEMA = "research"
GENERATOR = "reports.builders.move_census"
TABLE_MOVES = f"move_census_{VERSION}_moves"
TABLE_CELLS = f"move_census_{VERSION}_cells"
TABLE_META = f"move_census_{VERSION}_meta"

#: detector scales (minutes): 15m ... 3 days, doubling
SCALES: tuple[int, ...] = (15, 30, 60, 120, 240, 480, 960, 1920, 4320)
#: pre-registered size grid (multiples of the scale's sigma)
KS: tuple[float, ...] = (3.0, 4.0, 6.0)
RHO = 0.5
SIGMA_WIN = 30 * 1440
#: common analysis window (train period of the quant champion; OOS 2026-02 and holdout-2 are never loaded)
WINDOW_START = "2021-02-04 00:00:00"      # >= first bar + 4320 + SIGMA_WIN (all scales share the same exposure)
WINDOW_END = "2026-01-31 23:59:00"        # last usable (closed) bar of the train period

_EPOCH = datetime(1970, 1, 1)


@dataclass(frozen=True)
class CensusParams:
    scales: tuple[int, ...] = SCALES
    ks: tuple[float, ...] = KS
    rho: float = RHO
    sigma_win: int = SIGMA_WIN
    window_start: str = WINDOW_START
    window_end: str = WINDOW_END
    version: str = VERSION

    def to_json(self) -> str:
        import json

        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, text: str) -> CensusParams:
        import json

        d = json.loads(text)
        return cls(scales=tuple(int(x) for x in d["scales"]), ks=tuple(float(x) for x in d["ks"]), rho=float(d["rho"]),
                   sigma_win=int(d["sigma_win"]), window_start=str(d["window_start"]), window_end=str(d["window_end"]),
                   version=str(d["version"]))


# --------------------------------------------------------------------------------------------------------------
# data
# --------------------------------------------------------------------------------------------------------------

def load_minutes(asset_id: str, end: str = WINDOW_END) -> MinuteData:
    """Complete 1m OHLCV grid of the asset's live DB (read-only), loaded only up to ``end`` (train period:
    OOS and holdout-2 bars never enter memory)."""
    from reports.builders._common import read_live

    df = None
    for attempt in range(20):                      # the worker may hold the write lock for a moment
        df = read_live("SELECT open_time, open, high, low, close, volume FROM ohlcv WHERE open_time <= ? ORDER BY open_time",
                       [end], asset_id=asset_id)
        if df is not None:
            break
        logger.warning("live db of %s locked (attempt %d/20)", asset_id, attempt + 1)
        time.sleep(3.0)
    if df is None:
        raise RuntimeError(f"live db of {asset_id} is not readable")
    return to_minute_grid(pl.from_pandas(df))


def window_bounds(m: MinuteData, p: CensusParams) -> tuple[int, int]:
    """Bar indices ``(i0, i1)`` of the analysis window (both inclusive)."""
    a = int(np.datetime64(p.window_start, "m").astype("int64")) - m.m0
    b = int(np.datetime64(p.window_end, "m").astype("int64")) - m.m0
    return max(a, 0), min(b, m.n - 1)


def bar_time(m0: int, idx: np.ndarray | int) -> Any:
    return np.asarray(m0 + np.asarray(idx), dtype="int64").astype("datetime64[m]").astype("datetime64[ms]")


# --------------------------------------------------------------------------------------------------------------
# sigma + zigzag
# --------------------------------------------------------------------------------------------------------------

def trailing_sigma(logp: np.ndarray, scale: int, win: int = SIGMA_WIN) -> np.ndarray:
    """Std of overlapping ``scale``-minute log returns over the trailing ``win`` bars ending at ``t`` (NaN until valid)."""
    n = logp.size
    r = np.zeros(n)
    r[scale:] = logp[scale:] - logp[:-scale]
    c1 = np.concatenate(([0.0], np.cumsum(r)))
    c2 = np.concatenate(([0.0], np.cumsum(r * r)))
    out = np.full(n, np.nan)
    first = scale + win - 1                       # window t-win+1 .. t must lie fully in the valid-return range
    if first >= n:
        return out
    t = np.arange(first, n)
    s1 = c1[t + 1] - c1[t + 1 - win]
    s2 = c2[t + 1] - c2[t + 1 - win]
    var = s2 / win - (s1 / win) ** 2
    out[first:] = np.sqrt(np.maximum(var, 0.0))
    return out


@njit(cache=True)
def _zigzag(logp: np.ndarray, sigma: np.ndarray, k: float, rho: float, i0: int, i1: int):
    """Online sigma-normalised zigzag on ``logp[i0 .. i1]``.

    Returns ``(pivot_idx, pivot_type, confirm_idx)`` — type +1 = high pivot, -1 = low pivot.
    The reversal threshold of a leg is ``rho * k * sigma[start pivot]`` (fixed for the leg).
    """
    cap = (i1 - i0) // 2 + 4
    piv = np.empty(cap, np.int64)
    typ = np.empty(cap, np.int8)
    cnf = np.empty(cap, np.int64)
    n = 0
    lo = logp[i0]
    hi = logp[i0]
    lo_i = i0
    hi_i = i0
    th0 = rho * k * sigma[i0]
    mode = 0
    ext = 0.0
    ext_i = i0
    th = 0.0
    for i in range(i0 + 1, i1 + 1):
        x = logp[i]
        if mode == 0:
            if x < lo:
                lo = x
                lo_i = i
            if x > hi:
                hi = x
                hi_i = i
            if x - lo >= th0:
                piv[n] = lo_i
                typ[n] = -1
                cnf[n] = i
                n += 1
                mode = 1
                ext = x
                ext_i = i
                th = rho * k * sigma[lo_i]
            elif hi - x >= th0:
                piv[n] = hi_i
                typ[n] = 1
                cnf[n] = i
                n += 1
                mode = -1
                ext = x
                ext_i = i
                th = rho * k * sigma[hi_i]
        elif mode == 1:
            if x > ext:
                ext = x
                ext_i = i
            elif ext - x >= th:
                piv[n] = ext_i
                typ[n] = 1
                cnf[n] = i
                n += 1
                mode = -1
                ext = x
                ext_i = i
                th = rho * k * sigma[piv[n - 1]]
        else:
            if x < ext:
                ext = x
                ext_i = i
            elif x - ext >= th:
                piv[n] = ext_i
                typ[n] = -1
                cnf[n] = i
                n += 1
                mode = 1
                ext = x
                ext_i = i
                th = rho * k * sigma[piv[n - 1]]
    return piv[:n], typ[:n], cnf[:n]


@njit(cache=True)
def _leg_metrics(logp: np.ndarray, s: np.ndarray, e: np.ndarray, stride: int):
    """Per leg: max drawdown inside the leg (log) and the efficiency ratio on a ``stride``-subsampled path."""
    m = s.size
    mdd = np.zeros(m)
    er = np.zeros(m)
    for j in range(m):
        a = s[j]
        b = e[j]
        peak = logp[a]
        dd = 0.0
        for i in range(a, b + 1):
            if logp[i] > peak:
                peak = logp[i]
            d = peak - logp[i]
            if d > dd:
                dd = d
        mdd[j] = dd
        path = 0.0
        prev = logp[a]
        i = a + stride
        while i < b:
            path += abs(logp[i] - prev)
            prev = logp[i]
            i += stride
        path += abs(logp[b] - prev)
        net = logp[b] - logp[a]
        er[j] = net / path if path > 0 else 1.0
    return mdd, er


def up_legs(logp: np.ndarray, sigma: np.ndarray, k: float, rho: float, i0: int, i1: int) -> dict[str, np.ndarray]:
    """All completed up-legs (low pivot -> next high pivot) of the zigzag, with the ``big`` flag (``z >= k``)."""
    piv, typ, cnf = _zigzag(logp, sigma, k, rho, i0, i1)
    if piv.size < 2:
        z = np.zeros(0)
        return {"s": z.astype(np.int64), "e": z.astype(np.int64), "c": z.astype(np.int64), "net": z, "z": z, "sig": z}
    j = np.nonzero((typ[:-1] == -1) & (typ[1:] == 1))[0]
    s, e, c = piv[j], piv[j + 1], cnf[j + 1]
    net = logp[e] - logp[s]
    sig = sigma[s]
    return {"s": s, "e": e, "c": c, "net": net, "z": net / sig, "sig": sig}


def census_cell(logp: np.ndarray, sigma: np.ndarray, scale: int, k: float, p: CensusParams, i0: int, i1: int
                ) -> tuple[dict[str, Any], dict[str, np.ndarray]]:
    """One (scale, k) cell: summary row and the big-move arrays (indices + metrics)."""
    legs = up_legs(logp, sigma, k, p.rho, i0, i1)
    big = legs["z"] >= k
    s, e, c = legs["s"][big], legs["e"][big], legs["c"][big]
    mdd, er = _leg_metrics(logp, s, e, max(1, scale // 8))
    net = legs["net"][big]
    dur = (e - s).astype(np.float64)
    years = (i1 - i0 + 1) / (365.25 * 1440)
    has = bool(big.any())
    row = {
        "scale_min": scale, "k": k, "n_up_legs": int(legs["s"].size), "n_big": int(big.sum()),
        "big_per_year": float(big.sum() / years), "share_big": float(big.mean()) if big.size else float("nan"),
        "median_dur_min": float(np.median(dur)) if has else float("nan"),
        "median_z": float(np.median(legs["z"][big])) if has else float("nan"),
        "dur_p10": float(np.percentile(dur, 10)) if has else float("nan"),
        "dur_p90": float(np.percentile(dur, 90)) if has else float("nan"),
        "er_med": float(np.median(er)) if has else float("nan"),
        "mdd_ratio_med": float(np.median(mdd / net)) if has else float("nan"),
    }
    moves = {"s": s, "e": e, "c": c, "net": net, "z": legs["z"][big], "sigma": legs["sig"][big], "dur": dur,
             "mdd": mdd, "er": er}
    return row, moves


def run_grid(logp: np.ndarray, m0: int, p: CensusParams, i0: int, i1: int, *, with_moves: bool = True, log: bool = False
             ) -> tuple[list[dict[str, Any]], list[pl.DataFrame], list[dict[str, float]]]:
    """The whole (scale, k) grid on one log-price series (real or placebo).

    Returns ``(cell rows, move frames, scale rows)``; scale rows hold the variance-ratio diagnostic
    ``mean(sigma_T) / (mean(sigma_1m) * sqrt(T))`` (1 = random-walk scaling, > 1 = trending).
    """
    rows: list[dict[str, Any]] = []
    frames: list[pl.DataFrame] = []
    scale_rows: list[dict[str, float]] = []
    sigma_1 = float(np.nanmean(trailing_sigma(logp, 1, p.sigma_win)[i0:i1 + 1]))
    t_all = time.time()
    for scale in p.scales:
        sigma = trailing_sigma(logp, scale, p.sigma_win)
        if not np.isfinite(sigma[i0]):
            raise ValueError(f"sigma not valid at window start for scale {scale}")
        scale_rows.append({"scale_min": scale, "vr": float(np.mean(sigma[i0:i1 + 1])) / (sigma_1 * np.sqrt(scale))})
        for k in p.ks:
            row, mv = census_cell(logp, sigma, scale, k, p, i0, i1)
            rows.append(row)
            if with_moves and mv["s"].size:
                frames.append(pl.DataFrame({
                    "scale_min": np.full(mv["s"].size, scale, np.int32), "k": np.full(mv["s"].size, k),
                    "start_time": bar_time(m0, mv["s"]), "end_time": bar_time(m0, mv["e"]),
                    "confirm_time": bar_time(m0, mv["c"]), "dur_min": mv["dur"], "net_log": mv["net"],
                    "sigma": mv["sigma"], "z": mv["z"], "mdd_log": mv["mdd"],
                    "mdd_ratio": mv["mdd"] / mv["net"], "er": mv["er"],
                }))
        if log:
            logger.info("scale %d done (%.1fs total)", scale, time.time() - t_all)
    return rows, frames, scale_rows


def build_census(m: MinuteData, p: CensusParams | None = None, *, log: bool = True
                 ) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    """Run the whole grid. Returns ``(moves, cells, meta)`` (moves = big up-moves of all cells, one row each)."""
    p = p or CensusParams()
    if np.isnan(m.close).any():
        i_first = int(np.argmax(~np.isnan(m.close)))
        raise ValueError(f"minute grid has gaps (first valid bar {i_first}); census needs a complete grid")
    logp = np.log(m.close)
    i0, i1 = window_bounds(m, p)
    rows, frames, scale_rows = run_grid(logp, m.m0, p, i0, i1, log=log)
    moves = pl.concat(frames) if frames else pl.DataFrame()
    cells = pl.DataFrame(rows).join(pl.DataFrame(scale_rows), on="scale_min")
    meta = {
        "version": p.version, "generator": GENERATOR, "params_json": p.to_json(),
        "window_start": str(bar_time(m.m0, i0)), "window_end": str(bar_time(m.m0, i1)),
        "ohlcv_rows": m.n, "n_moves": moves.height, "created_at": datetime.now().replace(microsecond=0),
    }
    return moves, cells, meta


# --------------------------------------------------------------------------------------------------------------
# persistence
# --------------------------------------------------------------------------------------------------------------

def persist(moves: pl.DataFrame, cells: pl.DataFrame, meta: dict[str, Any], asset_id: str, *, tries: int = 20,
            delay: float = 3.0, lab_path: str | None = None) -> str:
    """Write the three tables into ``research`` of the asset's lab DB (never live); retries while the DB is locked."""
    import utils

    path = lab_path or utils.lab_db_path(asset_id)
    last_exc: Exception | None = None
    for attempt in range(tries):
        try:
            con = duckdb.connect(path)
        except duckdb.Error as exc:
            last_exc = exc
            logger.warning("lab db locked (attempt %d/%d): %s", attempt + 1, tries, exc)
            time.sleep(delay)
            continue
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            mv, ce, mt = moves.to_arrow(), cells.to_arrow(), pl.DataFrame([meta]).to_arrow()  # noqa: F841
            con.execute(f'CREATE OR REPLACE TABLE {SCHEMA}."{TABLE_MOVES}" AS SELECT * FROM mv')
            con.execute(f'CREATE OR REPLACE TABLE {SCHEMA}."{TABLE_CELLS}" AS SELECT * FROM ce')
            con.execute(f'CREATE OR REPLACE TABLE {SCHEMA}."{TABLE_META}" AS SELECT * FROM mt')
            return path
        finally:
            con.close()
    raise RuntimeError(f"lab db write failed after {tries} tries: {last_exc}")


def load_table(asset_id: str, table: str) -> pl.DataFrame:
    import utils

    con = duckdb.connect(utils.lab_db_path(asset_id), read_only=True)
    try:
        return con.execute(f'SELECT * FROM {SCHEMA}."{table}"').pl()
    finally:
        con.close()


def epoch_to_dt(epoch_min: int) -> datetime:
    return _EPOCH + timedelta(minutes=int(epoch_min))
