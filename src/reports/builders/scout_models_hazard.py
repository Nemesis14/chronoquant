# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Impulse Scout model 3 (epic_073 / t934, 5300 family P3, hypothesis H3): discrete-time competing-risks hazard.

Two competing events after ``t_c`` -- **impulse** (high > P*) and **false start** (low < low[t0]) -- with the sideways
class as administrative censoring at ``H = 480`` min (the harness' ``tau_min``). The window ``(0, H]`` is cut into
``N_BINS`` bins of ``BIN_MIN`` minutes; each event is expanded into person-period rows (one row per bin it is still at
risk), and a multinomial logit ``none / impulse / false_start`` on covariates known at ``t_c`` plus time-bin dummies
gives the cause-specific discrete hazards ``h_c(j)``. The cumulative incidence at ``H``

    CIF_c(H) = sum_j S(j) * h_c(j),   S(j) = prod_{k<j} h_none(k)

gives the 3-class probability: ``(CIF_impulse, CIF_false_start, 1 - CIF_impulse - CIF_false_start = S(N_BINS))``.

Round 1, pre-registered fixed config (:data:`CONFIG`), registered as ``scout_hazard_r1`` in a
:class:`scout_bench.ModelRegistry`. Covariates: the baseline scalars S0 + C (``scout_bench.S0C_COLS``) and a small fixed
order-flow set of ``research."scout_flow_v1"`` (:data:`FLOW_SET`); nothing after ``t_c`` (``tau_min`` / ``label_class``
are only the learning target). The person-period rows are built inside ``fit`` from the fold's training events, so the
rows of one event never cross a fold (the harness purge acts on events).

Data access (train only): :func:`load_frame` (``sb.load_scout_events()`` + flow join restricted to the train events) and
:func:`load_bars` (1m OHLCV with an SQL filter ``open_time < oos_start``): no OOS / holdout-2 row enters memory.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

import numpy as np
import polars as pl

from reports.builders import scout_bench as sb
from reports.builders import wave_events as we

logger = logging.getLogger(__name__)

MODEL_NAME = "scout_hazard_r1"
FAMILY = "P3"
HYPOTHESIS = "H3"
H_MIN = 480
BIN_MIN = 30
N_BINS = H_MIN // BIN_MIN                     # 16
#: small fixed order-flow subset (t930 descriptive table: the decline-leg activity and the rebound taker share)
FLOW_SET: tuple[str, ...] = ("vol_decline_rel", "trades_rel_decline", "qv_logz_prefix", "tb_ratio_rebound")
#: strictly positive ratio covariates entered on the log scale
LOG_COLS: tuple[str, ...] = ("vol_decline_rel", "trades_rel_decline")
COVARIATES: tuple[str, ...] = (*sb.S0C_COLS, *FLOW_SET)
CLIP_Z = 5.0
C_REG = 0.1
#: outcome coding of a person-period row
OUT_NONE, OUT_IMPULSE, OUT_FALSE = 0, 1, 2

CONFIG: dict[str, Any] = {
    "learner": "sklearn LogisticRegression multinomial (lbfgs), L2",
    "C": C_REG, "n_bins": N_BINS, "bin_min": BIN_MIN, "h_min": H_MIN,
    "time_terms": "one-hot bins 2..16 (bin 1 = intercept), proportional covariate effects",
    "covariates": list(COVARIATES), "log_cols": list(LOG_COLS),
    "preprocess": "fold-train median impute, fold-train standardise, clip +-5",
    "score": "CIF_impulse(H)", "censoring": "sideways = censored at H (no event in the last bin)",
}


# --------------------------------------------------------------------------- person-period expansion


def time_bin(tau_min: np.ndarray, bin_min: int = BIN_MIN, n_bins: int = N_BINS) -> np.ndarray:
    """Bin index of a first-touch time: bin ``j`` covers ``(j * bin_min, (j + 1) * bin_min]`` minutes (tau 0 -> bin 0)."""
    t = np.asarray(tau_min, dtype=float)
    return np.clip(np.ceil(t / bin_min).astype(np.int64) - 1, 0, n_bins - 1)


def expand_person_period(label_class: np.ndarray, tau_min: np.ndarray, *, bin_min: int = BIN_MIN,
                         n_bins: int = N_BINS) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Person-period rows of events: ``(event_pos, bin, outcome)``.

    Impulse / false start at bin ``j``: rows ``0 .. j``, outcome ``none`` before ``j`` and the event at ``j``.
    Sideways (censored at ``H``): rows ``0 .. n_bins - 1``, all ``none``. ``event_pos`` = position in the input arrays."""
    cls = np.asarray(label_class).astype(str)
    j = time_bin(tau_min, bin_min, n_bins)
    is_ev = cls != "sideways"
    last = np.where(is_ev, j, n_bins - 1)
    n_rows = last + 1
    pos = np.repeat(np.arange(cls.size), n_rows)
    start = np.repeat(np.cumsum(n_rows) - n_rows, n_rows)
    b = np.arange(pos.size) - start
    out = np.zeros(pos.size, dtype=np.int64)
    at_end = b == last[pos]
    out[at_end & (cls[pos] == "impulse")] = OUT_IMPULSE
    out[at_end & (cls[pos] == "false_start")] = OUT_FALSE
    return pos, b.astype(np.int64), out


def cif_from_hazards(h: np.ndarray) -> np.ndarray:
    """Cumulative incidence curves from discrete hazards ``h`` of shape ``(n, n_bins, 3)`` (none / impulse / false).

    Returns ``(n, n_bins, 2)``: ``CIF_c`` at the END of each bin for impulse and false start. ``CIF_imp + CIF_fs =
    1 - S`` with ``S`` the survival (still undecided), so the sum never exceeds 1."""
    h = np.asarray(h, dtype=float)
    surv_end = np.cumprod(h[:, :, OUT_NONE], axis=1)
    surv_start = np.concatenate([np.ones((h.shape[0], 1)), surv_end[:, :-1]], axis=1)
    inc = surv_start[:, :, None] * h[:, :, [OUT_IMPULSE, OUT_FALSE]]
    return np.cumsum(inc, axis=1)


def three_class_from_cif(cif: np.ndarray) -> np.ndarray:
    """(n, 3) = (CIF_impulse(H), CIF_false_start(H), sideways = 1 - sum) from :func:`cif_from_hazards` output."""
    end = cif[:, -1, :]
    side = np.clip(1.0 - end.sum(axis=1), 0.0, 1.0)
    return np.column_stack([end[:, 0], end[:, 1], side])


# --------------------------------------------------------------------------- model


class HazardModel(sb.ScoutModel):
    """Discrete-time competing-risks hazard (multinomial logit on person-period rows), fixed round-1 config."""

    def __init__(self, cols: tuple[str, ...] = COVARIATES, c: float = C_REG, n_bins: int = N_BINS,
                 bin_min: int = BIN_MIN) -> None:
        self.cols, self.c, self.n_bins, self.bin_min = tuple(cols), c, n_bins, bin_min

    def _raw(self, df: pl.DataFrame) -> np.ndarray:
        x = df.select([pl.col(c).cast(pl.Float64) for c in self.cols]).to_numpy().astype(float)
        for k, c in enumerate(self.cols):
            if c in LOG_COLS:
                v = x[:, k]
                x[:, k] = np.where(v > 0, np.log(np.where(v > 0, v, 1.0)), np.nan)
        return x

    def _x(self, df: pl.DataFrame) -> np.ndarray:
        x = self._raw(df)
        x = np.where(np.isfinite(x), x, self.med)
        return np.clip((x - self.mu) / self.sd, -CLIP_Z, CLIP_Z)

    def _design(self, x_ev: np.ndarray, pos: np.ndarray, b: np.ndarray) -> np.ndarray:
        dummies = np.zeros((pos.size, self.n_bins - 1))
        nz = b > 0
        dummies[np.flatnonzero(nz), b[nz] - 1] = 1.0
        return np.hstack([x_ev[pos], dummies])

    def fit(self, train: pl.DataFrame, prefix_data: Any, *, seed: int) -> None:
        from sklearn.linear_model import LogisticRegression

        raw = self._raw(train)
        med = np.nanmedian(raw, axis=0)
        self.med = np.where(np.isfinite(med), med, 0.0)
        filled = np.where(np.isfinite(raw), raw, self.med)
        self.mu = filled.mean(axis=0)
        sd = filled.std(axis=0)
        self.sd = np.where(sd > 1e-12, sd, 1.0)
        x = self._x(train)
        pos, b, out = expand_person_period(train["label_class"].to_numpy(), train["tau_min"].to_numpy(),
                                           bin_min=self.bin_min, n_bins=self.n_bins)
        self.n_pp_rows = int(pos.size)
        self.clf = LogisticRegression(C=self.c, max_iter=3000, tol=1e-6).fit(self._design(x, pos, b), out)
        if list(self.clf.classes_) != [OUT_NONE, OUT_IMPULSE, OUT_FALSE]:
            raise ValueError(f"training fold misses an outcome class: {self.clf.classes_}")

    def hazards(self, df: pl.DataFrame) -> np.ndarray:
        """``(n, n_bins, 3)`` discrete hazards (none / impulse / false start) of every bin for every event."""
        x = self._x(df)
        n = x.shape[0]
        pos = np.repeat(np.arange(n), self.n_bins)
        b = np.tile(np.arange(self.n_bins), n)
        return self.clf.predict_proba(self._design(x, pos, b)).reshape(n, self.n_bins, 3)

    def cif(self, df: pl.DataFrame) -> np.ndarray:
        return cif_from_hazards(self.hazards(df))

    def predict_proba(self, df: pl.DataFrame, prefix_data: Any) -> np.ndarray:
        return three_class_from_cif(self.cif(df))


def spec() -> sb.ModelSpec:
    return sb.ModelSpec(MODEL_NAME, HazardModel, family=FAMILY, hypothesis=HYPOTHESIS, round=1, config=CONFIG,
                        description="diszkrét idejű versengő kockázatok (multinomiális logit, 16 × 30 perc, S0 + C + 4 flow)")


def registry() -> sb.ModelRegistry:
    """Baseline + this model only (a fresh registry: the run persists only this model's rows)."""
    r = sb.new_registry()
    r.register(spec())
    return r


# --------------------------------------------------------------------------- data (train only)


def load_frame(flow_table: str = "scout_flow_v1") -> pl.DataFrame:
    """t930 train events (``sb.load_scout_events``) + the :data:`FLOW_SET` columns of the order-flow table, read only
    for the train events' ``flow_key`` (holdout-2 rows are never read)."""
    import duckdb

    import utils

    ev = sb.load_scout_events()
    keys = ev["flow_key"].unique().to_list()
    con = duckdb.connect(utils.lab_db_path(None), read_only=True)
    try:
        con.register("keys_df", pl.DataFrame({"flow_key": keys}).to_arrow())
        cols = ", ".join(f'f."{c}"' for c in FLOW_SET)
        fl = con.execute(f'SELECT f.flow_key, {cols} FROM research."{flow_table}" f '
                         "JOIN keys_df k ON f.flow_key = k.flow_key").pl()
    finally:
        con.close()
    out = ev.join(fl, on="flow_key", how="left")
    if out.height != ev.height:
        raise ValueError("flow join changed the event count")
    sb.guard_no_holdout(out)
    return out


def load_bars(assets: list[str], *, tries: int = 10, delay: float = 3.0) -> dict[str, we.MinuteData]:
    """1m grids per asset for the trade simulation, SQL-filtered to ``open_time < oos_start`` (train period only)."""
    import duckdb

    from data_handling.store.readonly_query import open_readonly_connection
    from reports.builders.wave_oos_eval import assert_no_holdout

    cut = datetime.fromisoformat(we.params_v2().oos_start)
    sql = ("SELECT open_time, open, high, low, close, volume FROM live.ohlcv WHERE open_time < ? ORDER BY open_time")
    bars: dict[str, we.MinuteData] = {}
    for a in assets:
        last: Exception | None = None
        df = None
        for attempt in range(tries):
            try:
                con = open_readonly_connection(a)
            except duckdb.Error as exc:
                last = exc
                logger.warning("live db locked (%s, attempt %d/%d): %s", a, attempt + 1, tries, exc)
                time.sleep(delay)
                continue
            try:
                df = con.execute(sql, [cut]).pl()
            finally:
                con.close()
            break
        if df is None:
            raise RuntimeError(f"live db unavailable for {a}: {last}")
        assert_no_holdout(df, sb.frame.HOLDOUT2_START)
        bars[a] = we.to_minute_grid(df)
    return bars


# --------------------------------------------------------------------------- persistence (own rows only)


def persist_model_rows(res: sb.BenchResult, model: str = MODEL_NAME, *, tries: int = 30, delay: float = 5.0,
                       lab_path: str | None = None) -> dict[str, Any]:
    """Write ONLY ``model``'s leaderboard row (+ the run row) to ``research."scout_leaderboard_v1"`` (``__runs``).

    Delete + insert keyed by ``(run_id, model)``; other models' / runs' rows are never touched. Columns the existing
    table lacks (a concurrent harness change may add columns) are added first, so ``INSERT BY NAME`` cannot fail on them.
    The write is one transaction (a failure rolls back, the row is never left deleted). Retries while another process holds the lab DB lock (never kills it)."""
    import duckdb

    import utils
    from reports.builders.scout_bench.runner import LEADERBOARD_TABLE, RUNS_TABLE, SCHEMA

    path = lab_path or utils.lab_db_path(None)
    lb = pl.from_pandas(res.leaderboard[res.leaderboard["model"] == model].reset_index(drop=True))
    if lb.height != 1:
        raise ValueError(f"expected exactly one leaderboard row of {model}, got {lb.height}")
    runs = pl.DataFrame([{k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in res.run.items()}])
    run_id = res.run["run_id"]
    last: Exception | None = None
    for attempt in range(tries):
        try:
            con = duckdb.connect(path)
        except duckdb.Error as exc:
            last = exc
            logger.warning("lab db locked/unavailable (attempt %d/%d): %s", attempt + 1, tries, exc)
            time.sleep(delay)
            continue
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            con.execute("BEGIN TRANSACTION")  # delete + insert atomic: a failure cannot leave the row missing
            for table, df, where, params in ((LEADERBOARD_TABLE, lb, "run_id = ? AND model = ?", [run_id, model]),
                                             (RUNS_TABLE, runs, "run_id = ?", [run_id])):
                arrow = df.to_arrow()  # noqa: F841
                con.execute(f'CREATE TABLE IF NOT EXISTS {SCHEMA}."{table}" AS SELECT * FROM arrow WHERE 1 = 0')
                have = {r[0] for r in con.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
                    [SCHEMA, table]).fetchall()}
                for name, typ in con.execute("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM arrow)").fetchall():
                    if name not in have:
                        con.execute(f'ALTER TABLE {SCHEMA}."{table}" ADD COLUMN "{name}" {typ}')
                con.execute(f'DELETE FROM {SCHEMA}."{table}" WHERE {where}', params)
                con.execute(f'INSERT INTO {SCHEMA}."{table}" BY NAME SELECT * FROM arrow')
            con.execute("COMMIT")
            return {"lab_path": path, "run_id": run_id, "model": model, "attempts": attempt + 1}
        finally:
            con.close()
    raise RuntimeError(f"lab db write failed after {tries} tries: {last}")


__all__ = ["BIN_MIN", "CONFIG", "COVARIATES", "FLOW_SET", "H_MIN", "MODEL_NAME", "N_BINS", "HazardModel",
           "cif_from_hazards", "expand_person_period", "load_bars", "load_frame", "persist_model_rows", "registry", "spec",
           "three_class_from_cif", "time_bin"]
