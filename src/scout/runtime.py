"""Impulse Scout live runtime (epic_074 / t940): detect -> score -> resolve with the frozen ``scout_s0c_r1`` artifact.

Pure functions, no database I/O: the caller (``data_handling`` sync, t941; worker job ``sync_scout``, t942) loads the
asset's 1m OHLCV and persists the returned frame. One call::

    from scout import load_artifact, run
    art = load_artifact()                                   # artifacts/scout_s0c_r1/ (hash verified)
    df = run("solusdt", ohlcv_1m, art)                      # one row per event (t0 >= art.live_start), OUTPUT_COLUMNS

Definitions (no new logic, everything is imported from the research code):

* **detection** — :func:`reports.builders.wave_events.build_events` with the 5300 primary parameters
  (:func:`reports.builders.scout_events.scout_params` ``a15``) restricted to the frozen cell ``W4h_H8h``; event id
  :func:`reports.builders.scout_events.event_id` (``asset|cell|t0``). An event exists once bar ``t_c - 1m`` is closed.
* **score** — the frozen S0 + C logit (standardisation + coefficients from the manifest, numerically identical to the
  pickled sklearn model), 3 classes via the train share ``P(false_start | not impulse)``; percentile and top-20% / 10%
  flags against the frozen train OOF score distribution. Only feature values known at ``t_c`` enter.
* **resolve** — :func:`reports.builders.scout_bench.rules.simulate_rule_event` for every frozen rule (default
  ``P* / low[t0] / 8h`` and ``R2_03`` ``+3 sigma_W / -1.5 sigma_W / 8h``), entry = open of the bar opening at ``t_c``.
  An event is resolved only when its whole ``H`` window is closed (``t_c + H <= as_of``); a missing bar anywhere in the
  window makes it ``invalid`` (the rules.py / label convention; no partial "touch so far" outcomes).

Determinism: the detection is anchored at ``artifact.detect_anchor`` (fixed in the manifest): rows before it are cut off,
and the input must reach back to it. The greedy non-overlap chain therefore always starts at the same bar, so the same
data give the same events, ids and predictions no matter how much older history the caller passes or when it runs.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from reports.builders import scout_events as se
from reports.builders import wave_events as we
from reports.builders.scout_bench.rules import DEFAULT_RULE, TradeRule, simulate_rule_event
from reports.builders.scout_bench.trades import Costs

MODEL_ID = "scout_s0c_r1"
ARTIFACT_DIR = Path(__file__).resolve().parents[2] / "artifacts" / MODEL_ID
MANIFEST = "manifest.json"
OHLCV_COLS: tuple[str, ...] = ("open_time", "open", "high", "low", "close", "volume")
#: tolerated distance between the anchor and the first input bar after trimming (a missing first minute is fine)
ANCHOR_SLACK = timedelta(hours=1)
STATUSES = ("open", "resolved", "invalid")

#: frozen rules (manifest order); ``key`` is the column prefix of the rule's plan / outcome columns
RULE_KEYS: tuple[str, ...] = ("default", "r2_03")
R2_03 = TradeRule("R2_03_tp3s_sl15s", tp=("sigma", 3.0), sl=("sigma", 1.5), description="TP +3 σ_W / SL −1,5 σ_W / 8 h")

_RULE_COLS: dict[str, Any] = {"tp": pl.Float64, "sl": pl.Float64, "outcome": pl.Utf8, "outcome_at": pl.Datetime("us"),
                              "exit_price": pl.Float64, "net_bps": pl.Float64}
#: output contract of :func:`run` (t941 ``scout_signals`` columns; ``scored_at`` / ``backfilled`` are the writer's)
OUTPUT_SCHEMA: dict[str, Any] = {
    # identification + event geometry (known at t_c, immutable)
    "event_id": pl.Utf8, "asset_id": pl.Utf8, "model_id": pl.Utf8, "cell_id": pl.Utf8,
    "t0": pl.Datetime("us"), "t_c": pl.Datetime("us"), "label_end": pl.Datetime("us"),
    "low_t0": pl.Float64, "close_tc": pl.Float64, "a_log": pl.Float64, "sigma_w": pl.Float64, "a_sigma": pl.Float64,
    "p_star": pl.Float64, "stop": pl.Float64,
    # features (S0 + C; a_sigma above)
    "velocity_1h": pl.Float64, "pretrend_3d": pl.Float64, "log_sigma_1m": pl.Float64, "drop_depth": pl.Float64,
    "volume_ratio": pl.Float64, "scorable": pl.Boolean,
    # prediction (immutable)
    "p_impulse": pl.Float64, "p_false_start": pl.Float64, "p_sideways": pl.Float64, "oof_percentile": pl.Float64,
    "top20": pl.Boolean, "top10": pl.Boolean,
    # trade plans: entry = open of the bar opening at t_c (null until that bar exists, then fixed)
    "entry": pl.Float64,
    **{f"{k}_{c}": t for k in RULE_KEYS for c, t in _RULE_COLS.items()},
    # resolution (mutable until status != "open")
    "status": pl.Utf8, "label_class": pl.Utf8, "as_of": pl.Datetime("us"),
}
OUTPUT_COLUMNS: tuple[str, ...] = tuple(OUTPUT_SCHEMA)
#: columns fixed once the event exists (t941: never updated); ``entry`` / ``r2_03_tp`` / ``r2_03_sl`` are set-once
IMMUTABLE_COLUMNS: tuple[str, ...] = OUTPUT_COLUMNS[:OUTPUT_COLUMNS.index("entry")]


# --------------------------------------------------------------------------- artifact


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def freeze_hash(manifest: dict[str, Any]) -> str:
    """Hash of everything that defines the predictions and outcomes (5300: the candidate is fixed by a hash)."""
    keys = ("model_id", "event_definition", "model", "thresholds", "rules", "costs", "runtime", "train")
    blob = json.dumps({k: manifest[k] for k in keys}, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def detection_params(variant: str, cell_id: str) -> we.WaveParams:
    """5300 parameters of ``variant`` restricted to one cell (cells are independent in ``build_events``)."""
    cell = next(c for c in we.CELLS_V2 if c[0] == cell_id)
    return se.scout_params(variant, cells=(cell,))


def rule_from_dict(d: dict[str, Any]) -> TradeRule:
    kw = dict(d)
    for k in ("tp", "sl", "tp1"):
        if kw.get(k) is not None:
            kw[k] = (str(kw[k][0]), float(kw[k][1]))
    return TradeRule(**kw)


@dataclass(frozen=True)
class ScoutArtifact:
    """The frozen model + thresholds + rules + costs (read-only view of ``manifest.json`` + ``train_oof.parquet``)."""

    model_id: str
    root: Path
    manifest: dict[str, Any]
    features: tuple[str, ...]
    mu: np.ndarray
    sd: np.ndarray
    coef: np.ndarray
    intercept: float
    q_false_start: float          # P(false_start | not impulse) on the train frame (3-class split of 1 - p)
    oof_sorted: np.ndarray        # train OOF impulse scores, ascending
    thr20: float
    thr10: float
    params: we.WaveParams
    cell_id: str
    h_min: int
    rules: tuple[tuple[str, TradeRule], ...]
    costs: Costs
    detect_anchor: datetime
    live_start: datetime

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        """``(n, 3)`` probabilities (impulse, false_start, sideways) of raw feature rows (``self.features`` order)."""
        z = ((np.asarray(x, dtype=float) - self.mu) / self.sd) @ self.coef + self.intercept
        p = 1.0 / (1.0 + np.exp(-z))
        q = self.q_false_start
        return np.column_stack([p, (1 - p) * q, (1 - p) * (1 - q)])

    def percentile(self, s: np.ndarray) -> np.ndarray:
        """Share (%) of train OOF scores ``<= s``."""
        return np.searchsorted(self.oof_sorted, np.asarray(s, dtype=float), side="right") / self.oof_sorted.size * 100.0


def load_artifact(root: Path | str | None = None, *, verify: bool = True) -> ScoutArtifact:
    """Read the frozen artifact; ``verify`` checks the file hashes, the freeze hash and the detection parameters."""
    root = Path(root or ARTIFACT_DIR)
    man = json.loads((root / MANIFEST).read_text(encoding="utf-8"))
    if verify:
        for name, sha in man["files"].items():
            if file_sha256(root / name) != sha:
                raise ValueError(f"{root / name}: sha256 mismatch (artifact modified after freeze)")
        if freeze_hash(man) != man["freeze_hash"]:
            raise ValueError(f"{root / MANIFEST}: freeze_hash mismatch")
    ed, mo, rt = man["event_definition"], man["model"], man["runtime"]
    params = detection_params(ed["variant"], ed["cell_id"])
    if verify and params.to_json() != ed["params_json"]:
        raise ValueError("event detection parameters in code differ from the frozen ones (wave_events / scout_events changed)")
    oof = pl.read_parquet(root / man["train"]["oof_file"])["oof_score"].to_numpy().astype(float)
    rules = tuple((r["key"], rule_from_dict(r["rule"])) for r in man["rules"])
    c = man["costs"]
    costs = Costs(float(c["maker_bps"]), float(c["taker_bps"]), float(c["slippage_bps"]), dict(c["exit_order_type"]),
                  c.get("entry_order_type", "taker"))
    return ScoutArtifact(
        model_id=man["model_id"], root=root, manifest=man, features=tuple(mo["features"]),
        mu=np.asarray(mo["mu"], dtype=float), sd=np.asarray(mo["sd"], dtype=float),
        coef=np.asarray(mo["coef"], dtype=float), intercept=float(mo["intercept"]),
        q_false_start=float(mo["q_false_start"]), oof_sorted=np.sort(oof),
        thr20=float(man["thresholds"]["top20"]), thr10=float(man["thresholds"]["top10"]),
        params=params, cell_id=ed["cell_id"], h_min=int(ed["h_min"]), rules=rules, costs=costs,
        detect_anchor=datetime.fromisoformat(rt["detect_anchor"]), live_start=datetime.fromisoformat(rt["live_start"]),
    )


# --------------------------------------------------------------------------- detect


def prepare_ohlcv(ohlcv: pl.DataFrame, art: ScoutArtifact, *, check_anchor: bool = True) -> pl.DataFrame:
    """Required columns, unique sorted ``open_time``, rows ``>= detect_anchor`` (the input must reach the anchor)."""
    missing = [c for c in OHLCV_COLS if c not in ohlcv.columns]
    if missing:
        raise KeyError(f"ohlcv misses columns: {missing}")
    df = (ohlcv.select(OHLCV_COLS).with_columns(pl.col("open_time").cast(pl.Datetime("us")))
          .unique(subset="open_time", keep="last").sort("open_time")
          .filter(pl.col("open_time") >= art.detect_anchor))
    if df.height == 0:
        raise ValueError(f"no ohlcv rows at / after the detection anchor {art.detect_anchor}")
    first: datetime = df["open_time"][0]
    if check_anchor and first - art.detect_anchor > ANCHOR_SLACK:
        raise ValueError(f"ohlcv starts at {first}, after the detection anchor {art.detect_anchor}: "
                         "load the asset's 1m bars from the anchor on (the greedy event chain starts there)")
    return df


def detect(ohlcv: pl.DataFrame, art: ScoutArtifact, asset_id: str, *, check_anchor: bool = True
           ) -> tuple[pl.DataFrame, we.MinuteData]:
    """All events of the frozen cell since the anchor (research columns + ``asset_id``, ``event_id``) and the minute
    grid they were detected on. A candidate needs every bar of ``[t0 - B, t_c)`` (rolling minima with full
    ``min_periods``), so a detected event never has a gap in its prefix window."""
    df = prepare_ohlcv(ohlcv, art, check_anchor=check_anchor)
    ev = we.build_events(df, art.params).events.filter(pl.col("cell_id") == art.cell_id)
    m = we.to_minute_grid(df)
    ids = [se.event_id(asset_id, art.cell_id, t) for t in ev["t0"].to_list()]
    ev = ev.with_columns(pl.lit(asset_id).alias("asset_id"), pl.Series("event_id", ids, dtype=pl.Utf8))
    return ev, m


# --------------------------------------------------------------------------- score


def score(ev: pl.DataFrame, art: ScoutArtifact) -> pl.DataFrame:
    """Prediction columns. Not scorable (null predictions): a non-finite feature (e.g. ``pretrend_3d`` when the bar
    ``t0 - 3d`` is missing; such events had no finite row in the training data either)."""
    x = ev.select(list(art.features)).to_numpy().astype(float) if ev.height else np.empty((0, len(art.features)))
    ok = np.asarray(np.isfinite(x).all(axis=1), dtype=bool)
    proba = np.full((ev.height, 3), np.nan)
    if ok.any():
        proba[ok] = art.predict_proba(x[ok])
    s = proba[:, 0]
    pct = np.where(ok, art.percentile(np.where(ok, s, 0.0)), np.nan)

    def col(name: str, a: np.ndarray) -> pl.Series:
        return pl.Series(name, a, dtype=pl.Float64).fill_nan(None)

    def flag(name: str, thr: float) -> pl.Series:
        return pl.Series(name, [bool(v >= thr) if k else None for v, k in zip(s, ok, strict=True)], dtype=pl.Boolean)

    return ev.with_columns(
        pl.Series("scorable", ok, dtype=pl.Boolean), col("p_impulse", proba[:, 0]), col("p_false_start", proba[:, 1]),
        col("p_sideways", proba[:, 2]), col("oof_percentile", pct), flag("top20", art.thr20), flag("top10", art.thr10),
    )


# --------------------------------------------------------------------------- resolve


def _rule_levels(rule: TradeRule, r: dict[str, Any], entry: float) -> tuple[float | None, float | None]:
    """(TP, SL) price of ``rule`` for the event row; None when it needs the entry price that is not known yet."""
    def lvl(kind: str, k: float, side: int) -> float | None:
        if kind == "pstar":
            return float(r["p_star"])
        if kind == "low0":
            return float(r["low_t0"])
        if not np.isfinite(entry):
            return None
        if kind == "sigma":
            return entry * float(np.exp(side * k * float(r["sigma_w"])))
        if kind == "atr":
            return entry + side * k * float(r["atr14_15m"])
        if kind == "half":
            return float(r["close_tc"]) * float(np.exp(float(r["a_log"]) / 2.0))
        raise ValueError(kind)
    return lvl(*rule.tp, +1), lvl(*rule.sl, -1)


def resolve_row(r: dict[str, Any], m: we.MinuteData, art: ScoutArtifact) -> dict[str, Any]:
    """Plan + outcome columns of one event row on the grid ``m``."""
    t_c_min = we._epoch_min(r["t_c"])
    h = int(r["h_min"])
    i0 = t_c_min - m.m0
    entry = float(m.open[i0]) if 0 <= i0 < m.n else float("nan")
    closed = t_c_min + h <= m.m0 + m.n                  # the whole H window is before as_of
    out: dict[str, Any] = {"entry": entry if np.isfinite(entry) else None}
    status = "open"
    for key, rule in art.rules:
        tp, sl = _rule_levels(rule, r, entry)
        res: dict[str, Any] = {"tp": tp, "sl": sl, "outcome": None, "outcome_at": None, "exit_price": None, "net_bps": None}
        if closed:
            sim = simulate_rule_event(m, t_c_min, h, r, rule, art.costs)
            if sim["exit_type"] == "invalid":
                res["outcome"], status = "invalid", "invalid"
            else:
                res["outcome"] = sim["exit_type"]
                res["outcome_at"] = r["t_c"] + timedelta(minutes=int(sim["bars_held"]))   # close of the exit bar
                res["exit_price"] = sim["entry"] * (1.0 + sim["gross"]) if sim["n_legs"] == 1 else None
                res["net_bps"] = sim["net"] * 1e4
                status = "invalid" if status == "invalid" else "resolved"
        out.update({f"{key}_{c}": v for c, v in res.items()})
    out["status"] = status
    out["label_class"] = r["label_class"] if status == "resolved" else None
    return out


def resolve(ev: pl.DataFrame, m: we.MinuteData, art: ScoutArtifact) -> pl.DataFrame:
    """Append the plan / outcome columns (:func:`resolve_row`) and ``as_of`` (close time of the last input bar)."""
    schema = {"entry": pl.Float64, **{f"{k}_{c}": t for k, _ in art.rules for c, t in _RULE_COLS.items()},
              "status": pl.Utf8, "label_class": pl.Utf8}
    rows = [resolve_row(r, m, art) for r in ev.iter_rows(named=True)]
    res = pl.DataFrame(rows, schema=schema, orient="row") if rows else pl.DataFrame(schema=schema)
    as_of = we._dt(m.m0 + m.n)
    return pl.concat([ev.drop([c for c in ("label_class",) if c in ev.columns]), res], how="horizontal").with_columns(
        pl.lit(as_of).cast(pl.Datetime("us")).alias("as_of"))


# --------------------------------------------------------------------------- run


def empty_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=OUTPUT_SCHEMA)


def run(asset_id: str, ohlcv: pl.DataFrame, art: ScoutArtifact | None = None, *, since: datetime | None = None,
        check_anchor: bool = True) -> pl.DataFrame:
    """Events of ``asset_id`` with ``t0 >= since`` (default and minimum: ``art.live_start``), scored with the frozen
    model and resolved where the ``H`` window is closed. Columns :data:`OUTPUT_COLUMNS`, sorted by ``t0``.

    ``ohlcv``: the asset's 1m bars (``open_time`` naive UTC + OHLCV) from ``art.detect_anchor`` (or earlier) to now.
    Events before ``since`` are dropped before scoring / resolution (holdout-2 hygiene: their labels are never formed)."""
    art = art or load_artifact()
    since = since or art.live_start
    if since < art.live_start:
        raise ValueError(f"since={since} is before the frozen live start {art.live_start} (holdout-2 hygiene)")
    ev, m = detect(ohlcv, art, asset_id, check_anchor=check_anchor)
    ev = ev.filter(pl.col("t0") >= since)
    if ev.height == 0:
        return empty_frame()
    ev = score(ev, art)
    ev = resolve(ev, m, art)
    ev = ev.with_columns(pl.lit(art.model_id).alias("model_id"), pl.col("low_t0").alias("stop"))
    out = ev.select([pl.col(c).cast(t) for c, t in OUTPUT_SCHEMA.items()])
    return out.with_columns(pl.col(pl.Float64).fill_nan(None)).sort("t0")              # NaN -> null (DB friendly)


__all__ = ["ARTIFACT_DIR", "DEFAULT_RULE", "IMMUTABLE_COLUMNS", "MODEL_ID", "OUTPUT_COLUMNS", "OUTPUT_SCHEMA", "R2_03", "RULE_KEYS",
           "STATUSES", "ScoutArtifact", "detect", "detection_params", "empty_frame", "freeze_hash", "load_artifact",
           "prepare_ohlcv", "resolve", "resolve_row", "run", "score"]
