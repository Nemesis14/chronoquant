# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Impulse Scout model 4 (epic_073 / t935, 5300 family X2, exploratory): frozen time-series foundation model embedding
(Chronos-Bolt) of the normalised prefix + a regularised multinomial logistic head.

**Prefix** (nothing after ``t_c``): the 1m bars opening in ``[t0 - B, t_c - 1m]`` (``L = B + W = 480`` bars, every bar
closed ``<= t_c``), built with t933's :func:`scout_models_rocket.build_prefix` at ``step = 1``; two channels:

* ``close_sigma`` : ``log(close / low[t0]) / sigma_W`` (5300: amplitude preserved)
* ``log_vol_rel`` : ``log1p(volume / vol7d_mean_1m)`` (the asset's own 7-day mean known at ``t0``)

**Embedding** (:func:`embed_prefix`): Chronos-Bolt is univariate, so each channel is embedded separately; the encoder
states of the patch tokens (``L / 16 = 30``; the ``[REG]`` token excluded) are mean-pooled -> ``d_model = 512`` per
channel. Chronos-Bolt instance-normalises its input (mean / std), which would discard the amplitude the 5300 protocol
relies on, so the normalisation statistics (``loc``, ``log scale``) of each channel are kept as 4 explicit scalars.
The model is frozen and never sees a label -> the embedding is computed ONCE per event (cached, :func:`load_or_embed`)
and is fold-leakage free; under the label permutation it is identical and only the head is refit.

**Head** (:class:`FoundationHead`, fixed round-1 config :data:`HEAD_CONFIG`): fold-train standardisation of the 1024
pooled dims -> fold-train PCA to :data:`N_PCA` components (label free; cached per fold-train row set) -> together with
the 4 loc / scale scalars (and, in the ``+S0C`` variant, the 6 baseline scalars) fold-train standardised, clipped
+-5 -> multinomial L2 logistic regression (lbfgs, ``C`` fixed) -> 3-class probabilities; score = P(impulse).

Two exploratory leaderboard rows (no hypothesis, outside the Holm family): ``scout_chronos_r1`` (embedding only) and
``scout_chronos_s0c_r1`` (embedding + S0 + C).

Data access (train only): ``sb.load_scout_events()`` (train split) and t933's ``load_bars`` (1m bars SQL-filtered to
``open_time < oos_start``): no OOS / holdout-2 row enters memory.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from reports.builders import scout_bench as sb

logger = logging.getLogger(__name__)

FAMILY = "X2"
EMB_NAME = "scout_chronos_r1"
EMB_S0C_NAME = "scout_chronos_s0c_r1"
#: frozen foundation model (Hugging Face), pinned revision; ~191 MB (model.safetensors)
FM_ID = "amazon/chronos-bolt-small"
FM_REVISION = "772f3d25d38aec6d914c8949dab4462e2d46f5d8"
FM_PACKAGE = "chronos-forecasting 2.3.2"
PREFIX_STEP = 1
PREFIX_LEN = 480                                   # B + W = 4h + 4h of 1m bars
CHANNELS: tuple[str, ...] = ("close_sigma", "log_vol_rel")
_ROCKET_CH = {"close_sigma": 0, "log_vol_rel": 1}  # channel index in scout_models_rocket.build_prefix
D_MODEL = 512
N_PCA = 64
C_HEAD = 0.1
CLIP_Z = 5.0
EMBED_BATCH = 64
CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "scout_foundation"

EMBED_CONFIG: dict[str, Any] = {
    "fm": FM_ID, "revision": FM_REVISION, "package": FM_PACKAGE, "dtype": "float32", "device": "cpu",
    "prefix": {"window": "[t0-B, t_c-1m] 1m bars", "step_min": PREFIX_STEP, "length": PREFIX_LEN,
               "channels": list(CHANNELS)},
    "pooling": "mean over patch-token encoder states (REG token excluded), per channel",
    "extra": "per channel instance-norm loc and log(scale) (amplitude kept)",
}
HEAD_CONFIG: dict[str, Any] = {
    "embed": EMBED_CONFIG, "standardise": "fold-train mean/sd", "pca": N_PCA, "pca_fit": "fold-train, label free",
    "learner": "sklearn LogisticRegression multinomial (lbfgs), L2", "C": C_HEAD, "clip_z": CLIP_Z,
    "score": "P(impulse)",
}


# --------------------------------------------------------------------------- embedding


@dataclass
class FoundationEmbedding:
    """``emb[row_id]`` = pooled encoder states ``(C * d_model,)``; ``stats[row_id]`` = loc / log-scale per channel.

    ``cache`` holds label-free fold transforms (PCA bases) keyed by the fold-train row set."""

    emb: np.ndarray
    stats: np.ndarray
    stat_names: tuple[str, ...]
    meta: dict[str, Any] = field(default_factory=dict)
    cache: dict[Any, Any] = field(default_factory=dict, repr=False)

    def __getstate__(self) -> dict[str, Any]:
        return {"emb": self.emb, "stats": self.stats, "stat_names": self.stat_names, "meta": self.meta, "cache": {}}

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)


def stat_names(channels: tuple[str, ...] = CHANNELS) -> tuple[str, ...]:
    return tuple(f"{c}_{k}" for c in channels for k in ("loc", "log_scale"))


def load_pipeline(model_id: str = FM_ID, revision: str = FM_REVISION) -> Any:
    """The frozen Chronos-Bolt pipeline on CPU (float32, eval mode, deterministic)."""
    import torch
    from chronos import BaseChronosPipeline

    torch.manual_seed(0)
    pipe = BaseChronosPipeline.from_pretrained(model_id, revision=revision, device_map="cpu", torch_dtype=torch.float32)
    pipe.model.eval()
    return pipe


def pool_states(states: np.ndarray, *, has_reg: bool = True) -> np.ndarray:
    """Mean over the patch tokens of ``(n, tokens, d)`` encoder states (the trailing ``[REG]`` token excluded)."""
    s = states[:, :-1, :] if has_reg else states
    return s.mean(axis=1)


def embed_series(pipe: Any, x: np.ndarray, *, batch: int = EMBED_BATCH) -> tuple[np.ndarray, np.ndarray]:
    """Univariate series ``(n, L)`` -> (pooled states ``(n, d)``, ``(n, 2)`` = instance-norm loc and log scale)."""
    import torch

    has_reg = bool(pipe.model.chronos_config.use_reg_token)
    pooled, ls = [], []
    with torch.no_grad():
        for i in range(0, x.shape[0], batch):
            e, (loc, scale) = pipe.embed(torch.as_tensor(np.ascontiguousarray(x[i:i + batch]), dtype=torch.float32))
            pooled.append(pool_states(e.numpy().astype(np.float64), has_reg=has_reg))
            ls.append(np.column_stack([loc.numpy(), np.log(np.maximum(scale.numpy(), 1e-8))]))
    return np.vstack(pooled), np.vstack(ls)


def embed_prefix(pipe: Any, x: np.ndarray, channels: tuple[str, ...] = CHANNELS, *, batch: int = EMBED_BATCH
                 ) -> tuple[np.ndarray, np.ndarray]:
    """Prefix ``(n, C_rocket, L)`` (t933 layout) -> (``(n, len(channels) * d)`` embedding, ``(n, 2 * len(channels))``
    loc / log-scale stats). Each channel is embedded on its own (Chronos-Bolt is univariate)."""
    embs, stats = [], []
    for c in channels:
        e, s = embed_series(pipe, x[:, _ROCKET_CH[c], :], batch=batch)
        embs.append(e)
        stats.append(s)
    return np.hstack(embs).astype(np.float32), np.hstack(stats).astype(np.float64)


def prefix_hash(x: np.ndarray, frame: pl.DataFrame) -> str:
    """Key of a prefix array + the frame's event identity (asset, t0, t_c) it is aligned with."""
    h = hashlib.sha256()
    h.update(np.ascontiguousarray(x, dtype=np.float32).tobytes())
    ids = frame.select(pl.col("asset_id"), pl.col("t0").cast(pl.Utf8), pl.col("t_c").cast(pl.Utf8))
    h.update(json.dumps(ids.rows()).encode())
    h.update(json.dumps(EMBED_CONFIG, sort_keys=True).encode())
    return h.hexdigest()[:16]


def load_or_embed(frame: pl.DataFrame, x: np.ndarray, *, cache_dir: Path | None = CACHE_DIR, pipe: Any = None
                  ) -> FoundationEmbedding:
    """The embedding of every event of the prepared ``frame`` (``x`` = its t933 prefix at step 1), from the cache
    (``<cache_dir>/emb_<hash>.npz``, gitignored ``data/``) or computed with the frozen model."""
    if x.shape[0] != frame.height or x.shape[-1] != PREFIX_LEN:
        raise ValueError(f"prefix shape {x.shape} does not match the frame ({frame.height}) / length {PREFIX_LEN}")
    key = prefix_hash(x, frame)
    path = Path(cache_dir) / f"emb_{key}.npz" if cache_dir else None
    if path is not None and path.exists():
        z = np.load(path, allow_pickle=False)
        meta = json.loads(str(z["meta"]))
        return FoundationEmbedding(emb=z["emb"], stats=z["stats"], stat_names=tuple(meta["stat_names"]), meta=meta)
    t = time.perf_counter()
    pipe = pipe or load_pipeline()
    t_load = time.perf_counter() - t
    t = time.perf_counter()
    emb, stats = embed_prefix(pipe, x)
    meta = {"key": key, "config": EMBED_CONFIG, "stat_names": list(stat_names()), "n": int(x.shape[0]),
            "dim": int(emb.shape[1]), "seconds_model_load": t_load, "seconds_embed": time.perf_counter() - t}
    if not (np.isfinite(emb).all() and np.isfinite(stats).all()):
        raise ValueError("non-finite embedding")
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(path, emb=emb, stats=stats, meta=np.array(json.dumps(meta)))
    return FoundationEmbedding(emb=emb, stats=stats, stat_names=stat_names(), meta=meta)


# --------------------------------------------------------------------------- head


def _rows_key(rows: np.ndarray) -> str:
    return hashlib.sha256(np.ascontiguousarray(rows, dtype=np.int64).tobytes()).hexdigest()[:16]


@dataclass
class PcaBasis:
    mu: np.ndarray
    sd: np.ndarray
    comps: np.ndarray            # (k, D)
    explained: np.ndarray        # explained variance ratio of the k components


def fit_pca(emb: np.ndarray, k: int = N_PCA) -> PcaBasis:
    """Fold-train standardisation + PCA (deterministic SVD, sign fixed by the largest |loading|)."""
    x = emb.astype(np.float64)
    mu = x.mean(axis=0)
    sd = x.std(axis=0)
    sd = np.where(sd > 1e-12, sd, 1.0)
    z = (x - mu) / sd
    _, s, vt = np.linalg.svd(z, full_matrices=False)
    k = min(k, vt.shape[0])
    comps = vt[:k]
    sign = np.sign(comps[np.arange(k), np.abs(comps).argmax(axis=1)])
    comps = comps * np.where(sign == 0, 1.0, sign)[:, None]
    var = s ** 2
    return PcaBasis(mu=mu, sd=sd, comps=comps, explained=var[:k] / var.sum())


def pca_transform(basis: PcaBasis, emb: np.ndarray) -> np.ndarray:
    return ((emb.astype(np.float64) - basis.mu) / basis.sd) @ basis.comps.T


def fold_pca(fe: FoundationEmbedding, rows: np.ndarray, k: int = N_PCA) -> PcaBasis:
    """PCA basis of the fold-train rows (label free -> cached; identical under the label permutation)."""
    key = ("pca", _rows_key(rows), k)
    if key not in fe.cache:
        fe.cache[key] = fit_pca(fe.emb[rows], k)
    return fe.cache[key]


class FoundationHead(sb.ScoutModel):
    """Frozen Chronos-Bolt embedding -> fold PCA -> (+ loc / scale, optionally + S0 + C) -> multinomial logit."""

    def __init__(self, use_s0c: bool = False, n_pca: int = N_PCA, c: float = C_HEAD) -> None:
        self.use_s0c, self.n_pca, self.c = use_s0c, n_pca, c

    def _features(self, df: pl.DataFrame, fe: FoundationEmbedding) -> np.ndarray:
        rows = df["row_id"].to_numpy()
        parts = [pca_transform(self.basis, fe.emb[rows]), fe.stats[rows]]
        if self.use_s0c:
            parts.append(df.select([pl.col(c).cast(pl.Float64) for c in sb.S0C_COLS]).to_numpy())
        return np.hstack(parts)

    def _z(self, x: np.ndarray) -> np.ndarray:
        x = np.where(np.isfinite(x), x, self.med)
        return np.clip((x - self.mu) / self.sd, -CLIP_Z, CLIP_Z)

    def fit(self, train: pl.DataFrame, prefix_data: Any, *, seed: int) -> None:
        from sklearn.linear_model import LogisticRegression

        if not isinstance(prefix_data, FoundationEmbedding):
            raise TypeError("prefix_data must be a FoundationEmbedding")
        self.basis = fold_pca(prefix_data, train["row_id"].to_numpy(), self.n_pca)
        raw = self._features(train, prefix_data)
        med = np.nanmedian(raw, axis=0)
        self.med = np.where(np.isfinite(med), med, 0.0)
        filled = np.where(np.isfinite(raw), raw, self.med)
        self.mu = filled.mean(axis=0)
        sd = filled.std(axis=0)
        self.sd = np.where(sd > 1e-12, sd, 1.0)
        y = sb.frame.y_class(train)
        self.clf = LogisticRegression(C=self.c, max_iter=3000, tol=1e-6).fit(self._z(raw), y)
        self.classes = np.asarray(self.clf.classes_, dtype=np.int64)

    def predict_proba(self, df: pl.DataFrame, prefix_data: Any) -> np.ndarray:
        p = self.clf.predict_proba(self._z(self._features(df, prefix_data)))
        out = np.zeros((df.height, len(sb.CLASSES)))
        out[:, self.classes] = p
        return out


def emb_spec() -> sb.ModelSpec:
    return sb.ModelSpec(EMB_NAME, lambda: FoundationHead(use_s0c=False), family=FAMILY, round=1,
                        config={**HEAD_CONFIG, "features": "PCA64(emb) + loc/scale(4)"},
                        description="Chronos-Bolt-small embedding (ár + volumen, mean-pool) → PCA 64 + loc/scale → multinomiális logit")


def emb_s0c_spec() -> sb.ModelSpec:
    return sb.ModelSpec(EMB_S0C_NAME, lambda: FoundationHead(use_s0c=True), family=FAMILY, round=1,
                        config={**HEAD_CONFIG, "features": "PCA64(emb) + loc/scale(4) + S0C(6)"},
                        description="Chronos-Bolt-small embedding + S0 + C skalárok → multinomiális logit")


def registry() -> sb.ModelRegistry:
    """Baseline + the two exploratory X2 rows (a fresh registry: the run persists only these rows)."""
    r = sb.new_registry()
    r.register(emb_spec())
    r.register(emb_s0c_spec())
    return r


MODEL_NAMES: tuple[str, ...] = (EMB_NAME, EMB_S0C_NAME)


# --------------------------------------------------------------------------- permutation (head refit only)


def permutation_null(fr: pl.DataFrame, folds: sb.FoldSet, fe: FoundationEmbedding, *, n_perm: int,
                     seed: int = 20260101) -> dict[str, np.ndarray]:
    """Day-bundle permutation null of ΔAUC for both X2 rows (+ the baseline AUC), identical permutation ``i`` as the
    harness (``scout_bench.runner.permutation_null``). The embedding is label free, so a 'full refit' = head refit."""
    from reports.builders.scout_bench import stats as st
    from reports.builders.scout_bench.runner import permutation_null as _pn

    bundles = st.day_bundles(fr["t0"].to_numpy(), fr["asset_id"].to_numpy())
    return _pn(fr, folds, sb.baseline_spec(), [emb_spec(), emb_s0c_spec()], bundles, n_perm=n_perm, seed=seed,
               prefix_data=fe)


__all__ = ["CHANNELS", "C_HEAD", "EMBED_CONFIG", "EMB_NAME", "EMB_S0C_NAME", "FAMILY", "FM_ID", "FM_REVISION",
           "HEAD_CONFIG", "MODEL_NAMES", "N_PCA", "PREFIX_LEN", "FoundationEmbedding", "FoundationHead", "PcaBasis",
           "emb_s0c_spec", "emb_spec", "embed_prefix", "embed_series", "fit_pca", "fold_pca", "load_or_embed",
           "load_pipeline", "pca_transform", "permutation_null", "pool_states", "prefix_hash", "registry", "stat_names"]
