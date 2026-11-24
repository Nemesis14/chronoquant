# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false, reportGeneralTypeIssues=false, reportReturnType=false
"""Wave-start prefix images, convolutional-autoencoder embedding and clustering (epic_072 / t919).

Pure library (no report). Definitions of ``_doc_/methodology_doc/5200_wave_start_image_model.md`` (v2):

* **image**: the 15m prefix ``[t0 - B, t_c]`` (only bars closed ``<= t_c``, :func:`reports.builders.wave_events.prefix_image`)
  as a black / white pixel matrix in the Jiang-Kelly-Xiu style: per bar 3 pixel columns (open tick left, high-low line in
  the middle, close tick right), the price panel on top, the volume panel below. Price axis: ``(price - low[t0]) / ATR14``
  mapped to a FIXED row range ``[0, p_hi]`` (so the image keeps the rebound size; ``p_hi`` is fitted on the fit set),
  volume axis: relative volume (``/ 7d mean``) up to ``v_hi`` (fit set quantile).
* **embedding**: small convolutional autoencoder (torch CPU, single thread, seeded -> deterministic), latent vector
  standardised on the fit set.
* **clustering**: k-means on the embedding (k in 4..10), baselines k-Shape / DTW k-means (tslearn) on the z-normalised
  raw close path.

Leakage discipline: every ``fit_*`` function refuses events whose ``split`` is not ``train`` and never sees anything but
the rows it is given; :func:`fit_fold` fits on a purged / embargoed fold's TRAIN events only and merely transforms the
test fold (the function t920 needs for its CV). OOS / holdout-2 rows must be passed to :func:`assign` / :func:`embed` only.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import polars as pl

from reports.builders import wave_events as we

logger = logging.getLogger(__name__)

CELL = "W4h_H8h"
KS: tuple[int, ...] = tuple(range(4, 11))
SEED = 42
PX_PER_BAR = 3
PRICE_ROWS = 48
GAP_ROWS = 1
VOL_ROWS = 15
IMG_H = PRICE_ROWS + GAP_ROWS + VOL_ROWS
LATENT = 16
SMOOTH_A = 20.0                 # prior strength of the shrunk cluster impulse rate (OOF log-loss)
METHODS = ("ae_kmeans", "kshape", "dtw_kmeans")
METHOD_HU = {"ae_kmeans": "AE + k-means", "kshape": "k-Shape (nyers út)", "dtw_kmeans": "DTW k-means (nyers út)"}


# --------------------------------------------------------------------------- raw prefixes


@dataclass
class Prefixes:
    """ATR-normalised 15m OHLC (relative to ``low[t0]``) + relative volume of the events' prefixes, right-aligned.

    Arrays are ``(n, n_bars)``; a prefix shorter than ``n_bars`` is NaN-padded on the LEFT (the last column is always the
    last 15m bar closed ``<= t_c``). ``t0`` (one per row) identifies the event and aligns the rows with the events table.
    """

    t0: np.ndarray                      # datetime64[us]
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    n_valid: np.ndarray                 # int, valid (non-NaN) bars per row

    @property
    def n(self) -> int:
        return int(self.t0.size)

    @property
    def n_bars(self) -> int:
        return int(self.close.shape[1])

    def take(self, idx: Any) -> Prefixes:
        idx = np.asarray(idx)
        return Prefixes(*(getattr(self, f)[idx] for f in ("t0", "open", "high", "low", "close", "volume", "n_valid")))

    def select(self, events: pl.DataFrame) -> Prefixes:
        """Rows of the given events (matched by ``t0``; unique inside a cell), in the events' order."""
        pos = pd.Index(self.t0).get_indexer(pd.Index(events["t0"].to_numpy().astype("datetime64[us]")))
        if (pos < 0).any():
            raise KeyError("events without prefix rows")
        return self.take(pos)


def build_prefixes(b15: we.Bars15, events: pl.DataFrame, n_bars: int | None = None) -> Prefixes:
    """Prefix arrays of ONE cell's events (all rows of ``events``; callers pass train rows only)."""
    if events.height == 0:
        raise ValueError("no events")
    rows = events.to_dicts()
    if n_bars is None:
        n_bars = int(2 * rows[0]["w_min"] // 15)
    n = len(rows)
    out = {k: np.full((n, n_bars), np.nan) for k in ("open", "high", "low", "close", "volume")}
    nv = np.zeros(n, dtype=int)
    for i, r in enumerate(rows):
        if pd.Timestamp(r["prefix_end"]) > pd.Timestamp(r["t_c"]):
            raise ValueError("prefix window reaches beyond t_c")      # look-ahead guard
        img = we.prefix_image(b15, r)
        m = min(int(img["close"].size), n_bars)
        nv[i] = m
        for k in out:
            out[k][i, n_bars - m:] = img[k][-m:]
    return Prefixes(t0=events["t0"].to_numpy().astype("datetime64[us]"), n_valid=nv, **out)


# --------------------------------------------------------------------------- images


@dataclass(frozen=True)
class RenderParams:
    p_hi: float          # price axis upper limit (ATR units above low[t0])
    v_hi: float          # volume axis upper limit (x the 7d mean 15m volume)


def fit_render(pre: Prefixes, *, price_q: float = 0.995, vol_q: float = 0.99) -> RenderParams:
    """Axis limits from the FIT set only (quantiles of the per-event price maximum / volume)."""
    p_hi = float(np.quantile(np.nanmax(pre.high, axis=1), price_q))
    v_hi = float(np.quantile(pre.volume[~np.isnan(pre.volume)], vol_q))
    return RenderParams(p_hi=max(p_hi, 1e-6), v_hi=max(v_hi, 1e-6))


def image_shape(n_bars: int) -> tuple[int, int]:
    return IMG_H, PX_PER_BAR * n_bars


def render_images(pre: Prefixes, rp: RenderParams) -> np.ndarray:
    """Binary image tensor ``(n, 1, IMG_H, 3 * n_bars)`` (row 0 = top = highest price)."""
    n, nb = pre.close.shape
    img = np.zeros((n, IMG_H, PX_PER_BAR * nb), dtype=np.float32)
    prow = np.arange(PRICE_ROWS)[None, :]
    vrow = np.arange(VOL_ROWS)[None, :]

    def to_row(x: np.ndarray) -> np.ndarray:
        r = np.clip(np.rint(np.nan_to_num(x) / rp.p_hi * (PRICE_ROWS - 1)), 0, PRICE_ROWS - 1).astype(int)
        return PRICE_ROWS - 1 - r

    ar = np.arange(n)
    for j in range(nb):
        ok = ~np.isnan(pre.close[:, j])
        if not ok.any():
            continue
        x0 = PX_PER_BAR * j
        top, bot = to_row(pre.high[:, j]), to_row(pre.low[:, j])
        img[:, :PRICE_ROWS, x0 + 1] = ((prow >= top[:, None]) & (prow <= bot[:, None]) & ok[:, None]).astype(np.float32)
        img[ar[ok], to_row(pre.open[:, j])[ok], x0] = 1.0
        img[ar[ok], to_row(pre.close[:, j])[ok], x0 + 2] = 1.0
        vh = np.clip(np.rint(np.nan_to_num(pre.volume[:, j]) / rp.v_hi * VOL_ROWS), 1, VOL_ROWS).astype(int)
        img[:, PRICE_ROWS + GAP_ROWS:, x0 + 1] = ((vrow >= (VOL_ROWS - vh)[:, None]) & ok[:, None]).astype(np.float32)
    return img[:, None, :, :]


# --------------------------------------------------------------------------- autoencoder


@dataclass
class AEConfig:
    latent: int = LATENT
    epochs: int = 30
    batch: int = 64
    lr: float = 2e-3
    weight_decay: float = 1e-5
    width: int = 8


def _require_train(events: pl.DataFrame, what: str) -> None:
    if events.height == 0:
        raise ValueError(f"{what}: no events")
    if "split" not in events.columns or (events["split"] != "train").any():
        raise ValueError(f"{what}: only split == 'train' events may be used for fitting")


def _build_net(shape: tuple[int, int], cfg: AEConfig) -> Any:
    import torch
    from torch import nn
    from torch.nn import functional as F  # noqa: N812

    h, w = shape
    sizes = [(h, w)]
    for _ in range(3):
        sizes.append(((sizes[-1][0] + 1) // 2, (sizes[-1][1] + 1) // 2))
    c = cfg.width

    class Net(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.e1, self.e2, self.e3 = (nn.Conv2d(i, o, 3, 2, 1) for i, o in ((1, c), (c, 2 * c), (2 * c, 4 * c)))
            self.flat = 4 * c * sizes[3][0] * sizes[3][1]
            self.to_z = nn.Linear(self.flat, cfg.latent)
            self.from_z = nn.Linear(cfg.latent, self.flat)
            self.d3, self.d2, self.d1 = (nn.Conv2d(i, o, 3, 1, 1) for i, o in ((4 * c, 2 * c), (2 * c, c), (c, 1)))

        def encode(self, x: torch.Tensor) -> torch.Tensor:
            x = F.relu(self.e1(x))
            x = F.relu(self.e2(x))
            x = F.relu(self.e3(x))
            return self.to_z(x.flatten(1))

        def decode(self, z: torch.Tensor) -> torch.Tensor:
            x = F.relu(self.from_z(z)).view(-1, 4 * c, sizes[3][0], sizes[3][1])
            x = F.relu(self.d3(F.interpolate(x, size=sizes[2], mode="nearest")))
            x = F.relu(self.d2(F.interpolate(x, size=sizes[1], mode="nearest")))
            return self.d1(F.interpolate(x, size=sizes[0], mode="nearest"))      # logits

    return Net()


@dataclass
class EmbeddingModel:
    """Frozen render params + autoencoder + latent standardisation (everything fitted on the fit set only)."""

    render: RenderParams
    net: Any
    mu: np.ndarray
    sd: np.ndarray
    cfg: AEConfig
    n_fit: int
    train_t0: np.ndarray                  # the fit events' t0 (leak audits)
    losses: list[float] = field(default_factory=list)


def fit_embedding(events: pl.DataFrame, pre: Prefixes, *, cfg: AEConfig | None = None, seed: int = SEED) -> EmbeddingModel:
    """Fit render axis + autoencoder on ``events`` (train only; rows aligned with ``pre``)."""
    import torch
    from torch.nn import functional as F  # noqa: N812

    _require_train(events, "fit_embedding")
    if pre.n != events.height or not np.array_equal(pre.t0, events["t0"].to_numpy().astype("datetime64[us]")):
        raise ValueError("prefix rows are not aligned with the events")
    cfg = cfg or AEConfig()
    torch.set_num_threads(1)
    torch.manual_seed(seed)
    rp = fit_render(pre)
    x = torch.from_numpy(render_images(pre, rp))
    dens = float(x.mean())
    pos_w = torch.tensor(float(np.clip(np.sqrt((1 - dens) / max(dens, 1e-6)), 1.0, 10.0)))
    net = _build_net(image_shape(pre.n_bars), cfg)
    opt = torch.optim.Adam(net.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)
    gen = torch.Generator().manual_seed(seed)
    losses: list[float] = []
    net.train()
    for _ in range(cfg.epochs):
        perm = torch.randperm(x.shape[0], generator=gen)
        tot = 0.0
        for s in range(0, x.shape[0], cfg.batch):
            xb = x[perm[s:s + cfg.batch]]
            loss = F.binary_cross_entropy_with_logits(net.decode(net.encode(xb)), xb, pos_weight=pos_w)
            opt.zero_grad()
            loss.backward()
            opt.step()
            tot += loss.item() * xb.shape[0]
        losses.append(tot / x.shape[0])
    net.eval()
    with torch.no_grad():
        z = net.encode(x).numpy().astype(np.float64)
    return EmbeddingModel(render=rp, net=net, mu=z.mean(axis=0), sd=np.where(z.std(axis=0) > 1e-9, z.std(axis=0), 1.0),
                          cfg=cfg, n_fit=int(x.shape[0]), train_t0=pre.t0.copy(), losses=losses)


def embed(model: EmbeddingModel, pre: Prefixes) -> np.ndarray:
    """Standardised latent vectors (transform only; any split)."""
    import torch

    torch.set_num_threads(1)
    x = torch.from_numpy(render_images(pre, model.render))
    with torch.no_grad():
        z = model.net.encode(x).numpy().astype(np.float64)
    return (z - model.mu) / model.sd


def reconstruct(model: EmbeddingModel, pre: Prefixes) -> np.ndarray:
    """Reconstruction probabilities ``(n, IMG_H, W)`` of the autoencoder."""
    import torch

    torch.set_num_threads(1)
    x = torch.from_numpy(render_images(pre, model.render))
    with torch.no_grad():
        return torch.sigmoid(model.net.decode(model.net.encode(x))).numpy()[:, 0]


# --------------------------------------------------------------------------- clustering


@dataclass
class ImageClusterModel:
    """AE embedding + k centroids (cluster 0 = largest in the fit set)."""

    emb: EmbeddingModel
    k: int
    centroids: np.ndarray                 # (k, latent) in the standardised space
    sizes: np.ndarray                     # fit-set cluster sizes

    def assign_z(self, z: np.ndarray) -> np.ndarray:
        d = ((z[:, None, :] - self.centroids[None, :, :]) ** 2).sum(axis=2)
        return np.argmin(d, axis=1).astype(int)


def _order_by_size(labels: np.ndarray, centers: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    k = centers.shape[0]
    sizes = np.bincount(labels, minlength=k)
    order = np.lexsort((np.arange(k), -sizes))            # size desc, index asc (deterministic)
    remap = np.empty(k, dtype=int)
    remap[order] = np.arange(k)
    return remap[labels], centers[order], sizes[order]


def fit_clusterer(emb: EmbeddingModel, z_fit: np.ndarray, k: int, *, seed: int = SEED) -> tuple[ImageClusterModel, np.ndarray]:
    """k-means on the fit-set embedding; returns the model and the fit-set labels."""
    from sklearn.cluster import KMeans
    from threadpoolctl import threadpool_limits

    with threadpool_limits(limits=1):                    # multi-threaded k-means reductions are not bit-reproducible
        km = KMeans(n_clusters=k, n_init=10, random_state=seed).fit(z_fit)
    labels, centers, sizes = _order_by_size(km.labels_.astype(int), km.cluster_centers_)
    return ImageClusterModel(emb=emb, k=k, centroids=centers, sizes=sizes), labels


def assign(model: ImageClusterModel, pre: Prefixes) -> tuple[np.ndarray, np.ndarray]:
    """(labels, embedding) of new events with the frozen embedding and nearest centroid (transform only)."""
    z = embed(model.emb, pre)
    return model.assign_z(z), z


@dataclass
class FoldFit:
    """One CV fold fitted on its TRAIN events only."""

    fold: we.CVFold
    emb: EmbeddingModel
    models: dict[int, ImageClusterModel]
    train_labels: dict[int, np.ndarray]
    test_labels: dict[int, np.ndarray]
    test_z: np.ndarray
    train_z: np.ndarray


def fit_fold(ev: pl.DataFrame, pre: Prefixes, test_block: int, bounds: list[datetime], embargo_min: int,
             ks: Sequence[int] = KS, *, cfg: AEConfig | None = None, seed: int = SEED) -> FoldFit:
    """Fit AE + k-means (for every k in ``ks``) on a purged / embargoed fold's training events and transform its test fold.

    ``ev`` = ONE cell's events (``cv_block`` / ``cv_straddle`` columns, v2 table); the test fold's rows are never given to
    :func:`fit_embedding` / :func:`fit_clusterer` — they are only embedded and assigned afterwards.
    """
    fold = we.cv_fold(ev, test_block, bounds, embargo_min)
    tr_pre, te_pre = pre.select(fold.train), pre.select(fold.test)
    emb = fit_embedding(fold.train, tr_pre, cfg=cfg, seed=seed)
    z_tr, z_te = embed(emb, tr_pre), embed(emb, te_pre)
    models: dict[int, ImageClusterModel] = {}
    tr_lab: dict[int, np.ndarray] = {}
    te_lab: dict[int, np.ndarray] = {}
    for k in ks:
        m, lab = fit_clusterer(emb, z_tr, k, seed=seed)
        models[k], tr_lab[k], te_lab[k] = m, lab, m.assign_z(z_te)
    return FoldFit(fold, emb, models, tr_lab, te_lab, z_te, z_tr)


# --------------------------------------------------------------------------- raw path baselines (tslearn)


def raw_paths(pre: Prefixes) -> np.ndarray:
    """z-normalised close path of each prefix (own mean / std, only bars closed <= t_c), resampled to ``n_bars`` points."""
    n, nb = pre.close.shape
    out = np.zeros((n, nb))
    for i in range(n):
        v = pre.close[i, nb - int(pre.n_valid[i]):]
        sd = v.std()
        v = (v - v.mean()) / (sd if sd > 1e-12 else 1.0)
        out[i] = v if v.size == nb else np.interp(np.linspace(0, 1, nb), np.linspace(0, 1, v.size), v)
    return out


@dataclass
class RawClusterModel:
    method: str
    k: int
    model: Any
    sizes: np.ndarray
    remap: np.ndarray

    def assign(self, pre: Prefixes) -> np.ndarray:
        """Nearest centre (transform only). k-Shape: own deterministic SBD (tslearn's ``predict`` is not bit-reproducible)."""
        x = raw_paths(pre)
        if self.method == "kshape":
            lab = _sbd_assign(x, np.asarray(self.model.cluster_centers_)[:, :, 0])
        else:
            from threadpoolctl import threadpool_limits

            with threadpool_limits(limits=1):
                lab = np.asarray(self.model.predict(x[:, :, None])).astype(int)
        return self.remap[lab]


def _sbd_assign(x: np.ndarray, centers: np.ndarray) -> np.ndarray:
    """argmin over centres of the shape-based distance ``1 - max_shift NCC`` (z-normalised series, ties -> lowest index)."""
    xn = (x - x.mean(axis=1, keepdims=True)) / np.maximum(x.std(axis=1, keepdims=True), 1e-12)
    cn = (centers - centers.mean(axis=1, keepdims=True)) / np.maximum(centers.std(axis=1, keepdims=True), 1e-12)
    out = np.zeros(x.shape[0], dtype=int)
    for i in range(x.shape[0]):
        d = [1.0 - np.max(np.correlate(xn[i], c, mode="full")) / max(np.linalg.norm(xn[i]) * np.linalg.norm(c), 1e-12) for c in cn]
        out[i] = int(np.argmin(d))
    return out


def fit_raw_clusterer(events: pl.DataFrame, pre: Prefixes, method: str, k: int, *, seed: int = SEED) -> tuple[RawClusterModel, np.ndarray]:
    """k-Shape or DTW k-means on the z-normalised raw close path (train only)."""
    from threadpoolctl import threadpool_limits
    from tslearn.clustering import KShape, TimeSeriesKMeans

    _require_train(events, "fit_raw_clusterer")
    x = raw_paths(pre)[:, :, None]
    if method == "kshape":
        mdl = KShape(n_clusters=k, n_init=2, max_iter=50, random_state=seed)
    elif method == "dtw_kmeans":
        mdl = TimeSeriesKMeans(n_clusters=k, metric="dtw", max_iter=12, max_iter_barycenter=8, n_init=1,
                               random_state=seed, n_jobs=1)
    else:
        raise ValueError(method)
    with threadpool_limits(limits=1):                    # BLAS reductions (k-Shape eigen step) are not bit-reproducible
        lab0 = np.asarray(mdl.fit_predict(x)).astype(int)
    sizes0 = np.bincount(lab0, minlength=k)
    order = np.lexsort((np.arange(k), -sizes0))
    remap = np.empty(k, dtype=int)
    remap[order] = np.arange(k)
    return RawClusterModel(method, k, mdl, sizes0[order], remap), remap[lab0]


# --------------------------------------------------------------------------- k-selection (5200: OOF log-loss, 6-block purged CV)


def shrunk_rates(labels: np.ndarray, y: np.ndarray, k: int, a: float = SMOOTH_A) -> tuple[np.ndarray, float]:
    """Cluster impulse rates shrunk to the fit-set prior with strength ``a``; returns (rates, prior)."""
    prior = float(y.mean())
    n = np.bincount(labels, minlength=k).astype(float)
    s = np.bincount(labels, weights=y.astype(float), minlength=k)
    return (s + a * prior) / (n + a), prior


def logloss(p: np.ndarray, y: np.ndarray) -> float:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).mean())


def pair_stability(parts: list[np.ndarray], k: int) -> tuple[float, float]:
    """Mean pairwise (ARI, matched Jaccard) of the partitions of the SAME events made by different fold fits."""
    from scipy.optimize import linear_sum_assignment
    from sklearn.metrics import adjusted_rand_score

    ari, jac = [], []
    for i in range(len(parts)):
        for j in range(i + 1, len(parts)):
            a, b = parts[i], parts[j]
            ari.append(adjusted_rand_score(a, b))
            cm = np.zeros((k, k))
            np.add.at(cm, (a, b), 1)
            ni, nj = cm.sum(axis=1), cm.sum(axis=0)
            jm = cm / np.maximum(ni[:, None] + nj[None, :] - cm, 1)
            r, c = linear_sum_assignment(-jm)
            jac.append(float(jm[r, c].mean()))
    return float(np.mean(ari)), float(np.mean(jac))


def select_k(ev: pl.DataFrame, pre: Prefixes, bounds: list[datetime], embargo_min: int, *, ks: Sequence[int] = KS,
             methods: Sequence[str] = METHODS, cfg: AEConfig | None = None, seed: int = SEED
             ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """6-block purged CV over ONE cell's train events: OOF log-loss of the cluster -> impulse-rate predictor + stability.

    For every fold the embedding / clusterer is fitted on the fold's training events only; the fold's test events are
    assigned afterwards. Stability = agreement of the folds' partitions of the common (non-straddling) train events.
    Returns (summary per method x k, per-fold table, extras).
    """
    ev = ev.filter(pl.col("split") == "train")
    all_ev = ev.filter(~pl.col("cv_straddle"))
    pre_all = pre.select(all_ev)
    nb = len(bounds) - 1
    per_fold: list[dict[str, Any]] = []
    # oof[method][k] -> (p list, y list); parts[method][k] -> list of labels over ``all_ev``
    oof: dict[str, dict[int, tuple[list[float], list[bool]]]] = {m: {k: ([], []) for k in ks} for m in methods}
    parts: dict[str, dict[int, list[np.ndarray]]] = {m: {k: [] for k in ks} for m in methods}
    min_share: dict[str, dict[int, list[float]]] = {m: {k: [] for k in ks} for m in methods}
    prior_rows: list[tuple[list[float], list[bool]]] = []
    for blk in range(nb):
        fold = we.cv_fold(ev, blk, bounds, embargo_min)
        y_tr = fold.train["label_impulse"].to_numpy().astype(float)
        y_te = fold.test["label_impulse"].to_numpy().astype(bool)
        prior = float(y_tr.mean())
        prior_rows.append(([prior] * y_te.size, y_te.tolist()))
        tr_pre, te_pre = pre.select(fold.train), pre.select(fold.test)
        fits: dict[str, Any] = {}
        if "ae_kmeans" in methods:
            ff = fit_fold(ev, pre, blk, bounds, embargo_min, ks, cfg=cfg, seed=seed)
            z_all = embed(ff.emb, pre_all)
            for k in ks:
                fits[("ae_kmeans", k)] = (ff.train_labels[k], ff.test_labels[k], ff.models[k].assign_z(z_all))
        for m in methods:
            if m == "ae_kmeans":
                continue
            for k in ks:
                mdl, lab_tr = fit_raw_clusterer(fold.train, tr_pre, m, k, seed=seed)
                fits[(m, k)] = (lab_tr, mdl.assign(te_pre), mdl.assign(pre_all))
        ll_prior = logloss(np.full(y_te.size, prior), y_te)
        for (m, k), (lab_tr, lab_te, lab_all) in fits.items():
            rates, _ = shrunk_rates(lab_tr, y_tr, k)
            p = rates[lab_te]
            oof[m][k][0].extend(p.tolist())
            oof[m][k][1].extend(y_te.tolist())
            parts[m][k].append(lab_all)
            min_share[m][k].append(float(np.bincount(lab_tr, minlength=k).min() / lab_tr.size))
            per_fold.append({"method": m, "k": k, "block": blk + 1, "n_train": int(y_tr.size), "n_test": int(y_te.size),
                             "ll": logloss(p, y_te), "ll_prior": ll_prior, "delta_ll": logloss(p, y_te) - ll_prior})
        logger.info("fold %d/%d done (train %d, test %d)", blk + 1, nb, y_tr.size, y_te.size)
    p_prior = np.array([p for ps, _ in prior_rows for p in ps])
    y_prior = np.array([v for _, ys in prior_rows for v in ys], dtype=float)
    ll_prior_all = logloss(p_prior, y_prior)
    pf = pd.DataFrame(per_fold)
    rows = []
    for m in methods:
        for k in ks:
            ps, ys = oof[m][k]
            ll = logloss(np.array(ps), np.array(ys, dtype=float))
            ari, jac = pair_stability(parts[m][k], k)
            sub = pf[(pf.method == m) & (pf.k == k)]
            rows.append({"method": m, "k": k, "oof_ll": ll, "prior_ll": ll_prior_all, "delta_ll": ll - ll_prior_all,
                         "blocks_better": int((sub["delta_ll"] < 0).sum()), "ari": ari, "jaccard": jac,
                         "min_share": float(np.mean(min_share[m][k]))})
    return pd.DataFrame(rows), pf, {"prior_ll": ll_prior_all, "n_oof": int(y_prior.size)}


def best_k(summary: pd.DataFrame, method: str) -> int:
    """5200 rule: k with the lowest OOF log-loss (ties -> the smaller k)."""
    s = summary[summary["method"] == method].sort_values(["oof_ll", "k"])
    return int(s.iloc[0]["k"])
