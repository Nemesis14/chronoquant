# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Impulse Scout model 2 (epic_073 / t933, 5300): MiniRocket (family P2, hypothesis H2, primary) and InceptionTime
(family X1, exploratory) on the raw, normalised prefix.

**Prefix** (:func:`build_prefix`, nothing after ``t_c``): the 1m bars opening in ``[t0 - B, t_c - 1m]`` (bar indexes
``i - B .. tc`` of ``wave_events``; every bar is closed ``<= t_c``), aggregated to ``PREFIX_STEP``-minute bars
(``L = (B + W) / step = 96``), three channels:

* ``close_sigma``  : ``log(close / low[t0]) / sigma_W`` (amplitude preserved, 5300: the signal is the relative rebound size)
* ``log_vol_rel``  : ``log1p(volume / (step * vol7d_mean_1m))`` (the asset's own 7-day mean known at ``t0``)
* ``taker_imb``    : ``taker_buy_base / volume - 0.5`` (order flow from the bars; 0 for an empty bar)

**MiniRocket** (Dempster, Schmidt & Webb 2021; implemented here in numpy, no sktime / aeon dependency): the 84 fixed
length-9 kernels ({-1, 2} weights), dilations from the series length, multivariate channel combinations and bias
quantiles drawn from fold-train examples with the ``seed``; PPV features (~10k). The transform is label free, so it is
computed once per (fold-train rows, seed) and cached in :class:`RocketPrefix` -- under the label permutation the
features are identical and only the label-dependent head is refit (a full refit in the 5300 sense). Head: ridge
regression on the one-hot classes (closed form via the fold-train Gram eigendecomposition), ``alpha`` by exact
leave-one-out error over a fixed grid (as ``RidgeClassifierCV``), then a multinomial logistic calibrator on the
leave-one-out ridge outputs -> 3-class probabilities.

**InceptionTime** (Ismail Fawaz et al. 2020), small fixed variant (:data:`INCEPTION_CONFIG`), torch CPU, single
thread, deterministic; early stopping on an inner validation split = the last 20% (time order) of the fold-train
events, never the test fold.

Data access: :func:`load_bars` reads the live DBs read-only with an SQL filter ``open_time < oos_start`` (no OOS /
holdout-2 bar enters memory); the frame is ``sb.load_scout_events()`` (train split only).
"""

from __future__ import annotations

import hashlib
import itertools
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from reports.builders import scout_bench as sb
from reports.builders import wave_events as we

logger = logging.getLogger(__name__)

ROCKET_NAME = "scout_minirocket_r1"
INCEPTION_NAME = "scout_inception_r1"
PREFIX_STEP = 5
CHANNELS: tuple[str, ...] = ("close_sigma", "log_vol_rel", "taker_imb")

#: pre-registered, fixed round-1 configuration of the H2 model (set before any evaluation)
ROCKET_CONFIG: dict[str, Any] = {
    "prefix": {"window": "[t0-B, t_c-1m] 1m bars", "step_min": PREFIX_STEP, "channels": list(CHANNELS)},
    "transform": "MiniRocket multivariate (numpy), 84 kernels x dilations, PPV",
    "num_features": 10_000, "max_dilations_per_kernel": 32,
    "head": "ridge one-hot (Gram eigh), alpha by exact LOO MSE, multinomial logistic calibrator (C=1) on LOO outputs",
    "alphas": [float(a) for a in np.logspace(0, 7, 15)], "calib_c": 1.0, "standardise": "fold-train mean/sd",
}
#: exploratory X1 configuration (fixed; no hypothesis)
INCEPTION_CONFIG: dict[str, Any] = {
    "prefix": ROCKET_CONFIG["prefix"], "depth": 3, "n_filters": 8, "kernel_sizes": [15, 7, 3], "bottleneck": 8,
    "residual": "input -> after module 3", "max_epochs": 25, "patience": 4, "batch_size": 128, "lr": 1e-3,
    "weight_decay": 1e-4, "inner_val_frac": 0.2, "inner_val": "last 20% of fold-train events (time order)",
    "ensemble": 1, "threads": 1, "input_norm": "fold-train per-channel mean/sd, clip +-10",
}


# --------------------------------------------------------------------------- data (train only)


def load_bars(assets: list[str], *, tries: int = 720, delay: float = 5.0, cache_dir: str | Path | None = None
              ) -> tuple[dict[str, we.MinuteData], dict[str, np.ndarray]]:
    """1m OHLCV grids (trade simulation + prefix) and taker-buy-base grids per asset, SQL-filtered to
    ``open_time < oos_start``: nothing from OOS / holdout-2 enters memory. Retries while the app's API / worker holds
    the live DB (never kills them). ``cache_dir``: the filtered train-period frames are kept as
    ``bars_<asset>.parquet`` and reused (re-filtered) on later calls."""
    import duckdb

    from data_handling.store.readonly_query import open_readonly_connection
    from reports.builders.wave_oos_eval import assert_no_holdout

    cut = datetime.fromisoformat(we.params_v2().oos_start)
    sql = ("SELECT open_time, open, high, low, close, volume, taker_buy_base FROM live.ohlcv "
           "WHERE open_time < ? ORDER BY open_time")
    bars: dict[str, we.MinuteData] = {}
    tbb: dict[str, np.ndarray] = {}
    for a in assets:
        last: Exception | None = None
        df = None
        cached = Path(cache_dir) / f"bars_{a}.parquet" if cache_dir else None
        if cached is not None and cached.exists():
            df = pl.read_parquet(cached).filter(pl.col("open_time") < cut)
        for attempt in range(0 if df is not None else tries):
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
        if df.height and df["open_time"].max() >= cut:
            raise ValueError("bar at / after the OOS start loaded")
        if cached is not None and not cached.exists():
            df.write_parquet(cached)                 # train-period bars only: later stages skip the live DB
        m = we.to_minute_grid(df)
        bars[a] = m
        tbb[a] = minute_col(df, m, "taker_buy_base")
    return bars, tbb


def minute_col(df: pl.DataFrame, m: we.MinuteData, col: str) -> np.ndarray:
    """One extra column of the 1m frame on the complete minute grid of ``m`` (missing minutes NaN)."""
    t = df["open_time"].to_numpy().astype("datetime64[m]").astype("int64") - m.m0
    arr = np.full(m.n, np.nan)
    arr[t] = df[col].cast(pl.Float64).to_numpy()
    return arr


# --------------------------------------------------------------------------- prefix


@dataclass
class RocketPrefix:
    """``x[row_id]`` = ``(C, L)`` prefix of the prepared frame's row; ``cache`` holds label-free fold transforms."""

    x: np.ndarray
    channels: tuple[str, ...] = CHANNELS
    step: int = PREFIX_STEP
    cache: dict[Any, Any] = field(default_factory=dict, repr=False)

    def __getstate__(self) -> dict[str, Any]:          # never ship the (large) cache to worker processes
        return {"x": self.x, "channels": self.channels, "step": self.step, "cache": {}}


def event_prefix(m: we.MinuteData, tbb: np.ndarray, event_start: Any, t_c: Any, low_t0: float, sigma_w: float,
                 vol7d_mean_1m: float, step: int = PREFIX_STEP) -> np.ndarray:
    """``(3, L)`` prefix of one event from the 1m bars opening in ``[event_start, t_c - 1m]`` (closed ``<= t_c``)."""
    i0 = we._epoch_min(event_start) - m.m0
    i1 = we._epoch_min(t_c) - m.m0                      # first bar NOT used: opens at t_c (label window)
    n = i1 - i0
    if i0 < 0 or i1 > m.n or n <= 0 or n % step:
        raise ValueError(f"prefix window out of range / not a multiple of {step}: {event_start} .. {t_c}")
    close = m.close[i0:i1].copy()
    vol = np.nan_to_num(m.volume[i0:i1], nan=0.0)
    buy = np.nan_to_num(tbb[i0:i1], nan=0.0)
    ok = np.flatnonzero(np.isfinite(close))
    if not ok.size:
        raise ValueError("empty prefix")
    idx = np.maximum.accumulate(np.where(np.isfinite(close), np.arange(n), -1))        # forward fill
    close = np.where(idx >= 0, close[np.maximum(idx, 0)], close[ok[0]])
    L = n // step
    c5 = close.reshape(L, step)[:, -1]
    v5 = vol.reshape(L, step).sum(axis=1)
    b5 = buy.reshape(L, step).sum(axis=1)
    ch0 = np.log(c5 / low_t0) / sigma_w
    ch1 = np.log1p(v5 / (step * vol7d_mean_1m)) if vol7d_mean_1m > 0 else np.zeros(L)
    with np.errstate(invalid="ignore", divide="ignore"):
        ch2 = np.where(v5 > 0, b5 / v5 - 0.5, 0.0)
    return np.vstack([ch0, ch1, ch2]).astype(np.float32)


def build_prefix(frame: pl.DataFrame, bars: dict[str, we.MinuteData], tbb: dict[str, np.ndarray],
                 step: int = PREFIX_STEP) -> RocketPrefix:
    """Prefix array aligned with the prepared frame's ``row_id`` (= row position)."""
    if "row_id" not in frame.columns or not np.array_equal(frame["row_id"].to_numpy(), np.arange(frame.height)):
        raise ValueError("frame must be prepared (row_id == position)")
    sb.guard_no_holdout(frame)
    rows = frame.select("asset_id", "event_start", "t_c", "low_t0", "sigma_w", "vol7d_mean_1m").iter_rows()
    x = np.stack([event_prefix(bars[a], tbb[a], es, tc, float(lo), float(sg), float(v7), step)
                  for a, es, tc, lo, sg, v7 in rows])
    if not np.isfinite(x).all():
        raise ValueError("non-finite prefix values")
    return RocketPrefix(x=x, step=step)


# --------------------------------------------------------------------------- MiniRocket (numpy)

_COMBS = np.array(list(itertools.combinations(range(9), 3)), dtype=np.int64)     # 84 kernels: positions of the "2"s
N_KERNELS = len(_COMBS)


@dataclass
class MiniRocketParams:
    dilations: np.ndarray
    feats_per_dilation: np.ndarray
    channels: list[np.ndarray]          # per (dilation, kernel) combination
    biases: list[np.ndarray]            # per combination


def _dilations(length: int, num_features: int, max_dil: int) -> tuple[np.ndarray, np.ndarray]:
    per_kernel = num_features // N_KERNELS
    true_max = min(per_kernel, max_dil)
    mult = per_kernel / true_max
    max_exp = np.log2((length - 1) / (9 - 1))
    dil, cnt = np.unique(np.floor(np.logspace(0, max_exp, true_max, base=2)).astype(np.int64), return_counts=True)
    fpd = (cnt * mult).astype(np.int64)
    rem, i = per_kernel - int(fpd.sum()), 0
    while rem > 0:
        fpd[i] += 1
        rem -= 1
        i = (i + 1) % len(fpd)
    return dil, fpd


def _quantiles(n: int) -> np.ndarray:
    return np.array([(k * ((np.sqrt(5) + 1) / 2)) % 1 for k in range(1, n + 1)], dtype=np.float64)


def _shifted(x: np.ndarray, d: int) -> np.ndarray:
    """``(9, n, C, L)``: ``x`` shifted by ``(k - 4) * d`` with zero padding (kernel tap ``k``)."""
    n, c, length = x.shape
    out = np.zeros((9, n, c, length), dtype=np.float32)
    for k in range(9):
        s = (k - 4) * d
        if s >= 0 and s < length:
            out[k, :, :, :length - s] = x[:, :, s:]
        elif s < 0 and -s < length:
            out[k, :, :, -s:] = x[:, :, :length + s]
    return out


def _conv(sh: np.ndarray, total: np.ndarray, kernel: int, ch: np.ndarray) -> np.ndarray:
    """Convolution output ``(n, L)`` of one kernel summed over the channel subset ``ch``."""
    a, b, c = _COMBS[kernel]
    return (-total[:, ch] + 3.0 * (sh[a][:, ch] + sh[b][:, ch] + sh[c][:, ch])).sum(axis=1)


def _valid(length: int, d: int, padded: bool) -> slice:
    p = ((9 - 1) * d) // 2
    return slice(None) if padded or 2 * p >= length else slice(p, length - p)


def minirocket_fit(x: np.ndarray, *, seed: int, num_features: int = 10_000, max_dil: int = 32) -> MiniRocketParams:
    """Dilations, channel combinations and bias quantiles from training series ``x`` ``(n, C, L)`` (label free)."""
    n, c, length = x.shape
    rng = np.random.default_rng(seed)
    dil, fpd = _dilations(length, num_features, max_dil)
    n_comb = N_KERNELS * len(dil)
    max_ch = min(c, 9)
    n_ch = np.floor(2 ** rng.uniform(0, np.log2(max_ch + 1), n_comb)).astype(np.int64)
    channels = [np.sort(rng.choice(c, int(k), replace=False)) for k in n_ch]
    q_all = _quantiles(N_KERNELS * int(fpd.sum()))
    biases: list[np.ndarray] = []
    qi, comb = 0, 0
    for di, d in enumerate(dil):
        f = int(fpd[di])
        ex = rng.integers(0, n, N_KERNELS)
        sh = _shifted(x[ex], int(d))                 # one random training example per kernel
        total = sh.sum(axis=0)
        for k in range(N_KERNELS):
            conv = _conv(sh[:, k:k + 1], total[k:k + 1], k, channels[comb])[0]
            biases.append(np.quantile(conv, q_all[qi:qi + f]).astype(np.float32))
            qi += f
            comb += 1
    return MiniRocketParams(dil, fpd, channels, biases)


def minirocket_transform(x: np.ndarray, params: MiniRocketParams, *, chunk: int = 512) -> np.ndarray:
    """PPV features ``(n, F)``; padding alternates over (dilation, kernel) as in the reference implementation."""
    n, _, length = x.shape
    n_feat = sum(len(b) for b in params.biases)
    out = np.empty((n, n_feat), dtype=np.float32)
    for s0 in range(0, n, chunk):
        xs = x[s0:s0 + chunk]
        col, comb = 0, 0
        for di, d in enumerate(params.dilations):
            sh = _shifted(xs, int(d))
            total = sh.sum(axis=0)
            for k in range(N_KERNELS):
                b = params.biases[comb]
                conv = _conv(sh, total, k, params.channels[comb])[:, _valid(length, int(d), (di + k) % 2 == 0)]
                out[s0:s0 + chunk, col:col + len(b)] = (conv[:, :, None] > b[None, None, :]).mean(axis=1)
                col += len(b)
                comb += 1
    return out


# --------------------------------------------------------------------------- MiniRocket model (P2 / H2)


def _rows_key(rows: np.ndarray, seed: int, tag: str) -> tuple[str, int, str]:
    return tag, seed, hashlib.sha1(np.ascontiguousarray(rows, dtype=np.int64).tobytes()).hexdigest()


@dataclass
class _FoldBasis:
    """Label-free part of one fold: eigenbasis of the fold-train Gram matrix and the projection of every row."""

    rows: np.ndarray            # fold-train row_ids
    u: np.ndarray               # (n_tr, r)
    s: np.ndarray               # (r,) singular values
    proj: np.ndarray            # (n_all, r) = Z_all V


def fold_basis(prefix: RocketPrefix, rows: np.ndarray, seed: int, cfg: dict[str, Any] = ROCKET_CONFIG) -> _FoldBasis:
    key = _rows_key(rows, seed, "rocket")
    hit = prefix.cache.get(key)
    if hit is not None:
        return hit
    params = minirocket_fit(prefix.x[rows], seed=seed, num_features=cfg["num_features"],
                            max_dil=cfg["max_dilations_per_kernel"])
    f = minirocket_transform(prefix.x, params).astype(np.float64)
    mu = f[rows].mean(axis=0)
    sd = f[rows].std(axis=0)
    z = (f - mu) / np.where(sd > 1e-12, sd, 1.0)
    del f
    ztr = z[rows]
    lam, u = np.linalg.eigh(ztr @ ztr.T)
    keep = lam > 1e-9 * lam.max()
    lam, u = lam[keep][::-1], u[:, keep][:, ::-1]
    s = np.sqrt(lam)
    proj = (z @ ztr.T) @ u / s
    basis = _FoldBasis(rows=np.asarray(rows), u=np.ascontiguousarray(u), s=s, proj=proj)
    prefix.cache[key] = basis
    return basis


def ridge_loo(u: np.ndarray, s: np.ndarray, y: np.ndarray, alphas: list[float]) -> tuple[float, np.ndarray, np.ndarray]:
    """Multi-output ridge with intercept in the eigenbasis: best ``alpha`` by exact LOO MSE, its LOO predictions
    ``(n, k)`` and the coefficient matrix ``a`` (``pred = proj @ a + mean``)."""
    n = y.shape[0]
    ym = y.mean(axis=0)
    uty = u.T @ (y - ym)
    u2 = u * u
    best: tuple[float, float, np.ndarray] = (np.inf, alphas[0], y)
    for a in alphas:
        shrink = s * s / (s * s + a)
        fit = u @ (shrink[:, None] * uty) + ym
        h = u2 @ shrink + 1.0 / n
        res = (y - fit) / (1.0 - h)[:, None]
        mse = float(np.mean(res * res))
        if mse < best[0]:
            best = (mse, a, y - res)
    alpha = float(best[1])
    coef = (s / (s * s + alpha))[:, None] * uty
    return alpha, best[2], coef


class MiniRocketModel(sb.ScoutModel):
    """P2: MiniRocket features + LOO-ridge + multinomial calibrator (see module docstring)."""

    def __init__(self, cfg: dict[str, Any] | None = None) -> None:
        self.cfg = cfg or ROCKET_CONFIG

    def fit(self, train: pl.DataFrame, prefix_data: Any, *, seed: int) -> None:
        from sklearn.linear_model import LogisticRegression

        rows = train["row_id"].to_numpy()
        self.basis = fold_basis(prefix_data, rows, seed, self.cfg)
        ycls = sb.frame.y_class(train)
        y = np.eye(len(sb.CLASSES))[ycls]
        self.alpha, loo, self.coef = ridge_loo(self.basis.u, self.basis.s, y, self.cfg["alphas"])
        self.ym = y.mean(axis=0)
        self.calib = LogisticRegression(C=self.cfg["calib_c"], max_iter=2000).fit(loo, ycls)

    def predict_proba(self, df: pl.DataFrame, prefix_data: Any) -> np.ndarray:
        r = self.basis.proj[df["row_id"].to_numpy()] @ self.coef + self.ym
        p = self.calib.predict_proba(r)
        out = np.full((df.height, len(sb.CLASSES)), 1e-6)
        out[:, self.calib.classes_] = p
        return out / out.sum(axis=1, keepdims=True)


# --------------------------------------------------------------------------- InceptionTime (X1, exploratory)


def _inception_net(c_in: int, cfg: dict[str, Any]) -> Any:
    import torch
    from torch import nn

    nf, ks, bott = cfg["n_filters"], cfg["kernel_sizes"], cfg["bottleneck"]

    class Module(nn.Module):
        def __init__(self, ci: int) -> None:
            super().__init__()
            self.bott = nn.Conv1d(ci, bott, 1, bias=False) if ci > 1 else nn.Identity()
            cb = bott if ci > 1 else ci
            self.convs = nn.ModuleList([nn.Conv1d(cb, nf, k, padding=k // 2, bias=False) for k in ks])
            self.pool = nn.MaxPool1d(3, stride=1, padding=1)
            self.pconv = nn.Conv1d(ci, nf, 1, bias=False)
            self.bn = nn.BatchNorm1d(nf * (len(ks) + 1))

        def forward(self, x: Any) -> Any:
            b = self.bott(x)
            return torch.relu(self.bn(torch.cat([c(b) for c in self.convs] + [self.pconv(self.pool(x))], dim=1)))

    class Net(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            width = nf * (len(ks) + 1)
            self.mods = nn.ModuleList([Module(c_in if i == 0 else width) for i in range(cfg["depth"])])
            self.short = nn.Sequential(nn.Conv1d(c_in, width, 1, bias=False), nn.BatchNorm1d(width))
            self.fc = nn.Linear(width, len(sb.CLASSES))

        def forward(self, x: Any) -> Any:
            h, res = x, x
            for i, m in enumerate(self.mods):
                h = m(h)
                if i % 3 == 2:
                    h = torch.relu(h + self.short(res))
                    res = h
            return self.fc(h.mean(dim=2))

    return Net()


class InceptionModel(sb.ScoutModel):
    """X1: small InceptionTime, early stopping on the last 20% (time order) of the fold-train events."""

    def __init__(self, cfg: dict[str, Any] | None = None) -> None:
        self.cfg = cfg or INCEPTION_CONFIG

    def _x(self, prefix: RocketPrefix, rows: np.ndarray) -> np.ndarray:
        return np.clip((prefix.x[rows] - self.mu) / self.sd, -10, 10).astype(np.float32)

    def fit(self, train: pl.DataFrame, prefix_data: Any, *, seed: int) -> None:
        import torch
        from torch import nn

        cfg = self.cfg
        torch.set_num_threads(int(cfg["threads"]))
        torch.manual_seed(seed)
        rows = train["row_id"].to_numpy()
        order = np.argsort(rows, kind="stable")           # row_id order == time order of the prepared frame
        rows = rows[order]
        y = sb.frame.y_class(train)[order]
        x = prefix_data.x[rows]
        self.mu = x.mean(axis=(0, 2), keepdims=True)[0]
        sd = x.std(axis=(0, 2), keepdims=True)[0]
        self.sd = np.where(sd > 1e-9, sd, 1.0)
        xs = torch.from_numpy(self._x(prefix_data, rows))
        ys = torch.from_numpy(y.astype(np.int64))
        n_val = max(1, int(round(cfg["inner_val_frac"] * len(rows))))
        xtr, ytr, xva, yva = xs[:-n_val], ys[:-n_val], xs[-n_val:], ys[-n_val:]
        net = _inception_net(xs.shape[1], cfg)
        opt = torch.optim.Adam(net.parameters(), lr=cfg["lr"], weight_decay=cfg["weight_decay"])
        loss_fn = nn.CrossEntropyLoss()
        gen = torch.Generator().manual_seed(seed)
        best, best_state, bad = np.inf, None, 0
        self.epochs = 0
        for _ in range(int(cfg["max_epochs"])):
            net.train()
            perm = torch.randperm(len(xtr), generator=gen)
            for i in range(0, len(xtr), int(cfg["batch_size"])):
                idx = perm[i:i + int(cfg["batch_size"])]
                if len(idx) < 2:
                    continue
                opt.zero_grad()
                loss_fn(net(xtr[idx]), ytr[idx]).backward()
                opt.step()
            net.eval()
            with torch.no_grad():
                v = float(loss_fn(net(xva), yva))
            self.epochs += 1
            if v < best - 1e-5:
                best, bad = v, 0
                best_state = {k: t.detach().clone() for k, t in net.state_dict().items()}
            else:
                bad += 1
                if bad >= int(cfg["patience"]):
                    break
        if best_state is not None:
            net.load_state_dict(best_state)
        net.eval()
        self.net, self.best_val = net, best

    def predict_proba(self, df: pl.DataFrame, prefix_data: Any) -> np.ndarray:
        import torch

        torch.set_num_threads(int(self.cfg["threads"]))
        with torch.no_grad():
            logits = self.net(torch.from_numpy(self._x(prefix_data, df["row_id"].to_numpy())))
            return torch.softmax(logits, dim=1).double().numpy()


# --------------------------------------------------------------------------- registry


def rocket_spec() -> sb.ModelSpec:
    return sb.ModelSpec(ROCKET_NAME, MiniRocketModel, family="P2", hypothesis="H2", round=1, config=ROCKET_CONFIG,
                        description="MiniRocket (numpy) a nyers σ_W-prefixen (5m, 3 csatorna) + LOO-ridge + kalibrátor")


def inception_spec() -> sb.ModelSpec:
    return sb.ModelSpec(INCEPTION_NAME, InceptionModel, family="X1", hypothesis=None, round=1, config=INCEPTION_CONFIG,
                        description="kis InceptionTime (3 modul, 8 szűrő), belső train-split early stopping; kísérleti")


def register(registry: sb.ModelRegistry) -> sb.ModelRegistry:
    """Register both t933 models into ``registry`` (e.g. ``sb.REGISTRY`` for t936)."""
    registry.register(rocket_spec())
    registry.register(inception_spec())
    return registry


def registry() -> sb.ModelRegistry:
    """Baseline + the t933 models (this ticket's own run)."""
    return register(sb.new_registry())


# --------------------------------------------------------------------------- exploratory permutation (parallel)


def _perm_chunk(args: tuple[Any, ...]) -> list[tuple[int, float, float]]:
    from reports.builders.scout_bench import metrics as mt
    from reports.builders.scout_bench import stats as st

    fr, folds, base, spec, prefix, bundles, idx, seed = args
    out = []
    for i in idx:
        rng = np.random.default_rng([seed, i])
        fp = st.permute_frame_labels(fr, st.permutation_source(fr.height, bundles, rng))
        yp = sb.frame.y_binary(fp)
        a = mt.auc(yp, sb.cross_val(base, fp, folds, prefix, seed=seed, thresholds=False).score)
        d = mt.auc(yp, sb.cross_val(spec, fp, folds, prefix, seed=seed, thresholds=False).score) - a
        out.append((int(i), a, d))
    return out


def permutation_null_parallel(fr: pl.DataFrame, folds: sb.FoldSet, base: sb.ModelSpec, spec: sb.ModelSpec,
                              prefix: RocketPrefix, *, n_perm: int, seed: int = 20260101, workers: int = 4
                              ) -> np.ndarray:
    """Same null as ``scout_bench.runner.permutation_null`` (identical permutation ``i`` <- rng ``[seed, i]``, full
    refit of baseline and model) for ONE exploratory model, split over worker processes."""
    from reports.builders.scout_bench import stats as st

    bundles = st.day_bundles(fr["t0"].to_numpy(), fr["asset_id"].to_numpy())
    chunks = [list(range(w, n_perm, workers)) for w in range(workers)]
    null = np.full(n_perm, np.nan)
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for res in ex.map(_perm_chunk, [(fr, folds, base, spec, prefix, bundles, c, seed) for c in chunks]):
            for i, _, d in res:
                null[i] = d
    return null


__all__ = ["CHANNELS", "INCEPTION_CONFIG", "INCEPTION_NAME", "PREFIX_STEP", "ROCKET_CONFIG", "ROCKET_NAME",
           "InceptionModel", "MiniRocketModel", "MiniRocketParams", "RocketPrefix", "build_prefix", "event_prefix",
           "fold_basis", "inception_spec", "load_bars", "minirocket_fit", "minirocket_transform", "minute_col",
           "permutation_null_parallel", "register", "registry", "ridge_loo", "rocket_spec"]
