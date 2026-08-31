#!/usr/bin/env python3
"""Walk-forward evaluation of the triple-barrier model (epic_058).

Simulates how the system would actually be operated rather than how it is
convenient to backtest. Earlier measurements in this project trained one model
and let it run untouched for twelve months; in live use the model would be
refitted monthly, so the backtest was harsher than reality in a way that had
nothing to do with whether the edge exists.

Protocol, fixed before the first run:

    hyperparameter search   ONCE, on 2023-01..2024-06 with an inner split.
                            It never sees a single evaluation month.

    then, per month M:      refit on everything up to M (minus purge),
                            recalibrate the entry threshold from that fit's
                            own training predictions, predict M, retire.

Every month is therefore predicted by a model that has never seen it, and the
25 one-month out-of-sample slices concatenate into one continuous series
covering both a +21% and a -57% regime.

Economics per trade (Binance VIP0, no effective BNB discount):

    win  (TP first)  +100 bps - 6.5 entry - 2.0 maker exit  = +91.5 bps
    loss (SL first)  -100 bps - 6.5 entry - 6.5 taker exit  = -113.0 bps
    open (neither)   actual 4h close return - 13.0 bps

    break-even hit rate = 113.0 / (91.5 + 113.0) = 55.3%

Usage:
    python -m modeling.experiments.walk_forward_tb --trials 100
    python -m modeling.experiments.walk_forward_tb --trials 20 --quick
"""

import argparse
import json
import logging
import pathlib
import sys
import time
import warnings

import duckdb
import lightgbm as lgb
import numpy as np
import optuna
import pandas as pd

import utils

logger = logging.getLogger(__name__)

LABEL_COL = "tb_label_240_100"
# Volatility-scaled labels (epic_060) carry their own barrier width per bar, in
# a companion column. With a fixed barrier this stays None and every trade is
# priced at BARRIER_BPS, which is the old behaviour exactly.
BPS_COL: str | None = None
BARRIER_BPS = 100.0
# Feature-name prefixes excluded from the model (epic_061 ablation). The
# columns are still loaded so the row set is identical between arms -- only
# what the model may look at changes.
DROP_PREFIXES: list[str] = []
HORIZON_MIN = 240
SEARCH_TRAIN_START = "2023-01-01"
SEARCH_TRAIN_END = "2024-06-30"
EVAL_START = "2024-07-01"
EVAL_END = "2026-07-31"
EMBARGO_MIN = 240
PURGE_MIN = HORIZON_MIN  # a training label must not reach into the eval month
CALIB_DAYS = 30          # trailing window the daily threshold is drawn from
SEED = 42

# Fee components kept separate so a discount can be applied to the exchange fee
# without touching slippage -- BNB pays down the fee, not the spread.
MAKER_FEE_BPS = 2.0
TAKER_FEE_BPS = 5.0
SLIPPAGE_BPS = 1.5

ENTRY_BPS = TAKER_FEE_BPS + SLIPPAGE_BPS  # market entry
MAKER_ENTRY_BPS = MAKER_FEE_BPS  # resting limit entry, no spread crossed
EXIT_MAKER_BPS = MAKER_FEE_BPS  # resting TP limit, no spread crossed
EXIT_TAKER_BPS = TAKER_FEE_BPS + SLIPPAGE_BPS  # stop or forced close
WIN_NET_BPS = 100.0 - ENTRY_BPS - EXIT_MAKER_BPS
LOSS_NET_BPS = -100.0 - ENTRY_BPS - EXIT_TAKER_BPS
BREAKEVEN_HIT = -LOSS_NET_BPS / (WIN_NET_BPS - LOSS_NET_BPS)

FIXED_PARAMS: dict = {
    "objective": "multiclass",
    "num_class": 3,
    "boosting_type": "gbdt",
    "metric": "multi_logloss",
    "verbosity": -1,
    "n_jobs": 4,
    "force_col_wise": True,
    "random_state": SEED,
}


def _connect(asset_id: str) -> duckdb.DuckDBPyConnection:
    path = utils.load_asset_config(asset_id)["database"]["db_path"]
    for _ in range(15):
        try:
            return duckdb.connect(path, read_only=True)
        except duckdb.IOException:
            time.sleep(8)
    raise RuntimeError("A live DuckDB tartosan zarolt (fut a Streamlit dashboard?)")


def load_dataset(asset_id: str = "solusdt") -> tuple[pd.DataFrame, list[str]]:
    """Hourly sample: features, label, and the horizon close return for timeouts."""
    conn = _connect(asset_id)
    try:
        cols = [
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'quant_train'"
            ).fetchall()
        ]
        if LABEL_COL not in cols:
            raise ValueError(f"{LABEL_COL} hianyzik a quant_train-bol")
        feats = [c for c in cols if c.startswith("feat_")]
        feat_sql = ", ".join(f'q."{c}"' for c in feats)
        bps_sql = (
            f'q."{BPS_COL}"::DOUBLE'
            if BPS_COL and BPS_COL in cols
            else f"{BARRIER_BPS}::DOUBLE"
        )

        frame = conn.execute(f"""
            WITH picked AS (
                SELECT open_time,
                       row_number() OVER (
                           PARTITION BY date_trunc('hour', open_time)
                           ORDER BY hash(epoch_ms(open_time) + {SEED})
                       ) AS rn
                FROM quant_train
                WHERE "{LABEL_COL}" IS NOT NULL
                  AND open_time >= '{SEARCH_TRAIN_START}' AND open_time <= '{EVAL_END}'
            )
            SELECT q.open_time, {feat_sql}, q."{LABEL_COL}" AS label,
                   {bps_sql} AS barrier_bps,
                   ln(o2.close / o1.close) AS close_ret,
                   o1.close AS signal_close,
                   o3.low  AS next_low,
                   o3.high AS next_high,
                   o4.low  AS exit_next_low,
                   o4.high AS exit_next_high
            FROM quant_train q
            JOIN picked p ON p.open_time = q.open_time AND p.rn = 1
            JOIN ohlcv o1 ON o1.open_time = q.open_time
            LEFT JOIN ohlcv o2 ON o2.open_time = q.open_time + INTERVAL {HORIZON_MIN} MINUTE
            LEFT JOIN ohlcv o3 ON o3.open_time = q.open_time + INTERVAL 1 MINUTE
            LEFT JOIN ohlcv o4 ON o4.open_time = q.open_time
                                  + INTERVAL {HORIZON_MIN} MINUTE + INTERVAL 1 MINUTE
            ORDER BY q.open_time
        """).df()
    finally:
        conn.close()

    if DROP_PREFIXES:
        kept = [c for c in feats if not any(c.startswith(p) for p in DROP_PREFIXES)]
        logger.info("kizarva %d feature (%s)", len(feats) - len(kept),
                    ", ".join(DROP_PREFIXES))
        feats = kept

    logger.info("betoltve: %d sor, %d feature", len(frame), len(feats))
    return frame, feats


def monthly_funding(asset_id: str = "solusdt") -> pd.Series:
    """Average settled funding rate per month, for the holding-cost estimate."""
    conn = _connect(asset_id)
    try:
        df = conn.execute("""
            SELECT strftime(funding_ts, '%Y-%m') AS ho, avg(funding_rate) AS rate
            FROM funding_rate GROUP BY 1 ORDER BY 1
        """).df()
    finally:
        conn.close()
    return df.set_index("ho")["rate"]


def _score_trial(params: dict, x_tr, y_tr, x_va, y_va) -> float:
    """Lift of the top decile of P(+1): what the strategy actually consumes.

    Raw log-loss would reward getting the (tiny) timeout class right, which no
    trade ever depends on.
    """
    model = lgb.LGBMClassifier(**{**FIXED_PARAMS, **params})
    model.fit(
        x_tr, y_tr + 1,
        eval_set=[(x_va, y_va + 1)],
        callbacks=[lgb.early_stopping(80, verbose=False), lgb.log_evaluation(period=-1)],
    )
    classes = list(model.classes_)
    proba = model.predict_proba(x_va)
    best = 0.0
    for idx, target in ((classes.index(2), 1), (classes.index(0), -1)):
        base = float((y_va == target).mean())
        thr = float(np.quantile(proba[:, idx], 0.90))
        sel = proba[:, idx] >= thr
        if sel.sum() >= 20:
            best = max(best, float((y_va[sel] == target).mean()) - base)
    return best


def search_params(frame: pd.DataFrame, feats: list[str], n_trials: int) -> dict:
    """Optuna search on the earliest window only -- never touches evaluation."""
    ts = pd.to_datetime(frame["open_time"])
    lo = pd.Timestamp(SEARCH_TRAIN_START) + pd.Timedelta(minutes=EMBARGO_MIN)
    hi = pd.Timestamp(SEARCH_TRAIN_END) + pd.Timedelta(days=1) - pd.Timedelta(minutes=PURGE_MIN)
    block = frame[(ts >= lo) & (ts < hi)]
    cut = int(len(block) * 0.80)
    x_tr, y_tr = block[feats].iloc[:cut], block["label"].to_numpy()[:cut]
    x_va, y_va = block[feats].iloc[cut:], block["label"].to_numpy()[cut:]
    logger.info("kereses: train=%d  belso valid=%d", len(y_tr), len(y_va))

    def objective(trial: optuna.Trial) -> float:
        params = {
            "n_estimators": 3000,
            "learning_rate": trial.suggest_float("learning_rate", 0.005, 0.08, log=True),
            "num_leaves": trial.suggest_int("num_leaves", 4, 64, log=True),
            "min_child_samples": trial.suggest_int("min_child_samples", 50, 800, log=True),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "subsample_freq": 1,
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.3, 1.0),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-2, 100.0, log=True),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-3, 10.0, log=True),
        }
        return _score_trial(params, x_tr, y_tr, x_va, y_va)

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction="maximize",
                                sampler=optuna.samplers.TPESampler(seed=SEED))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    logger.info("legjobb lift a belso validon: %.4f", study.best_value)

    best = dict(study.best_params)
    best.update({"n_estimators": 3000, "subsample_freq": 1})
    return best


def walk_forward(
    frame: pd.DataFrame,
    feats: list[str],
    params: dict,
    quantile: float,
    funding: pd.Series,
    maker_entry: bool = False,
    maker_timeout: bool = False,
) -> pd.DataFrame:
    """Refit monthly, predict that month, and price the resulting trades."""
    ts = pd.to_datetime(frame["open_time"])
    months = pd.period_range(EVAL_START, EVAL_END, freq="M")
    rows = []

    for period in months:
        m_start = period.to_timestamp()
        m_end = (period + 1).to_timestamp()

        train_hi = m_start - pd.Timedelta(minutes=PURGE_MIN)
        train_lo = pd.Timestamp(SEARCH_TRAIN_START) + pd.Timedelta(minutes=EMBARGO_MIN)
        tr = frame[(ts >= train_lo) & (ts < train_hi)]
        te = frame[(ts >= m_start) & (ts < m_end)]
        if len(tr) < 3000 or len(te) < 50:
            continue

        cut = int(len(tr) * 0.85)
        model = lgb.LGBMClassifier(**{**FIXED_PARAMS, **params})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(
                tr[feats].iloc[:cut], tr["label"].to_numpy()[:cut] + 1,
                eval_set=[(tr[feats].iloc[cut:], tr["label"].to_numpy()[cut:] + 1)],
                callbacks=[lgb.early_stopping(80, verbose=False),
                           lgb.log_evaluation(period=-1)],
            )

        classes = list(model.classes_)
        i_dn, i_up = classes.index(0), classes.index(2)
        proba_tr = model.predict_proba(tr[feats])
        proba_te = model.predict_proba(te[feats])

        y = te["label"].to_numpy()
        ret = te["close_ret"].to_numpy()
            # ~0.5 settlements per 4h hold on the 8h funding grid
        fund = float(funding.get(str(period), 0.0)) * 0.5

        # Daily recalibration: the threshold for day D is the quantile of the
        # model's own scores over D-30..D-1. Fixing it monthly lets the
        # selection rate drift whenever the score distribution shifts; a
        # trailing quantile keeps the number of signals stable. No lookahead --
        # only bars strictly before D enter the quantile.
        te_days = pd.to_datetime(te["open_time"]).dt.normalize().to_numpy()
        tr_days = pd.to_datetime(tr["open_time"]).dt.normalize().to_numpy()
        hist_days = np.concatenate([tr_days, te_days])

        for direction, idx, target in (("LONG", i_up, 1), ("SHORT", i_dn, -1)):
            hist_scores = np.concatenate([proba_tr[:, idx], proba_te[:, idx]])
            sel = np.zeros(len(te), dtype=bool)
            for day in np.unique(te_days):
                lo = day - np.timedelta64(CALIB_DAYS, "D")
                window = (hist_days >= lo) & (hist_days < day)
                if window.sum() < 200:
                    continue
                thr = float(np.quantile(hist_scores[window], quantile))
                sel |= (te_days == day) & (proba_te[:, idx] >= thr)
            if sel.sum() == 0:
                continue
            lab = y[sel]
            if maker_entry:
                # A resting limit at the signal close only fills if the next
                # minute trades through it. That is a real filter, not a
                # discount: a long fills only when price ticks DOWN first, so
                # the entries that survive are adversely selected. Measuring
                # the fill rate and its effect on the hit rate is the point.
                sig = te["signal_close"].to_numpy()[sel]
                nlo = te["next_low"].to_numpy()[sel]
                nhi = te["next_high"].to_numpy()[sel]
                filled = (nlo <= sig) if direction == "LONG" else (nhi >= sig)
                filled = filled & ~np.isnan(nlo) & ~np.isnan(nhi)
                if filled.sum() == 0:
                    continue
                lab = lab[filled]
            # Funding is paid by longs when positive, received by shorts.
            fund_bps = (fund if direction == "LONG" else -fund) * 10_000.0
            entry_cost = MAKER_ENTRY_BPS if maker_entry else ENTRY_BPS
            ret_sel = ret[sel][filled] if maker_entry else ret[sel]

            # A timeout exit is not urgent -- the clock ran out, nothing is
            # racing away -- so it can rest as a limit like the entry does.
            # The stop stays a market order on purpose: a stop-limit that does
            # not fill leaves an open position in exactly the move it exists
            # to escape, which costs far more than the 4.5 bps it saves.
            timeout_cost = EXIT_TAKER_BPS
            if maker_timeout:
                xlo = te["exit_next_low"].to_numpy()[sel]
                xhi = te["exit_next_high"].to_numpy()[sel]
                cls = te["signal_close"].to_numpy()[sel] * np.exp(ret[sel])
                if maker_entry:
                    xlo, xhi, cls = xlo[filled], xhi[filled], cls[filled]
                # Exiting a long means selling: the resting offer fills when the
                # next minute trades up to it.
                exit_fills = (xhi >= cls) if direction == "LONG" else (xlo <= cls)
                exit_fills = exit_fills & ~np.isnan(xhi) & ~np.isnan(xlo)
                timeout_cost = np.where(exit_fills, EXIT_MAKER_BPS, EXIT_TAKER_BPS)

            # With a scaled barrier a win is worth what the barrier was when the
            # trade opened, while the fee stays fixed -- so the break-even hit
            # rate is a per-trade number, not a constant. Averaging it is the
            # only fair way to state "how much lift was actually required".
            bar = te["barrier_bps"].to_numpy()[sel]
            if maker_entry:
                bar = bar[filled]
            win_bps = bar - entry_cost - EXIT_MAKER_BPS
            loss_bps = -bar - entry_cost - EXIT_TAKER_BPS
            breakeven = -loss_bps / (win_bps - loss_bps)

            net = np.where(
                lab == target, win_bps,
                np.where(lab == -target, loss_bps,
                         ret_sel * target * 10_000.0 - entry_cost - timeout_cost),
            ) - fund_bps
            hit = float((lab == target).mean())
            be = float(np.nanmean(breakeven))
            rows.append({
                "ho": str(period), "irany": direction, "n": int(len(lab)),
                "jel": int(sel.sum()),
                "korlat": round(float(np.nanmean(bar)), 0),
                "talalat_%": round(hit * 100, 1),
                "fedezeti_%": round(be * 100, 1),
                "tobblet_pp": round((hit - be) * 100, 1),
                "netto_bps": round(float(np.nansum(net)), 1),
                "bps_per_kotes": round(float(np.nanmean(net)), 2),
            })

    return pd.DataFrame(rows)


def main() -> None:
    # The label, horizon and purge are switched at runtime so one script can
    # evaluate several barrier geometries; they are module-level because the
    # loaders read them directly.
    global LABEL_COL, BPS_COL, BARRIER_BPS, HORIZON_MIN, PURGE_MIN  # noqa: PLW0603
    global DROP_PREFIXES  # noqa: PLW0603
    global MAKER_FEE_BPS, TAKER_FEE_BPS  # noqa: PLW0603
    global ENTRY_BPS, MAKER_ENTRY_BPS, EXIT_MAKER_BPS, EXIT_TAKER_BPS  # noqa: PLW0603
    parser = argparse.ArgumentParser(description="Triple-barrier walk-forward")
    parser.add_argument("--trials", type=int, default=100)
    parser.add_argument("--quantile", type=float, default=0.95)
    parser.add_argument("--asset-id", default="solusdt")
    parser.add_argument("--maker-entry", action="store_true",
                        help="Limit belepes a jel utani percben (2.0 bps, de nem mindig telik)")
    parser.add_argument("--maker-timeout", action="store_true",
                        help="Limit kilepes timeoutnal is (a stop marad market)")
    parser.add_argument("--horizon", type=int, default=240,
                        help="Fuggoleges korlat percben (240 vagy 1440)")
    parser.add_argument("--barrier-bps", type=int, default=100)
    parser.add_argument("--label", default=None,
                        help="Cimke oszlop neve. Alapertelmezetten a horizontbol "
                             "es a korlatbol all ossze (tb_label_240_100). A "
                             "vol-skalazott valtozat: tb_label_240_vol -- ilyenkor "
                             "a soronkenti korlat a _bps parjabol jon.")
    parser.add_argument("--fee-discount", type=float, default=0.0,
                        help="Tozsdei dij kedvezmeny aranyban (0.10 = BNB-vel "
                             "fizetett futures dij, -10%%). A csuszasra NEM "
                             "vonatkozik -- azt nem lehet kedvezmenyezni.")
    parser.add_argument("--drop-features", default=None,
                        help="Vesszovel elvalasztott feature-prefixek, amiket a "
                             "modell NEM lathat (pl. feat_depth_). Ablacios "
                             "meresekhez: a sorhalmaz valtozatlan, csak a "
                             "bemenet szukul, igy a ket ag osszevetheto.")
    parser.add_argument("--params-cache", default=None,
                        help="JSON ut: ha letezik, onnan tolti a parametereket a "
                             "kereses helyett; ha nem, kereses utan ide menti. "
                             "Igy a koltseg-variansok azonos modellen merhetok.")
    args = parser.parse_args()

    if args.fee_discount:
        MAKER_FEE_BPS = 2.0 * (1.0 - args.fee_discount)
        TAKER_FEE_BPS = 5.0 * (1.0 - args.fee_discount)
        ENTRY_BPS = TAKER_FEE_BPS + SLIPPAGE_BPS
        MAKER_ENTRY_BPS = MAKER_FEE_BPS
        EXIT_MAKER_BPS = MAKER_FEE_BPS
        EXIT_TAKER_BPS = TAKER_FEE_BPS + SLIPPAGE_BPS

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    entry_cost = MAKER_ENTRY_BPS if args.maker_entry else ENTRY_BPS
    win = 100.0 - entry_cost - EXIT_MAKER_BPS
    loss = -100.0 - entry_cost - EXIT_TAKER_BPS
    mode = "MAKER (limit, 1 perc)" if args.maker_entry else "TAKER (market)"
    print(f"belepes: {mode} -> {entry_cost:.1f} bps")
    print(f"  nyero {win:+.1f} bps   vesztes {loss:+.1f} bps   "
          f"fedezeti {-loss / (win - loss) * 100:.1f}%")

    DROP_PREFIXES = (
        [p.strip() for p in args.drop_features.split(",")]
        if args.drop_features
        else []
    )
    LABEL_COL = args.label or f"tb_label_{args.horizon}_{args.barrier_bps}"
    BARRIER_BPS = float(args.barrier_bps)
    BPS_COL = f"{LABEL_COL}_bps" if args.label else None
    HORIZON_MIN = args.horizon
    PURGE_MIN = args.horizon
    scaled = " · soronkenti korlat" if BPS_COL else f" · fix {BARRIER_BPS:.0f} bps"
    print(f"cimke: {LABEL_COL}  (fuggoleges korlat {args.horizon} perc{scaled})")

    frame, feats = load_dataset(args.asset_id)
    funding = monthly_funding(args.asset_id)

    cache = pathlib.Path(args.params_cache) if args.params_cache else None
    if cache is not None and cache.exists():
        params = json.loads(cache.read_text(encoding="utf-8"))
        logger.info("parameterek a cache-bol: %s", cache)
    else:
        logger.info("hiperparameter kereses indul (%d trial)...", args.trials)
        params = search_params(frame, feats, args.trials)
        if cache is not None:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(json.dumps(params, indent=2), encoding="utf-8")
            logger.info("parameterek mentve: %s", cache)
    print("\nlegjobb parameterek:")
    for k, v in sorted(params.items()):
        print(f"  {k:20} {v}")

    logger.info("gorduló mérés indul (havi ujratanitas)...")
    res = walk_forward(frame, feats, params, args.quantile, funding,
                       args.maker_entry, args.maker_timeout)
    if res.empty:
        print("nincs eredmeny")
        return

    print(f"\n=== HAVI EREDMENY (kuszob = train p{int(args.quantile * 100)}) ===")
    print(res.to_string(index=False))

    print("\n=== OSSZESITES IRANYONKENT ===")
    for direction in ("LONG", "SHORT"):
        d = res[res["irany"] == direction]
        if d.empty:
            continue
        n = int(d["n"].sum())
        total = float(d["netto_bps"].sum())
        hit = float((d["talalat_%"] * d["n"]).sum() / n)
        be = float((d["fedezeti_%"] * d["n"]).sum() / n)
        pos_months = int((d["netto_bps"] > 0).sum())
        fill = f"  telesules {n / int(d['jel'].sum()) * 100:4.1f}%" if "jel" in d else ""
        print(f"  {direction:6} n={n:5}  talalat {hit:5.1f}%  fedezeti {be:5.1f}%  "
              f"tobblet {hit - be:+5.1f}pp  "
              f"netto {total:9.1f} bps  ({total / n:+.2f} bps/kotes)  "
              f"pozitiv honap: {pos_months}/{len(d)}{fill}")

    n_all = int(res["n"].sum())
    tot_all = float(res["netto_bps"].sum())
    print(f"\n  EGYUTT n={n_all}  netto {tot_all:.1f} bps  ({tot_all / n_all:+.2f} bps/kotes)")
    print(f"  1000 USD notionalon, kotesenkent ujranyitva: {tot_all / 100:.0f} USD")


if __name__ == "__main__":
    main()
