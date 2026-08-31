#!/usr/bin/env python3
"""Triple-barrier multiclass experiment (epic_058 / t702-t703).

Deliberately bypasses the snapshot/sample pipeline. That machinery is built
around regression targets named `{direction}_mfe_fw{horizon}` -- the sampler
parses the direction and horizon straight out of the column name -- so wiring
`tb_label_240_100` through it means touching the sampling layer, the trainer
dispatcher and the search module before a single number comes back.

The hypothesis is cheap to test and expensive to be wrong about, so it gets
tested first and integrated second. What this script does keep is the sampling
discipline that makes the result comparable to the regression runs:

* one randomly chosen minute per hour, seed 42;
* a 240-minute feature-lookback embargo at the start of train;
* a 240-minute target purge at the end of train, so a training label cannot
  see into the validation window;
* the threshold fixed on train and applied unchanged to valid -- three
  earlier experiments in this project died from in-sample selection.

Usage:
    python -m modeling.experiments.triple_barrier_eval
    python -m modeling.experiments.triple_barrier_eval --barrier-bps 150
"""

import argparse
import logging
import sys

import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd

import utils

logger = logging.getLogger(__name__)

TRAIN_START = "2023-01-01"
TRAIN_END = "2025-07-31"
VALID_START = "2025-08-01"
VALID_END = "2026-07-31"
EMBARGO_MIN = 240
PURGE_MIN = 240
SEED = 42

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

# Deliberately conservative: the regression search found that a heavily
# regularized, slow-learning fit generalized best on the 4-hour horizon
# (num_leaves 4, lr 0.011 vs 57 / 0.043 at 60 minutes).
TUNED_PARAMS: dict = {
    "n_estimators": 3000,
    "learning_rate": 0.02,
    "num_leaves": 8,
    "min_child_samples": 300,
    "subsample": 0.8,
    "subsample_freq": 1,
    "colsample_bytree": 0.6,
    "reg_lambda": 10.0,
}


def load_dataset(
    label_col: str,
    asset_id: str = "solusdt",
    start: str = TRAIN_START,
    end: str = VALID_END,
) -> pd.DataFrame:
    """Hourly-sampled feature matrix plus the triple-barrier label."""
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]
    conn = duckdb.connect(db_path, read_only=True)
    try:
        cols = [
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'quant_train' ORDER BY ordinal_position"
            ).fetchall()
        ]
        feats = [c for c in cols if c.startswith("feat_")]
        if label_col not in cols:
            raise ValueError(
                f"{label_col} nincs a quant_train-ben -- futott a "
                "sync_triple_barrier es a 03_build_quant_train?"
            )

        feat_sql = ", ".join(f'"{c}"' for c in feats)
        # One random minute per hour, deterministic in the timestamp so the
        # sample is reproducible without materializing a seed column.
        sql = f"""
            WITH ranked AS (
                SELECT open_time, {feat_sql}, "{label_col}" AS label,
                       row_number() OVER (
                           PARTITION BY date_trunc('hour', open_time)
                           ORDER BY hash(epoch_ms(open_time) + {SEED})
                       ) AS rn
                FROM quant_train
                WHERE "{label_col}" IS NOT NULL
                  AND open_time >= '{start}'
                  AND open_time <= '{end}'
            )
            SELECT * EXCLUDE(rn) FROM ranked WHERE rn = 1 ORDER BY open_time
        """
        frame = conn.execute(sql).df()
    finally:
        conn.close()

    logger.info("betoltve: %d sor, %d feature", len(frame), len(feats))
    return frame


def split_train_valid(
    frame: pd.DataFrame,
    train_start: str,
    train_end: str,
    valid_start: str,
    valid_end: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the embargo and purge, then split."""
    ts = pd.to_datetime(frame["open_time"])
    train_lo = pd.Timestamp(train_start) + pd.Timedelta(minutes=EMBARGO_MIN)
    train_hi = pd.Timestamp(train_end) + pd.Timedelta(days=1) - pd.Timedelta(minutes=PURGE_MIN)

    train = frame[(ts >= train_lo) & (ts < train_hi)]
    valid = frame[(ts >= pd.Timestamp(valid_start)) & (ts <= pd.Timestamp(valid_end))]
    return train, valid


def evaluate(
    y_true: np.ndarray,
    p_up: np.ndarray,
    p_dn: np.ndarray,
    label: str,
) -> pd.DataFrame:
    """Actual class rates inside each confidence band of P(+1) and P(-1)."""
    rows = []
    base_up = float((y_true == 1).mean() * 100)
    base_dn = float((y_true == -1).mean() * 100)
    rows.append({
        "irany": "-", "kuszob": "alap", "n": len(y_true),
        "talalat_%": round(base_up, 1), "ellen_%": round(base_dn, 1), "lift_pp": 0.0,
    })

    for q in (0.50, 0.80, 0.90, 0.95, 0.98, 0.99):
        thr = float(np.quantile(p_up, q))
        sel = p_up >= thr
        if sel.sum() >= 10:
            hit = float((y_true[sel] == 1).mean() * 100)
            rows.append({
                "irany": "LONG", "kuszob": f"p{int(q * 100)}", "n": int(sel.sum()),
                "talalat_%": round(hit, 1),
                "ellen_%": round(float((y_true[sel] == -1).mean() * 100), 1),
                "lift_pp": round(hit - base_up, 1),
            })
    for q in (0.90, 0.95, 0.98, 0.99):
        thr = float(np.quantile(p_dn, q))
        sel = p_dn >= thr
        if sel.sum() >= 10:
            hit = float((y_true[sel] == -1).mean() * 100)
            rows.append({
                "irany": "SHORT", "kuszob": f"p{int(q * 100)}", "n": int(sel.sum()),
                "talalat_%": round(hit, 1),
                "ellen_%": round(float((y_true[sel] == 1).mean() * 100), 1),
                "lift_pp": round(hit - base_dn, 1),
            })

    out = pd.DataFrame(rows)
    print(f"\n=== {label} ===")
    print(out.to_string(index=False))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Triple-barrier multiclass experiment")
    parser.add_argument("--barrier-bps", type=int, default=100)
    parser.add_argument("--horizon", type=int, default=240)
    parser.add_argument("--asset-id", default="solusdt")
    parser.add_argument("--train-start", default=TRAIN_START)
    parser.add_argument("--train-end", default=TRAIN_END)
    parser.add_argument("--valid-start", default=VALID_START)
    parser.add_argument("--valid-end", default=VALID_END)
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    label_col = f"tb_label_{args.horizon}_{args.barrier_bps}"
    frame = load_dataset(label_col, args.asset_id, args.train_start, args.valid_end)
    train, valid = split_train_valid(
        frame, args.train_start, args.train_end, args.valid_start, args.valid_end
    )

    feats = [c for c in frame.columns if c.startswith("feat_")]
    x_tr, y_tr = train[feats], train["label"].to_numpy()
    x_va, y_va = valid[feats], valid["label"].to_numpy()

    print(f"\ntrain: {len(y_tr):,} sor ({TRAIN_START} .. {TRAIN_END})")
    print(f"valid: {len(y_va):,} sor ({VALID_START} .. {VALID_END})")
    for name, y in (("train", y_tr), ("valid", y_va)):
        print(f"  {name} eloszlas: +1={np.mean(y == 1) * 100:.1f}%  "
              f"-1={np.mean(y == -1) * 100:.1f}%  0={np.mean(y == 0) * 100:.1f}%")

    # The tree count must be decided honestly, not guessed: hold out the last
    # slice of TRAIN as an inner validation set for early stopping. The real
    # valid window stays untouched -- using it here would be the same in-sample
    # selection that has already invalidated three earlier experiments.
    inner_cut = int(len(y_tr) * 0.85)
    x_in, y_in = x_tr.iloc[:inner_cut], y_tr[:inner_cut]
    x_es, y_es = x_tr.iloc[inner_cut:], y_tr[inner_cut:]

    # LightGBM needs 0..k-1 classes; -1/0/+1 maps to 0/1/2.
    model = lgb.LGBMClassifier(**{**FIXED_PARAMS, **TUNED_PARAMS})
    model.fit(
        x_in, y_in + 1,
        eval_set=[(x_es, y_es + 1)],
        callbacks=[lgb.early_stopping(100, verbose=False), lgb.log_evaluation(period=-1)],
    )
    best_iter = model.best_iteration_ or TUNED_PARAMS["n_estimators"]
    print(f"\nearly stopping: {best_iter} fa "
          f"(felso korlat {TUNED_PARAMS['n_estimators']})")

    classes = list(model.classes_)
    idx_dn, idx_up = classes.index(0), classes.index(2)
    proba_tr = model.predict_proba(x_tr)
    proba_va = model.predict_proba(x_va)

    evaluate(y_tr, proba_tr[:, idx_up], proba_tr[:, idx_dn], "TRAIN (in-sample)")
    evaluate(y_va, proba_va[:, idx_up], proba_va[:, idx_dn], "VALID (out-of-sample)")

    # The decisive test: threshold fixed on train, applied unchanged to valid.
    # Picking the threshold after seeing valid is exactly the in-sample
    # selection that invalidated three earlier experiments in this project.
    print("\n=== TRAIN-EN ROGZITETT KUSZOB, VALIDON ALKALMAZVA ===")
    print(f"{'irany':>7}{'kuszob':>8}{'ertek':>9}{'n':>7}{'talalat':>10}{'alap':>8}{'verdikt':>10}")
    for direction, idx, target in (("LONG", idx_up, 1), ("SHORT", idx_dn, -1)):
        base = float((y_va == target).mean() * 100)
        for q in (0.95, 0.98, 0.99):
            thr = float(np.quantile(proba_tr[:, idx], q))
            sel = proba_va[:, idx] >= thr
            if sel.sum() < 10:
                continue
            hit = float((y_va[sel] == target).mean() * 100)
            verdict = "FELETTE" if hit > 55 else "alatta"
            label = f"p{int(q * 100)}"
            print(f"{direction:>7}{label:>8}{thr:9.4f}{int(sel.sum()):7}"
                  f"{hit:9.1f}%{base:7.1f}%{verdict:>10}")

    print("\n=== NEGYEDEVES BONTAS a SHORT p98 kuszobon ===")
    thr_dn = float(np.quantile(proba_tr[:, idx_dn], 0.98))
    quarters = pd.to_datetime(valid["open_time"]).dt.to_period("Q").astype(str).to_numpy()
    picked = proba_va[:, idx_dn] >= thr_dn
    for q_lab in sorted(set(quarters)):
        mask = picked & (quarters == q_lab)
        if mask.sum() >= 5:
            rate = float((y_va[mask] == -1).mean() * 100)
            print(f"  {q_lab}: n={int(mask.sum()):3}  talalat {rate:5.1f}%")
        else:
            print(f"  {q_lab}: n={int(mask.sum()):3}  (keves)")


if __name__ == "__main__":
    main()
