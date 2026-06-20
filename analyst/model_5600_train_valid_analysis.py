"""Helpers for the combined 5600 train-valid analysis notebook."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd


MODEL_ID = "lgbm_solusdt_l_fw60_2021"
TARGET_COL = "long_mfe_fw60"
PRED_COL = "pred_long"
CACHE_SUBDIR = Path("analysis_cache") / "5600_train_valid_analysis"


def artifact_dir(root: Path) -> Path:
    """Return artifact directory path."""
    return root / "artifacts" / MODEL_ID


def cache_dir(root: Path) -> Path:
    """Return cache directory path under the model artifact tree."""
    return artifact_dir(root) / CACHE_SUBDIR


def legacy_cache_dir(root: Path) -> Path:
    """Return the legacy cache directory under _doc_."""
    return root / "_doc_" / "5600_model_2021_train_valid_analysis_cache"


def ensure_cache(root: Path) -> Path:
    """Ensure cached CV predictions and metrics exist."""
    out_dir = cache_dir(root)
    required = [
        out_dir / "cv_predictions.parquet",
        out_dir / "fold_metrics.json",
        out_dir / "metadata_snapshot.json",
    ]
    if all(path.exists() for path in required):
        return out_dir

    legacy_dir = legacy_cache_dir(root)
    legacy_required = [
        legacy_dir / "cv_predictions.parquet",
        legacy_dir / "fold_metrics.json",
        legacy_dir / "metadata_snapshot.json",
    ]
    if all(path.exists() for path in legacy_required):
        out_dir.mkdir(parents=True, exist_ok=True)
        for src, dst in zip(legacy_required, required):
            shutil.copy2(src, dst)
        return out_dir

    out_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["uv", "run", "python", str(Path(__file__).resolve()), "--build-cache", str(root)],
        check=True,
        cwd=root,
    )
    return out_dir


def _build_cache(root: Path) -> None:
    """Build CV prediction cache in the uv environment."""
    import lightgbm as lgb
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    art_dir = artifact_dir(root)
    sample = pd.read_parquet(art_dir / "sample_train_valid.parquet")
    sample["open_time"] = pd.to_datetime(sample["open_time"])
    sample["date"] = sample["open_time"].dt.floor("D")

    feature_set = json.loads(
        (art_dir / "feature_engineering" / "feature_set.json").read_text(encoding="utf-8")
    )
    best_params = json.loads((art_dir / "search" / "best_params.json").read_text(encoding="utf-8"))
    metadata = json.loads((art_dir / "metadata.json").read_text(encoding="utf-8"))
    features = list(feature_set["selected"])

    fixed = {
        "objective": "regression",
        "boosting_type": "gbdt",
        "metric": "rmse",
        "n_estimators": 3000,
        "subsample_freq": 1,
        "force_col_wise": True,
        "verbosity": -1,
        "n_jobs": 4,
    }

    metric_rows: list[dict[str, float | int]] = []
    pred_frames: list[pd.DataFrame] = []
    for fold in sorted(sample["fold_id"].unique().tolist()):
        train_mask = sample["fold_id"] != fold
        valid_mask = sample["fold_id"] == fold

        x_train = sample.loc[train_mask, features]
        y_train = sample.loc[train_mask, TARGET_COL].astype(float)
        x_valid = sample.loc[valid_mask, features]
        y_valid = sample.loc[valid_mask, TARGET_COL].astype(float)

        model = lgb.LGBMRegressor(**fixed, **best_params, random_state=42)
        model.fit(
            x_train,
            y_train,
            eval_set=[(x_train, y_train), (x_valid, y_valid)],
            eval_names=["train", "valid"],
            callbacks=[
                lgb.early_stopping(stopping_rounds=100, verbose=False),
                lgb.log_evaluation(period=-1),
            ],
        )

        pred_train = model.predict(x_train)
        pred_valid = model.predict(x_valid)

        metric_rows.append(
            {
                "fold": int(fold),
                "best_iteration": int(
                    getattr(model, "best_iteration_", fixed["n_estimators"]) or fixed["n_estimators"]
                ),
                "train_rmse": float(mean_squared_error(y_train, pred_train) ** 0.5),
                "valid_rmse": float(mean_squared_error(y_valid, pred_valid) ** 0.5),
                "train_mae": float(mean_absolute_error(y_train, pred_train)),
                "valid_mae": float(mean_absolute_error(y_valid, pred_valid)),
                "train_r2": float(r2_score(y_train, pred_train)),
                "valid_r2": float(r2_score(y_valid, pred_valid)),
                "train_n": int(train_mask.sum()),
                "valid_n": int(valid_mask.sum()),
            }
        )

        train_df = sample.loc[train_mask, ["open_time", "date", "fold_id", TARGET_COL]].copy()
        train_df["cv_fold"] = int(fold)
        train_df["segment"] = "train"
        train_df["pred"] = pred_train
        pred_frames.append(train_df)

        valid_df = sample.loc[valid_mask, ["open_time", "date", "fold_id", TARGET_COL]].copy()
        valid_df["cv_fold"] = int(fold)
        valid_df["segment"] = "valid"
        valid_df["pred"] = pred_valid
        pred_frames.append(valid_df)

    out_dir = cache_dir(root)
    out_dir.mkdir(parents=True, exist_ok=True)

    pred_df = pd.concat(pred_frames, ignore_index=True).sort_values(["cv_fold", "open_time", "segment"])
    pred_df.to_parquet(out_dir / "cv_predictions.parquet", index=False)
    (out_dir / "fold_metrics.json").write_text(json.dumps(metric_rows, indent=2), encoding="utf-8")
    (out_dir / "metadata_snapshot.json").write_text(
        json.dumps(
            {
                "model_id": MODEL_ID,
                "target_col": TARGET_COL,
                "pred_col": PRED_COL,
                "fold_time_windows": metadata.get("fold_time_windows", []),
                "fold_row_counts": metadata.get("fold_row_counts", {}),
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def load_cv_predictions(root: Path) -> pd.DataFrame:
    """Load cached CV prediction frame."""
    ensure_cache(root)
    df = pd.read_parquet(cache_dir(root) / "cv_predictions.parquet")
    df["open_time"] = pd.to_datetime(df["open_time"])
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_fold_metrics(root: Path) -> pd.DataFrame:
    """Load cached fold metrics."""
    ensure_cache(root)
    rows = json.loads((cache_dir(root) / "fold_metrics.json").read_text(encoding="utf-8"))
    return pd.DataFrame(rows).sort_values("fold").reset_index(drop=True)


def load_metadata_snapshot(root: Path) -> dict:
    """Load cached metadata snapshot."""
    ensure_cache(root)
    return json.loads((cache_dir(root) / "metadata_snapshot.json").read_text(encoding="utf-8"))


def load_manifest(root: Path) -> dict:
    """Load model manifest."""
    return json.loads((artifact_dir(root) / "manifest.json").read_text(encoding="utf-8"))


def load_sampling_metadata(root: Path) -> dict:
    """Load raw sampling metadata."""
    return json.loads((artifact_dir(root) / "metadata.json").read_text(encoding="utf-8"))


def load_search_best(root: Path) -> dict:
    """Load search best summary."""
    return json.loads((artifact_dir(root) / "search" / "search_best.json").read_text(encoding="utf-8"))


def load_oos_predictions(root: Path) -> pd.DataFrame:
    """Load OOS prediction frame."""
    df = pd.read_parquet(artifact_dir(root) / "sample_oos.parquet")
    df["open_time"] = pd.to_datetime(df["open_time"])
    return df


def split_summary_metrics(pred_df: pd.DataFrame) -> pd.DataFrame:
    """Return overall metrics by train/valid split across all folds."""
    rows: list[dict[str, float | str | int]] = []
    for segment, sub in pred_df.groupby("segment"):
        y = sub[TARGET_COL].astype(float).to_numpy()
        p = sub["pred"].astype(float).to_numpy()
        rmse = float(np.sqrt(np.mean((y - p) ** 2)))
        mae = float(np.mean(np.abs(y - p)))
        mean_y = float(np.mean(y))
        ss_res = float(np.sum((y - p) ** 2))
        ss_tot = float(np.sum((y - mean_y) ** 2))
        target_std = float(np.std(y))
        corr = float(np.corrcoef(p, y)[0, 1]) if len(sub) > 1 else np.nan
        rows.append(
            {
                "segment": segment,
                "rows": int(len(sub)),
                "pred_mean": float(np.mean(p)),
                "target_mean": mean_y,
                "bias": float(np.mean(p - y)),
                "rmse": rmse,
                "mae": mae,
                "target_std": target_std,
                "r2": float(1.0 - ss_res / ss_tot) if ss_tot > 0 else np.nan,
                "corr": corr,
                "rmse_vs_target_std": float(rmse / target_std) if target_std > 0 else np.nan,
            }
        )
    return pd.DataFrame(rows).sort_values("segment").reset_index(drop=True)


def scatter_sample(pred_df: pd.DataFrame, n_per_segment: int = 3500, seed: int = 42) -> pd.DataFrame:
    """Downsample prediction-target scatter for readable plotting."""
    frames = []
    for _, sub in pred_df.groupby("segment"):
        if len(sub) > n_per_segment:
            frames.append(sub.sample(n=n_per_segment, random_state=seed))
        else:
            frames.append(sub.copy())
    return pd.concat(frames, ignore_index=True)


def residual_summary(pred_df: pd.DataFrame) -> pd.DataFrame:
    """Return residual distribution summary by split."""
    rows: list[dict[str, float | str]] = []
    for segment, sub in pred_df.groupby("segment"):
        resid = sub["pred"] - sub[TARGET_COL]
        rows.append(
            {
                "segment": segment,
                "resid_mean": float(resid.mean()),
                "resid_std": float(resid.std()),
                "resid_q05": float(resid.quantile(0.05)),
                "resid_median": float(resid.quantile(0.50)),
                "resid_q95": float(resid.quantile(0.95)),
            }
        )
    return pd.DataFrame(rows).sort_values("segment").reset_index(drop=True)


def _safe_spearman(y: np.ndarray, s: np.ndarray) -> float:
    """Return Spearman correlation or NaN for degenerate inputs."""
    if len(y) < 2 or np.unique(s).size < 2 or np.unique(y).size < 2:
        return np.nan
    return float(pd.Series(y).corr(pd.Series(s), method="spearman"))


def top10_lift(y_true: np.ndarray, scores: np.ndarray) -> float:
    """Top decile mean(y_true) - overall mean(y_true)."""
    ranks = pd.qcut(scores, q=10, labels=False, duplicates="drop")
    top_mask = ranks == ranks.max()
    return float(np.mean(y_true[top_mask]) - np.mean(y_true))


def decile_monotonicity(y_true: np.ndarray, scores: np.ndarray) -> float:
    """Fraction of adjacent decile pairs where mean(y_true) is non-decreasing."""
    tmp = pd.DataFrame({"y": y_true, "score": scores}).copy()
    tmp["decile"] = pd.qcut(tmp["score"], q=10, labels=False, duplicates="drop")
    decile_means = (
        tmp.groupby("decile", observed=True)["y"].mean().sort_index().tolist()
    )
    mono_pairs = sum(1 for a, b in zip(decile_means[:-1], decile_means[1:]) if b >= a)
    return float(mono_pairs / max(len(decile_means) - 1, 1))


def fold_rank_metrics(pred_df: pd.DataFrame, segment: str = "valid") -> pd.DataFrame:
    """Compute fold-level ranking metrics for one segment."""
    rows: list[dict[str, float | int | str]] = []
    for fold, sub in pred_df.loc[pred_df["segment"] == segment].groupby("cv_fold"):
        y = sub[TARGET_COL].to_numpy(dtype=float)
        s = sub["pred"].to_numpy(dtype=float)
        rows.append(
            {
                "segment": segment,
                "fold": int(fold),
                "rows": int(len(sub)),
                "top10_lift": top10_lift(y, s),
                "spearman_rho": _safe_spearman(y, s),
                "decile_monotonicity": decile_monotonicity(y, s),
            }
        )
    return pd.DataFrame(rows).sort_values(["segment", "fold"]).reset_index(drop=True)


def decile_summary(pred_df: pd.DataFrame, n_bins: int = 10) -> pd.DataFrame:
    """Summarize mean target by prediction decile for each split."""
    rows: list[pd.DataFrame] = []
    for segment, sub in pred_df.groupby("segment"):
        tmp = sub.copy()
        tmp["decile"] = pd.qcut(tmp["pred"], q=n_bins, labels=False, duplicates="drop") + 1
        overall_target_mean = float(tmp[TARGET_COL].mean())
        agg = (
            tmp.groupby("decile", observed=True)
            .agg(
                rows=("pred", "size"),
                pred_mean=("pred", "mean"),
                target_mean=(TARGET_COL, "mean"),
                target_median=(TARGET_COL, "median"),
                target_std=(TARGET_COL, "std"),
            )
            .reset_index()
        )
        agg["segment"] = segment
        agg["overall_target_mean"] = overall_target_mean
        agg["lift_vs_segment_mean"] = agg["target_mean"] - overall_target_mean
        rows.append(agg)
    return pd.concat(rows, ignore_index=True)


def monotonicity_summary(decile_df: pd.DataFrame) -> pd.DataFrame:
    """Return compact monotonicity summary from a decile table."""
    rows: list[dict[str, float | str]] = []
    for segment, sub in decile_df.groupby("segment"):
        vals = sub.sort_values("decile")["target_mean"].tolist()
        good = sum(1 for a, b in zip(vals[:-1], vals[1:]) if b >= a)
        rows.append(
            {
                "segment": segment,
                "monotonic_pairs": int(good),
                "adjacent_pairs": int(max(len(vals) - 1, 1)),
                "decile_monotonicity": float(good / max(len(vals) - 1, 1)),
                "top_minus_bottom": float(vals[-1] - vals[0]),
                "top_div_bottom": float(vals[-1] / vals[0]) if vals[0] != 0 else np.nan,
            }
        )
    return pd.DataFrame(rows).sort_values("segment").reset_index(drop=True)


def decile_comparison(decile_df: pd.DataFrame) -> pd.DataFrame:
    """Return train-valid side-by-side decile comparison table."""
    pivot = (
        decile_df.pivot(
            index="decile",
            columns="segment",
            values=["pred_mean", "target_mean", "lift_vs_segment_mean"],
        )
        .sort_index()
    )
    pivot.columns = [f"{metric}_{segment}" for metric, segment in pivot.columns]
    pivot = pivot.reset_index()
    pivot["target_mean_valid_minus_train"] = pivot["target_mean_valid"] - pivot["target_mean_train"]
    pivot["pred_mean_valid_minus_train"] = pivot["pred_mean_valid"] - pivot["pred_mean_train"]
    return pivot


def fold_decile_summary(pred_df: pd.DataFrame, segment: str = "valid", n_bins: int = 10) -> pd.DataFrame:
    """Return fold-level decile summary for one segment."""
    rows: list[pd.DataFrame] = []
    for fold, sub in pred_df.loc[pred_df["segment"] == segment].groupby("cv_fold"):
        tmp = sub.copy()
        tmp["decile"] = pd.qcut(tmp["pred"], q=n_bins, labels=False, duplicates="drop") + 1
        agg = (
            tmp.groupby("decile", observed=True)
            .agg(
                rows=("pred", "size"),
                pred_mean=("pred", "mean"),
                target_mean=(TARGET_COL, "mean"),
            )
            .reset_index()
        )
        agg["segment"] = segment
        agg["cv_fold"] = int(fold)
        rows.append(agg)
    return pd.concat(rows, ignore_index=True)


def oos_rank_summary(oos_df: pd.DataFrame) -> pd.DataFrame:
    """Return a compact OOS ranking summary."""
    y = oos_df[TARGET_COL].to_numpy(dtype=float)
    s = oos_df[PRED_COL].to_numpy(dtype=float)
    dec = pd.qcut(s, q=10, labels=False, duplicates="drop") + 1
    top_mean = float(np.mean(y[dec == dec.max()]))
    bottom_mean = float(np.mean(y[dec == dec.min()]))
    return pd.DataFrame(
        [
            {
                "rows": int(len(oos_df)),
                "target_mean": float(np.mean(y)),
                "pred_mean": float(np.mean(s)),
                "top10_lift": top10_lift(y, s),
                "spearman_rho": _safe_spearman(y, s),
                "decile_monotonicity": decile_monotonicity(y, s),
                "top_minus_bottom": top_mean - bottom_mean,
            }
        ]
    )


def fold_window_table(root: Path) -> pd.DataFrame:
    """Return readable fold window table from sampling metadata."""
    metadata = load_sampling_metadata(root)
    rows = []
    for fw in metadata.get("fold_time_windows", []):
        t_start = pd.Timestamp(fw["train_start"])
        t_end = pd.Timestamp(fw["train_end"])
        v_start = pd.Timestamp(fw["valid_start"])
        v_end = pd.Timestamp(fw["valid_end"])
        rows.append(
            {
                "fold_id": int(fw["fold_id"]),
                "train_start": fw["train_start"],
                "train_end": fw["train_end"],
                "train_months": round((t_end - t_start).days / 30.44),
                "valid_start": fw["valid_start"],
                "valid_end": fw["valid_end"],
                "valid_months": round((v_end - v_start).days / 30.44),
            }
        )
    return pd.DataFrame(rows).sort_values("fold_id").reset_index(drop=True)


def search_summary_table(root: Path) -> pd.DataFrame:
    """Return concise search summary table."""
    best = load_search_best(root)
    fold_summary = best.get("fold_summary", [])
    return pd.DataFrame(
        [
            {
                "trial_no": int(best["trial_no"]),
                "objective_score": float(best["objective_score"]),
                "mean_top10_lift": float(best["mean_top10_lift"]),
                "std_top10_lift": float(best["std_top10_lift"]),
                "mean_spearman_rho": float(best["mean_spearman_rho"]),
                "mean_decile_monotonicity": float(best["mean_decile_monotonicity"]),
                "mean_valid_rmse": float(best["mean_valid_rmse"]),
                "folds_in_search_summary": int(len(fold_summary)),
            }
        ]
    )


def commentary_lines(split_metrics: pd.DataFrame, fold_metrics: pd.DataFrame) -> list[str]:
    """Generate short interpretation bullets from train-valid diagnostics."""
    valid_row = split_metrics.loc[split_metrics["segment"] == "valid"].iloc[0]
    train_row = split_metrics.loc[split_metrics["segment"] == "train"].iloc[0]
    best_fold = int(fold_metrics.sort_values("valid_rmse").iloc[0]["fold"])
    worst_fold = int(fold_metrics.sort_values("valid_rmse", ascending=False).iloc[0]["fold"])
    return [
        (
            f"A validációs átlagos RMSE `{valid_row['rmse']:.4f}`, a train oldali `{train_row['rmse']:.4f}`; "
            "a két oldal közel van egymáshoz, ezért a modell nem látszik durván overfiteltnek."
        ),
        (
            f"A validációs R² csak `{valid_row['r2']:.3f}`, ezért a modell jelet fog, "
            "de a folytonos target varianciájának jelentős része továbbra is megmagyarázatlan."
        ),
        (
            f"A legjobb valid fold a `{best_fold}`, a leggyengébb a `{worst_fold}`; "
            "tehát a teljesítmény szemmel láthatóan rezsim- és időszakfüggő."
        ),
        (
            "A predikció-target kapcsolat pozitív, de zajos. Ez használható rangsorolási jellegű kimenetre, "
            "de nem tekinthető pontos pontbecslő regressziónak."
        ),
    ]


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for cache building."""
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) == 2 and argv[0] == "--build-cache":
        root = Path(argv[1]).resolve()
        _build_cache(root)
        return 0
    print("Usage: python analyst/model_5600_train_valid_analysis.py --build-cache <root>")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
