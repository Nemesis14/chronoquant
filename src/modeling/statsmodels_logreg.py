# =============================================================================
# Statsmodels logistic regression with p-value filtering
# =============================================================================
# Purpose:
#  - Replace legacy long/short dev scripts with a reusable trainer
#  - Treat p-value filtering rounds as the model-complexity tuning parameter
#  - Reuse shared samples, metrics, artifacts, and HTML reports
# =============================================================================

from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

from modeling.artifacts import save_training_artifacts
from modeling.datasets import ModelingDataset, load_modeling_dataset
from modeling.metrics import binary_classification_metrics
from modeling.reports import write_training_report
from modeling.sampling import load_sample_definition, validate_sample_definition
from modeling.training_windows import (
    DatasetSplit,
    final_train_test_split,
    fold_sample_size_row,
    fold_split,
)


# =============================================================================
# train_statsmodels_pvalue_logreg(...) -> dict
# =============================================================================
# Purpose:
#  - Tune p-value filtering rounds and save a compact statsmodels Logit artifact
# =============================================================================
def train_statsmodels_pvalue_logreg(
    model_id: str,
    target_col: str,
    sample_dir: str | Path,
    output_dir: str | Path,
    p_threshold: float = 0.01,
    pvalue_rounds: list[int] | None = None,
    row_stride: int = 1,
    max_fit_iter: int = 100,
    min_features: int = 1,
    verbose: bool = False,
) -> dict:
    pvalue_rounds = pvalue_rounds or [0, 1, 2, 3, 4]
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sample = load_sample_definition(sample_dir)
    validate_sample_definition(sample)

    dataset = load_modeling_dataset(
        target_col=target_col,
        start=sample["data"]["start"],
        end=sample["data"]["end"],
        row_stride=row_stride,
        dropna_features=True,
    )
    if len(dataset.y) == 0:
        raise ValueError("Training dataset is empty")

    cv_results, sample_sizes = _cross_validate_rounds(
        dataset=dataset,
        sample=sample,
        p_threshold=p_threshold,
        pvalue_rounds=pvalue_rounds,
        max_fit_iter=max_fit_iter,
        min_features=min_features,
        verbose=verbose,
    )
    cv_df = pd.DataFrame(cv_results)
    best_rounds = _select_best_rounds(cv_df, p_threshold)

    final_model, final_metrics, final_features, pvalue_path, feature_rows = _fit_final_model(
        dataset=dataset,
        sample=sample,
        p_threshold=p_threshold,
        pvalue_rounds=best_rounds,
        max_fit_iter=max_fit_iter,
        min_features=min_features,
        verbose=verbose,
    )

    artifacts = {
        "model_id": model_id,
        "trainer": "statsmodels_pvalue_logreg",
        "target_col": target_col,
        "sample_id": sample["sample_id"],
        "sample_dir": str(sample_dir),
        "output_dir": str(output_dir),
        "tuning_param": "pvalue_rounds",
        "best_tuning_value": best_rounds,
        "best_pvalue_rounds": best_rounds,
        "p_threshold": p_threshold,
        "pvalue_rounds_grid": pvalue_rounds,
        "row_stride": row_stride,
        "max_fit_iter": max_fit_iter,
        "min_features": min_features,
        "n_features_input": len(dataset.feature_cols),
        "n_features_selected": len(final_features),
        "selected_features": final_features,
        "final_max_pvalue": _max_pvalue(final_model),
        "final_is_stable": _is_stable(final_model, p_threshold),
        "pvalue_path": pvalue_path,
        "final_metrics": final_metrics,
        "feature_coefficients": feature_rows,
        "sample_sizes": sample_sizes,
    }

    _remove_model_data(final_model)
    save_training_artifacts(
        output_dir=output_dir,
        model=final_model,
        feature_cols=dataset.feature_cols,
        cv_df=cv_df,
        artifacts=artifacts,
        selected_features=final_features,
    )
    write_training_report(
        output_dir=output_dir,
        model_id=model_id,
        target_col=target_col,
        sample=sample,
        cv_df=cv_df,
        sample_sizes=sample_sizes,
        artifacts=artifacts,
        tuning_param="pvalue_rounds",
        tuning_label="p-value filter rounds",
        tuning_xscale="linear",
        auxiliary_columns={
            "selected_features": "mean",
            "max_pvalue": "mean",
            "is_stable": "mean",
        },
        feature_rows=feature_rows,
        feature_table_title="Statsmodels coefficients and p-values",
    )
    return artifacts


# =============================================================================
# _cross_validate_rounds(...) -> tuple[list[dict], list[dict]]
# =============================================================================
# Purpose:
#  - Evaluate each p-value filtering round count on shared CV folds
# =============================================================================
def _cross_validate_rounds(
    dataset: ModelingDataset,
    sample: dict,
    p_threshold: float,
    pvalue_rounds: list[int],
    max_fit_iter: int,
    min_features: int,
    verbose: bool,
) -> tuple[list[dict], list[dict]]:
    results = []
    sample_sizes = []
    max_rounds = max(pvalue_rounds)

    for fold in sample["folds"]:
        split = fold_split(dataset, fold)
        sample_sizes.append(fold_sample_size_row(fold, split))
        if verbose:
            print(f"CV fold {fold['fold']} p-value path to round {max_rounds}", flush=True)

        path = _fit_pvalue_path(
            X=split.X_train,
            y=split.y_train,
            p_threshold=p_threshold,
            max_rounds=max_rounds,
            max_fit_iter=max_fit_iter,
            min_features=min_features,
        )
        by_round = {state["round"]: state for state in path}

        for round_count in pvalue_rounds:
            state = by_round.get(round_count) or path[-1]
            train_pred = _predict(state["model"], split.X_train, state["features"])
            valid_pred = _predict(state["model"], split.X_eval, state["features"])
            train_metrics = binary_classification_metrics(split.y_train, train_pred)
            valid_metrics = binary_classification_metrics(split.y_eval, valid_pred)

            results.append(
                {
                    "fold": fold["fold"],
                    "pvalue_rounds": round_count,
                    "actual_rounds": state["round"],
                    "selected_features": len(state["features"]),
                    "max_pvalue": state["max_pvalue"],
                    "is_stable": state["is_stable"],
                    "removed_features": len(state["removed_features_total"]),
                    "train_roc_auc": train_metrics["roc_auc"],
                    "train_pr_auc": train_metrics["pr_auc"],
                    "train_brier_score": train_metrics["brier_score"],
                    "train_log_loss": train_metrics["log_loss"],
                    "valid_roc_auc": valid_metrics["roc_auc"],
                    "valid_pr_auc": valid_metrics["pr_auc"],
                    "valid_brier_score": valid_metrics["brier_score"],
                    "valid_log_loss": valid_metrics["log_loss"],
                }
            )

    return results, sample_sizes


# =============================================================================
# _fit_final_model(...) -> tuple
# =============================================================================
# Purpose:
#  - Refit selected p-value round count on pre-test data and evaluate holdout
# =============================================================================
def _fit_final_model(
    dataset: ModelingDataset,
    sample: dict,
    p_threshold: float,
    pvalue_rounds: int,
    max_fit_iter: int,
    min_features: int,
    verbose: bool,
):
    split = final_train_test_split(dataset, sample)
    if verbose:
        print(f"Final fit pvalue_rounds={pvalue_rounds}", flush=True)

    path = _fit_pvalue_path(
        X=split.X_train,
        y=split.y_train,
        p_threshold=p_threshold,
        max_rounds=pvalue_rounds,
        max_fit_iter=max_fit_iter,
        min_features=min_features,
    )
    state = path[-1]

    train_pred = _predict(state["model"], split.X_train, state["features"])
    test_pred = _predict(state["model"], split.X_eval, state["features"])
    return (
        state["model"],
        {
            "final_train": binary_classification_metrics(split.y_train, train_pred),
            "final_test": binary_classification_metrics(split.y_eval, test_pred),
        },
        state["features"],
        _path_rows(path),
        _coefficient_rows(state["model"]),
    )


# =============================================================================
# _fit_pvalue_path(...) -> list[dict]
# =============================================================================
# Purpose:
#  - Fit round 0 with all features, then remove all p-values above threshold
# =============================================================================
def _fit_pvalue_path(
    X: pd.DataFrame,
    y: pd.Series,
    p_threshold: float,
    max_rounds: int,
    max_fit_iter: int,
    min_features: int,
) -> list[dict]:
    remaining = _usable_features(X)
    removed_total = []
    path = []

    for round_no in range(max_rounds + 1):
        model = _fit_logit(X[remaining], y, max_fit_iter)
        pvalues = _feature_pvalues(model)
        high_p = [
            feature
            for feature, pvalue in pvalues.items()
            if pvalue > p_threshold
        ]
        state = {
            "round": round_no,
            "features": list(remaining),
            "model": model,
            "max_pvalue": _max_pvalue(model),
            "is_stable": _is_stable(model, p_threshold),
            "removed_features_this_round": [],
            "removed_features_total": list(removed_total),
        }
        path.append(state)

        if round_no == max_rounds or not high_p or len(remaining) <= min_features:
            break

        removable_count = max(0, len(remaining) - min_features)
        to_remove = high_p[:removable_count]
        state["removed_features_this_round"] = [
            {
                "feature": feature,
                "p_value": float(pvalues[feature]),
            }
            for feature in to_remove
        ]
        removed_total.extend(state["removed_features_this_round"])
        remaining = [feature for feature in remaining if feature not in set(to_remove)]

    return path


# =============================================================================
# _fit_logit(X: pd.DataFrame, y: pd.Series, max_fit_iter: int)
# =============================================================================
# Purpose:
#  - Fit statsmodels Logit with a constant column
# =============================================================================
def _fit_logit(X: pd.DataFrame, y: pd.Series, max_fit_iter: int):
    X_const = sm.add_constant(X, has_constant="add")
    try:
        return sm.Logit(y, X_const).fit(disp=False, maxiter=max_fit_iter)
    except Exception:
        return sm.Logit(y, X_const).fit_regularized(disp=False, maxiter=max_fit_iter)


# =============================================================================
# _predict(model, X: pd.DataFrame, features: list[str]) -> pd.Series
# =============================================================================
# Purpose:
#  - Predict probabilities for the selected feature set
# =============================================================================
def _predict(model, X: pd.DataFrame, features: list[str]):
    X_const = sm.add_constant(X[features], has_constant="add")
    return model.predict(X_const)


# =============================================================================
# _select_best_rounds(cv_df: pd.DataFrame, p_threshold: float) -> int
# =============================================================================
# Purpose:
#  - Select by PR AUC, preferring p-stable and smaller feature sets on ties
# =============================================================================
def _select_best_rounds(cv_df: pd.DataFrame, p_threshold: float) -> int:
    summary = cv_df.groupby("pvalue_rounds", as_index=False).agg(
        valid_pr_auc=("valid_pr_auc", "mean"),
        valid_brier_score=("valid_brier_score", "mean"),
        selected_features=("selected_features", "mean"),
        max_pvalue=("max_pvalue", "mean"),
        stable_rate=("is_stable", "mean"),
    )
    stable = summary[summary["stable_rate"] == 1.0]
    candidates = stable if not stable.empty else summary
    candidates = candidates.sort_values(
        by=["valid_pr_auc", "valid_brier_score", "selected_features", "pvalue_rounds"],
        ascending=[False, True, True, True],
    )
    return int(candidates.iloc[0]["pvalue_rounds"])


# =============================================================================
# _usable_features(X: pd.DataFrame) -> list[str]
# =============================================================================
# Purpose:
#  - Remove columns that cannot contribute to a statsmodels fit
# =============================================================================
def _usable_features(X: pd.DataFrame) -> list[str]:
    features = []
    for col in X.columns:
        values = pd.to_numeric(X[col], errors="coerce")
        if values.notna().all() and values.nunique(dropna=True) > 1:
            features.append(col)
    return features


# =============================================================================
# _feature_pvalues(model) -> pd.Series
# =============================================================================
# Purpose:
#  - Return p-values excluding the constant
# =============================================================================
def _feature_pvalues(model) -> pd.Series:
    pvalues = getattr(model, "pvalues", pd.Series(dtype="float64"))
    pvalues = pd.Series(pvalues).drop(labels=["const"], errors="ignore")
    return pvalues.replace([np.inf, -np.inf], np.nan).fillna(1.0)


# =============================================================================
# _max_pvalue(model) -> float | None
# =============================================================================
# Purpose:
#  - Return the largest feature p-value for stability reporting
# =============================================================================
def _max_pvalue(model) -> float | None:
    pvalues = _feature_pvalues(model)
    if pvalues.empty:
        return None
    return float(pvalues.max())


# =============================================================================
# _is_stable(model, p_threshold: float) -> bool
# =============================================================================
# Purpose:
#  - Check whether all remaining feature p-values are under threshold
# =============================================================================
def _is_stable(model, p_threshold: float) -> bool:
    max_pvalue = _max_pvalue(model)
    return bool(max_pvalue is not None and max_pvalue <= p_threshold)


# =============================================================================
# _coefficient_rows(model) -> list[dict]
# =============================================================================
# Purpose:
#  - Return coefficient and p-value rows for the final report
# =============================================================================
def _coefficient_rows(model) -> list[dict]:
    params = pd.Series(model.params).drop(labels=["const"], errors="ignore")
    pvalues = _feature_pvalues(model)
    rows = []
    for feature, coefficient in params.items():
        rows.append(
            {
                "feature": feature,
                "coefficient": float(coefficient),
                "p_value": float(pvalues.get(feature, np.nan)),
            }
        )
    rows.sort(key=lambda row: row["p_value"])
    return rows


# =============================================================================
# _path_rows(path: list[dict]) -> list[dict]
# =============================================================================
# Purpose:
#  - Serialize p-value filtering path without fitted model objects
# =============================================================================
def _path_rows(path: list[dict]) -> list[dict]:
    rows = []
    for state in path:
        rows.append(
            {
                "round": state["round"],
                "n_features": len(state["features"]),
                "max_pvalue": state["max_pvalue"],
                "is_stable": state["is_stable"],
                "removed_features_this_round": state["removed_features_this_round"],
            }
        )
    return rows


# =============================================================================
# _remove_model_data(model) -> None
# =============================================================================
# Purpose:
#  - Shrink statsmodels artifact after all metrics/report values are computed
# =============================================================================
def _remove_model_data(model) -> None:
    try:
        model.remove_data()
    except Exception:
        pass
