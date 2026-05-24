# =============================================================================
# L1-regularized logistic regression training
# =============================================================================
# Purpose:
#  - Train Lasso-style logistic regression on shared time-based CV folds
#  - Save model artifacts, selected features, metrics, CV results, and HTML report
# =============================================================================

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from modeling.artifacts import save_training_artifacts
from modeling.datasets import ModelingDataset, load_modeling_dataset
from modeling.metrics import binary_classification_metrics
from modeling.reports import write_training_report
from modeling.sampling import load_sample_definition, validate_sample_definition
from modeling.training_windows import (
    between,
    final_train_test_split,
    fold_sample_size_row,
    fold_split,
)


# =============================================================================
# train_lasso_logreg(...) -> dict
# =============================================================================
# Purpose:
#  - Run L1 Logistic Regression CV, refit best model, save artifacts and report
# =============================================================================
def train_lasso_logreg(
    model_id: str,
    target_col: str,
    sample_dir: str | Path,
    output_dir: str | Path,
    alphas: list[float] | None = None,
    class_weight: str | None = None,
    solver: str = "liblinear",
    row_stride: int = 1,
    max_iter: int = 300,
    random_state: int = 42,
    verbose: bool = False,
) -> dict:
    alphas = alphas or [0.001, 0.003, 0.01, 0.03, 0.10]
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sample = load_sample_definition(sample_dir)
    validate_sample_definition(sample)

    dataset = load_modeling_dataset(
        target_col=target_col,
        start=sample["data"]["start"],
        end=sample["data"]["end"],
        row_stride=row_stride,
    )
    if len(dataset.y) == 0:
        raise ValueError("Training dataset is empty")

    cv_results, sample_sizes = _cross_validate_alphas(
        dataset=dataset,
        sample=sample,
        alphas=alphas,
        class_weight=class_weight,
        solver=solver,
        max_iter=max_iter,
        random_state=random_state,
        verbose=verbose,
    )
    cv_df = pd.DataFrame(cv_results)
    best_alpha = _select_best_alpha(cv_df)

    final_model, final_metrics, selected_features = _fit_final_model(
        dataset=dataset,
        sample=sample,
        alpha=best_alpha,
        class_weight=class_weight,
        solver=solver,
        max_iter=max_iter,
        random_state=random_state,
        verbose=verbose,
    )
    validation_predictions_df = validation_predictions_for_alpha(
        dataset=dataset,
        sample=sample,
        alpha=best_alpha,
        class_weight=class_weight,
        solver=solver,
        max_iter=max_iter,
        random_state=random_state,
        verbose=verbose,
    )

    artifacts = {
        "model_id": model_id,
        "trainer": "sklearn_lasso_logreg",
        "target_col": target_col,
        "sample_id": sample["sample_id"],
        "sample_dir": str(sample_dir),
        "output_dir": str(output_dir),
        "tuning_param": "alpha",
        "best_tuning_value": best_alpha,
        "alphas": alphas,
        "best_alpha": best_alpha,
        "best_C": 1 / best_alpha,
        "class_weight": class_weight,
        "solver": solver,
        "row_stride": row_stride,
        "max_iter": max_iter,
        "n_features_input": len(dataset.feature_cols),
        "n_features_selected": len(selected_features),
        "selected_features": selected_features,
        "final_metrics": final_metrics,
        "validation_predictions_path": "validation_predictions.csv",
        "sample_sizes": sample_sizes,
    }

    save_training_artifacts(
        output_dir=output_dir,
        model=final_model,
        feature_cols=dataset.feature_cols,
        cv_df=cv_df,
        artifacts=artifacts,
        selected_features=selected_features,
        validation_predictions_df=validation_predictions_df,
    )
    write_training_report(
        output_dir=output_dir,
        model_id=model_id,
        target_col=target_col,
        sample=sample,
        cv_df=cv_df,
        sample_sizes=sample_sizes,
        artifacts=artifacts,
        tuning_param="alpha",
        tuning_label="alpha (regularization strength)",
        tuning_xscale="log",
        auxiliary_columns={
            "C": "first",
            "selected_features": "mean",
        },
        feature_rows=[{"feature": feature} for feature in selected_features],
        feature_table_title="Selected Lasso variables",
    )
    return artifacts


# =============================================================================
# _cross_validate_alphas(...) -> tuple[list[dict], list[dict]]
# =============================================================================
# Purpose:
#  - Train/evaluate every alpha on every shared fold
# =============================================================================
def _cross_validate_alphas(
    dataset: ModelingDataset,
    sample: dict,
    alphas: list[float],
    class_weight: str | None,
    solver: str,
    max_iter: int,
    random_state: int,
    verbose: bool,
) -> tuple[list[dict], list[dict]]:
    results = []
    sample_sizes = []

    for fold in sample["folds"]:
        split = fold_split(dataset, fold)
        sample_sizes.append(fold_sample_size_row(fold, split))

        for alpha in alphas:
            if verbose:
                print(f"CV fold {fold['fold']} alpha={alpha}", flush=True)
            model = _build_model(alpha, class_weight, solver, max_iter, random_state)
            model.fit(split.X_train, split.y_train)

            train_pred = model.predict_proba(split.X_train)[:, 1]
            valid_pred = model.predict_proba(split.X_eval)[:, 1]
            train_metrics = binary_classification_metrics(split.y_train, train_pred)
            valid_metrics = binary_classification_metrics(split.y_eval, valid_pred)
            selected_count = _selected_feature_count(model)

            results.append(
                {
                    "fold": fold["fold"],
                    "alpha": alpha,
                    "C": 1 / alpha,
                    "selected_features": selected_count,
                    "train_roc_auc": train_metrics["roc_auc"],
                    "train_pr_auc": train_metrics["pr_auc"],
                    "train_brier_score": train_metrics["brier_score"],
                    "valid_roc_auc": valid_metrics["roc_auc"],
                    "valid_pr_auc": valid_metrics["pr_auc"],
                    "valid_brier_score": valid_metrics["brier_score"],
                    "train_log_loss": train_metrics["log_loss"],
                    "valid_log_loss": valid_metrics["log_loss"],
                }
            )

    return results, sample_sizes


# =============================================================================
# validation_predictions_for_alpha(...) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Collect out-of-fold validation predictions for the selected alpha only
# =============================================================================
def validation_predictions_for_alpha(
    dataset: ModelingDataset,
    sample: dict,
    alpha: float,
    class_weight: str | None,
    solver: str,
    max_iter: int,
    random_state: int,
    verbose: bool,
) -> pd.DataFrame:
    rows = []
    for fold in sample["folds"]:
        split = fold_split(dataset, fold)
        if verbose:
            print(f"Validation predictions fold {fold['fold']} alpha={alpha}", flush=True)
        model = _build_model(alpha, class_weight, solver, max_iter, random_state)
        model.fit(split.X_train, split.y_train)
        valid_pred = model.predict_proba(split.X_eval)[:, 1]
        valid_time = dataset.open_time.loc[
            between(dataset.open_time, fold["valid_start"], fold["valid_end"])
        ]
        rows.append(
            pd.DataFrame(
                {
                    "fold": fold["fold"],
                    "open_time": valid_time.reset_index(drop=True),
                    "y_true": split.y_eval.reset_index(drop=True),
                    "y_pred": valid_pred,
                    "alpha": alpha,
                }
            )
        )
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


# =============================================================================
# _fit_final_model(...) -> tuple[Pipeline, dict, list[str]]
# =============================================================================
# Purpose:
#  - Refit best alpha on all pre-test rows and evaluate train/test
# =============================================================================
def _fit_final_model(
    dataset: ModelingDataset,
    sample: dict,
    alpha: float,
    class_weight: str | None,
    solver: str,
    max_iter: int,
    random_state: int,
    verbose: bool,
) -> tuple[Pipeline, dict, list[str]]:
    split = final_train_test_split(dataset, sample)

    model = _build_model(alpha, class_weight, solver, max_iter, random_state)
    if verbose:
        print(f"Final fit alpha={alpha}", flush=True)
    model.fit(split.X_train, split.y_train)

    train_pred = model.predict_proba(split.X_train)[:, 1]
    test_pred = model.predict_proba(split.X_eval)[:, 1]
    selected_features = _selected_features(model, dataset.feature_cols)

    return (
        model,
        {
            "final_train": binary_classification_metrics(split.y_train, train_pred),
            "final_test": binary_classification_metrics(split.y_eval, test_pred),
        },
        selected_features,
    )


# =============================================================================
# _build_model(...) -> Pipeline
# =============================================================================
# Purpose:
#  - Create the preprocessing + L1 LogisticRegression pipeline
# =============================================================================
def _build_model(
    alpha: float,
    class_weight: str | None,
    solver: str,
    max_iter: int,
    random_state: int,
) -> Pipeline:
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    solver=solver,
                    C=1 / alpha,
                    l1_ratio=1.0,
                    class_weight=class_weight,
                    max_iter=max_iter,
                    random_state=random_state,
                ),
            ),
        ]
    )


# =============================================================================
# _select_best_alpha(cv_df: pd.DataFrame) -> float
# =============================================================================
# Purpose:
#  - Select alpha by highest mean validation PR AUC
# =============================================================================
def _select_best_alpha(cv_df: pd.DataFrame) -> float:
    summary = cv_df.groupby("alpha", as_index=False).agg(
        valid_pr_auc=("valid_pr_auc", "mean"),
        selected_features=("selected_features", "mean"),
    )
    summary = summary.sort_values(
        by=["valid_pr_auc", "selected_features", "alpha"],
        ascending=[False, True, False],
    )
    return float(summary.iloc[0]["alpha"])


# =============================================================================
# _selected_feature_count(model: Pipeline) -> int
# =============================================================================
# Purpose:
#  - Count non-zero coefficients in pipeline model step
# =============================================================================
def _selected_feature_count(model: Pipeline) -> int:
    coef = model.named_steps["model"].coef_[0]
    return int(np.sum(np.abs(coef) > 1e-12))


# =============================================================================
# _selected_features(model: Pipeline, feature_cols: list[str]) -> list[str]
# =============================================================================
# Purpose:
#  - Return feature names with non-zero coefficients
# =============================================================================
def _selected_features(model: Pipeline, feature_cols: list[str]) -> list[str]:
    coef = model.named_steps["model"].coef_[0]
    return [feature for feature, value in zip(feature_cols, coef) if abs(value) > 1e-12]
