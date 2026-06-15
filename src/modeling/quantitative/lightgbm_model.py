# =============================================================================
# LightGBM binary classifier training
# =============================================================================
# Purpose:
#  - Train LightGBM candidates on shared time-based CV folds
#  - Reuse standard metrics, artifact layout, and HTML report generation
# =============================================================================

from pathlib import Path

import lightgbm as lgb
import pandas as pd

import utils
from modeling.quantitative.artifacts import save_training_artifacts
from modeling.quantitative.datasets import ModelingDataset, load_modeling_dataset
from modeling.quantitative.metrics import binary_classification_metrics
from modeling.quantitative.reports import write_training_report
from modeling.quantitative.sampling import load_sample_definition, validate_sample_definition
from modeling.quantitative.training_windows import (
    between,
    final_train_test_split,
    fold_sample_size_row,
    fold_split,
)


# =============================================================================
# train_lightgbm_binary(...) -> dict
# =============================================================================
# Purpose:
#  - Tune one LightGBM complexity parameter and save final model artifacts
# =============================================================================
def train_lightgbm_binary(
    model_id: str,
    target_col: str,
    sample_dir: str | Path,
    output_dir: str | Path,
    param_profile: str = "lightgbm_binary_stable_v1",
    tuning_param: str | None = None,
    tuning_values: list[int | float] | None = None,
    row_stride: int = 1,
    random_state: int = 42,
    verbose: bool = False,
    asset_id: str | None = None,
) -> dict:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    profile = _load_param_profile(param_profile)
    base_params = dict(profile["params"])
    tuning_cfg = profile.get("tuning", {})
    tuning_param = tuning_param or tuning_cfg.get("primary_param", "num_leaves")
    tuning_values = tuning_values or tuning_cfg.get("values", [7, 15, 31])

    sample = load_sample_definition(sample_dir)
    validate_sample_definition(sample)

    dataset = load_modeling_dataset(
        target_col=target_col,
        start=sample["data"]["start"],
        end=sample["data"]["end"],
        row_stride=row_stride,
        asset_id=asset_id,
    )
    if len(dataset.y) == 0:
        raise ValueError("Training dataset is empty")

    cv_results, sample_sizes = _cross_validate_values(
        dataset=dataset,
        sample=sample,
        base_params=base_params,
        tuning_param=tuning_param,
        tuning_values=tuning_values,
        random_state=random_state,
        verbose=verbose,
    )
    cv_df = pd.DataFrame(cv_results)
    best_value = _select_best_value(cv_df, tuning_param)

    final_model, final_metrics, feature_importance = _fit_final_model(
        dataset=dataset,
        sample=sample,
        base_params=base_params,
        tuning_param=tuning_param,
        tuning_value=best_value,
        random_state=random_state,
        verbose=verbose,
    )
    validation_predictions_df = validation_predictions_for_value(
        dataset=dataset,
        sample=sample,
        base_params=base_params,
        tuning_param=tuning_param,
        tuning_value=best_value,
        random_state=random_state,
        verbose=verbose,
    )
    final_params = _params_for_value(base_params, tuning_param, best_value, random_state)

    artifacts = {
        "model_id": model_id,
        "trainer": "lightgbm_binary",
        "target_col": target_col,
        "sample_id": sample["sample_id"],
        "sample_dir": str(sample_dir),
        "output_dir": str(output_dir),
        "param_profile": param_profile,
        "tuning_param": tuning_param,
        "tuning_values": tuning_values,
        "best_tuning_value": best_value,
        f"best_{tuning_param}": best_value,
        "row_stride": row_stride,
        "random_state": random_state,
        "n_features_input": len(dataset.feature_cols),
        "n_features_selected": len(dataset.feature_cols),
        "selected_features": dataset.feature_cols,
        "model_params": final_params,
        "final_metrics": final_metrics,
        "feature_importance": feature_importance,
        "validation_predictions_path": "validation_predictions.csv",
        "sample_sizes": sample_sizes,
    }

    save_training_artifacts(
        output_dir=output_dir,
        model=final_model,
        feature_cols=dataset.feature_cols,
        cv_df=cv_df,
        artifacts=artifacts,
        selected_features=dataset.feature_cols,
        validation_predictions_df=validation_predictions_df,
        model_params={
            "param_profile": param_profile,
            "tuning_param": tuning_param,
            "tuning_values": tuning_values,
            "best_tuning_value": best_value,
            "params": final_params,
        },
    )
    write_training_report(
        output_dir=output_dir,
        model_id=model_id,
        target_col=target_col,
        sample=sample,
        cv_df=cv_df,
        sample_sizes=sample_sizes,
        artifacts=artifacts,
        tuning_param=tuning_param,
        tuning_label=tuning_param,
        tuning_xscale="linear",
        auxiliary_columns={
            "n_estimators": "first",
            "max_depth": "first",
            "min_child_samples": "first",
        },
        feature_rows=feature_importance,
        feature_table_title="LightGBM feature importance",
    )
    return artifacts


# =============================================================================
# _cross_validate_values(...) -> tuple[list[dict], list[dict]]
# =============================================================================
# Purpose:
#  - Train/evaluate each tuning value on every shared fold
# =============================================================================
def _cross_validate_values(
    dataset: ModelingDataset,
    sample: dict,
    base_params: dict,
    tuning_param: str,
    tuning_values: list[int | float],
    random_state: int,
    verbose: bool,
) -> tuple[list[dict], list[dict]]:
    results = []
    sample_sizes = []

    for fold in sample["folds"]:
        split = fold_split(dataset, fold)
        sample_sizes.append(fold_sample_size_row(fold, split))

        for tuning_value in tuning_values:
            if verbose:
                print(f"CV fold {fold['fold']} {tuning_param}={tuning_value}", flush=True)
            params = _params_for_value(base_params, tuning_param, tuning_value, random_state)
            model = _build_model(params)
            model.fit(split.X_train, split.y_train)

            train_pred = model.predict_proba(split.X_train)[:, 1]
            valid_pred = model.predict_proba(split.X_eval)[:, 1]
            train_metrics = binary_classification_metrics(split.y_train, train_pred)
            valid_metrics = binary_classification_metrics(split.y_eval, valid_pred)

            results.append(
                {
                    "fold": fold["fold"],
                    tuning_param: tuning_value,
                    "n_estimators": params.get("n_estimators"),
                    "max_depth": params.get("max_depth"),
                    "min_child_samples": params.get("min_child_samples"),
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
# validation_predictions_for_value(...) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Collect out-of-fold validation predictions for the selected tuning value only
# =============================================================================
def validation_predictions_for_value(
    dataset: ModelingDataset,
    sample: dict,
    base_params: dict,
    tuning_param: str,
    tuning_value: int | float,
    random_state: int,
    verbose: bool,
) -> pd.DataFrame:
    rows = []
    params = _params_for_value(base_params, tuning_param, tuning_value, random_state)
    for fold in sample["folds"]:
        split = fold_split(dataset, fold)
        if verbose:
            print(f"Validation predictions fold {fold['fold']} {tuning_param}={tuning_value}", flush=True)
        model = _build_model(params)
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
                    tuning_param: tuning_value,
                }
            )
        )
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


# =============================================================================
# _fit_final_model(...) -> tuple[lgb.LGBMClassifier, dict, list[dict]]
# =============================================================================
# Purpose:
#  - Refit best LightGBM on pre-test rows and evaluate final holdout
# =============================================================================
def _fit_final_model(
    dataset: ModelingDataset,
    sample: dict,
    base_params: dict,
    tuning_param: str,
    tuning_value: int | float,
    random_state: int,
    verbose: bool,
) -> tuple[lgb.LGBMClassifier, dict, list[dict]]:
    split = final_train_test_split(dataset, sample)
    params = _params_for_value(base_params, tuning_param, tuning_value, random_state)
    model = _build_model(params)

    if verbose:
        print(f"Final fit {tuning_param}={tuning_value}", flush=True)
    model.fit(split.X_train, split.y_train)

    train_pred = model.predict_proba(split.X_train)[:, 1]
    test_pred = model.predict_proba(split.X_eval)[:, 1]
    return (
        model,
        {
            "final_train": binary_classification_metrics(split.y_train, train_pred),
            "final_test": binary_classification_metrics(split.y_eval, test_pred),
        },
        _feature_importance_rows(model, dataset.feature_cols),
    )


# =============================================================================
# _build_model(params: dict) -> lgb.LGBMClassifier
# =============================================================================
# Purpose:
#  - Create LightGBM classifier from resolved parameters
# =============================================================================
def _build_model(params: dict) -> lgb.LGBMClassifier:
    return lgb.LGBMClassifier(**params)


# =============================================================================
# _select_best_value(cv_df: pd.DataFrame, tuning_param: str) -> int | float
# =============================================================================
# Purpose:
#  - Select tuning value by highest mean validation PR AUC
# =============================================================================
def _select_best_value(cv_df: pd.DataFrame, tuning_param: str) -> int | float:
    summary = cv_df.groupby(tuning_param, as_index=False).agg(
        valid_pr_auc=("valid_pr_auc", "mean"),
        valid_brier_score=("valid_brier_score", "mean"),
    )
    summary = summary.sort_values(
        by=["valid_pr_auc", "valid_brier_score", tuning_param],
        ascending=[False, True, True],
    )
    value = summary.iloc[0][tuning_param]
    if float(value).is_integer():
        return int(value)
    return float(value)


# =============================================================================
# _params_for_value(...) -> dict
# =============================================================================
# Purpose:
#  - Merge fixed LightGBM parameters with the active tuning value
# =============================================================================
def _params_for_value(
    base_params: dict,
    tuning_param: str,
    tuning_value: int | float,
    random_state: int,
) -> dict:
    params = dict(base_params)
    params[tuning_param] = tuning_value
    params["random_state"] = random_state
    return params


# =============================================================================
# _feature_importance_rows(...) -> list[dict]
# =============================================================================
# Purpose:
#  - Return sorted split/gain feature importance rows for report and metrics
# =============================================================================
def _feature_importance_rows(
    model: lgb.LGBMClassifier,
    feature_cols: list[str],
) -> list[dict]:
    split_importance = model.booster_.feature_importance(importance_type="split")
    gain_importance = model.booster_.feature_importance(importance_type="gain")
    rows = []
    for feature, split_value, gain_value in zip(feature_cols, split_importance, gain_importance):
        rows.append(
            {
                "feature": feature,
                "split_importance": int(split_value),
                "gain_importance": float(gain_value),
            }
        )
    rows.sort(key=lambda row: row["gain_importance"], reverse=True)
    return rows


# =============================================================================
# _load_param_profile(profile_name: str) -> dict
# =============================================================================
# Purpose:
#  - Load one named LightGBM parameter profile from config/model_params.json
# =============================================================================
def _load_param_profile(profile_name: str) -> dict:
    params_cfg = utils.load_model_params_config()
    profiles = params_cfg if "params" not in params_cfg else {"default": params_cfg}
    if profile_name not in profiles:
        raise ValueError(f"Parameter profile not found in config/model_params.json: {profile_name}")
    return profiles[profile_name]
