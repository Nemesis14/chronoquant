"""Model development pipeline — runs all steps for a single model artifact.

Usage:
    uv run python src/modeling/pipeline.py --model <model_id>
    uv run python src/modeling/pipeline.py --model <model_id> --step feature_engineering
    uv run python src/modeling/pipeline.py --model <model_id> --step search --stage smoke
    uv run python src/modeling/pipeline.py --model <model_id> --step train

Steps (in order):
    setup               Create artifact directory and write manifest.json
    feature_engineering Run 01_feature_engineering.ipynb via papermill → artifact/feature_engineering/
    search              Hyperparameter search → artifact/search/
    train               Fit final model → artifact/model.pkl, features.json, params.json

When --step is omitted, all steps run in order.
"""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

_ROOT = next(p for p in [Path(__file__).resolve().parent, *Path(__file__).resolve().parents] if (p / "pyproject.toml").exists())
sys.path.insert(0, str(_ROOT / "src"))

import utils  # noqa: E402

NOTEBOOK_TEMPLATE = _ROOT / "src" / "modeling" / "01_feature_engineering.ipynb"
ALL_STEPS = ["setup", "sample", "feature_engineering", "search", "train"]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ChronoQuant model development pipeline.")
    parser.add_argument(
        "--model", required=True,
        help="Model ID from config/models.json (e.g. <model_id>)",
    )
    parser.add_argument(
        "--step", choices=ALL_STEPS, default=None,
        help="Single pipeline step to run. Omit to run all steps in order. "
             "Steps: setup → sample → feature_engineering → search → train",
    )
    parser.add_argument(
        "--stage", choices=["smoke", "explore", "refine"], default="smoke",
        help="Search stage (only used for 'search' step). Default: smoke",
    )
    parser.add_argument(
        "--n-trials", type=int, default=60,
        help="Max search trials (only for 'search' step). Default: 60",
    )
    parser.add_argument(
        "--timeout-hours", type=float, default=None,
        help="Hard time limit in hours for the search step. Default: no limit",
    )
    parser.add_argument(
        "--fold-limit", type=int, default=None,
        help="Limit to first N validation folds in search step. Default: stage default",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Step: setup
# ---------------------------------------------------------------------------

def step_setup(model_id: str, meta: dict, artifact_dir: Path) -> None:
    print(f"[setup] Creating artifact directory: {artifact_dir}")
    artifact_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "model_id"    : model_id,
        "display_name": meta.get("display_name", model_id),
        "description" : meta.get("description", ""),
        "asset_id"    : meta["asset_id"],
        "target_name" : meta["target_name"],
        "family"      : meta["family"],
        "trainer"     : meta["trainer"],
        "sampling"    : meta["sampling"],
        "created_at"  : datetime.now(UTC).isoformat(),
        "pipeline_status": "setup",
    }
    manifest_path = artifact_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=4), encoding="utf-8")
    print(f"[setup] manifest.json written -> {manifest_path}")


# ---------------------------------------------------------------------------
# Step: sample
# ---------------------------------------------------------------------------

def step_sample(model_id: str, artifact_dir: Path) -> None:
    """Create model-specific sample.

    Dispatches to walk-forward or yearly sampling based on the 'sampling_mode'
    key in config/models.json.  If absent or 'yearly', uses the legacy yearly
    random-week sampler.  If 'walk_forward', uses the walk-forward CV sampler.
    """
    print(f"[sample] Creating model-specific sample in: {artifact_dir}")
    models_cfg   = utils.load_models_config()
    meta         = models_cfg["models"][model_id]
    sampling_mode = meta.get("sampling", {}).get("sampling_mode", "yearly")

    if sampling_mode == "walk_forward":
        from modeling.sampling import create_model_walk_forward_sample
        create_model_walk_forward_sample(model_id)
        print(f"[sample] Walk-forward sample written -> {artifact_dir / 'sample_train_valid.parquet'}")
    else:
        from modeling.sampling import create_model_sample
        create_model_sample(model_id)
        print(f"[sample] Yearly sample written -> {artifact_dir / 'sample_train_valid.parquet'}")

    _update_manifest_status(artifact_dir, "sample_done")


# ---------------------------------------------------------------------------
# Step: feature_engineering
# ---------------------------------------------------------------------------

def step_feature_engineering(model_id: str, meta: dict, artifact_dir: Path) -> None:
    try:
        import papermill as pm
    except ImportError:
        print("[feature_engineering] ERROR: papermill not installed. Run: uv add papermill")
        sys.exit(1)

    fe_dir = artifact_dir / "feature_engineering"
    fe_dir.mkdir(parents=True, exist_ok=True)

    output_nb = fe_dir / "01_feature_engineering.ipynb"
    output_html = fe_dir / "01_feature_engineering.html"

    # Sample is now model-specific and lives in the artifact directory.
    sample_dir = str(artifact_dir)

    print("[feature_engineering] Running notebook via papermill...")
    print(f"  template  : {NOTEBOOK_TEMPLATE}")
    print(f"  output    : {output_nb}")
    print(f"  sample_dir: {sample_dir}")

    pm.execute_notebook(
        str(NOTEBOOK_TEMPLATE),
        str(output_nb),
        parameters={
            "ARTIFACT_DIR": str(artifact_dir),
            "SAMPLE_DIR"  : sample_dir,
            "MODEL_ID"    : model_id,
        },
        kernel_name="python3",
    )

    import subprocess
    result = subprocess.run(
        ["uv", "run", "quarto", "render", str(output_nb), "--no-execute"],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        print(f"[feature_engineering] HTML rendered -> {output_html}")
    else:
        print(f"[feature_engineering] WARNING: quarto render failed:\n{result.stderr}")

    _update_manifest_status(artifact_dir, "feature_engineering_done")


# ---------------------------------------------------------------------------
# Step: search
# ---------------------------------------------------------------------------

def step_search(
    model_id:      str,
    stage:         str,
    n_trials:      int,
    timeout_hours: float | None = None,
    fold_limit:    int | None   = None,
) -> None:
    from modeling.search.lgbm_search import run_search
    print(f"[search] Starting hyperparameter search — stage={stage}, n_trials={n_trials}")
    run_search(
        model_id      = model_id,
        stage         = stage,
        n_trials      = n_trials,
        timeout_hours = timeout_hours,
        fold_limit    = fold_limit,
    )
    _update_manifest_status(_artifact_dir_for(model_id), "search_done")


# ---------------------------------------------------------------------------
# Step: train
# ---------------------------------------------------------------------------

def step_train(model_id: str) -> None:
    from modeling.training.train import train_model
    print(f"[train] Training model: {model_id}")
    result = train_model(model_id)
    print(
        f"[train] Done — n_features={result.get('n_features')}, "
        f"n_estimators={result.get('n_estimators')}, "
        f"output_dir={result.get('artifact_dir')}"
    )
    _update_manifest_status(_artifact_dir_for(model_id), "train_done")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _artifact_dir_for(model_id: str) -> Path:
    meta = utils.load_models_config()["models"][model_id]
    return Path(utils._resolve_path(meta["artifact_dir"]))


def _update_manifest_status(artifact_dir: Path, status: str) -> None:
    manifest_path = artifact_dir / "manifest.json"
    if not manifest_path.exists():
        return
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["pipeline_status"] = status
    manifest["updated_at"] = datetime.now(UTC).isoformat()
    manifest_path.write_text(json.dumps(manifest, indent=4), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()
    model_id = args.model

    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        print(f"ERROR: '{model_id}' not found in config/models.json")
        sys.exit(1)

    meta         = models_cfg["models"][model_id]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))

    steps = [args.step] if args.step else ALL_STEPS

    print(f"\n{'='*60}")
    print(f"Pipeline: {model_id}")
    print(f"Steps   : {', '.join(steps)}")
    print(f"Artifact: {artifact_dir}")
    print(f"{'='*60}\n")

    for step in steps:
        if step == "setup":
            step_setup(model_id, meta, artifact_dir)
        elif step == "sample":
            step_sample(model_id, artifact_dir)
        elif step == "feature_engineering":
            step_feature_engineering(model_id, meta, artifact_dir)
        elif step == "search":
            step_search(
                model_id,
                args.stage,
                args.n_trials,
                timeout_hours = args.timeout_hours,
                fold_limit    = args.fold_limit,
            )
        elif step == "train":
            step_train(model_id)

    print(f"\n[pipeline] All steps complete for {model_id}")


if __name__ == "__main__":
    main()
