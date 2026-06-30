"""Model development pipeline — runs all steps for a single model artifact.

Usage:
    uv run python src/modeling/pipeline.py --model <model_id>
    uv run python src/modeling/pipeline.py --model <model_id> --step feature_engineering
    uv run python src/modeling/pipeline.py --model <model_id> --step search --stage smoke
    uv run python src/modeling/pipeline.py --model <model_id> --step train
    uv run python src/modeling/pipeline.py --model <model_id> --step analyze

Steps (in order):
    setup               Create artifact directory and write manifest.json
    feature_engineering Run 01_feature_engineering.ipynb via papermill → artifact/feature_engineering/
    search              Hyperparameter search → artifact/search/
    train               Fit final model → artifact/model.pkl, features.json, params.json
    predict             Score the full snapshot range → model."<model_id>__pred" table
    analyze             Instantiate and run analysis notebooks → artifact/analysis/ + artifact/*.html

When --step is omitted, all steps run in order.
"""

import argparse
import json
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

_ROOT = next(p for p in [Path(__file__).resolve().parent, *Path(__file__).resolve().parents] if (p / "pyproject.toml").exists())
sys.path.insert(0, str(_ROOT / "src"))

import utils  # noqa: E402

NOTEBOOK_TEMPLATE      = _ROOT / "src" / "modeling" / "01_feature_engineering.ipynb"
ANALYSIS_TEMPLATES_DIR = _ROOT / "analyst" / "notebooks"
ALL_STEPS = ["setup", "sample", "feature_engineering", "search", "train", "predict", "analyze"]


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
             "Steps: setup → sample → feature_engineering → search → train → predict → analyze",
    )
    parser.add_argument(
        "--stage", choices=["smoke", "explore", "refine"], default=None,
        help="Search stage (only used for 'search' step). Default: None (no stage cap — n-trials governs)",
    )
    parser.add_argument(
        "--n-trials", type=int, default=100,
        help="Max search trials (only for 'search' step). Default: 100",
    )
    parser.add_argument(
        "--timeout-hours", type=float, default=None,
        help="Hard time limit in hours for the search step. Default: no limit",
    )
    parser.add_argument(
        "--fold-limit", type=int, default=None,
        help="Limit to first N validation folds in search step. Default: stage default",
    )
    parser.add_argument(
        "--snapshot", default=None,
        help="Snapshot id for the 'sample' step. Overrides sampling.snapshot_id in models.json.",
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

    # Provenance: reg.models draft + manifest provenance (snapshot_id, content_sha256).
    from modeling import provenance
    provenance.register_model_draft(model_id, meta, artifact_dir)
    provenance.register_artifacts(model_id, [("manifest", manifest_path)])
    print("[setup] reg.models draft + manifest provenance written")


# ---------------------------------------------------------------------------
# Step: sample
# ---------------------------------------------------------------------------

def step_sample(model_id: str, artifact_dir: Path, snapshot_id: str | None = None) -> None:
    """Create the model's ``model."<model_id>__sample"`` table from a snapshot.

    Snapshot-native path (plan 5, steps 2-3): the source is an immutable
    ``snap."<snapshot_id>"`` table and the output is a small DuckDB table
    (open_time + target(s) + fold_id) in the lab database; the feature selection
    is registered in ``reg.feature_sets``.  The ``snapshot_id`` may be passed
    explicitly (e.g. from ``--snapshot`` CLI arg) or resolved from the model's
    sampling config (``sampling.snapshot_id``).
    """
    print(f"[sample] Creating model.\"{model_id}__sample\" from snapshot")
    models_cfg  = utils.load_models_config()
    meta        = models_cfg["models"][model_id]
    if not snapshot_id:
        snapshot_id = meta.get("sampling", {}).get("snapshot_id")
    if not snapshot_id:
        raise ValueError(
            f"Model {model_id} has no sampling.snapshot_id — pass --snapshot <id> "
            "or set sampling.snapshot_id in config/models.json after running "
            "src/data_handling/05_create_snapshot.py."
        )

    from modeling.sampling import create_model_sample
    summary = create_model_sample(model_id, snapshot_id)
    splits_or_folds = summary.get("split_row_counts") or summary.get("fold_row_counts", {})
    print(
        f"[sample] {summary['sample_table']} created: rows={summary['n_rows']} "
        f"splits={splits_or_folds} feature_set={summary['feature_set_id']} "
        f"(n_input={summary['n_input']} n_selected={summary['n_selected']})"
    )

    # Provenance: create_model_sample already upserts reg.models (status='sampled')
    # + snapshot/feature_set link; here we record the manifest provenance fields.
    from modeling import provenance
    provenance.update_manifest_provenance(
        artifact_dir,
        snapshot_id    = summary["snapshot_id"],
        feature_set_id = summary["feature_set_id"],
    )
    _update_manifest_status(artifact_dir, "sample_done")


# ---------------------------------------------------------------------------
# Step: feature_engineering
# ---------------------------------------------------------------------------

def step_feature_engineering(
    model_id: str,
    meta: dict,
    artifact_dir: Path,
    snapshot_id: str | None = None,
) -> None:
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

    # Resolve snapshot_id: explicit arg > manifest.json > models.json fallback.
    # The notebook has its own fallback chain (models.json → reg.models), but
    # passing SNAPSHOT_ID explicitly ensures --snapshot CLI overrides propagate.
    if not snapshot_id:
        snapshot_id = meta.get("sampling", {}).get("snapshot_id", "")
    snapshot_id = snapshot_id or ""

    print("[feature_engineering] Running notebook via papermill...")
    print(f"  template   : {NOTEBOOK_TEMPLATE}")
    print(f"  output     : {output_nb}")
    print(f"  sample_dir : {sample_dir}")
    if snapshot_id:
        print(f"  snapshot_id: {snapshot_id}")

    pm.execute_notebook(
        str(NOTEBOOK_TEMPLATE),
        str(output_nb),
        parameters={
            "ARTIFACT_DIR": str(artifact_dir),
            "SAMPLE_DIR"  : sample_dir,
            "MODEL_ID"    : model_id,
            "SNAPSHOT_ID" : snapshot_id,
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

    # Provenance: link the FE-selected feature_set + register notebook/html artifacts.
    from modeling import provenance
    feature_set_id = provenance.link_feature_set(model_id, asset_id=meta.get("asset_id"))
    provenance.register_artifacts(
        model_id,
        [("fe_notebook", output_nb), ("fe_html", output_html)],
        asset_id=meta.get("asset_id"),
    )
    print(f"[feature_engineering] reg.feature_sets linked: {feature_set_id}")

    _update_manifest_status(artifact_dir, "feature_engineering_done")


# ---------------------------------------------------------------------------
# Step: search
# ---------------------------------------------------------------------------

def step_search(
    model_id:      str,
    stage:         str,
    n_trials:      int,
    timeout_hours: float | None = None,
) -> None:
    from modeling.search.lgbm_search import run_search
    print(f"[search] Starting hyperparameter search — stage={stage}, n_trials={n_trials}")
    run_search(
        model_id      = model_id,
        stage         = stage,
        n_trials      = n_trials,
        timeout_hours = timeout_hours,
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
# Step: predict
# ---------------------------------------------------------------------------

def step_predict(model_id: str) -> None:
    """Score the full snapshot range and write ``model."<model_id>__pred"`` (plan 5 step 6).

    Offline prediction: the trained model scores the entire immutable snapshot
    range into a separate ``model."<id>__pred"`` table (open_time, pred), leaving
    the snapshot untouched. Sets ``reg.models`` status to ``predicted``.
    """
    from modeling.predict import predict_offline
    print(f"[predict] Scoring full snapshot range for: {model_id}")
    result = predict_offline(model_id, verify_snapshot=False)
    print(
        f"[predict] {result['pred_table']} written: rows={result['n_rows']} "
        f"snapshot={result['snapshot_id']} immutable={result['snapshot_immutable']}"
    )
    _update_manifest_status(_artifact_dir_for(model_id), "predict_done")


# ---------------------------------------------------------------------------
# Step: analyze
# ---------------------------------------------------------------------------

_ANALYSIS_NOTEBOOKS: list[tuple[str, str]] = [
    ("01_sampling",              "manifest.json"),
    ("02_feature_engineering",   "feature_engineering/feature_set.json"),
    ("03_hyperparameter_search", "search/search_best.json"),
    ("04_strategy",              "strategy/strategy_artifact.json"),
]


def _instantiate_analysis_notebook(
    template_path : Path,
    output_path   : Path,
    placeholders  : dict[str, str],
) -> None:
    """Phase 1: replace {{PLACEHOLDER}} in raw cells, ensure parameters tag on Cell 1."""
    try:
        import nbformat
    except ImportError:
        print("[analyze] ERROR: nbformat not installed. Run: uv add nbformat")
        sys.exit(1)

    nb = nbformat.read(str(template_path), as_version=4)
    for cell in nb.cells:
        if cell.cell_type == "raw":
            src = cell.source if isinstance(cell.source, str) else "".join(cell.source)
            for key, val in placeholders.items():
                src = src.replace(key, val)
            cell.source = src

    output_path.parent.mkdir(parents=True, exist_ok=True)
    nbformat.write(nb, str(output_path))


def step_analyze(model_id: str, artifact_dir: Path) -> None:
    """Instantiate and execute analysis notebooks, then render HTML via Quarto.

    Templates live in analyst/notebooks/. For each notebook:
      1. nbformat: replace {{PLACEHOLDER}} in raw frontmatter cell.
      2. papermill: execute with MODEL_ID / DIRECTION / TARGET parameters.
      3. quarto render --no-execute: produce HTML at artifact root.

    Skips any notebook whose prerequisite artifact is missing.
    """
    try:
        import papermill as pm
    except ImportError:
        print("[analyze] ERROR: papermill not installed. Run: uv add papermill")
        sys.exit(1)

    direction       = "long"  if "_l_" in model_id else "short"
    direction_label = "Long"  if direction == "long"  else "Short"
    target          = "long_mfe_fw60" if direction == "long" else "short_mfe_fw60"
    today           = date.today().isoformat()

    placeholders = {
        "{{MODEL_ID}}":         model_id,
        "{{DIRECTION_LABEL}}":  direction_label,
        "{{DATE}}":             today,
    }

    # Resolve SNAPSHOT_ID from manifest for 02_feature_engineering
    manifest_path = artifact_dir / "manifest.json"
    snapshot_id   = ""
    if manifest_path.exists():
        _manifest   = json.loads(manifest_path.read_text(encoding="utf-8"))
        snapshot_id = (
            _manifest.get("snapshot_id")
            or _manifest.get("sampling", {}).get("snapshot_id", "")
            or _manifest.get("provenance", {}).get("snapshot_id", "")
            or ""
        )

    # Resolve strategy VALID_START/VALID_END for 04_strategy papermill params
    strategy_artifact_path = artifact_dir / "strategy" / "strategy_artifact.json"
    valid_start = valid_end = ""
    if strategy_artifact_path.exists():
        _art = json.loads(strategy_artifact_path.read_text(encoding="utf-8"))
        valid_start = _art.get("fit_period", {}).get("start", "")
        valid_end   = _art.get("fit_period", {}).get("end",   "")

    # Per-notebook papermill parameter sets
    extra_params: dict[str, dict] = {
        "01_sampling": {
            "MODEL_ID" : model_id,
            "DIRECTION": direction,
            "TARGET"   : target,
        },
        "02_feature_engineering": {
            "ARTIFACT_DIR": str(artifact_dir),
            "SAMPLE_DIR"  : str(artifact_dir),
            "MODEL_ID"    : model_id,
            "SNAPSHOT_ID" : snapshot_id,
        },
        "03_hyperparameter_search": {
            "model_id": model_id,
        },
        "04_strategy": {
            "MODEL_ID"    : model_id,
            "DIRECTION"   : direction,
            "STRATEGY_DIR": str(artifact_dir / "strategy"),
            "VALID_START" : valid_start,
            "VALID_END"   : valid_end,
        },
    }

    analysis_dir = artifact_dir / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    for nb_stem, prereq in _ANALYSIS_NOTEBOOKS:
        prereq_path = artifact_dir / prereq
        if not prereq_path.exists():
            print(f"[analyze] SKIP {nb_stem} — prerequisite missing: {prereq}")
            continue

        template_path = ANALYSIS_TEMPLATES_DIR / f"{nb_stem}.ipynb"
        if not template_path.exists():
            print(f"[analyze] SKIP {nb_stem} — template not found: {template_path}")
            continue

        output_nb   = analysis_dir / f"{nb_stem}.ipynb"
        output_html = artifact_dir / f"{nb_stem}.html"

        print(f"[analyze] {nb_stem} — instantiating template...")
        _instantiate_analysis_notebook(template_path, output_nb, placeholders)

        print(f"[analyze] {nb_stem} — executing via papermill...")
        try:
            pm.execute_notebook(
                str(output_nb),
                str(output_nb),
                parameters = extra_params[nb_stem],
                kernel_name = "python3",
            )
        except Exception as exc:
            msg = str(exc).encode("ascii", errors="replace").decode("ascii")
            print(f"[analyze] WARNING: papermill execution failed for {nb_stem}: {msg[:300]}")
            continue

        print(f"[analyze] {nb_stem} — rendering HTML via Quarto...")
        result = subprocess.run(
            ["uv", "run", "quarto", "render", f"{nb_stem}.ipynb", "--no-execute",
             "--embed-resources"],
            capture_output=True, text=True, cwd=str(analysis_dir),
        )
        html_in_analysis = analysis_dir / f"{nb_stem}.html"
        if html_in_analysis.exists():
            import shutil
            shutil.move(str(html_in_analysis), str(output_html))
            print(f"[analyze] {nb_stem} — HTML rendered -> {output_html}")
            if result.returncode != 0:
                print(f"[analyze] NOTE: quarto returned non-zero but HTML produced:\n{result.stderr[:400]}")
        else:
            print(f"[analyze] WARNING: quarto render failed for {nb_stem}:\n{result.stderr[:400]}")

    _update_manifest_status(artifact_dir, "analyze_done")


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
            step_sample(model_id, artifact_dir, snapshot_id=args.snapshot)
        elif step == "feature_engineering":
            step_feature_engineering(model_id, meta, artifact_dir, snapshot_id=args.snapshot)
        elif step == "search":
            step_search(
                model_id,
                args.stage,
                args.n_trials,
                timeout_hours = args.timeout_hours,
            )
        elif step == "train":
            step_train(model_id)
        elif step == "predict":
            step_predict(model_id)
        elif step == "analyze":
            step_analyze(model_id, artifact_dir)

    print(f"\n[pipeline] All steps complete for {model_id}")


if __name__ == "__main__":
    main()
