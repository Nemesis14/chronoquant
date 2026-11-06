"""Model development pipeline — runs all steps for a single model artifact.

Usage:
    uv run python src/modeling/pipeline.py --model <model_id>
    uv run python src/modeling/pipeline.py --model <model_id> --step feature_engineering
    uv run python src/modeling/pipeline.py --model <model_id> --step search --stage smoke
    uv run python src/modeling/pipeline.py --model <model_id> --step train
    uv run python src/modeling/pipeline.py --model <model_id> --step analyze

Steps (in order):
    setup               Create artifact directory and write manifest.json
    feature_engineering IV/MI feature selection → artifact/feature_engineering/feature_set.json
                        + report artifact/reports/02_feature_engineering/ (reports.builders)
    search              Hyperparameter search → artifact/search/
    train               Fit final model → artifact/model.pkl, features.json, params.json
    predict             Score the full snapshot range → model."<model_id>__pred" table
    analyze             Build the 4 model reports → artifact/reports/<slug>/ (manifest.json + report.md
                        + charts/*.json + tables/*.json; no notebook / HTML / Quarto)

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
             "Steps: setup -> sample -> feature_engineering -> search -> train -> predict -> analyze",
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
    (open_time + target(s) + split) in the lab database; the feature selection
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
    print(
        f"[sample] {summary['sample_table']} created: rows={summary['n_rows']} "
        f"splits={summary['split_row_counts']} feature_set={summary['feature_set_id']} "
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
    """Feature selection (IV / MI / quality / redundancy) + the ``02_feature_engineering`` report.

    Writes ``feature_engineering/feature_set.json`` (input of search / train) and the report
    folder ``reports/02_feature_engineering/``; no notebook, HTML or Quarto involved.
    """
    from reports.builders import feature_engineering

    # Resolve snapshot_id: explicit arg > manifest.json > models.json (handled by the builder);
    # an explicit --snapshot override must win, so it is passed through unchanged.
    if not snapshot_id:
        snapshot_id = meta.get("sampling", {}).get("snapshot_id") or None

    print("[feature_engineering] Running feature selection (reports.builders.feature_engineering)...")
    print(f"  artifact   : {artifact_dir}")
    if snapshot_id:
        print(f"  snapshot_id: {snapshot_id}")

    report_dir = feature_engineering.build(
        model_id, artifact_dir, snapshot_id, write_selection=True
    )
    feature_set_path = artifact_dir / "feature_engineering" / "feature_set.json"
    print(f"[feature_engineering] feature_set.json -> {feature_set_path}")
    print(f"[feature_engineering] report -> {report_dir}")

    # Provenance: link the FE-selected feature_set + register feature_set / report artifacts.
    from modeling import provenance
    feature_set_id = provenance.link_feature_set(model_id, asset_id=meta.get("asset_id"))
    provenance.register_artifacts(
        model_id,
        [("fe_feature_set", feature_set_path), ("fe_report", report_dir / "manifest.json")],
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
    meta         = utils.load_models_config()["models"][model_id]
    target_name  = meta["target_name"]
    direction    = "long" if target_name.startswith("long") else "short"
    print(f"[search] Starting hyperparameter search — stage={stage}, n_trials={n_trials}, direction={direction}")
    run_search(
        model_id          = model_id,
        stage             = stage,
        n_trials          = n_trials,
        timeout_hours     = timeout_hours,
        feature_selection = "fixed",
        feature_key       = "selected",
        direction         = direction,
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

# (report slug, reports.builders module, prerequisite artifact relative to the artifact dir)
_ANALYSIS_REPORTS: list[tuple[str, str, str]] = [
    ("01_sampling",              "sampling",              "manifest.json"),
    ("02_feature_engineering",   "feature_engineering",   "feature_engineering/feature_set.json"),
    ("03_hyperparameter_search", "hyperparameter_search", "search/search_best.json"),
    ("04_strategy",              "strategy",              "strategy/strategy_artifact.json"),
]


def step_analyze(model_id: str, artifact_dir: Path) -> None:
    """Build the four model reports (``reports.builders``) into ``<artifact>/reports/<slug>/``.

    Each report is a folder of ``manifest.json`` + ``report.md`` + ``charts/*.json`` +
    ``tables/*.json`` (Plotly JSON, no notebook / HTML / Quarto).  Skips a report whose
    prerequisite artifact is missing; a failing builder is reported and does not stop the others.
    ``02_feature_engineering`` is refreshed here without rewriting ``feature_set.json``.
    """
    import importlib
    import traceback

    for slug, builder, prereq in _ANALYSIS_REPORTS:
        if not (artifact_dir / prereq).exists():
            print(f"[analyze] SKIP {slug} - prerequisite missing: {prereq}")
            continue
        print(f"[analyze] {slug} - building report...")
        try:
            module = importlib.import_module(f"reports.builders.{builder}")
            if slug == "02_feature_engineering":
                out = module.build(model_id, artifact_dir, None, write_selection=False)
            else:
                out = module.build(model_id, artifact_dir)
        except Exception as exc:  # noqa: BLE001 - one broken report must not stop the rest
            msg = str(exc).encode("ascii", errors="replace").decode("ascii")
            print(f"[analyze] WARNING: report {slug} failed: {type(exc).__name__}: {msg[:300]}")
            traceback.print_exc()
            continue
        print(f"[analyze] {slug} - report written -> {out}")

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
