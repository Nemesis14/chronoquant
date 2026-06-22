"""Pipeline provenance — registry writes + data-provenance recording (plan 4.3, 5).

Every model pipeline step records its provenance: the registry (``reg.*``) is the
catalog that links ``snapshot -> feature_set -> model -> search_run -> artifacts``,
and the model's ``manifest.json`` carries the data-provenance fields
(``snapshot_id``, ``feature_set_id``, ``content_sha256``) so a model artifact is
self-describing (P2 in the architecture plan).

This module is the modeling-side gateway over the registry CRUD (t311): it opens
the lab connection via :func:`utils.open_lab_connection` (config-gateway — never a
hardcoded path) and wraps :mod:`data_handling.store.registry`.  It is additive to
the t313 sampling integration, which already upserts ``reg.models`` (status
``sampled``) + the snapshot/feature_set link; here the full status chain
(``draft -> sampled -> trained -> predicted``), the ``reg.search_runs`` row and the
``reg.artifacts`` rows are added, and the manifest provenance fields are filled in.

Reference: _doc_/_plans_/data_process_architecture.md sections 4.3, 5 (step table).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import utils
from data_handling.store import registry

logger = logging.getLogger(__name__)

# reg.models lifecycle the pipeline drives, in order (plan 5 step table).
MODEL_STATUS_CHAIN: tuple[str, ...] = ("draft", "sampled", "trained", "predicted")

# reg.artifacts kinds (plan 6 artifact layout).
ARTIFACT_KINDS: tuple[str, ...] = (
    "manifest",
    "fe_notebook",
    "fe_html",
    "search_best",
    "search_trials",
    "search_summary",
    "model_pkl",
    "features",
    "params",
    "metrics",
    "cv_results",
)


# %% Model lifecycle


def register_model_draft(model_id: str, meta: dict[str, Any], artifact_dir: Path) -> None:
    """Insert/refresh the ``reg.models`` draft row and fill manifest provenance.

    Called from ``step_setup`` (plan 5 step 0).  Upserts ``reg.models`` with
    ``status='draft'`` and the model's direction, and writes the data-provenance
    fields (``snapshot_id`` and, when resolvable, ``content_sha256``) into
    ``manifest.json``.  Idempotent — re-running setup does not duplicate the row.

    Args:
        model_id     : Model key from config/models.json.
        meta         : The model's config entry (asset_id, target_name, sampling...).
        artifact_dir : The model's artifact directory (holds manifest.json).
    """
    snapshot_id = meta.get("sampling", {}).get("snapshot_id")
    direction   = _direction_from_target(meta.get("target_name", ""))

    conn = utils.open_lab_connection(meta.get("asset_id"))
    try:
        registry.upsert(
            conn,
            "models",
            {
                "model_id":    model_id,
                "snapshot_id": snapshot_id,
                "direction":   direction,
                "status":      "draft",
            },
        )
        content_sha256 = _snapshot_content_sha256(conn, snapshot_id) if snapshot_id else None
    finally:
        conn.close()

    update_manifest_provenance(
        artifact_dir,
        snapshot_id    = snapshot_id,
        content_sha256 = content_sha256,
    )
    logger.info("register_model_draft: %s snapshot=%s status=draft", model_id, snapshot_id)


def set_model_status(model_id: str, status: str, asset_id: str | None = None) -> None:
    """Set ``reg.models.status`` for a model (drives the draft->...->predicted chain).

    Args:
        model_id : Model key.
        status   : New status (one of :data:`MODEL_STATUS_CHAIN`).
        asset_id : Asset key for the lab connection; resolved from config if None.
    """
    if status not in MODEL_STATUS_CHAIN:
        logger.warning("set_model_status: %s unexpected status %r", model_id, status)
    conn = utils.open_lab_connection(asset_id or _asset_id_for(model_id))
    try:
        registry.set_status(conn, "models", model_id, status)
    finally:
        conn.close()
    logger.info("set_model_status: %s -> %s", model_id, status)


def mark_model_trained(
    model_id      : str,
    oos_metric    : float | None = None,
    search_run_id : str | None   = None,
    asset_id      : str | None   = None,
) -> None:
    """Upsert the trained model row (status='trained', oos_metric, search_run_id).

    Args:
        model_id      : Model key.
        oos_metric    : Out-of-sample objective to record on ``reg.models``.
        search_run_id : Linking the model to its tuning run (``reg.search_runs``).
        asset_id      : Asset key for the lab connection; resolved if None.
    """
    row: dict[str, Any] = {"model_id": model_id, "status": "trained"}
    if oos_metric is not None:
        row["oos_metric"] = oos_metric
    if search_run_id is not None:
        row["search_run_id"] = search_run_id

    conn = utils.open_lab_connection(asset_id or _asset_id_for(model_id))
    try:
        registry.upsert(conn, "models", row)
    finally:
        conn.close()
    logger.info(
        "mark_model_trained: %s oos_metric=%s search_run=%s", model_id, oos_metric, search_run_id
    )


# %% Feature engineering provenance


def link_feature_set(model_id: str, asset_id: str | None = None) -> str | None:
    """Link the FE-selected feature_set to the model on ``reg.models`` (plan 5 step 3).

    The sampling step (t313) registers ``reg.feature_sets`` for the full feat_*
    superset (or the FE selection if already present).  The feature_engineering
    step writes the actual selection to ``feature_set.json``; this re-derives the
    selected feature_set_id and ensures ``reg.feature_sets`` + the ``reg.models``
    link reflect that selection.

    Args:
        model_id : Model key.
        asset_id : Asset key for the lab connection; resolved if None.

    Returns:
        The linked feature_set_id, or None if no FE selection is available.
    """
    from modeling.sampling.snapshot_sampler import build_feature_set_id

    meta         = utils.load_models_config()["models"][model_id]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))
    target_name  = meta["target_name"]
    asset_id     = asset_id or meta["asset_id"]
    snapshot_id  = meta.get("sampling", {}).get("snapshot_id")

    selected = _selected_features(artifact_dir)
    if not selected:
        logger.info("link_feature_set: %s no FE selection found — skipped", model_id)
        return None

    horizon   = _horizon_from_target(target_name)
    direction = _direction_from_target(target_name)
    feature_set_id = build_feature_set_id(asset_id, horizon, direction, selected)

    conn = utils.open_lab_connection(asset_id)
    try:
        registry.upsert(
            conn,
            "feature_sets",
            {
                "feature_set_id": feature_set_id,
                "snapshot_id":    snapshot_id,
                "n_selected":     len(selected),
                "selected_cols":  selected,
                "status":         "candidate",
            },
        )
        registry.upsert(
            conn,
            "models",
            {"model_id": model_id, "feature_set_id": feature_set_id},
        )
    finally:
        conn.close()

    update_manifest_provenance(artifact_dir, feature_set_id=feature_set_id)
    logger.info("link_feature_set: %s -> %s (n_selected=%d)", model_id, feature_set_id, len(selected))
    return feature_set_id


# %% Search provenance


def latest_search_run_id(model_id: str, asset_id: str | None = None) -> str | None:
    """Return the most recently updated ``reg.search_runs`` id for a model, if any.

    The train step does not know which search stage ran; this looks up the model's
    search run(s) so the model can be linked to its tuning run.

    Args:
        model_id : Owning model id.
        asset_id : Asset key for the lab connection; resolved if None.

    Returns:
        The latest search_run_id for the model, or None if no search run exists.
    """
    conn = utils.open_lab_connection(asset_id or _asset_id_for(model_id))
    try:
        row = conn.execute(
            "SELECT search_run_id FROM reg.search_runs WHERE model_id = ? "
            "ORDER BY updated_at DESC LIMIT 1",
            [model_id],
        ).fetchone()
    finally:
        conn.close()
    return str(row[0]) if row else None


def register_search_run(
    model_id    : str,
    stage       : str,
    best        : dict[str, Any],
    asset_id    : str | None = None,
) -> str:
    """Record the search run's best params + objective in ``reg.search_runs`` (plan 5 step 4).

    Args:
        model_id : Owning model id.
        stage    : Search stage (smoke/explore/refine) — part of the search_run_id.
        best     : The best-trial record from the search (params + objective_score).
        asset_id : Asset key for the lab connection; resolved if None.

    Returns:
        The search_run_id written.
    """
    search_run_id = f"{model_id}__search_{stage}"
    best_params   = best.get("params", {}) if best else {}
    objective     = best.get("objective_score") if best else None

    conn = utils.open_lab_connection(asset_id or _asset_id_for(model_id))
    try:
        registry.upsert(
            conn,
            "search_runs",
            {
                "search_run_id": search_run_id,
                "model_id":      model_id,
                "stage":         stage,
                "objective":     objective,
                "best_params":   best_params,
                "status":        "candidate",
            },
        )
    finally:
        conn.close()
    logger.info("register_search_run: %s objective=%s", search_run_id, objective)
    return search_run_id


# %% Artifact provenance


def register_artifacts(
    model_id  : str,
    artifacts : list[tuple[str, Path | str]],
    asset_id  : str | None = None,
) -> None:
    """Register file artifact paths in ``reg.artifacts`` (plan 5; model.pkl/ipynb/html/logs).

    Only existing files are recorded; missing paths are skipped (a step may not
    produce every artifact).  The artifact_id is ``{model_id}__{kind}`` so re-runs
    upsert in place.

    Args:
        model_id  : Owning model id (``owner_id`` on ``reg.artifacts``).
        artifacts : (kind, path) pairs — kind ideally from :data:`ARTIFACT_KINDS`.
        asset_id  : Asset key for the lab connection; resolved if None.
    """
    rows = [
        (kind, Path(path)) for kind, path in artifacts if Path(path).exists()
    ]
    if not rows:
        logger.info("register_artifacts: %s no existing artifact files — skipped", model_id)
        return

    conn = utils.open_lab_connection(asset_id or _asset_id_for(model_id))
    try:
        for kind, path in rows:
            registry.upsert(
                conn,
                "artifacts",
                {
                    "artifact_id": f"{model_id}__{kind}",
                    "owner_id":    model_id,
                    "kind":        kind,
                    "path":        str(path),
                    "status":      "candidate",
                },
            )
    finally:
        conn.close()
    logger.info("register_artifacts: %s registered %d artifacts", model_id, len(rows))


# %% Manifest provenance


def update_manifest_provenance(
    artifact_dir   : Path,
    snapshot_id    : str | None = None,
    feature_set_id : str | None = None,
    content_sha256 : str | None = None,
) -> None:
    """Merge data-provenance fields into the model's ``manifest.json`` (plan 5 step 0).

    Writes only the fields that are provided (None values are ignored), so a step
    can fill in its own provenance without clobbering earlier ones.

    Args:
        artifact_dir   : The model's artifact directory.
        snapshot_id    : Immutable snapshot id the model is built from.
        feature_set_id : Logical feature_set id selected for the model.
        content_sha256 : Snapshot content hash (data fingerprint).
    """
    manifest_path = artifact_dir / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    provenance = dict(manifest.get("provenance", {}))
    for key, value in (
        ("snapshot_id", snapshot_id),
        ("feature_set_id", feature_set_id),
        ("content_sha256", content_sha256),
    ):
        if value is not None:
            provenance[key] = value
    manifest["provenance"] = provenance

    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=4), encoding="utf-8")
    logger.debug("update_manifest_provenance: %s %s", manifest_path, provenance)


# %% Internal helpers


def _asset_id_for(model_id: str) -> str | None:
    """Resolve the asset_id for a model from config (None falls back to default asset)."""
    meta = utils.load_models_config().get("models", {}).get(model_id, {})
    return meta.get("asset_id")


def _snapshot_content_sha256(conn: Any, snapshot_id: str) -> str | None:
    """Look up the snapshot's content_sha256 from ``reg.snapshots`` (data fingerprint)."""
    row = registry.get(conn, "snapshots", snapshot_id)
    if row is None:
        return None
    return row.get("content_sha256")


def _selected_features(artifact_dir: Path) -> list[str]:
    """Read the FE-selected feat_* columns from feature_set.json (empty if absent)."""
    fe_path = artifact_dir / "feature_engineering" / "feature_set.json"
    if not fe_path.exists():
        return []
    payload  = json.loads(fe_path.read_text(encoding="utf-8"))
    selected = payload.get("selected") or []
    return [c for c in selected if isinstance(c, str) and c.startswith("feat_")]


def _direction_from_target(target_name: str) -> str:
    """Map a target prefix to the direction token (long -> l, short -> s, else combo)."""
    prefix = target_name.split("_", 1)[0] if target_name else ""
    return {"long": "l", "short": "s"}.get(prefix, "combo")


def _horizon_from_target(target_name: str) -> int:
    """Parse the forward-window bar count from a target name (e.g. long_mfe_fw60 -> 60)."""
    token = target_name.rsplit("_fw", 1)
    if len(token) == 2 and token[1].isdigit():
        return int(token[1])
    return 60
