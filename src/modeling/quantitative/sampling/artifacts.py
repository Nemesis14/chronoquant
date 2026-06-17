"""Sample artifact IO — write, load, and validate sample definition files.

Writes three JSON files per sample: metadata.json, folds.json, audit.json.
No pandas import — stdlib only (json, pathlib, datetime).
"""

import json
from datetime import UTC, datetime
from pathlib import Path

# %% Write


def write_sample_artifacts(
    sample_dir : Path,
    metadata   : dict,
    folds      : dict,
    audit      : dict,
) -> None:
    """Write metadata.json, folds.json, and audit.json to sample_dir.

    Injects generated_at (UTC ISO string) into the metadata before writing.
    Creates sample_dir if it does not exist.

    Args:
        sample_dir : Target directory (created if absent).
        metadata   : Sample metadata dict (sample_id, asset_id, parameters, source, …).
        folds      : Output of build_expanding_window_splits — {'folds': […], 'test': {…}}.
        audit      : Output of audit_feature_table.
    """
    sample_dir = Path(sample_dir)
    sample_dir.mkdir(parents=True, exist_ok=True)

    meta_out = {
        **metadata,
        "generated_at": datetime.now(UTC).isoformat(),
    }
    _write_json(sample_dir / "metadata.json", meta_out)
    _write_json(sample_dir / "folds.json",    folds)
    _write_json(sample_dir / "audit.json",    audit)


# %% Load


def load_sample_definition(sample_dir: str | Path) -> dict:
    """Load a sample definition from its artifact directory.

    Merges metadata.json and folds.json into one dict.  The result exposes
    sample["folds"], sample["test"], and sample["data"]["start"/"end"] which
    are the keys expected by lightgbm_model and lgbm_search.

    Args:
        sample_dir : Path to the directory produced by write_sample_artifacts.

    Returns:
        Merged dict suitable for validate_sample_definition and model training.

    Raises:
        FileNotFoundError: If metadata.json or folds.json are missing.
    """
    sample_dir = Path(sample_dir)
    metadata   = json.loads((sample_dir / "metadata.json").read_text(encoding="utf-8"))
    folds      = json.loads((sample_dir / "folds.json").read_text(encoding="utf-8"))
    return {
        **metadata,
        "folds": folds["folds"],
        "test" : folds["test"],
    }


# %% Validate


def validate_sample_definition(sample: dict) -> None:
    """Validate chronological order and non-overlap of folds and test range.

    Args:
        sample : Dict as returned by load_sample_definition.

    Raises:
        ValueError: If test range is inverted or any fold has an invalid/overlapping window.
    """
    def _dt(s: str) -> datetime:
        return datetime.fromisoformat(s)

    test_start = _dt(sample["test"]["start"])
    test_end   = _dt(sample["test"]["end"])

    if test_end <= test_start:
        raise ValueError("Test end must be after test start")

    for fold in sample["folds"]:
        ts = _dt(fold["train_start"])
        te = _dt(fold["train_end"])
        vs = _dt(fold["valid_start"])
        ve = _dt(fold["valid_end"])

        if not ts < te < vs < ve < test_start:
            raise ValueError(f"Invalid or overlapping fold: {fold}")


# %% Internal


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=4), encoding="utf-8")
