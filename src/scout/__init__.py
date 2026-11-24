"""Impulse Scout live runtime (epic_074): frozen ``scout_s0c_r1`` artifact, detect -> score -> resolve.

Experimental, NO-GO (5300): not a trading signal and not part of the quant model. Public API: :func:`run`,
:func:`load_artifact`, :data:`OUTPUT_SCHEMA`; the artifact is built once by ``python -m scout.freeze``.
"""

from scout.runtime import (
    ARTIFACT_DIR,
    IMMUTABLE_COLUMNS,
    MODEL_ID,
    OUTPUT_COLUMNS,
    OUTPUT_SCHEMA,
    RULE_KEYS,
    STATUSES,
    ScoutArtifact,
    detect,
    empty_frame,
    load_artifact,
    resolve,
    run,
    score,
)

__all__ = ["ARTIFACT_DIR", "IMMUTABLE_COLUMNS", "MODEL_ID", "OUTPUT_COLUMNS", "OUTPUT_SCHEMA", "RULE_KEYS", "STATUSES",
           "ScoutArtifact", "detect", "empty_frame", "load_artifact", "resolve", "run", "score"]
