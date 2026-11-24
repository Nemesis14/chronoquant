"""Impulse Scout benchmark harness (epic_073 / t931; methodology: ``_doc_/methodology_doc/5300_impulse_scout.md``).

Typical use::

    from reports.builders import scout_bench as sb
    frame = sb.load_wave_events_v2()                      # SOL adapter; t930: sb.load_scout_events()
    res = sb.run_benchmark(frame, sb.REGISTRY, bars={"solusdt": minute_grid}, n_perm=1000)
    sb.persist(res)                                       # research."scout_leaderboard_v1"

Models are registered in :data:`REGISTRY` (see ``models.py``). Reports: ``python -m reports.builders.scout_bench.report``.
"""

from reports.builders.scout_bench import (  # noqa: E402  (t936: trade rules, universe inference)
    rules,
    universe,
)
from reports.builders.scout_bench.folds import Fold, FoldSet, make_folds
from reports.builders.scout_bench.frame import (
    CLASSES,
    LABEL_COLS,
    PRIMARY_CELL,
    REQUIRED_COLS,
    S0C_COLS,
    describe,
    from_scout_events,
    from_wave_events_v2,
    guard_no_holdout,
    load_scout_events,
    load_wave_events_v2,
    prepare_frame,
)
from reports.builders.scout_bench.metrics import (
    auc,
    calibration,
    fold_rank_select,
    logloss_3class,
    top_q,
    top_q_mask,
)
from reports.builders.scout_bench.models import (
    BASELINE_NAME,
    REGISTRY,
    ModelRegistry,
    ModelSpec,
    ScalarLogit,
    ScoutModel,
    baseline_spec,
    new_registry,
    three_class_from_binary,
)
from reports.builders.scout_bench.runner import (
    BenchResult,
    cross_val,
    go_no_go,
    persist,
    run_benchmark,
)
from reports.builders.scout_bench.stats import (
    day_bundles,
    day_clusters,
    holm,
    perm_p,
    permutation_source,
    spa_test,
)
from reports.builders.scout_bench.trades import (
    Costs,
    load_costs,
    simulate,
    simulate_event,
    summarise,
)

__all__ = [
    "BASELINE_NAME", "CLASSES", "LABEL_COLS", "PRIMARY_CELL", "REGISTRY", "REQUIRED_COLS", "S0C_COLS", "BenchResult",
    "Costs", "Fold", "FoldSet", "ModelRegistry", "ModelSpec", "ScalarLogit", "ScoutModel", "auc", "baseline_spec",
    "calibration", "cross_val", "fold_rank_select", "go_no_go", "day_bundles", "day_clusters", "describe", "from_scout_events", "from_wave_events_v2",
    "guard_no_holdout", "holm", "load_costs", "load_scout_events", "load_wave_events_v2", "logloss_3class", "make_folds",
    "new_registry", "perm_p", "rules", "universe", "permutation_source", "persist", "prepare_frame", "run_benchmark", "simulate",
    "simulate_event", "spa_test", "summarise", "three_class_from_binary", "top_q", "top_q_mask",
]
