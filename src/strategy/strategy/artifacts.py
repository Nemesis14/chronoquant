"""Strategy artifact read/write for ChronoQuant strategy sessions.

Two persistence surfaces (plan 6):
  * **DuckDB tables** ``strat."<session_id>__trades / __equity / __cutoffs /
    __summary / __grid_results"`` in the lab database (``strat`` schema) — the
    realized backtest outputs the UI queries directly.  Written via
    :func:`write_realized_outputs`.
  * **Files** ``strategy_artifact.json`` (+ isotonic/rank_lookup, written by the
    calibrate/search steps) under ``artifacts/{session_id}/`` — the live service
    loads these.  Written via :func:`write_strategy_artifact`.

The registry (``reg.strategies`` + ``reg.artifacts``) links the session to its
two models and to both the strat tables and the file artifacts.

Callers reach the lab/registry topology through ``utils.open_lab_connection``
(config-gateway), never by opening DuckDB files directly.
"""

import json
import logging
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

import utils
from data_handling.store import registry

logger = logging.getLogger(__name__)

# strat schema (lab database) holds the per-session realized backtest tables.
STRAT_SCHEMA = "strat"

# %%  Path helper


def _artifact_dir(session_id: str) -> Path:
    """Return the absolute artifact directory path for a session.

    Args:
        session_id: Strategy session identifier (e.g. 'strategy_2101_2605_202605').

    Returns:
        Path to artifacts/{session_id}/.
    """
    repo_root = Path(utils._resolve_path("."))
    return repo_root / "artifacts" / session_id


# %% strat.* table names


def strat_table_fqn(session_id: str, kind: str) -> str:
    """Return the fully-qualified, quoted strat table name (plan 6).

    Args:
        session_id : Strategy session identifier.
        kind       : One of ``trades`` / ``equity`` / ``cutoffs`` /
                     ``summary`` / ``grid_results``.

    Returns:
        e.g. ``strat."<session_id>__trades"``.
    """
    return f'{STRAT_SCHEMA}."{session_id}__{kind}"'


# %% Realized backtest outputs (strat.* DuckDB tables)


def _trades_dataframe(trades: list[dict[str, Any]]) -> pd.DataFrame:
    """Build the trades ledger DataFrame (typed, empty-safe).

    Supports both the legacy dict shape (bucket_mean_mfe, no fact_log_return)
    and the new grid-search shape (fact_log_return, tp_lr, sl_lr, etc.).
    """
    n = len(trades)
    if n > 0:
        return pd.DataFrame({
            "entry_time"         : pd.to_datetime([t["entry_time"]  for t in trades]),
            "exit_time"          : pd.to_datetime([t["exit_time"]   for t in trades]),
            "direction"          : [t["direction"]                   for t in trades],
            "entry_price"        : pd.Series([t.get("entry_price")   for t in trades], dtype="float64"),
            "exit_price"         : pd.Series([t.get("exit_price")    for t in trades], dtype="float64"),
            "hold_minutes"       : [int(t["hold_minutes"])            for t in trades],
            "exit_reason"        : [t["exit_reason"]                  for t in trades],
            "score_pct_at_entry" : [float(t["score_pct_at_entry"])    for t in trades],
            "bucket_mean_mfe"    : [float(t["bucket_mean_mfe"])       for t in trades],
            "fact_log_return"    : pd.Series(
                [float(t["fact_log_return"]) if "fact_log_return" in t else float("nan")
                 for t in trades], dtype="float64",
            ),
            "tp_lr"              : pd.Series(
                [float(t["tp_lr"]) if "tp_lr" in t else float("nan") for t in trades],
                dtype="float64",
            ),
            "sl_lr"              : pd.Series(
                [float(t["sl_lr"]) if "sl_lr" in t else float("nan") for t in trades],
                dtype="float64",
            ),
            "entry_cutoff"       : pd.Series(
                [float(t["entry_cutoff"]) if "entry_cutoff" in t else float("nan")
                 for t in trades], dtype="float64",
            ),
            "tp_spec"            : [t.get("tp_spec", "")             for t in trades],
            "sl_spec"            : [t.get("sl_spec", "")             for t in trades],
        })
    return pd.DataFrame({
        "entry_time"         : pd.Series([], dtype="datetime64[ns]"),
        "exit_time"          : pd.Series([], dtype="datetime64[ns]"),
        "direction"          : pd.Series([], dtype="object"),
        "entry_price"        : pd.Series([], dtype="float64"),
        "exit_price"         : pd.Series([], dtype="float64"),
        "hold_minutes"       : pd.Series([], dtype="int64"),
        "exit_reason"        : pd.Series([], dtype="object"),
        "score_pct_at_entry" : pd.Series([], dtype="float64"),
        "bucket_mean_mfe"    : pd.Series([], dtype="float64"),
        "fact_log_return"    : pd.Series([], dtype="float64"),
        "tp_lr"              : pd.Series([], dtype="float64"),
        "sl_lr"              : pd.Series([], dtype="float64"),
        "entry_cutoff"       : pd.Series([], dtype="float64"),
        "tp_spec"            : pd.Series([], dtype="object"),
        "sl_spec"            : pd.Series([], dtype="object"),
    })


def _equity_dataframe(trades: list[dict[str, Any]]) -> pd.DataFrame:
    """Build the cumulative equity curve DataFrame (typed, empty-safe).

    Tracks both cumulative_fact_log_return (primary) and cumulative_mfe
    (legacy, for backward compatibility).
    """
    n = len(trades)
    if n > 0:
        fact_lrs    = [
            float(t["fact_log_return"]) if "fact_log_return" in t
            else float(t.get("bucket_mean_mfe", 0.0))
            for t in trades
        ]
        mfes        = [float(t["bucket_mean_mfe"]) for t in trades]
        cum_flr     = []
        cum_mfe     = []
        running_flr = 0.0
        running_mfe = 0.0
        for flr, mfe in zip(fact_lrs, mfes, strict=True):
            running_flr += flr
            running_mfe += mfe
            cum_flr.append(running_flr)
            cum_mfe.append(running_mfe)
        return pd.DataFrame({
            "trade_index"               : list(range(n)),
            "entry_time"                : pd.to_datetime([t["entry_time"] for t in trades]),
            "bucket_mean_mfe"           : mfes,
            "cumulative_mfe"            : cum_mfe,
            "fact_log_return"           : fact_lrs,
            "cumulative_fact_log_return": cum_flr,
        })
    return pd.DataFrame({
        "trade_index"               : pd.Series([], dtype="int64"),
        "entry_time"                : pd.Series([], dtype="datetime64[ns]"),
        "bucket_mean_mfe"           : pd.Series([], dtype="float64"),
        "cumulative_mfe"            : pd.Series([], dtype="float64"),
        "fact_log_return"           : pd.Series([], dtype="float64"),
        "cumulative_fact_log_return": pd.Series([], dtype="float64"),
    })


def _summary_dataframe(
    trades     : list[dict[str, Any]],
    best_setup : dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Aggregate all trades into a 1-row summary DataFrame (empty-safe).

    Args:
        trades     : Trade dicts from _simulate_direction.
        best_setup : The winning grid setup dict (carries tp_spec / sl_spec).

    Returns:
        Single-row DataFrame with aggregate metrics, or typed empty DataFrame.
    """
    n = len(trades)
    if n > 0:
        fact_lrs   = [float(t["fact_log_return"]) if "fact_log_return" in t else 0.0
                      for t in trades]
        mfes       = [float(t["bucket_mean_mfe"]) for t in trades]
        total_flr  = sum(fact_lrs)
        return pd.DataFrame({
            "n_trades"                    : [n],
            "avg_entry_price"             : [float(sum(t.get("entry_price") or 0.0 for t in trades) / n)],
            "avg_exit_price"              : [float(sum(t.get("exit_price")  or 0.0 for t in trades) / n)],
            "avg_profit_pct"              : [float(sum((math.exp(f) - 1) * 100 for f in fact_lrs) / n)],
            "avg_expected_log_return"     : [float(sum(mfes) / n)],
            "avg_fact_log_return"         : [float(total_flr / n)],
            "total_fact_log_return"       : [float(total_flr)],
            "compounded_return_pct"       : [float((math.exp(total_flr) - 1) * 100)],
            "realized_directional_win_rate": [float(sum(1 for f in fact_lrs if f > 0) / n)],
            "avg_hold_minutes"            : [float(sum(t["hold_minutes"] for t in trades) / n)],
            "take_profit_spec"            : [best_setup.get("tp_spec", "") if best_setup else ""],
            "stop_loss_spec"              : [best_setup.get("sl_spec", "") if best_setup else ""],
        })
    return pd.DataFrame({
        "n_trades"                    : pd.Series([], dtype="int64"),
        "avg_entry_price"             : pd.Series([], dtype="float64"),
        "avg_exit_price"              : pd.Series([], dtype="float64"),
        "avg_profit_pct"              : pd.Series([], dtype="float64"),
        "avg_expected_log_return"     : pd.Series([], dtype="float64"),
        "avg_fact_log_return"         : pd.Series([], dtype="float64"),
        "total_fact_log_return"       : pd.Series([], dtype="float64"),
        "compounded_return_pct"       : pd.Series([], dtype="float64"),
        "realized_directional_win_rate": pd.Series([], dtype="float64"),
        "avg_hold_minutes"            : pd.Series([], dtype="float64"),
        "take_profit_spec"            : pd.Series([], dtype="object"),
        "stop_loss_spec"              : pd.Series([], dtype="object"),
    })


def _grid_results_dataframe(grid_results: list[dict[str, Any]] | None) -> pd.DataFrame:
    """Build the grid-search results DataFrame (typed, empty-safe).

    Args:
        grid_results: All setup result dicts from the grid search.

    Returns:
        DataFrame with one row per (direction, entry_cutoff, tp_spec, sl_spec).
    """
    if grid_results:
        return pd.DataFrame({
            "direction"             : [r["direction"]             for r in grid_results],
            "entry_cutoff"          : pd.Series([r["entry_cutoff"] for r in grid_results], dtype="float64"),
            "tp_spec"               : [r["tp_spec"]               for r in grid_results],
            "sl_spec"               : [r["sl_spec"]               for r in grid_results],
            "n_trades"              : pd.Series([r["n_trades"]     for r in grid_results], dtype="int64"),
            "total_fact_log_return" : pd.Series([r["total_fact_log_return"] for r in grid_results], dtype="float64"),
            "avg_fact_log_return"   : pd.Series([r["avg_fact_log_return"]   for r in grid_results], dtype="float64"),
            "compounded_return_pct" : pd.Series([r["compounded_return_pct"] for r in grid_results], dtype="float64"),
            "win_rate"              : pd.Series([r["win_rate"]              for r in grid_results], dtype="float64"),
            "avg_hold_minutes"      : pd.Series([r["avg_hold_minutes"]      for r in grid_results], dtype="float64"),
        })
    return pd.DataFrame({
        "direction"             : pd.Series([], dtype="object"),
        "entry_cutoff"          : pd.Series([], dtype="float64"),
        "tp_spec"               : pd.Series([], dtype="object"),
        "sl_spec"               : pd.Series([], dtype="object"),
        "n_trades"              : pd.Series([], dtype="int64"),
        "total_fact_log_return" : pd.Series([], dtype="float64"),
        "avg_fact_log_return"   : pd.Series([], dtype="float64"),
        "compounded_return_pct" : pd.Series([], dtype="float64"),
        "win_rate"              : pd.Series([], dtype="float64"),
        "avg_hold_minutes"      : pd.Series([], dtype="float64"),
    })


def _cutoffs_dataframe(cutoffs: list[dict[str, Any]] | None) -> pd.DataFrame:
    """Build the decile-cutoff DataFrame (per-direction bucket stats), empty-safe.

    Each row is one (direction, bucket_id) decile boundary with its lower/upper
    raw-score cut and the bucket's realized stats — the table the UI reads to
    render the score-to-edge mapping.
    """
    if cutoffs:
        return pd.DataFrame({
            "direction"       : [c["direction"]              for c in cutoffs],
            "bucket_id"       : [int(c["bucket_id"])         for c in cutoffs],
            "score_raw_lower" : pd.Series([c.get("score_raw_lower") for c in cutoffs], dtype="float64"),
            "score_raw_upper" : pd.Series([c.get("score_raw_upper") for c in cutoffs], dtype="float64"),
            "score_pct_upper" : pd.Series([c.get("score_pct_upper") for c in cutoffs], dtype="float64"),
            "bucket_mean_mfe" : pd.Series([c.get("bucket_mean_mfe") for c in cutoffs], dtype="float64"),
            "bucket_hit_rate" : pd.Series([c.get("bucket_hit_rate") for c in cutoffs], dtype="float64"),
        })
    return pd.DataFrame({
        "direction"      : pd.Series([], dtype="object"),
        "bucket_id"      : pd.Series([], dtype="int64"),
        "score_raw_lower": pd.Series([], dtype="float64"),
        "score_raw_upper": pd.Series([], dtype="float64"),
        "score_pct_upper": pd.Series([], dtype="float64"),
        "bucket_mean_mfe": pd.Series([], dtype="float64"),
        "bucket_hit_rate": pd.Series([], dtype="float64"),
    })


def _write_strat_table(
    conn       : duckdb.DuckDBPyConnection,
    session_id : str,
    kind       : str,
    frame      : pd.DataFrame,
) -> int:
    """CREATE OR REPLACE one ``strat."<session>__<kind>"`` table from a DataFrame.

    Idempotent: a re-run replaces the table in place. Returns the row count.
    """
    fqn = strat_table_fqn(session_id, kind)
    conn.register("_strat_df", frame)
    try:
        conn.execute(f"CREATE OR REPLACE TABLE {fqn} AS SELECT * FROM _strat_df")
    finally:
        conn.unregister("_strat_df")
    row = conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()
    return int(row[0]) if row else 0


def write_realized_outputs(
    session_id   : str,
    trades       : list[dict[str, Any]],
    grid_results : list[dict[str, Any]] | None = None,
    cutoffs      : list[dict[str, Any]] | None = None,
    asset_id     : str | None = None,
    best_setup   : dict[str, Any] | None = None,
) -> dict[str, str]:
    """Write the realized backtest outputs as ``strat.*`` DuckDB tables (plan 6).

    Creates five tables in the lab database's ``strat`` schema:
      * ``strat."<session>__trades"``       — trade ledger,
      * ``strat."<session>__equity"``       — cumulative equity curve per trade,
      * ``strat."<session>__cutoffs"``      — per-direction decile cutoffs,
      * ``strat."<session>__summary"``      — aggregate 1-row summary,
      * ``strat."<session>__grid_results"`` — all grid search setups.

    Args:
        session_id   : Strategy session identifier (table-name token).
        trades       : Trade dicts from _simulate_direction().
        grid_results : Optional list of all grid setup result dicts.
        cutoffs      : Optional decile-cutoff rows (see _cutoffs_dataframe).
        asset_id     : Asset key for the lab connection; resolved if None.
        best_setup   : The winning grid setup dict (for summary tp_spec / sl_spec).

    Returns:
        Mapping of table kind -> fqn for all written tables.
    """
    trades_df       = _trades_dataframe(trades)
    equity_df       = _equity_dataframe(trades)
    cutoffs_df      = _cutoffs_dataframe(cutoffs)
    summary_df      = _summary_dataframe(trades, best_setup)
    grid_results_df = _grid_results_dataframe(grid_results)

    conn = utils.open_lab_connection(asset_id)
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {STRAT_SCHEMA}")
        n_trades  = _write_strat_table(conn, session_id, "trades",       trades_df)
        _write_strat_table(conn, session_id, "equity",       equity_df)
        n_cutoffs = _write_strat_table(conn, session_id, "cutoffs",      cutoffs_df)
        _write_strat_table(conn, session_id, "summary",      summary_df)
        _write_strat_table(conn, session_id, "grid_results", grid_results_df)
    finally:
        conn.close()

    tables = {
        "trades"       : strat_table_fqn(session_id, "trades"),
        "equity"       : strat_table_fqn(session_id, "equity"),
        "cutoffs"      : strat_table_fqn(session_id, "cutoffs"),
        "summary"      : strat_table_fqn(session_id, "summary"),
        "grid_results" : strat_table_fqn(session_id, "grid_results"),
    }
    logger.info(
        "write_realized_outputs: %s trades=%d cutoffs=%d -> strat.*",
        session_id, n_trades, n_cutoffs,
    )
    return tables


def register_strategy(
    session_id     : str,
    long_model_id  : str,
    short_model_id : str,
    artifact_files : list[tuple[str, Path | str]] | None = None,
    asset_id       : str | None = None,
    status         : str = "candidate",
) -> str:
    """Register the strategy session in ``reg.strategies`` + ``reg.artifacts``.

    Upserts a ``reg.strategies`` row linking the session to its long/short models,
    and registers both the strat tables and the file artifacts in ``reg.artifacts``
    (only existing files are recorded; strat tables are recorded by table name).

    Args:
        session_id     : Strategy session identifier (the ``strategy_id``).
        long_model_id  : Long-direction model id.
        short_model_id : Short-direction model id.
        artifact_files : Optional (kind, path) file artifacts to register
                         (strategy_artifact.json, isotonic_*, rank_lookup_*).
        asset_id       : Asset key for the lab connection; resolved if None.
        status         : Lifecycle status for the strategy row (default candidate).

    Returns:
        The strategy_id written.
    """
    conn = utils.open_lab_connection(asset_id)
    try:
        registry.upsert(
            conn,
            "strategies",
            {
                "strategy_id":    session_id,
                "model_id_long":  long_model_id,
                "model_id_short": short_model_id,
                "session_id":     session_id,
                "status":         status,
            },
        )

        # strat.* tables as artifacts (table names, not files).
        for kind in ("trades", "equity", "cutoffs", "summary", "grid_results"):
            registry.upsert(
                conn,
                "artifacts",
                {
                    "artifact_id": f"{session_id}__strat_{kind}",
                    "owner_id":    session_id,
                    "kind":        f"strat_{kind}",
                    "path":        strat_table_fqn(session_id, kind),
                    "status":      "candidate",
                },
            )

        # File artifacts (only existing files).
        for kind, path in artifact_files or []:
            if Path(path).exists():
                registry.upsert(
                    conn,
                    "artifacts",
                    {
                        "artifact_id": f"{session_id}__{kind}",
                        "owner_id":    session_id,
                        "kind":        kind,
                        "path":        str(path),
                        "status":      "candidate",
                    },
                )
    finally:
        conn.close()

    logger.info(
        "register_strategy: %s long=%s short=%s status=%s",
        session_id, long_model_id, short_model_id, status,
    )
    return session_id


# %% Write / Read


def write_strategy_artifact(
    session_id      : str,
    long_model_id   : str,
    short_model_id  : str,
    fit_period      : dict,     # {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}
    decision_params : dict,
    metrics         : dict,
    search_info     : dict,
) -> Path:
    """Write strategy_artifact.json to artifacts/{session_id}/.

    Args:
        session_id     : Strategy session identifier.
        long_model_id  : Model ID for the long direction.
        short_model_id : Model ID for the short direction.
        fit_period     : Dict with 'start' and 'end' date strings.
        decision_params: Dict with grid-search decision parameters:
                         entry_cutoff, tp_spec, sl_spec, directions,
                         max_hold_minutes, same_bar_conflict_rule.
        metrics        : Performance metrics dict for the best setup.
        search_info    : Dict describing the search:
                         search_type, n_setups_evaluated,
                         best_objective, best_value.

    Returns:
        Path to the written strategy_artifact.json file.
    """
    artifact = {
        "session_id"             : session_id,
        "long_model"             : long_model_id,
        "short_model"            : short_model_id,
        "signal_mode"            : "rank_first",
        "evaluation_mode"        : "same_window",
        "fit_period"             : fit_period,
        "rank_lookup_long_path"  : "rank_lookup_long.parquet",
        "rank_lookup_short_path" : "rank_lookup_short.parquet",
        "isotonic_long_path"     : "isotonic_long.pkl",
        "isotonic_short_path"    : "isotonic_short.pkl",
        "decision_params"        : decision_params,
        "search_info"            : search_info,
        "metrics"                : metrics,
        "trades_table"           : strat_table_fqn(session_id, "trades"),
        "equity_table"           : strat_table_fqn(session_id, "equity"),
        "cutoffs_table"          : strat_table_fqn(session_id, "cutoffs"),
        "summary_table"          : strat_table_fqn(session_id, "summary"),
        "grid_results_table"     : strat_table_fqn(session_id, "grid_results"),
        "calibrated_at"          : datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    out_dir = _artifact_dir(session_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "strategy_artifact.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2, ensure_ascii=False)
        f.write("\n")

    logger.info("strategy_artifact.json written: %s", out_path)
    return out_path


def read_strategy_artifact(session_id: str) -> dict:
    """Read strategy_artifact.json from artifacts/{session_id}/.

    Args:
        session_id: Strategy session identifier.

    Returns:
        Parsed artifact dict. Keys follow the grid-search contract:
        session_id, long_model, short_model, signal_mode, evaluation_mode,
        fit_period, rank_lookup_long_path, rank_lookup_short_path,
        isotonic_long_path, isotonic_short_path, decision_params,
        search_info, metrics, trades_table, equity_table, cutoffs_table,
        summary_table, grid_results_table, calibrated_at.

    Raises:
        FileNotFoundError: If the artifact file does not exist.
    """
    path = _artifact_dir(session_id) / "strategy_artifact.json"
    if not path.exists():
        raise FileNotFoundError(f"strategy_artifact.json not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)
