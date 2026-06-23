"""Smoke tests for strategy.strategy.artifacts — strat.* writes + artifact JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import duckdb
import pandas as pd
import pytest

from data_handling.store import registry
from strategy.strategy import artifacts as strat_artifacts
from strategy.strategy.artifacts import (
    read_strategy_artifact,
    register_strategy,
    strat_table_fqn,
    write_realized_outputs,
    write_strategy_artifact,
)

pytestmark = pytest.mark.smoke


_DECISION_PARAMS = {
    "entry_cutoff"           : 0.95,
    "tp_spec"                : "bucket_mean_mfe",
    "sl_spec"                : "0.5x_tp",
    "directions"             : ["long", "short"],
    "max_hold_minutes"       : 60,
    "same_bar_conflict_rule" : "sl_first",
}

_SEARCH_INFO = {
    "search_type"        : "grid",
    "n_setups_evaluated" : 200,
    "best_objective"     : "total_fact_log_return",
    "best_value"         : 0.342,
}

_METRICS = {
    "n_trades"              : 120,
    "total_fact_log_return" : 0.342,
    "win_rate"              : 0.55,
    "compounded_return_pct" : 40.7,
    "avg_hold_minutes"      : 35.0,
    "sufficient_sample"     : True,
}


class _NoCloseConn:
    """Delegating proxy whose ``close`` is a no-op (keeps the lab conn alive)."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def close(self) -> None:
        pass

    def __getattr__(self, name: str):
        return getattr(self._conn, name)


def _lab_proxy(tmp_path: Path) -> _NoCloseConn:
    """In-memory lab connection with registry attached as ``reg``."""
    reg_path = str(tmp_path / "registry.duckdb")
    registry.ensure_registry(reg_path)
    conn = duckdb.connect(":memory:")
    registry.attach_registry(conn, reg_path, read_only=False)
    return _NoCloseConn(conn)


# ---------------------------------------------------------------------------
# strategy_artifact.json round-trip
# ---------------------------------------------------------------------------


def test_write_strategy_artifact_creates_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """write_strategy_artifact() writes a readable JSON file with strat table refs."""
    import utils

    monkeypatch.setattr(utils, "_resolve_path", lambda p: str(tmp_path / p) if p != "." else str(tmp_path))

    out_path = write_strategy_artifact(
        session_id      = "test_session",
        long_model_id   = "model_long",
        short_model_id  = "model_short",
        fit_period      = {"start": "2025-05-01", "end": "2026-05-31"},
        decision_params = _DECISION_PARAMS,
        metrics         = _METRICS,
        search_info     = _SEARCH_INFO,
    )

    assert out_path.exists(), "strategy_artifact.json was not created"

    with open(out_path, encoding="utf-8") as f:
        data = json.load(f)

    assert data["session_id"]         == "test_session"
    assert data["long_model"]         == "model_long"
    assert data["short_model"]        == "model_short"
    assert data["trades_table"]       == strat_table_fqn("test_session", "trades")
    assert data["equity_table"]       == strat_table_fqn("test_session", "equity")
    assert data["cutoffs_table"]      == strat_table_fqn("test_session", "cutoffs")
    assert data["summary_table"]      == strat_table_fqn("test_session", "summary")
    assert data["grid_results_table"] == strat_table_fqn("test_session", "grid_results")


def test_read_strategy_artifact_returns_required_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """read_strategy_artifact() returns dict with all required keys."""
    import utils

    monkeypatch.setattr(utils, "_resolve_path", lambda p: str(tmp_path / p) if p != "." else str(tmp_path))

    write_strategy_artifact(
        session_id      = "test_session",
        long_model_id   = "model_long",
        short_model_id  = "model_short",
        fit_period      = {"start": "2025-05-01", "end": "2026-05-31"},
        decision_params = _DECISION_PARAMS,
        metrics         = _METRICS,
        search_info     = _SEARCH_INFO,
    )

    result = read_strategy_artifact("test_session")

    required_keys = [
        "session_id", "long_model", "short_model",
        "signal_mode", "evaluation_mode",
        "fit_period",
        "rank_lookup_long_path", "rank_lookup_short_path",
        "isotonic_long_path", "isotonic_short_path",
        "decision_params",
        "search_info", "metrics", "calibrated_at",
        "summary_table", "grid_results_table",
    ]
    for key in required_keys:
        assert key in result, f"Missing required key in artifact: {key}"

    assert result["session_id"]      == "test_session"
    assert result["signal_mode"]     == "rank_first"
    assert result["evaluation_mode"] == "same_window"

    dp = result["decision_params"]
    assert dp["entry_cutoff"]           == 0.95
    assert dp["same_bar_conflict_rule"] == "sl_first"

    si = result["search_info"]
    assert si["search_type"]        == "grid"
    assert si["n_setups_evaluated"] == 200

    # optuna_best_trial must NOT be present in new artifact
    assert "optuna_best_trial" not in result


# ---------------------------------------------------------------------------
# write_realized_outputs -> strat.* tables
# ---------------------------------------------------------------------------


def _make_trade(i: int) -> dict:
    """Return a minimal trade dict for testing (new grid-search shape)."""
    return {
        "entry_time"         : pd.Timestamp(f"2026-01-{i+1:02d} 09:00:00"),
        "exit_time"          : pd.Timestamp(f"2026-01-{i+1:02d} 10:00:00"),
        "direction"          : "long" if i % 2 == 0 else "short",
        "entry_price"        : 100.0 + i,
        "exit_price"         : 101.0 + i,
        "score_pct_at_entry" : 0.90,
        "bucket_mean_mfe"    : 0.01 * (i + 1),
        "fact_log_return"    : 0.01 * (i + 1),
        "tp_lr"              : 0.02,
        "sl_lr"              : 0.01,
        "entry_cutoff"       : 0.90,
        "tp_spec"            : "bucket_mean_mfe",
        "sl_spec"            : "0.5x_tp",
        "n_bars"             : 60,
        "hold_minutes"       : 60,
        "exit_reason"        : "tp",
    }


def test_write_realized_outputs_empty_trades(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """write_realized_outputs() with empty list writes empty strat.* tables."""
    proxy = _lab_proxy(tmp_path)
    monkeypatch.setattr(strat_artifacts.utils, "open_lab_connection", lambda asset_id=None: proxy)

    tables = write_realized_outputs(session_id="empty_session", trades=[])

    assert set(tables.keys()) == {"trades", "equity", "cutoffs", "summary", "grid_results"}
    for kind in ("trades", "equity", "cutoffs", "summary", "grid_results"):
        fqn = strat_table_fqn("empty_session", kind)
        n   = proxy.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
        assert n == 0, f"{fqn} should be empty"


def test_write_realized_outputs_with_trades(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """write_realized_outputs() with 3 trades writes the trade + equity + summary tables."""
    proxy = _lab_proxy(tmp_path)
    monkeypatch.setattr(strat_artifacts.utils, "open_lab_connection", lambda asset_id=None: proxy)

    trades  = [_make_trade(i) for i in range(3)]
    cutoffs = [{
        "direction": "long", "bucket_id": 10,
        "score_raw_lower": 0.9, "score_raw_upper": 1.0,
        "score_pct_upper": 1.0, "bucket_mean_mfe": 0.012, "bucket_hit_rate": 0.7,
    }]
    best_setup = {"tp_spec": "bucket_mean_mfe", "sl_spec": "0.5x_tp"}

    write_realized_outputs(
        session_id = "full_session",
        trades     = trades,
        cutoffs    = cutoffs,
        best_setup = best_setup,
    )

    trades_df  = proxy.execute(f'SELECT * FROM {strat_table_fqn("full_session", "trades")} ORDER BY entry_time').df()
    equity_df  = proxy.execute(f'SELECT * FROM {strat_table_fqn("full_session", "equity")} ORDER BY trade_index').df()
    cutoffs_df = proxy.execute(f'SELECT * FROM {strat_table_fqn("full_session", "cutoffs")}').df()
    summary_df = proxy.execute(f'SELECT * FROM {strat_table_fqn("full_session", "summary")}').df()

    # trades table
    assert len(trades_df) == 3
    assert "fact_log_return" in trades_df.columns
    assert "tp_lr" in trades_df.columns
    assert "sl_lr" in trades_df.columns
    assert "entry_cutoff" in trades_df.columns
    assert "tp_spec" in trades_df.columns
    assert "sl_spec" in trades_df.columns
    assert trades_df["entry_price"].tolist() == [100.0, 101.0, 102.0]

    # equity table
    assert "cumulative_fact_log_return" in equity_df.columns
    assert len(equity_df) == 3
    assert equity_df["cumulative_fact_log_return"].iloc[-1] > equity_df["cumulative_fact_log_return"].iloc[0]

    # cutoffs table
    assert len(cutoffs_df) == 1
    assert cutoffs_df["direction"].iloc[0] == "long"

    # summary table
    assert len(summary_df) == 1
    assert "total_fact_log_return"        in summary_df.columns
    assert "realized_directional_win_rate" in summary_df.columns
    assert summary_df["take_profit_spec"].iloc[0] == "bucket_mean_mfe"
    assert summary_df["stop_loss_spec"].iloc[0]   == "0.5x_tp"


def test_write_realized_outputs_with_grid_results(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """write_realized_outputs() with grid_results writes the grid_results table."""
    proxy = _lab_proxy(tmp_path)
    monkeypatch.setattr(strat_artifacts.utils, "open_lab_connection", lambda asset_id=None: proxy)

    trades = [_make_trade(0)]
    grid_results = [
        {
            "direction"            : "long",
            "entry_cutoff"         : 0.90,
            "tp_spec"              : "bucket_mean_mfe",
            "sl_spec"              : "none",
            "n_trades"             : 5,
            "total_fact_log_return": 0.10,
            "avg_fact_log_return"  : 0.02,
            "compounded_return_pct": 10.5,
            "win_rate"             : 0.6,
            "avg_hold_minutes"     : 30.0,
        },
    ]

    write_realized_outputs(
        session_id   = "grid_session",
        trades       = trades,
        grid_results = grid_results,
    )

    grid_df = proxy.execute(f'SELECT * FROM {strat_table_fqn("grid_session", "grid_results")}').df()
    assert len(grid_df) == 1
    assert grid_df["direction"].iloc[0]     == "long"
    assert grid_df["entry_cutoff"].iloc[0]  == 0.90
    assert grid_df["tp_spec"].iloc[0]       == "bucket_mean_mfe"


def test_register_strategy_writes_reg_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """register_strategy() writes reg.strategies + reg.artifacts rows."""
    proxy = _lab_proxy(tmp_path)
    monkeypatch.setattr(strat_artifacts.utils, "open_lab_connection", lambda asset_id=None: proxy)

    # First create the strat tables so their artifact rows reference real tables.
    write_realized_outputs(session_id="reg_session", trades=[_make_trade(0)])

    artifact_file = tmp_path / "strategy_artifact.json"
    artifact_file.write_text("{}", encoding="utf-8")

    register_strategy(
        session_id     = "reg_session",
        long_model_id  = "lgbm_solusdt_l_fw60_2021",
        short_model_id = "lgbm_solusdt_s_fw60_2021",
        artifact_files = [("strategy_artifact", artifact_file)],
    )

    row = registry.get(cast(duckdb.DuckDBPyConnection, proxy), "strategies", "reg_session")
    assert row is not None
    assert row["model_id_long"]  == "lgbm_solusdt_l_fw60_2021"
    assert row["model_id_short"] == "lgbm_solusdt_s_fw60_2021"

    # strat table artifacts + file artifact registered
    art = registry.get(cast(duckdb.DuckDBPyConnection, proxy), "artifacts", "reg_session__strat_trades")
    assert art is not None
    assert art["path"] == strat_table_fqn("reg_session", "trades")
    file_art = registry.get(cast(duckdb.DuckDBPyConnection, proxy), "artifacts", "reg_session__strategy_artifact")
    assert file_art is not None

    # New tables also registered
    summary_art = registry.get(cast(duckdb.DuckDBPyConnection, proxy), "artifacts", "reg_session__strat_summary")
    assert summary_art is not None
    grid_art = registry.get(cast(duckdb.DuckDBPyConnection, proxy), "artifacts", "reg_session__strat_grid_results")
    assert grid_art is not None
