---
epic: epic_027
id: t122
title: Keszits realized strategy backtest outputokat
assignee: modeling_agent
status: done
blocks: [t123, t124]
blocked_by: [t119]
---

## Goal

A strategy session ne csak optimalizált paramétereket és proxy metrikákat mentsen,
hanem olyan realized backtest outputokat is, amelyekből közvetlenül összefoglalható
egy futás eredménye.

## Scope

- `src/strategy/strategy/optimize.py`
- strategy artifact output struktúra
- trade ledger és summary persistence

## Acceptance Criteria

- [x] A strategy session írjon ki trade-szintű outputot legalább ezekkel:
  `entry_time`, `exit_time`, `direction`, `entry_price`, `exit_price`, `hold_minutes`, `exit_reason`
- [x] A strategy session írjon ki összegző outputot legalább ezekkel:
  `initial_capital`, `final_equity`, `n_trades`, `win_rate`, `gross_return`, `net_return`
- [x] A summary explicit jelezze, ha a PnL vagy equity még proxy-alapú vagy közelítő
- [x] Az output a strategy artifact mappában éljen, hogy analyst onnan tudjon dolgozni

## Notes

Minimum javasolt outputok:

- `trades.parquet` vagy `trades.csv`
- `equity_curve.parquet` vagy `equity_curve.csv`
- `summary.json`

Az output legyen ugyanazon strategy session artifact része, mint a `strategy_table.parquet`.

---

### Implementation (modeling_agent, 2026-06-20)

**`src/strategy/strategy/optimize.py`**
- Import `write_realized_outputs` mellé az existing `write_strategy_artifact` importhoz adva.
- `_simulate_strategy()`: Mind a 6 trade-append helyen (3 × IN_LONG, 3 × IN_SHORT) hozzáadva:
  - `"hold_minutes"`: ugyanaz az `elapsed` int érték mint `n_bars`
  - `"exit_reason"`: `"max_hold"` | `"opposite_edge"` | `"signal_decay"` az ág szerint
- `optimize_strategy()`: `metrics = _compute_metrics(best_trades)` után hívja `write_realized_outputs(artifact_dir, best_trades)`.

**`src/strategy/strategy/artifacts.py`**
- Hozzáadva `pandas` és `typing.Any` import.
- Új `write_realized_outputs(artifact_dir: Path, trades: list[dict]) -> None` függvény:
  - `trades.parquet`: összes szükséges oszlop, `entry_price`/`exit_price` null float64.
  - `equity_curve.parquet`: `trade_index`, `entry_time`, `bucket_mean_mfe`, `cumulative_mfe`.
  - `summary.json`: `initial_capital`, `final_equity`, `n_trades`, `win_rate`, `gross_return`,
    `net_return`, `equity_basis: "mfe_proxy"`, `note` (proxy figyelmeztetés). Üres trades esetén
    is kiírja a `note` mezőt.
- `write_strategy_artifact()` artifact dictbe hozzáadva: `trades_path`, `equity_curve_path`, `summary_path`.

**Döntések:**
- `entry_price`/`exit_price` null (float64 NaN) — strategy_table nem tartalmaz OHLCV árat.
- `net_return == gross_return` — fees nincsenek modellezve.
- `write_realized_outputs` nem hív `utils._resolve_path`-t — a caller már feloldott `Path`-ot ad át,
  így a meglévő test mock (`strategy.strategy.artifacts.utils._resolve_path`) nem érintett.
- Meglévő teszt (`test_simulate_strategy_trade_keys`) subset-check, az új kulcsok (`hold_minutes`,
  `exit_reason`) nem törik meg.

---

### Validation (validator_agent, 2026-06-20)

- ruff check src\strategy\: all checks passed, no fixes needed
- pyright src\strategy\: 0 errors, 0 warnings, 0 informations
- pytest src\strategy\tests\ (11 tests, 8 original + 3 new): all passed
  - New tests added: `test_write_realized_outputs_empty_trades`, `test_write_realized_outputs_with_trades`,
    `test_simulate_strategy_exit_reason_values`
  - `test_simulate_strategy_trade_keys` updated to assert `hold_minutes` and `exit_reason` keys
- All acceptance criteria verified by code inspection and test coverage

[validator] done — 2026-06-20
