# Component: Live Trading

The trading component converts runtime predictions and strategy config into
orders, positions, and audit records.

## Modules

| Module | Responsibility |
|---|---|
| `src/trading/strategy.py` | Decision logic |
| `src/trading/state.py` | Runtime state |
| `src/trading/service.py` | Trading orchestration |
| `src/trading/exchange.py` | Exchange adapter |
| `src/trading/journal.py` | Runtime persistence/audit |

## Data

- Operational state lives in `database/trading.db`.
- Run summaries/signals may be exported under `trading_reports/<run_id>/`.
- Strategy parameters come from `config/strategies.json` and runtime config.

