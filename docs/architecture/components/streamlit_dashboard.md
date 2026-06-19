# Component: Streamlit Dashboard

The Streamlit dashboard monitors data freshness, predictions, strategies,
backtest summaries, logs, and charts.

## Modules

| Module | Responsibility |
|---|---|
| `src/ui/main.py` | App entry point |
| `src/ui/data.py` | Dashboard data access |
| `src/ui/sync_runner.py` | Runtime sync orchestration |
| `src/ui/components/` | Chart and formatting components |

## Rule

The dashboard should read data and trigger approved sync flows. It should not
become the owner of modeling, schema, or strategy selection logic.
