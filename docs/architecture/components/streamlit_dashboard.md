# Component: Streamlit Dashboard

The Streamlit dashboard monitors data freshness, predictions, strategies,
backtest summaries, logs, and charts.

## Modules

| Module | Responsibility |
|---|---|
| `src/streamlit_app/main.py` | App entry point |
| `src/streamlit_app/data.py` | Dashboard data access |
| `src/streamlit_app/sync_runner.py` | Runtime sync orchestration |
| `src/streamlit_app/components/` | Chart and formatting components |

## Rule

The dashboard should read data and trigger approved sync flows. It should not
become the owner of modeling, schema, or strategy selection logic.

