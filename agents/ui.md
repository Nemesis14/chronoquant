# UI Agent

## Responsibility

Owns Streamlit dashboard UX, chart components, dashboard data display, and
read-only monitoring flows.

## Must Read

- `docs/architecture/overview.md`
- `docs/engineering/code_style.md`
## Primary Scope

- `src/streamlit_app/`

## Rules

- Keep the dashboard read-only unless an active plan explicitly adds controls.
- Keep sync state handling in `sync_runner.py`.
- Keep chart rendering in `components/charts.py`.

## Development Concept

UI work should integrate with the existing Streamlit dashboard instead of
creating separate app flows:

1. Read data through the existing dashboard data layer where possible.
2. Keep sync orchestration separate from rendering.
3. Keep charts in component modules.
4. Make state changes explicit in `st.session_state`.
5. Preserve the monitoring-first workflow unless an active plan introduces
   controls.

Dashboard changes should stay connected to the model and data-pipeline
architecture: display configured runtime model state, prediction history, sync
status, logs, and evaluation outputs without mixing candidate artifacts into the
live table.
