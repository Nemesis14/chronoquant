# ChronoQuant UI Redesign Plan

## Goal

Redesign the Streamlit dashboard into a Binance-like dark monitoring screen.
This plan is an instruction document for an AI coding agent that will implement
the UI changes.

Only the UI should be changed. Backend, trading execution, Binance order API
integration, database schema, and model/data-pipeline behavior are out of scope
unless explicitly called out as a future dependency.

## Scope

Primary files:

- `src/streamlit_app/main.py`
- `src/streamlit_app/components/charts.py`
- `src/streamlit_app/data.py` only for read-only UI queries or lightweight
  adapters.
- `src/streamlit_app/dashboard_logging.py` only if the log panel needs a better
  read format.

Rules:

- Keep the dashboard read-only.
- Keep sync orchestration in `sync_runner.py`.
- Keep chart rendering in `components/charts.py`.
- Load config through `src/utils.py`; do not read JSON config directly in UI
  business logic.
- Do not mix candidate model evaluation output into the live predictions table.

## Target Experience

The first screen should be the usable monitoring dashboard, not a landing page.

Layout:

- Use dark mode by default, visually close to Binance dark charts.
- Split the main body into a fixed 60/40 working layout:
  - left 60 percent: chart area.
  - right 40 percent: active trade and recent trade/order panel.
- Keep the log panel below the main chart/trade area as a full-width terminal
  style panel.
- Avoid the current full-body stretched chart layout.

Navigation:

- Keep BCH and SOL asset selection.
- The left sidebar must be asset-aware:
  - If SOL is selected, the sidebar sync button and status refer only to SOL.
  - If BCH is selected, the sidebar sync button and status refer only to BCH.
- Do not render both BCH and SOL sync panels at the same time.
- Remove the sidebar `Chart window` metric and the `4h` display.

## Chart Requirements

### Binance-Like Candlestick

Use Binance as the visual reference:

- dark chart background.
- muted grid lines.
- green rising candles and red falling candles.
- right-side price axis.
- crosshair/spike behavior retained where practical.
- pan/zoom behavior retained.

The chart currently looks strange because gaps appear between bars. Review this
before making visual-only changes.

Review checklist:

- Confirm the UI receives one OHLC row per expected interval for the selected
  asset.
- Confirm `open_time` is parsed as UTC-like chronological timestamps and sorted
  ascending before plotting.
- Confirm duplicate `open_time` rows are not being plotted.
- Confirm Plotly is not treating missing timestamps as wide categorical/time
  gaps in a way that makes candles look disconnected.
- Confirm the candlestick trace uses the actual OHLCV table columns:
  `open`, `high`, `low`, `close`.
- Confirm `close` of one bar and `open` of the next bar are expected to differ
  in market data; candle bodies do not physically need to touch. The real issue
  to fix is unexpected time gaps, dropped rows, wrong interval data, or wrong
  x-axis spacing.

Acceptance:

- The chart renders continuous expected 1m/asset interval candles for the loaded
  data.
- Any remaining gaps are explainable by missing source rows and should be
  visible in a diagnostic/log note, not silently hidden.

### Loaded Window And Initial Focus

The visible chart should initially focus on the most recent 2 hours.

However, more history should be loaded so the user can scroll/pan backward:

- Load about 8 hours of chart data.
- Set the initial x-axis range to the latest 2 hours.
- Keep the full 8 hours available through pan/scroll.
- Avoid a menu item for `Chart window`.

Implementation direction:

- Use a separate loaded lookback constant, for example
  `CHART_LOAD_LOOKBACK_HOURS = 8`.
- Use an initial visible window constant, for example
  `CHART_INITIAL_FOCUS_HOURS = 2`.
- In `prediction_price_figure`, set `xaxis.range` to latest timestamp minus 2
  hours through latest timestamp, while keeping all loaded rows in the trace.
- Preserve Plotly `uirevision` so sync refreshes do not unnecessarily reset the
  user's current view while interacting.

### Remove Top Metrics

The current top metric row should be simplified.

Remove:

- `Open time` label and value.
- `Close` label and value.

The symbol can stay if needed, but avoid a large metric strip above the chart.
The chart should be the primary visual surface.

## Trade And Order Panel

Create a right-side panel for active and recent trading state.

This is a UI plan, but some desired data comes from Binance and is not yet
implemented in the backend. The UI agent must not implement Binance order API
calls in this task. Instead, it should:

- use existing read-only local data if available;
- render empty states/placeholders when backend data is not available;
- clearly isolate future backend dependencies in TODO notes or plan comments.

### Active Trade

At the top of the right panel, show the currently active/open trade if one
exists.

Required fields:

- open time.
- entry price.
- side: long or short.
- stop loss, if set.
- take profit, if set.

Chart overlay:

- Show active trade levels on the price chart using Binance-like horizontal
  dashed lines.
- Entry line should be distinct from stop loss and take profit lines.
- Stop loss and take profit should be clearly labeled near the right price axis
  or via hover.
- If no active trade is available, do not draw fake lines.

Backend dependency:

- Live Binance open trade/order state is a backend task because Binance has the
  required API and it is not implemented yet.
- Until that exists, use existing `trading_positions` / `trading_orders` tables
  if present, otherwise show an empty active trade state.

### Recent Trades / Orders

Below the active trade, show a scrollable list of previous trades/orders from
the last 24 hours. Newest items should be at the top.

Required fields:

- open time.
- entry price.
- side: long or short.
- stop loss, if set.
- take profit, if set.
- close reason/status.
- closing amount.
- closing price.

Data source preference:

1. Existing local trading tables, if present.
2. Existing backtest/dry-run artifacts only as a clearly marked fallback.
3. Empty state until backend order sync is implemented.

Acceptance:

- Right panel stays usable at 40 percent width.
- Recent trade list is independently scrollable.
- Empty state does not break layout.
- The panel does not call Binance directly from Streamlit.

## Log Panel

The existing log panel concept is good, but it should look and behave like an
embedded terminal panel, not a separate window-like card.

Requirements:

- Render logs inside a dark panel below the main 60/40 area.
- Use monospace font.
- Keep chronological order with the oldest visible entry at the top and newest
  at the bottom.
- Show logger info lines as they are appended, similar to a terminal.
- Keep auto-refresh behavior.
- Keep `Clear log` if it remains useful, but style it consistently with the dark
  UI.

Avoid:

- iframe styling that looks like a detached browser window.
- newest-first reversing of log entries.
- large colored cards for each log line.

Acceptance:

- The panel reads like a compact terminal.
- Long messages wrap without overlapping neighboring UI.
- The oldest visible log line is at the top.

## Sidebar And Asset State

The current sidebar shows both BCH and SOL panels and both sync controls. This
should be changed.

Requirements:

- Asset selection controls the sidebar content.
- When the active asset is SOL:
  - show SOL status.
  - `Sync` starts SOL sync only.
  - `Stop live sync` stops SOL live sync only.
- When the active asset is BCH:
  - show BCH status.
  - `Sync` starts BCH sync only.
  - `Stop live sync` stops BCH live sync only.
- Preserve the existing per-asset sync state keys from `sync_runner.py`.
- Do not make sync state global.

Possible implementation:

- Replace current tabs with a single asset selector in sidebar or a top segmented
  control.
- Store active asset in `st.session_state["active_asset_id"]`.
- Render only `_render_sync_controls(active_asset_id)`.
- Render only the selected asset dashboard in the main body.

Acceptance:

- The inactive asset's sync panel is not visible.
- Clicking `Sync` on SOL cannot start BCH sync, and clicking `Sync` on BCH
  cannot start SOL sync.

## Visual Design

Use a restrained Binance-like dark theme:

- page background near `#0b0e11`.
- panel background near `#111418` or `#181a20`.
- borders near `#2b3139`.
- text near `#eaecef`.
- muted text near `#848e9c`.
- green candle/trade color near `#0ecb81`.
- red candle/trade color near `#f6465d`.
- Binance yellow can be used sparingly for active highlights.

Do not create a purple/blue-gradient dashboard.

UI details:

- Keep cards/panels compact with border radius 8px or less.
- Do not nest cards inside cards.
- Avoid explanatory marketing text.
- Ensure text fits at desktop and narrow widths.

## Implementation Tasks

1. Review current OHLC chart data flow.
   - Inspect `data.prediction_history`.
   - Inspect `prediction_price_figure`.
   - Verify ordering, duplicates, missing timestamps, and loaded interval.

2. Add chart loaded-window and initial-focus behavior.
   - Load 8 hours.
   - Initially focus latest 2 hours.
   - Preserve pan/scroll.

3. Convert chart style to dark Binance-like theme.
   - Update Plotly template/layout/colors.
   - Update candle colors and axes/grid styling.
   - Keep drawing/zoom config functional.

4. Rework main layout to 60/40.
   - Left: selected asset chart.
   - Right: active trade and recent trade/order panel.
   - Remove `Open time` and `Close` top metrics.

5. Make sidebar asset-aware.
   - Render one selected asset.
   - Render one sync panel for that selected asset.
   - Remove `Chart window`.

6. Add active trade overlay support.
   - Read active trade from existing local table if available.
   - Pass optional active trade levels into chart component.
   - Draw dashed entry/stop/take-profit lines only when data exists.

7. Add recent trade/order panel.
   - Prefer local tables.
   - Limit to last 24 hours.
   - Newest first.
   - Scrollable panel.
   - Empty state when unavailable.

8. Redesign log panel as a terminal-like panel.
   - Oldest visible line at top.
   - Newest visible line at bottom.
   - Dark styling.
   - Keep refresh and optional clear.

9. Verify with Streamlit.
   - Run `streamlit run src/streamlit_app/main.py`.
   - Check BCH and SOL selection.
   - Check sync button targets the selected asset.
   - Check chart pan/zoom and initial 2h focus.
   - Check no UI text overlaps.

## Out Of Scope

- Implementing Binance order API calls.
- Creating or changing live trading backend tables.
- Changing sync, feature, prediction, model, or strategy logic.
- Changing database schema.
- Adding manual trading buttons.
- Adding public deployment/authentication changes.

## Future Backend Dependencies

These are needed later for the full intended trade panel:

- Binance open orders / open positions sync.
- Binance recent fills/order history sync.
- Local normalized tables for active order state and recent fills.
- Stop loss and take profit extraction from Binance orders.
- Reconciliation between local strategy state and Binance account state.

The UI should be written so these future sources can be plugged into
`src/streamlit_app/data.py` without rewriting the layout.

## Acceptance Criteria

- Dashboard opens in dark mode.
- Main area uses a 60/40 chart/trade layout.
- Chart loads about 8 hours but initially focuses on the latest 2 hours.
- Candlestick gaps are reviewed and any data issue is identified.
- `Open time`, `Close`, and sidebar `Chart window` are removed.
- Sidebar sync controls are shown only for the selected asset.
- Active trade panel and chart lines render when data exists.
- Recent trades/orders panel is scrollable and newest-first.
- Log panel is terminal-like and chronological oldest-to-newest.
- No backend trading or Binance API implementation is added.
