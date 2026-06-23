# 8120 - ui/data.py

`src/ui/data.py`

A dashboard olvasási rétege. Egyetlen modulban fogja össze a konfigurációs,
DuckDB, trading journal és artifact alapú olvasásokat, hogy a Streamlit oldal
és a komponensek ne közvetlenül SQL-lel dolgozzanak.

> Módszertani háttér (adatréteg design, UI olvasási kontraktus):
> → [`../methodology_doc/8100_dashboard.md`](../methodology_doc/8100_dashboard.md)

---

## Overview

```mermaid
flowchart TD
  CFG["config/*.json"] --> DATA["ui/data.py"]
  DB["DuckDB predictions/ohlcv"] --> DATA
  TDB["trading.db"] --> DATA
  ART["artifacts/<session>/summary/trades/equity"] --> DATA
  DATA --> UI["main.py + components/*"]
```

---

## Config és strategy lookup

### `load_dashboard_config(asset_id=None)`

Összerakja a dashboard szempontjából fontos asset, model és strategy metadata-t.

Returns: `dict`

### `active_strategy(strategies_cfg=None, asset_id=None)`

Kiválasztja az assethez tartozó strategy rekordot.

Returns: `tuple[str | None, dict]`

### `load_long_short_strategies(asset_id=None)`

A current `strategy_artifact.json` decision paramétereiből long/short threshold párost ad vissza.

Returns: `tuple[dict, dict]`

## DB utility függvények

### `table_exists(...)`, `table_columns(...)`, `table_row_count(...)`, `latest_table_timestamp(...)`

Általános introspection segédek.

Returns:
- `table_exists`: `bool`
- `table_columns`: `list[str]`
- `table_row_count`: `int`
- `latest_table_timestamp`: `str | None`

## Prediction és chart adat

### `prediction_history(lookback_hours=24, asset_id=None)`

Predikció és OHLCV history összeillesztése chartoláshoz.

Returns: `pd.DataFrame`

### `latest_prediction(asset_id=None)`

Utolsó predikciós rekord JSON-safe dictként.

Returns: `dict | None`

## Trading és backtest adat

### `active_position(asset_id=None)`

Az utolsó nyitott pozíció trading DB-ből.

Returns: `dict | None`

### `closed_trades(limit=500, asset_id=None)`

Elsődlegesen `trading_positions` táblából, fallbackként `trades.parquet`-ből olvas.

Returns: `pd.DataFrame`

### `equity_curve(asset_id=None)`

Elsődlegesen DB snapshot táblából, fallbackként `equity_curve.parquet`-ből olvas.

Returns: `pd.DataFrame`

### `backtest_summary(asset_id=None)`

`summary.json` tartalmát adja vissza.

Returns: `dict`

### `recent_orders(limit=200, asset_id=None)`, `recent_errors(limit=100, asset_id=None)`

Trading journal táblák legutóbbi rekordjai.

Returns: `pd.DataFrame`

### `table_health(asset_id=None)`

Rövid health summary a konfigurált üzleti és trading táblákról.

Returns: `pd.DataFrame`

## Alacsony szintű helper réteg

### `_coerce_prediction_frame(df)`

Predikciós DataFrame tipizálása és normalizálása.

### `_resolve_long_short_pred_cols(...)`

Long/short predikció oszlopnevek feloldása.

### `_read_sql(...)`, `_scalar(...)`, `_db_path(...)`, `_session_artifact_path(...)`, `_repo_path(...)`, `_quote_identifier(...)`, `_json_safe(...)`

Közös SQL/path/JSON segédek.

---

## Kapcsolódó dokumentumok

- [`8110_ui_main.md`](8110_ui_main.md) — a fő orchestrációs oldal
- [`8150_ui_components.md`](8150_ui_components.md) — komponensek, amelyek ezt a réteget hívják
- [`7130_trading_journal.md`](7130_trading_journal.md) — journal read API
- [`../methodology_doc/8100_dashboard.md`](../methodology_doc/8100_dashboard.md) — dashboard módszertan
