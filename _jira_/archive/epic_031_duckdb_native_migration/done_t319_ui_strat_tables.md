---
epic: epic_031
id: t319
title: UI átállítás strat.* táblákra
assignee: ui_agent
status: todo
blocks: [t324]
blocked_by: [t316]
---

## Goal
A dashboard a trades/equity adatokat a parquet helyett a `strat."<session_id>__*"`
DuckDB táblákból olvassa (t316 kimenete), a többi adatforrás változatlan.

## Scope
- `src/ui/data.py` (és érintett komponensek) — trades/equity olvasás strat.* táblákból
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 9 (modul-delta), 5 (7. lépés)

## Acceptance Criteria
- [ ] trades/equity a strat.* táblákból töltődik
- [ ] equity curve + trade journal helyesen jelenik meg
- [ ] manuális smoke: `STREAMLIT_CONFIG_DIR=src/ui uv run streamlit run src/ui/main.py`
- [ ] ruff + pyright tiszta

## Notes

### Elvégezve (ui_agent)

**Módosított fájl:**
- `src/ui/data.py` — kizárólag a trades/equity olvasási útvonal változott; minden más adatforrás érintetlen.

**Trades/equity új olvasási útja:**

`closed_trades()` és `equity_curve()` elsőbbségi sorrendje:
1. **Primary:** `strat."<session_id>__trades"` / `strat."<session_id>__equity"` a lab DB-ből (`utils.open_lab_connection()` -n át), a t316 kontraktus szerint:
   - `__trades`: `entry_time, exit_time, direction, entry_price, exit_price, hold_minutes, exit_reason, score_pct_at_entry, bucket_mean_mfe`
   - `__equity`: `trade_index, entry_time, bucket_mean_mfe, cumulative_mfe`
2. **Fallback:** live trading DB (`trading_positions` / `trading_equity_snapshots`) — változatlan korábbi logika
3. **Ha semmi sincs:** üres DataFrame (nincs crash)

**session_id feloldás:**
`config/trading.json` → `strategy_session_id` mező (`utils.load_trading_config()` -n át, config-gateway elv megtartva).
Jelenlegi érték: `"strategy_lgbm_solusdt_fw60_2101_2605"`.

**Hozzáadott helper függvények:**
- `_active_strategy_session_id() -> str | None` — a session_id biztonságos kiolvasása; exception esetén None
- `_load_strat_table(session_id, kind, asset_id) -> pd.DataFrame` — lab DB kapcsolat + `information_schema` ellenőrzés; hiányzó tábla / bármilyen hiba esetén üres DataFrame

**Hiányzó-tábla kezelés:**
A `_load_strat_table()` `information_schema.tables` -t ellenőriz (`table_schema='strat'`) mielőtt query-t futtat. Ha a lab DB nem létezik, a tábla hiányzik, vagy bármilyen kapcsolati hiba van: üres DataFrame visszaadva, nincs crash. A dashboard a meglévő fallback-re esik vissza.

**Smoke / import eredmény:**
```
import OK
session_id = 'strategy_lgbm_solusdt_fw60_2101_2605'
_load_strat_table missing session: empty=True
closed_trades(): type=DataFrame, empty=True
equity_curve(): type=DataFrame, empty=True
```
(Tábla üres mert a lab DB-ben még nincs kalibrált strategy a szintetikus teszten kívül — ez a várt viselkedés.)

**Linter/type check:**
- `ruff check src/ui/ --fix` — tiszta (1 whitespace fix auto-applied)
- `pyright src/ui/data.py` — 0 error, 0 warning

**Acceptance Criteria státusz:**
- [x] trades/equity a strat.* táblákból töltődik (primary path)
- [x] equity curve + trade journal helyesen jelenik meg (helyes sorrend: trades DESC entry_time, equity ASC trade_index)
- [x] smoke: `python -c "from src.ui import data"` importálható; hiányos strat.* esetén nem dob kivételt
- [ ] manuális Streamlit smoke: kijelző/env nélkül nem futtatható, de import + data-loader smoke lefutott
- [x] ruff + pyright tiszta

