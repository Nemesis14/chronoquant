---
epic: epic_055
id: t312
title: Recent Trades panel a legfrissebb zárt ügyleteket mutassa
assignee: ui_agent
status: done
blocks: []
blocked_by: []
---

## Goal
A dashboard `Recent Trades (Binance)` paneljén elavult ügyletek jelentek meg
(utolsó: 2026-08-19), miközben aznap (2026-08-25) is voltak zárt trade-ek.

Ok: a `/fapi/v1/userTrades` endpoint az alapértelmezett (7 napos) ablakát
**növekvő id sorrendben** adja vissza, és a `limit` a **legrégebbi** végről vág.
A `futures_account_trades(symbol, limit=50)` hívás tehát az ablak legrégebbi 50
filljét adta vissza, nem a legfrissebbeket.

Élőben mérve (SOLUSDT):

| Hívás | Sorok | Időtartomány |
|---|---|---|
| `limit=50` | 50 | 08-18 13:52 -> 08-19 18:25 |
| `limit=1000` | 150 | 08-18 13:52 -> 08-25 13:29 |

## Scope
- `src/ui/binance_data.py` — `recent_trades()`, `_normalize_futures()`, `_normalize_spot()`
- `src/ui/components/trade_panel.py` — `_render_recent_trades_panel()`, `render_trade_panel()`

## Acceptance Criteria
- [x] A teljes ablak lekérése az API maximumával (`_FETCH_LIMIT = 1000`), a szeletelés lokálisan történik
- [x] `_normalize_futures` / `_normalize_spot` idő szerint csökkenő rendezés után `head(limit)` — a legfrissebb sorok maradnak
- [x] `recent_trades()` default `limit` 200 normalizált (záró) fill
- [x] A panel a percenként csoportosított ügyletekből a legfrissebb 20-at mutatja (`_RECENT_TRADE_ROWS = 20`)
- [x] Ellenőrizve élő Binance adaton: a lista teteje az aznapi (2026-08-25 13:26) zárás

## Notes
Döntés: a 20-as limit a **csoportosított** ügyletekre vonatkozik, nem a nyers
fillekre. Egy zárás gyakran 5-20 fillből áll (`_group_trades_by_minute`), így
20 fill csak ~4-5 látható sort adna. Így a panelen ténylegesen 20 zárt ügylet fér el.

Verifikáció (2026-08-25, élő futures account):
- 67 normalizált záró fill a 7 napos ablakban -> 14 csoportosított sor (cap 20)
- legfrissebb sor: 2026-08-25 13:26:33 LONG 97.33 (korábban: 2026-08-19 18:25)

Megjegyzés (nem része ennek a tasknak): a `render_trade_panel()` a
`st.session_state[trade_binance_*]` cache-t csak akkor üríti, ha egy sync
befejeződik (`finished_at` változás). Ha a sync nem fut, a panel a session
elején lekért adatot mutatja. Külön ticketet érdemelhet, ha zavaró.

A `config/trading.json` a `_2101_2607` strategy sessionökre mutat, míg a
`_doc_/0000_project_overview.md` `_2101_2605`-öt dokumentál — eltérés jelezve,
nem ennek a tasknak a hatóköre.
