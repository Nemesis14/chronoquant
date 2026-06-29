---
epic: epic_043
id: t6
title: Dashboard methodology frissítés — UI változások (8100)
assignee: methodology_agent
status: pr
blocks: []
blocked_by: []
---

## Goal

`_doc_/methodology_doc/8100_dashboard.md` frissítése a session szétválasztás és UI változások dokumentálásával.

## Scope

- `_doc_/methodology_doc/8100_dashboard.md`

## Acceptance Criteria

- [x] UI layout változások dokumentálva
- [x] `load_long_short_strategies()` adatforrás magyarázva
- [x] Binance fallback pozíció-lekérdezés megemlítve

## Notes

2026-06-28 — methodology_agent végrehajtotta.

Változások a `8100_dashboard.md`-ben:

1. **Overview flowchart** kibővítve: `Strategy artifacts — Long és Short` node hozzáadva; új `UI panel elrendezés` diagram hozzáadva (main.py → Long/Short kártya → Active Trade → Recent Trades → Prediction chart).

2. **Read-layer diagram** kibővítve: két külön artifact forrás (Long + Short), Binance API fallback mint ötödik forrás.

3. **Új kulcsfogalom szekció** — `Long Strategy / Short Strategy kártyák`: indokolja az egyetlen kombinált kártya felváltását, bemutatja a `load_long_short_strategies()` mechanizmust, példaértékekkel (`cutoff=98%`, `trades=78`, stb.), és `flowchart LR` diagrammal.

4. **Új kulcsfogalom szekció** — `Active Trade panel: Binance fallback`: leírja a DB-first / Binance-fallback logikát, a `[Binance]` tag szemantikáját és az unrealized PnL megjelenítését; `flowchart TD` diagrammal.

5. **Új kulcsfogalom szekció** — `Recent Trades (Binance) panel`: magyarázza a `realizedPnl != 0` szűrőt, a BUY→SHORT / SELL→LONG label-fordítást; `flowchart LR` diagrammal.

6. **Új kulcsfogalom szekció** — `Prediction chart: chunked backfill és teljes history`: indokolja a chunked betöltést (OOM elkerülés), dokumentálja a ~3 042 799 soros, 2020-09 → 2026-06 terjedelmet; `flowchart TD` diagrammal.

7. **Paraméter tábla** kibővítve: 4 új sor (stratégia kártya elrendezés, active_position fallback, Recent Trades szűrő, prediction backfill ablak). `chart fókusz` sor frissítve (teljes history visszagörgethető megjegyzéssel).

8. **Ismert kockázatok tábla** kibővítve: 4 új kockázat (Binance/journal eltérés, Recent Trades szűrési edge-case, prediction chart OOM, Long/Short kártya keveredés).

9. **Validációs checklist** kibővítve: 10 tételre (korábban 6); 4 új, a UI változásokhoz kötött ellenőrzési pont hozzáadva.
