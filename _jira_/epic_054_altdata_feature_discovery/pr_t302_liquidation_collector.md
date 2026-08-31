---
epic: epic_054
id: t302
title: Likvidációs stream collector — az egyetlen nem backfillelhető adat
assignee: database_agent
status: pr
blocks: []
blocked_by: []
---

## Goal

Elindítani a likvidációs események gyűjtését, mert **ez az egyetlen
derivatíva-adat, amit később nem lehet visszamenőleg beszerezni.**

A t301 felmérés kimutatta, hogy az eredeti feltevés — miszerint az OI és a
long/short arányok is time-critical lennének — **téves**. Azok a Binance Vision
`metrics` archívumból 2021-12-01-ig visszatölthetők. A likvidációkra viszont
sem daily, sem monthly szinten nincs archívum.

**Ez a task nincs a kritikus úton** — a t307–t311 modellezési lánchoz nem kell.
De minden nap késés véglegesen elveszett adat, ezért párhuzamosan fut.

## Scope

- `src/data_handling/collectors/derivatives/liquidations.py`
- `data/analyst_signal/raw/source=liquidations/` — partícionált parquet
- `data/analyst_signal/collection_log/`

## Forrás

`wss://fstream.binance.com/ws/solusdt@forceOrder` — esemény alapú stream.
Mezők: symbol, side, order type, quantity, price, average price, order status,
last filled quantity, filled accumulated quantity, trade time.

## ⚠️ Kötelező szabályok

1. **Két időbélyeg:** `value_ts` (esemény ideje a streamből) és `observed_at`
   (mikor érkezett hozzánk).
2. **Parquet append, nem DuckDB write** — a DuckDB single-writer.
3. **Újraindulásra képes process.** A websocket természeténél fogva folyamatos;
   a kiesést naplózni kell, mert az adathiány itt nem pótolható.
4. **A stream throttle-olt** (~1 esemény/mp a Binance oldalán). A gyűjtött
   likvidációs volumen szisztematikusan alulmér, és nagy kilengéskor a
   legrosszabbul. Ezt a modul README-jébe is írd bele, ne csak a riportba.

## Acceptance Criteria

- [ ] Collector fut, újraindul kapcsolatvesztés után
- [ ] Parquet séma: `value_ts | observed_at | side | qty | price | avg_price | status`
- [ ] `collection_log`: futásonként kezdet/vég, esemény-szám, kiesések
- [ ] Legalább 24 órás megszakítás nélküli próbafutás naplózva
- [ ] README rögzíti a throttle-korlátot
- [ ] `ruff check` + `pyright` tiszta

## Notes

[orchestrator] Elvegezve — 2026-08-25

Modul: `src/data_handling/collectors/derivatives/liquidations.py`.
`ruff` es `pyright` tiszta. Kapcsolodas elesben ellenorizve.

- Stream: `wss://fstream.binance.com/ws/solusdt@forceOrder`
- Raw reteg: `data/analyst_signal/raw/source=liquidations/liquidations-YYYY-MM-DD.parquet`
- Naplo: `data/analyst_signal/collection_log/liquidations.jsonl`
- Ket idobelyeg minden soron: `value_ts` (tozsdei esemenyido) + `observed_at`
- Dedup: `(value_ts, symbol, side, qty, price)` — ujracsatlakozas utani ismetles ellen
- Exponencialis backoff (2 -> 60 mp), minden szakadas naplozva

Flush ket triggerre: 50 esemeny VAGY 300 masodperc. Az idozitett flush azert
kell, mert a likvidacio ritka — egy csendes ora buffere elveszne, ha a process
meghalna a darabszam-trigger elott.

⚠️ **Nyitott uzemeltetesi kerdes:** a collector jelenleg a fejlesztoi gepen fut
egy hattervfolyamatkent. Ez NEM tartos megoldas — gepujrainditasnal leall, es a
kimaradt idot nem lehet potolni. Tartos otthont kell neki adni (Windows Task
Scheduler auto-restarttal, vagy felho, mint az epic_050-nel tervezett).
