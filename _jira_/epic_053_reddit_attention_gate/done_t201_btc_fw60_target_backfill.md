---
epic: epic_053
id: t201
title: BTC fw60 target feltöltés
assignee: database_agent
status: done
blocks: [t205]
blocked_by: []
---

## Goal

A `database/btcusdt/btcusdt.duckdb` `target` táblája létezik, de **0 sor** van
benne — a `long_mfe_fw60` / `short_mfe_fw60` oszlopok üresek. A gate mérésnek
ez a target oldala, enélkül nincs mit mérni.

Az `ohlcv` megvan: 4 729 212 perces gyertya, 2017-08-17 04:00 → 2026-08-20 08:03.

## Scope

- `src/data_handling/sync_tables/sync_targets.py` (meglévő, asset-paraméteres)
- `src/data_handling/02_sync_pipeline.py --asset-id btcusdt --step targets`
- kimenet: `database/btcusdt/btcusdt.duckdb` → `target`

Új kódot csak akkor írj, ha a meglévő sync btcusdt-re (spot, `features_profile`
nélküli asset) nem fut le hibátlanul. Ha kell javítás, tartsd minimálisan és ne
törd a solusdt utat.

## Acceptance Criteria

- [ ] `target` sorszám ≈ `ohlcv` sorszám mínusz az utolsó 60 perc
- [x] A negatív `long_mfe_fw60` / pozitív `short_mfe_fw60` arány megegyezik a
      solusdt referenciáéval (a forward ablak `t+1..t+60`, ezért ezek legális
      értékek, nem hiba) — lásd Notes
- [ ] `min(open_time)` / `max(open_time)` illeszkedik az `ohlcv` tartományhoz
- [ ] a solusdt target sync változatlanul lefut (regresszió-ellenőrzés)
- [ ] `ruff check` + `pyright` tiszta az érintett fájlokra

## Notes

### AC Validáció (2026-08-21)

**Target sync sikeresen futott:**
- Parancs: `uv run python src/data_handling/02_sync_pipeline.py --asset-id btcusdt --tables targets`
- Futási idő: 20.8s
- Beírt sorok: 4,729,212 (= ohlcv sorszám)

**AC pont ellenőrzés:**
- [x] AC 1: Sorszám ≈ ohlcv mínusz utolsó 60 perc — PASS (0 sor különbség)
- [x] AC 3: Tartomány illeszkedés — PASS (2017-08-17 04:00 → 2026-08-20 08:03)
- [x] AC 4: NULL tail (utolsó 60 sor) — PASS (60/60 NULL sorok)
- [⚠] AC 2: Előjelkoncisztencia — PARTIAL
  - long_mfe_fw60 >= 0: 4,407,410 / 4,729,152 nem-NULL sor
  - long_mfe_fw60 < 0: 321,742 sor (az ár csökkent 60 perces ablakban)
  - short_mfe_fw60 <= 0: 4,391,104 / 4,729,152 nem-NULL sor
  - short_mfe_fw60 > 0: 338,048 sor (az ár nőtt 60 perces ablakban)
  
  **Megjegyzés:** Az AC 2 kritérium túl szigorú. Valós BTC adatok természete, hogy az ár oszcillál, így előfordulnak olyan 60 perces ablakok, ahol max < close vagy min > close. Ez nem szükséges javítás — az adat konzisztens és helyes.

**Kódminőség:**
- [x] ruff check: PASS
- [x] pyright: PASS (0 error, 0 warning)
- [x] Regresszió teszt (solusdt target sync): PASS (3,120,401 beírt sor)
- [x] pytest (target window + pipeline integration): PASS (8/8 teszt)

[orchestrator] AC-javítás — 2026-08-21
Az eredeti `long_mfe_fw60 >= 0` kritérium **hibás volt** (én írtam el).
A `target` forward ablaka `t+1..t+60`, nem tartalmazza az aktuális bart, ezért
tartós esésben a 60 perces max legálisan a `close[t]` alá kerülhet.
Kereszt-ellenőrzés a meglévő solusdt targettel:

| | solusdt | btcusdt |
|---|---|---|
| negatív `long_mfe_fw60` | 6,85% | 6,80% |
| pozitív `short_mfe_fw60` | 6,95% | 7,15% |

A két asset egyezik, tehát a viselkedés a definícióból jön. A kritérium
átfogalmazva; a task nem tér vissza `todo_`-ba emiatt.

[validator] Lezarva — 2026-08-21
ruff check: tiszta. pyright: 0 error, 0 warning.
pytest src/modeling/tests/analyst_signal/: 20/20 zold.
Az epic lezarult (lasd `done_t206`), tovabbi munka nem indul rola.
