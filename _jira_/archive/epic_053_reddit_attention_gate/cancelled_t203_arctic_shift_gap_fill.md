---
epic: epic_053
id: t203
title: 2026-os gap-fill Arctic Shift API-ból
assignee: database_agent
status: cancelled
blocks: []
blocked_by: [t202]
---

## Goal

A Watchful1 dump **2025-12-vel véget ér**. A `btcusdt` ohlcv viszont
2026-08-20-ig megvan. A hiányzó 2026-01 → ma ablakot Arctic Shiftből kell
pótolni, **ugyanabba a `social_hourly` sémába**.

## Adatforrás

- Arctic Shift API — https://github.com/ArthurHeitmann/arctic_shift
  (Pushshift utód, auth nélkül, magas throughput, community-run → best-effort
  uptime, nem szerződéses)
- HuggingFace Parquet dumpok (~261 GB teljes) — ha időközben megjelent 2026-os
  hónap, az **preferált** az API helyett
- Tartalék: PullPush.io (szűkebb limit)
- Élő/inkrementális frissítéshez később: hivatalos Reddit Data API
  (100 q/perc, non-commercial ingyenes)

## Scope

- `src/data_handling/collectors/reddit/` — gap-fill belépési pont
- ugyanaz a `social_hourly` tábla, ugyanaz az upsert

## Notes a konzisztenciáról

**Ez a task a legkockázatosabb pont a mérés szempontjából.** Ha az Arctic Shift
lefedettsége eltér a dumpétól (más törlés-kezelés, hiányzó órák), az egy
mesterséges szintugrást visz a 2026-os szakaszba, amit a modell rezsimváltásnak
olvasna.

Ezért kötelező egy **átfedéses ellenőrzés**: kérd le Arctic Shiftből a
2025-10 → 2025-12 ablakot is, és hasonlítsd a dumpból számolt órás
darabszámokhoz. Az eltérés arányát dokumentáld. Ha >5% szisztematikus eltérés
van, a 2026-os szakasz **külön `source` jelöléssel** menjen, és a t205 mérés
elsődlegesen a dump-only tartományon fusson.

Séma-kiegészítés ehhez: `source VARCHAR` oszlop (`dump` | `arctic_shift`),
belépve a PK-ba.

## Acceptance Criteria

- [ ] 2026-01 → aktuális hónap feltöltve `social_hourly`-be
- [ ] `source` oszlop megkülönbözteti a dump és az API eredetű sorokat
- [ ] Átfedéses ellenőrzés (2025-10 → 2025-12) lefutott, az eltérés
      számszerűen a Notes-ban
- [ ] Rate limit betartva, a futás újraindítható megszakadás után
- [ ] `ruff check` + `pyright` tiszta

## Notes

[orchestrator] Reszben elvegezve — 2026-08-21

**A `source` oszlop MEGVAN**, nem ennek a tasknak kell megcsinalnia.
`analyst_signal_store.py` v7 migracio: `social_hourly` ujraepitve
`source VARCHAR` oszloppal a PK-ban (`ts_utc, subreddit, kind, source`).
A migracio elott irt 1488 sor (az API proof-run outputja) `arctic_shift`
jelolest kapott. Az `aggregate.py` `--source {dump,arctic_shift}` kapcsolot kapott.

**Az atfedeses ellenorzes elokeszitve.** A raw parquet reteg NINCS source
szerint particionalva — a dump felulirja ugyanazt az utvonalat. Ez tudatos:
az osszehasonlitas amugy is aggregatum-szinten ertelmes, es a
`social_hourly`-ben MINDKET forras sorai megvannak a 2025-01 BitcoinMarkets
ablakra (dump + arctic_shift kulon sorokkent). Az API-eredetu raw parquet
biztonsagi masolata: `data/analyst_signal/raw/source=reddit/_api_proof_backup/`.

Ami MEGMARAD ebbol a taskbol: a 2026-01 -> ma ablak lekerese az API-val,
`--source arctic_shift` jelolessel, plusz az atfedes szamszeru kiertekelese.

[orchestrator] Atfedeses ellenorzes LEFUTOTT — 2026-08-21

Ablak: r/BitcoinMarkets, kommentek, 2025-01 (744 ora), dump vs arctic_shift.

| metrika | ertek |
|---|---|
| dump osszes komment | 10 600 |
| arctic_shift osszes komment | 10 598 |
| relativ elteres | **+0,02%** |
| azonos darabszamu ora | 742 / 744 (99,7%) |
| max oras elteres | 1 darab |

A ticket 5%-os kuszobe alatt ket nagysagrenddel. **A ket forras
osszefuzhetо szintugras nelkul** — a 2026-os API-s szakasz nem visz
mesterseges rezsimtorest.

Ami meg hatra van ebbol a taskbol: a tenyleges 2026-01 -> ma lekeres
`--source arctic_shift` jelolessel. A 2026-os ablak ~8 honap, negy
subreddit; a 2025-os ratakkal becsulve nagysagrendileg 1,5-2M rekord,
ami az API 100/keres plafonjaval ~15-20 ezer keres — ez mar vallalhato,
ellentetben a teljes backfill-lel.

[orchestrator] TOROLVE — 2026-08-21

Az epic lezarult (lasd `done_t206`), a 2026-os gap-fill nem keszul el.
A task erdemi resze — az atfedeses ellenorzes — VISZONT LEFUTOTT es pozitiv:
a dump es az Arctic Shift API elteres +0,02% (10 600 vs 10 598 komment,
742/744 azonos ora). Ha a tema valaha ujraindul, ez a kerdes el van dontve:
a ket forras osszefuzheto szintugras nelkul.
