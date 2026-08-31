# Epic 050: Copy-trading leaderboard snapshot collector

## Goal

Exchange copy-trading leaderboardokról **időbélyegzett snapshotokat** gyűjteni
felhőben futó ütemezett jobbal, hogy később kimutatható legyen, mely traderek
prediktívek — és hogy a top traderek nettó pozicionáltsága feature-ként
becsatornázható legyen a modellbe.

**Ez a terv egyetlen időérzékeny eleme.** Pozíció szinten historikus backfill
nem létezik: egyik exchange sem ad vissza „mit csinált X trader 2024-ben".
Amit ma nem mentünk le, az véglegesen elveszett. A gyűjtésnek ezért **az
elemzés előtt, önállóan** el kell indulnia.

Az adat értéke az idővel nő — 3-6 hónap után lesz belőle survivorship-mentes
panel. Addig ez az epic **csak infrastruktúra, nulla ML**.

## Scope

- `src/data_handling/collectors/leaderboard/` — új modul
- `data/analyst_signal/raw/source=leaderboard/` — partícionált Parquet store
- `data/analyst_signal/collection_log/`
- felhő deployment (ütemezett job, nem daemon)

Nem érinti: `src/modeling/`, `src/strategy/` — az elemzés külön epic lesz.

## Stories

- s101: Leaderboard snapshot collector és felhő deployment

## Key Decisions

- **Nem daemon, hanem ütemezett job.** Rövid életű process: indul → lekér →
  Parquet append → kilép. Futásonként 10-30 mp. Nincs mit összeomlani éjjel.
- **Óránkénti mintavétel.** A 15 perces felbontás nem ér annyival többet,
  hogy megérje a ~4x ban-kockázatot.
- **Felhőben fut**, nem lokálisan — a saját gép 0-24 üzeme nem opció, és a
  kihagyott futások pont a legdrágább adatot (a listáról lekerülést) veszítik el.
- **Parquet, nem DuckDB write.** A DuckDB single-writer; ha a poller írna,
  miközben egy notebook olvas, a gyűjtés csendben elhalna.
- **Teljes lista mentése, nem top 10.** Ha csak a csúcsot snapshotoljuk,
  visszakapjuk pontosan azt a survivorship biast, ami ellen az egész készül.

## Notes

Ismert forráshelyzet (2026-08):
- **Bitget** — van hivatalos, dokumentált Copy Trading API. A legtisztább út.
- **Binance** — a legjobb adat, de a pozíció-endpoint
  (`.../leaderboard/getOtherPosition`) publikusból **private-ba került**, ma
  auth kell hozzá; nem dokumentált, bármikor eltörhet. Pozíció-megosztás **opt-in**.
- **OKX** — v5 API-ban vannak copy trading endpointok lead trader kóddal.
- **Bybit** — leaderboard scrape-elhető, nyitott pozíciók látszanak.
