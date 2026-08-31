---
epic: epic_050
id: s101
title: Leaderboard snapshot collector és felhő deployment
---

## Goal

Felhőben, óránként futó ütemezett job, ami exchange copy-trading
leaderboardokról teljes snapshotot ment partícionált Parquet-be, hibatűrően és
idempotensen.

A cél **kizárólag az adatgyűjtés elindítása**. Elemzés, scoring, feature
generálás nem tartozik ide — azok csak akkor lesznek lehetségesek, amikor
összegyűlt 3-6 hónap panel.

## Az ötlet

A szöveges források (TradingView, Reddit) azt mérik, mit **mond** valaki.
A leaderboard azt mutatja, mit **csinál** — valós tőkével, valós P&L-lel.

Egy `Long SOL @ 142.30, 5x, nyitva 2026-03-04 08:12` szerkezetileg pontosan
ugyanaz, mint egy TradingView idea: időbélyegzett, szerzőhöz kötött, irányos
állítás. Csak itt pénz van mögötte. Ezért ugyanabba a `call` absztrakcióba
képződik le (lásd [plan.md](../../plan.md) 3. szakasz), és ugyanaz a scoring
réteg fogja kiértékelni.

Mellékhozadék, ami önmagában is érhet valamit: a top traderek **nettó
pozicionáltsága** SOL-ban egy közvetlen order-flow proxy, ami feature lehet
a meglévő modellben — függetlenül attól, hogy a szerző-skill hipotézis igaz-e.

## Mit publikálnak — két adatréteg

**(A) Trader-szintű statisztika** (mindenhol nyilvános)

| Mező | Megjegyzés |
|---|---|
| ROI (7d / 30d / 90d / all-time) | Binance „indicative optimized portfolio value"-val számol, hogy a be/kifizetések ne torzítsanak |
| PnL (abszolút USDT) | |
| Win rate | *nyereséges orderek / összes order* — **order-alapú, nem trade-alapú** |
| MDD | csúcstól völgyig |
| Sharpe ratio | Binance publikálja |
| AUM / copier count | mennyi tőkét kezel, hányan másolják |
| rank | **mentendő** — a listáról lekerülés maga az esemény |

**(B) Pozíció-szintű adat** — ez az igazi érték, de csak ahol elérhető

`symbol`, `side`, `entryPrice`, `markPrice`, `leverage`, `size`,
`unrealizedPnl`, `roe`, `openTime`, néha `closeTime`.

## Terv

**Fázis 1 — most (infrastruktúra)**

- [ ] T1: Forrásfelderítés és forrásválasztás — endpointok, auth-igény,
      rate limit, mezőséma; döntés melyik exchange(ek)ről indulunk
      → `database_agent`
- [ ] T2: Snapshot collector modul — teljes lista + rank + pozíciók,
      normalizált séma, idempotens kulcs `(snapshot_ts, exchange, trader_id, symbol)`
      → `database_agent`
- [ ] T3: Parquet store + `collection_log` tábla (`ts`, `source`, `status`,
      `error`, `row_count`) → `database_agent`
- [ ] T4: Felhő deployment — ütemezett job óránként, nem daemon
      → `code_doc_agent`
- [ ] T5: Monitoring — hiányzó snapshot detektálás és riasztás
      → `code_doc_agent`

**Fázis 2 — +3-6 hónap múlva, külön epic**

- Panel elemzés, `call` normalizálás, scoring, feature generálás.
  **Nem itt.** Ez a story akkor lezárul, ha a gyűjtés stabilan fut.

## Acceptance Criteria (a story egészére)

- [ ] A job felhőben fut, óránként, saját gép bekapcsolt állapotától függetlenül
- [ ] Egy hét után a `collection_log` alapján a futások ≥95%-a sikeres
- [ ] Újrafuttatás nem duplikál sort
- [ ] A teljes rangsor mentve (nem csak top N), ranggal együtt
- [ ] Egy elhalt futás nem korruptálja a store-t, és nyomot hagy a logban

## Notes

**Amit előre tudni kell, mert visszamenőleg nem javítható:**

- **Nincs historikus backfill.** Csak forward gyűjtés. Ezért sürgős.
- **Survivorship bias, brutális mértékben.** A leaderboard definíció szerint a
  nyerteseket mutatja; aki felrobbant, lekerül róla, és vele a története is.
  Bármilyen backtest, ami a *mai* listát nézi visszamenőleg, garantáltan hamis
  pozitív. Egyetlen kezelés: mi magunk snapshotoljuk a **teljes** listát időben,
  és megőrizzük a lehullottakat is.
- **A win rate félrevezető.** „Nyereséges orderek / összes order" — egy
  martingál-jellegű trader 90%-ot mutat, aztán egyszer mindent elveszít.
  A scoring nem erre épülhet.
- **Snapshot ≠ event stream.** Aki két mintavétel között nyit és zár, annál a
  trade nem létezik az adatunkban. Ez szisztematikusan kiszűri a scalp
  jellegű tradeket → a mérés a hosszabb tartású traderek felé torzít.
  Ezzel együtt lehet élni, de a végkövetkeztetésben ki kell mondani.

**Nyitott kérdés:** melyik exchange induljon? A Bitget dokumentált API-ja
stabilabb, a Binance adata gazdagabb de törékenyebb. Lehet mindkettő,
de akkor T1-ben el kell dönteni a prioritást.
