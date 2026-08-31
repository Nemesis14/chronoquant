---
epic: epic_051
id: t2
title: TradingView backfill runner az /api/v1/ideas/ endpointon
assignee: —          # főszálon fut, agent nélkül (a t1 folytatása)
status: done
blocks: [t3]
blocked_by: []
---

## Goal

BTC-fókuszú TradingView idea-backfill a `t1`-ben felderített
`/api/v1/ideas/` endpointon, nyers JSON réteg megőrzésével.

**Nem HTML-scraping.** A `t1b` kimutatta, hogy a HTML út zsákutca
(szimbólum-feed ~10 nap, szerzői oldal JS-renderelt). A `t1c` megtalálta
az API-t — az egyetlen hívás adja a `created_at`-et, `updated_at`-et,
`symbol.direction`-t, `symbol.interval`-t és a `user.id`-t.

Teljes API-szerződés és budget-becslés:
[research/tradingview_poc/README.md](../../research/tradingview_poc/README.md) „T1c".

## Scope

```
src/data_handling/collectors/tradingview/
  __init__.py
  api.py         ← HTTP kliens: rate limit, retry, raw perzisztencia, resume
  backfill.py    ← két fázis + CLI
data/analyst_signal/raw/source=tradingview/
  discover/{symbol}_p{n}.json
  authors/{username}_p{n}.json
  _manifest.jsonl        ← minden fetch: url, fetched_at_utc, status, bytes
```

**BTC-only** (user döntés, lásd `epic.md` „BTC-first"). SOL és ETH elhalasztva.

## Terv

**1. fázis — felderítés.** `symbol=BTCUSD` és `symbol=BTCUSDT`,
`per_page=100`, `page=1..10` → ~2000 idea az elmúlt ~2 hónapból.
Ebből a distinct `user.username` halmaz. ~20 kérés.

**2. fázis — szerzőnkénti teljes history.** Minden felderített szerzőre
`by={username}`, `per_page=100`, lapozva `page_count`-ig. Minden szimbólum
jön, a BTC-szűrés a T3 normalizálás dolga — a raw réteg teljes marad.
~800–1600 kérés.

## Rate policy (a `t1` README „Rate" szakasza alapján)

- **4–8s véletlen jitter** — a fix intervallum bot-ujjlenyomat
- 1 szál, nincs párhuzamosság, cookie jar + keep-alive
- **429 / 403 → azonnali teljes leállás**, nincs retry-őrlés
- 5xx → exponenciális backoff, max 3 próba
- napi keret ~1500 kérés; a futás **resume-olható**

## Acceptance Criteria

- [x] `api.py` — kliens jitterrel, hard-stop 429/403-ra, raw mentéssel
- [x] Cache: már letöltött lapot nem kér le újra (resume)
- [x] `_manifest.jsonl` minden kérésről, **saját `fetched_at_utc`-vel**
- [x] `backfill.py discover` lefut, szerzőlistát ír
- [x] `backfill.py authors` lefut, szerzőnkénti history-t ment
- [x] Nyers JSON válaszok érintetlenül megőrizve (nincs menet közbeni parse)
- [x] `ruff check` + `pyright` tiszta

## Notes

**A raw réteg érintetlen marad.** A `plan.md` `raw/` ↔ `calls/`
szétválasztása szerint itt semmilyen normalizálás nem történik — a
`direction` dekódolás, a BTC-szűrés és a `call_id` képzés a T3 dolga.
Operatív indoka is van: ha a parser hibás, a hibát ne a teljes crawl
árán tudjuk meg.

**`first_seen`:** a `_manifest.jsonl` `fetched_at_utc` mezője a mi saját
észlelésünk. A backfillnél ez a letöltés ideje (nem a publikálásé), de a
későbbi inkrementális gyűjtésnél ez lesz a leak-mentes időbélyeg.

**Nincs entry/target ár.** A `t1` igazolta: az API-ban egyetlen ár-mező
sincs, és 0/5 detail oldal tartalmazott pozíció-eszközt. A mérés
`direction` + `ts_utc` alapú, ahogy a `plan.md` `call` absztrakciója
eleve feltételezi.

---

### Futás — 2026-08-17

**1. fázis kész:** `BTCUSD` + `BTCUSDT`, 10-10 lap, **20 kérés, ~2 perc**.
Mindkettő a `count=1000` plafonon. → **741 distinct szerző**
(`_authors.json`).

**2. fázis fut** (háttérben): 741 szerző × `by={username}`.
Mért tempó ~4,4 szerző/perc → **becslés ~2,5-3 óra**.
Megszakítás esetén az újraindítás cache-ből folytatja.

**Hozzáadva:** `review.py` — read-only adatminőségi kimutatás a raw
rétegen (mennyiség, szerzőnkénti call-eloszlás, mező-kitöltöttség,
irány- és timeframe-eloszlás, időbeli lefedettség, plusz explicit
„ami nincs" szekció). `ruff` + `pyright` tiszta.

---

### T2 LEZÁRVA — 2026-08-17

**2. fázis kész:** 741/741 szerző, **1913 kérés, 191 perc, 397 MB,
minden státusz 200** — nulla rate-limit, nulla retry.

Eredmény: **144 966 idea**, ebből **30 235 BTC**, ebből
**18 236 használható call** (long/short).
**281 szerző ≥10 callal, 146 szerző ≥30 callal.**
Időtartomány 2015-03 .. 2026-08.

Teljes kimutatás: [research/tradingview_poc/REVIEW.md](../../research/tradingview_poc/REVIEW.md)

**A review egy T4 spec-módosítást tár fel:** a callok 61,5%-a long, és ez
minden évben 50% felett van (2019-ben 81%). BTC-only scope mellett a
„BTC-béta korrekció" jelentése megváltozik — nem másik asset ellen kell
korrigálni, hanem **a BTC saját driftje ellen** (excess return vs.
buy-and-hold). Enélkül a rangsor a leginkább long-torzított szerzőt hozza
elsőnek, nem a legjobbat.
