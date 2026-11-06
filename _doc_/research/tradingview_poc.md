# TradingView Ideas scraper PoC és adatminőségi review — epic_051 (t1–t2)

> **Verdikt: az adat elbírja a szerző-skill mérést** (a scraper maradt), három megkötéssel. A mérés eredménye: [tradingview_author_skill.md](tradingview_author_skill.md).
>
> Futtatva: 2026-08-17 · PoC 6 HTTP kérés; backfill 1913 kérés, 191 perc, 397 MB, nulla rate-limit

## Mit néztünk
Kinyerhetők-e anonim, böngésző-UA-val a TradingView publikus ideái úgy, hogy egy szerző-szintű prediktivitás-mérés érvényes legyen (timestamp, irány, szerző-azonosító, lapozás, árszintek)?

## Módszer
Listaoldal + a detail oldalba ágyazott JSON + a `/api/v1/ideas` végpont vizsgálata kézi próbákkal (`probe*.py`), majd teljes BTC-backfill és minőség-riport (`collectors/tradingview/review.py`).

## Fő megállapítások
- **Timestamp:** a detail JSON `created_at` (µs, UTC) megvan, de a listakártya az `updated_at`-et mutatja (a mintán 3,9–90,2 óra eltérés) → detail fetch kötelező, különben lookahead leak.
- **Irány:** a detail JSON-ben numerikus (2 = short igazolt; 1 = long, 0 = nincs irány következtetett); a kártyák ~57%-ánál van long/short irány.
- **Szerző:** nincs numerikus user id, a `username` a kulcs (névváltásnál a történet kettéválik).
- **Lapozás:** átfedésmentes, de nem időrendi → a backfill nem állhat le dátumra, az inkrementális frissítés nem triviális.
- **Entry/stop/target:** nincs strukturáltan; `direction + ts` elég a méréshez.
- **Backfill:** 144 966 idea, ebből 30 235 BTC, 18 236 használható call (60%), 741 szerző; ≥10 callos szerző 281, ≥30 callos 146. Long-torzítás korrekció kötelező.
- A sitemap (5401 idea) backfillre alkalmatlan.

## Döntés / következmény
A `data_handling/collectors/tradingview/` collector marad (a `tv_report` használja, lásd [tv_report.md](tv_report.md); költözés `src/sentiment/`-be: t896). A szerző-mérés nemleges eredménye: [tradingview_author_skill.md](tradingview_author_skill.md). A `research/tradingview_poc/` (9,4 MB: nyers HTML/JS/JSON minták, `probe*.py`, README/REVIEW) törölve.

## Kód a git historyban
Commit `73f2156`: `research/tradingview_poc/{README.md,REVIEW.md,probe.py,probe2.py,probe3.py,review_report.json,samples/}`. Az `out/` (9,3 MB nyers HTML/JS) git-ignorált volt, nem visszaállítható. Az éles collector: `src/data_handling/collectors/tradingview/`.
