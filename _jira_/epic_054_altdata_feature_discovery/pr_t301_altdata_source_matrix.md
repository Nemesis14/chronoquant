---
epic: epic_054
id: t301
title: Adatforrás-mátrix — mélység, granularitás, ár, Python-hozzáférés
assignee: analyst_agent
status: pr
blocks: [t303]
blocked_by: []
---

## Goal

Eldönteni **tényadat alapján**, mit érdemes egyáltalán begyűjteni. A user
research-jegyzete állításokat tartalmaz a CoinGlass / CryptoQuant / Glassnode /
Coin Metrics kínálatáról — ezeket ellenőrizni kell, nem átvenni.

A kimenet nem esszé, hanem egy **döntési tábla** és egy **10-20 elemű
shortlist**, amiből a t303 dolgozik.

## Scope

- `_doc_/research/altdata_source_matrix.md` — új dokumentum
- nincs kódírás

## A tábla kötelező oszlopai

| oszlop | tartalom |
|---|---|
| `metrika` | pl. `open_interest`, `funding_rate`, `exchange_netflow` |
| `csoport` | derivatives / flow / on-chain / stablecoin / market structure |
| `mit mér` | egy mondat, üzleti jelentés |
| `asset` | SOLUSDT / BTC / ETH / aggregált |
| `granularitás` | 1m / 5m / 1h / 8h / 1d |
| `historikus mélység` | **ellenőrzött** érték, nem marketing-ígéret |
| `backfillelhető?` | igen / nem / részben — **ez a legfontosabb oszlop** |
| `forrás` | konkrét endpoint vagy csomag |
| `auth` | nincs / API key / fizetős |
| `költség` | havi USD, tier néven nevezve |
| `rate limit` | kérés/perc |
| `revideálódik?` | igen/nem — point-in-time kockázat |

## Kötelezően megvizsgálandó források

- **Binance futures public data** (`/futures/data/*`, `fundingRate`,
  `openInterestHist`) — ez az alap, mert ingyenes és pont a mi assetünkre szól
- **Binance Vision** historikus dump (`data.binance.vision`) — van-e benne
  derivatíva-metrika, milyen mélységig
- **Bybit / OKX** public derivatíva endpointok — keresztvalidációhoz
- **CoinGlass V4** — ingyenes tier mit ad, mi van fizetős mögött
- **CryptoQuant / Glassnode** — csak ár és minimális belépő tier; ne
  regisztrálj, ne köss szerződést
- **DefiLlama / CoinGecko** — stablecoin supply, ingyenes historikus
- **Blockchain.com charts API** — BTC on-chain aggregátumok, ingyenes

## Acceptance Criteria

- [ ] `altdata_source_matrix.md` létezik, a fenti oszlopokkal
- [ ] Minden `historikus mélység` és `backfillelhető?` cella mellett ott a
      forrás (doc-link vagy megjegyzés, hogy honnan jött az érték)
- [ ] Külön szekció: **"Nem backfillelhető"** lista — ezt a t302 gyűjti
- [ ] Shortlist: 10-20 metrika, priorizálva (érték / költség / munka)
- [ ] Explicit "nem csináljuk" szekció: mit hagyunk ki és miért
- [ ] Nincs fizetős előfizetés megkötve — csak árazás dokumentálva

## Notes

[orchestrator] Elvegezve — 2026-08-21

Kimenet: `_doc_/research/altdata_source_matrix.md` (170 sor).
Minden ertek elo lekerdezessel ellenorizve, nem dokumentaciobol atveve.

Fo eredmenyek:
- 1m pozicio-adat sehol nincs (Binance/Bybit/OKX mind 5m); a Binance `[]`-t
  ad vissza HTTP 200-zal, nem hibat — ez collector-csapda.
- Binance Vision `metrics/SOLUSDT` 2021-12-01-tol, 5m, ~20 MB — az epic
  eredeti "nem backfillelheto" premisszaja ennyiben TEVES volt.
- Likvidacio az EGYETLEN nem backfillelheto elem (nincs archivum).
- CoinGlass: a melyseg a felbontassal forditottan aranyos, a 4,7 eves 5m
  ablakot egyik tier sem adja. CryptoQuant: Solana nincs a listan.
- Fizetos szolgaltato a derivativa-aghoz nem kell.
