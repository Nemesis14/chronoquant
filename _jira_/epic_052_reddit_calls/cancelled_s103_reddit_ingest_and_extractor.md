---
status: cancelled
epic: epic_052
id: s103
title: Reddit archívum ingest és LLM call-extraktor
---

## Goal

Reddit historikus archívum betöltése, szabad szövegből irányos `call`-ok
kinyerése kalibrált LLM-extraktorral, majd az `epic_051` scoring pipeline
lefuttatása a nagyobb mintán.

**Blokkolva** — csak az `epic_051` pozitív validációs riportja után indul.

## Az ötlet

A Reddit ingyenes és 2005-ig visszamegy, tehát nagyságrendekkel nagyobb minta,
mint a TradingView. De van egy alapvető különbség:

> A TradingView-nál a szerző **megmondja** az irányt. A Redditen **mi
> következtetjük ki**. Az extraktor hibája beépül a mérésbe, és utólag nem
> választható szét: „a szerző volt rossz" vagy „az extraktorunk volt rossz"?

Erre a válasz az, ami miatt az `epic_051` előbb fut: a TV ideáknál van szöveg
**és** ismert ground-truth irány. Vagyis a TradingView adat egy **ingyen
címkézett kalibrációs set** — pontosan megmérhető rajta, hogy az extraktorunk
milyen pontosan olvassa ki az irányt szabad szövegből, mielőtt éles Reddit
adatra engednénk.

Az extraktor mért precision/recall-ja ezután **bekerül a scoring
bizonytalanságába**, nem tűnik el.

## Adatforrás

| Opció | Mélység | Rate limit | Megjegyzés |
|---|---|---|---|
| **Watchful1 academic torrents** | 2005–2025 | nincs (offline) | több TB, a legrobosztusabb |
| **Arctic Shift** | 2005→ma | ~120k req/óra, auth nélkül | Pushshift utódja |
| **PullPush.io** | — | — | Pushshift drop-in klón |
| Hivatalos Reddit Data API | élő | 100 q/perc, non-commercial ingyen | inkrementális frissítéshez |

Historikus backfillre a **dump** a preferált (nincs szolgáltatásfüggés),
inkrementális frissítésre a hivatalos API.

## Terv

- [ ] T1: Archívum beszerzés + subreddit szűrés → raw Parquet
      (r/CryptoCurrency, r/solana, r/CryptoMarkets, r/SatoshiStreetBets)
      → `database_agent`
- [ ] T2: Olcsó előszűrő — asset-említés + irány-jelző kulcsszavak;
      cél a volumen nagyságrendi csökkentése LLM előtt → `database_agent`
- [ ] T3: LLM call-extraktor — input: szöveg + timestamp, output:
      `{asset, direction, horizon, confidence}` vagy `null` (nincs call)
      → `analyst_agent`
- [ ] T4: **Extraktor kalibráció a TradingView adaton** — precision/recall
      mérés ismert ground-truth irány ellen → `analyst_agent`
- [ ] T5: `calls` tábla feltöltés `extraction=llm_v1` jelöléssel
      → `database_agent`
- [ ] T6: Az `epic_051` scoring + validációs pipeline lefuttatása
      a Reddit mintán (változatlan kód) → `modeling_agent`
- [ ] T7: Riport + feature javaslat a meglévő modellhez → `analyst_agent`

## Acceptance Criteria

- [ ] Raw archívum betöltve, subredditenként particionálva
- [ ] Az előszűrő mért szűrési aránya és false-negative rátája dokumentált
- [ ] Az extraktor precision/recall-ja **számszerűen** megvan a TV kalibrációs seten
- [ ] Minden Reddit call `extraction=llm_v1` jelölést kap
- [ ] Placebo teszt + FDR itt is lefutott
- [ ] A riport kimondja, hogy a TV-n mért jel a nagyobb mintán **megerősödik-e**
      vagy eltűnik

## Notes

**A legfontosabb eredmény nem a pontszám, hanem a konzisztencia.** Ha a TV-n
volt jel, de a Redditen eltűnik, annak két oka lehet: (a) a TV jel overfit
volt, (b) az extraktorunk gyenge. A T4 kalibráció **pontosan ezt a kétértelműséget
zárja ki** — enélkül a T6 eredménye nem értelmezhető. T4 ezért nem
opcionális és nem halasztható a végére.

**Költség:** teljes archívumon LLM-et futtatni irreális. A T2 előszűrő
minősége határozza meg a projekt költségét. Cserébe az előszűrő false
negative-jei szisztematikus torzítást visznek be (a nem-nyilvánvalóan
megfogalmazott callok kiesnek) — ezt mérni és dokumentálni kell, nem
elhallgatni.

**Licenc:** saját kutatásra a Reddit non-commercial tier tiszta út.
Ha a projekt valaha kereskedelmivé válna, a Reddit kereskedelmi engedélye
külön tárgyalást igényel (~$0.24 / 1000 hívás, enterprise minimum ~$12k/év).
Ezért él a raw réteg külön a származtatott aggregátumtól.
