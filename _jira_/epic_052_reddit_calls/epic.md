# Epic 052: Reddit — szöveges call extrakció és skálázott skill mérés

> ## ⛔ TÖRÖLVE — 2026-08-20
>
> Az `epic_051` gate-je **nem ment át**. A TradingView-n, ahol a címke
> tökéletes (a szerző maga jelöli az irányt, nincs extraktor-hiba),
> **FDR-korrekció után nulla szerző** bizonyult azonosíthatóan jónak,
> mind az öt horizonton.
>
> A Reddit track nagyságrenddel több munka, és ott **mi gyártanánk a
> címkét** — extraktor-zajjal. Ha a tiszta címkés forráson nincs jel,
> itt végképp nem lesz. Pontosan ezért volt ez a sorrend.
>
> Riport: [_doc_/research/tradingview_author_skill.md](../../_doc_/research/tradingview_author_skill.md)
>
> A fájlok megmaradtak, hogy az indoklás követhető legyen. Ha nem kellenek,
> a mappa bármikor törölhető.
>
> **Az aggregált hipotézis is lefutott azóta.** A törlés a *szerző-skill*
> hipotézist zárta le; az aggregált figyelem-idősort külön gate mérte meg
> (`epic_053_reddit_attention_gate`, 2026-08-21). Eredmény: van gyenge jel a
> kilengés-**nagyságra** (IC 0,02–0,10), az irányra nulla — de instabil és
> nem bizonyítottan több a volatilitásnál, ezért nem visszük tovább.
> Riport: [_doc_/research/reddit_attention_gate.md](../../_doc_/research/reddit_attention_gate.md)



## Goal

A `epic_051`-ben validált szerző-skill mérést kiterjeszteni a Reddit teljes
historikus archívumára (2005→ma), LLM-alapú call-extrakcióval — hogy a mérés
nagyságrendekkel nagyobb mintán, statisztikailag értelmes szerzőnkénti
call-számokkal fusson.

## ⚠️ BLOKKOLT — függ az `epic_051`-től

Ez az epic **csak akkor indul**, ha az `epic_051` validációs riportja jelet
mutat. Ha nem, ez az epic törlődik. Nem részleges indítás, nem „azért nézzük
meg" — a Reddit track nagyságrenddel több munka, és jel nélkül mind kidobott.

## Scope

- `src/data_handling/collectors/reddit/` — új modul
- `src/modeling/analyst_signal/extractor/` — LLM call-extraktor
- `data/analyst_signal/raw/source=reddit/`, `calls/`, `scores/`

Újrahasznált: az `epic_051` scoring és validációs pipeline-ja **változatlanul**.
Csak az adatforrás és az extrakciós réteg új.

## Stories

- s103: Reddit archívum ingest és LLM call-extraktor

## Key Decisions

- **Ingyenes és legális.** A Reddit Data API non-commercial/kutatási
  használatra ingyenes (100 query/perc). Historikus archívumhoz nem is kell:
  a Pushshift halott, de az **Arctic Shift** (2005→ma, auth nélkül,
  ~120k req/óra) és a **Watchful1 academic torrent dumpok** (több TB, offline,
  nulla rate limit) elérhetők. A dumpok a robosztusabbak — nincs rate limit,
  nincs szolgáltatásfüggés.
- **Az extraktor a TradingView adaton kalibrálódik.** A TV ideáknál van szöveg
  *és* ismert ground-truth irány → mérhető precision/recall. Kalibrálatlan
  extraktorral a mérés értelmezhetetlen.
- **`extraction=llm_v1` jelölés kötelező** minden Reddit callon. A natív és a
  kikövetkeztetett irányt soha nem szabad összekeverni a scoringban.
- **Kétlépcsős szűrés a költség miatt.** Olcsó előszűrő (asset-említés +
  irány-kulcsszavak) után csak a gyanús posztokra megy LLM.

## Notes

Célzott subredditek: r/CryptoCurrency, r/solana, r/CryptoMarkets,
r/SatoshiStreetBets. (r/CryptoMoonShots megfontolandó — nagy volumen, de
szinte tiszta pump-zaj.)
