# Epic 053: Reddit figyelem-idősor — olcsó gate a BTC fw60 kilengésre

> ## LEZÁRVA — 2026-08-21
>
> **Eredmény: van jel, de nem erőltetjük.**
>
> Az aggregált Reddit-figyelem statisztikailag prediktív a BTC 60 perces
> kilengés-**nagyságára** (nem az irányára), de a hatás gyenge (IC 0,02–0,10),
> subredditek között instabil, és nincs bizonyítva, hogy bármit hozzáad a
> modell meglévő volatilitás-feature-jeihez.
>
> Az irány-kontroll mindkét rezsimben tiszta nulla — a mérés érvényes,
> a pipeline nem gyárt jelet a semmiből.
>
> Riport: [_doc_/research/reddit_attention_gate.md](../../_doc_/research/reddit_attention_gate.md)
>
> Az egyetlen érdemi nyitott kérdés, ha valaha folytatódik: **ad-e a feature
> bármit a trailing realized volatility felett** (parciális korreláció).
> Azzal kell kezdeni.
>
> Az infrastruktúra megmarad: 66,7M Reddit rekord, `social_hourly`,
> `social_features`, `btcusdt` fw60 target, és egy mutációval igazolt
> leak-teszt-minta.


## Goal

Eldönteni **olcsón**, hogy az aggregált Reddit-figyelem (komment/poszt/szerző
volumen) hordoz-e prediktív információt a BTC **60 perces kilengés-nagyságára**
(`long_mfe_fw60` / `short_mfe_fw60` abszolút értéke).

Ez **nem** az `epic_052` újranyitása. Ott a hipotézis a *szerző-skill* volt
("kit érdemes követni"), amit az `epic_051` FDR-korrekció után elvetett — az
törölve marad. Itt nincs szerző, nincs irány-extrakció, nincs LLM: egy
aggregált idősor és egy szigorú mérés.

## A hipotézis

> A Reddit-forgalom többnyire **reagál** az árra, nem előzi. Ezért nem az
> irányt jósoljuk, hanem a **magnitúdót**: "most sokan beszélnek róla" →
> nagyobb 60 perces kilengés.

Ez illeszkedik a targetünkhöz, ami folytonos MFE-nagyság, nem előjel.
Az előjeles hozamot is megmérjük, de kontrollként, nem elvárásként.

## Gate

Ha a magnitúdó-ág **sem** éli túl a permutációs placebót és az FDR-t, a Reddit
track véglegesen lezárul — ezúttal az aggregált hipotézisre is. Szentiment-réteg
(NLP/LLM) **csak** pozitív gate után indulhat, külön epicben. A költség ott van,
nem a letöltésben.

## Scope

- `src/data_handling/collectors/reddit/` — új modul (dump ingest, aggregálás)
- `database/analyst_signal/analyst_signal.duckdb` — új `social_hourly` tábla
- `data/analyst_signal/raw/source=reddit/` — raw parquet réteg
- `src/modeling/analyst_signal/` — feature normalizálás + gate mérés
- `_doc_/research/reddit_attention_gate.md` — riport

Nem érinti: `src/strategy/`, `src/trading/`, `src/ui/`, a solusdt pipeline.

## Tasks

- t201: BTC fw60 target feltöltés (`target` tábla üres) → database_agent
- t202: Reddit dump ingest + órás aggregátum (2010 → 2025-12) → database_agent
- t203: 2026-os gap-fill Arctic Shift API-ból → database_agent
- t204: Figyelem-feature réteg — deszezonalizálás + trailing z-score → modeling_agent
- t205: Gate mérés — IC/MI + permutációs placebo + FDR → modeling_agent
- t206: Riport és go/no-go döntés → analyst_agent

Sorrend: (t201 ∥ t202) → (t203 ∥ t204) → t205 → t206

## Key Decisions

- **BTC-only.** A `btcusdt` store 2017-08-17-től 4,73M perces gyertyát tart.
  A SOL-t szándékosan nem érintjük — ha BTC-n nincs jel, SOL-on végképp nincs.
- **Nulla NLP a gate-ben.** Csak darabszám-alapú feature-ök. Ha a legolcsóbb
  réteg nem ad jelet, a drága rétegnek nincs értelme.
- **Score-mezők tiltva a dumpokból.** A `score`, `num_comments`, `upvote_ratio`
  a dumpban a *crawl-időpontbeli* érték, nem a keletkezéskori — ez ugyanaz a
  lookahead csapda, mint a TradingView `updated_at` volt. Kizárólag
  `created_utc`-hez horgonyzott nyers darabszám használható.
- **Raw és derivált külön rétegben.** Raw → parquet, aggregátum → DuckDB.
  Ha a forrás jogilag/technikailag ellehetetlenül, csak a raw-t kell újra
  beszerezni (`plan.md` 2. szakasz elve).
- **2021-től egységes rezsim.** A subreddit-populáció 2013→2021 nagyságrendekkel
  nőtt, a 2023-06 API-lezárás tartósan letörte a forgalmat. Nyers darabszám nem
  hasonlítható korszakok között; a fő mérés 2021-től fut, a korábbi évek
  robusztusság-ellenőrzésnek maradnak.
- **Az `epic_051` validációs fegyelme kötelező.** Permutációs placebo + FDR,
  nem nyers p-érték. Enélkül a gate nem gate.

## Notes

Előzmény és indoklás: [_doc_/research/tradingview_author_skill.md](../../_doc_/research/tradingview_author_skill.md),
`plan.md` (LEZÁRVA — a szerző-skill hipotézisre).
