---
epic: epic_053
id: t204
title: Figyelem-feature réteg — deszezonalizálás és trailing z-score
assignee: modeling_agent
status: done
blocks: [t205]
blocked_by: [t202]
---

## Goal

A nyers órás darabszámból **leak-mentes, rezsim-normalizált** feature-öket
képezni, amiket a t205 mérni fog.

Nyers darabszám önmagában használhatatlan: a subreddit-populáció 2013→2021
nagyságrendekkel nőtt, a 2023-06 API-lezárás pedig tartósan letörte a
forgalmat. Egy naiv `n_comments` feature a Reddit növekedési görbéjét mérné,
nem a piacot.

## Scope

- `src/modeling/analyst_signal/` — új feature modul
- input: `social_hourly` (analyst_signal.duckdb)
- output: órás feature tábla ugyanott (`social_features`)

## Feature-ek

Subredditenként és összevontan:

| feature | definíció |
|---|---|
| `att_z` | `n_items` deszezonalizálva (óra-a-héten profil) majd trailing z-score |
| `att_accel` | `att_z` változása az előző órához |
| `authors_z` | `n_authors` ugyanígy |
| `new_author_share` | `n_new_authors / n_authors`, trailing-normalizálva |
| `items_per_author` | `n_items / n_authors` (intenzitás vs. szélesség) |
| `chars_per_item` | `n_chars / n_items` (hosszú elemzés vs. rövid pánik) |

## ⚠️ Leak-szabályok — kötelező

1. **Minden normalizálás trailing.** Az óra-a-héten profil és a z-score
   baseline **kizárólag `t`-nél korábbi adatból** számolható. Teljes mintás
   átlag/szórás tiltott — az a jövőt csempészi be.
2. **`t-1` lag kötelező**, a projekt feature-konvenciójával egyezően. A `t`
   órában keletkezett kommentek nem használhatók a `t` órabeli bar
   predikciójához.
3. Baseline ablak: 30 nap trailing, minimum 14 nap warmup — a warmup alatti
   sorok NULL-ok, nem 0-k.

## Acceptance Criteria

- [ ] `social_features` tábla feltöltve, órás rács
- [ ] Egyetlen feature sem használ `t`-nél későbbi adatot — ezt egy explicit
      teszt ellenőrzi (shift-teszt: a jövő adat megváltoztatása nem
      változtathatja a múlt feature-értékét)
- [ ] Warmup ablak NULL-lal jelölve, nem 0-val
- [ ] A 2023-06 törés utáni szintugrás a normalizálás után **eltűnik** —
      ezt ábrával vagy statisztikával mutasd meg
- [ ] `ruff check` + `pyright` tiszta

## Notes

[orchestrator] Implementalva szintetikus adaton — 2026-08-21

`src/modeling/analyst_signal/social_features.py` (302 sor) +
`src/modeling/tests/analyst_signal/smoke/` (285 sor, 7 teszt, mind zold).

Architektura: minden szarmaztatott oszlop egyetlen `shift(1)`-en megy at
subredditenkent, a szamitas legvegen. Ez egy fojtopont — ami elotte van,
szabadon hasznalhat az aktualis sort is tartalmazo ablakot, mert a shift
az egeszet egy oraval visszatolja, mielott barmi kimenne.

**A leak-teszt ervenyesitve mutaciós teszttel** (orchestrator, nem az agent):
a szezonalis baseline-t szandekosan teljes-mintas atlagra cserelve
(`cum_sum/cum_count` -> `mean().over()`) a `test_shift_...` es a
`test_lag_...` AZONNAL ELBUKOTT, majd visszaallitas utan ujra 7/7 zold.
A teszt tehat nem vakuum — valodi leaket fog el.

**MEG NEM FUTOTT VALOS ADATON.** A dump-betoltes utan ra kell engedni,
es a ticket AC-je (a 2023-06 szintugras eltunese a normalizalas utan)
csak akkor ellenorizheto.

[orchestrator] VALOS ADATON LEFUTOTT — 2026-08-21

`social_features` tabla (v8 migracio) feltoltve: **315 552 sor**
= 78 888 ora x 4 subreddit, 2017-01-01 -> 2025-12-31. Futasido 5,6 mp.
CLI hozzaadva: `python -m modeling.analyst_signal.social_features
--source dump --kinds comment`. Default `--kinds comment`, mert a posztok
tul ritkak (r/BitcoinMarkets: az orak 61,8%-a nulla).

**A rezsim-AC TELJESULT.** Nyers oras darabszam vs normalizalt att_z,
Bitcoin + CryptoCurrency kommentek:

| ev | nyers atlag/ora | att_z atlag | att_z szoras |
|---|---:|---:|---:|
| 2017 | 184 | +0,326 | 1,244 |
| 2018 | 261 | -0,142 | 1,028 |
| 2019 | 112 | -0,042 | 1,054 |
| 2020 | 116 | +0,157 | 1,117 |
| 2021 | **1229** | +0,086 | 1,168 |
| 2022 | 526 | -0,096 | 1,128 |
| 2023 | 353 | -0,037 | 1,107 |
| 2024 | 156 | +0,040 | 1,109 |
| 2025 | 129 | -0,049 | 1,044 |

A nyers jel 11x-et ingadozik, a normalizalt atlag [-0,14; +0,33] savban
marad, a szoras [1,03; 1,24] savban. A 2023-06 tores eltunt.
A 2017-es sor kisse kiugrik (a szezonalis baseline meg beall) — a fo meres
2021-tol fut, ott ez nem szamit.

**Warmup**: subredditenkent pontosan 336 NULL ora (14 nap x 24), nem 0.

Marad: a validator futtassa a teljes teszt-suite-ot. A t205 ezzel indulhat.

[validator] Lezarva — 2026-08-21
ruff check: tiszta. pyright: 0 error, 0 warning.
pytest src/modeling/tests/analyst_signal/: 20/20 zold.
Az epic lezarult (lasd `done_t206`), tovabbi munka nem indul rola.
