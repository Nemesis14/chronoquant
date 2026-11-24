---
epic: epic_069
id: t906
title: BCH long + short modell: snapshot -> sample -> FE -> search -> train -> predict -> analyze
assignee: modeling_agent
status: done
blocks: ["t907"]
blocked_by: ["t901", "t902"]
---

## Goal
A teljes modellfejlesztési folyamat BCH-ra, `_doc_/database_and_code_doc/0004_model_lifecycle.md` és `model_lifecycle_skill.md` szerint.

## Scope
`config/models.json` (új modellek), `src/modeling/pipeline.py`, `artifacts/lgbm_bchusdt_{l,s}_fw60_<range>/`, lab DB (`database/bchusdt/bchusdt_lab.duckdb`).

## Acceptance Criteria
- [x] `models.json` v4: `lgbm_bchusdt_l_fw60_<start>_<end>` és `..._s_...` (a t901 ajánlása szerinti ablak, alapból a SOL-é), `asset_id: bchusdt`, `active: true`
- [x] Snapshot (`05_create_snapshot.py --asset-id bchusdt`), majd mindkét modellre: setup, sample, feature_engineering, search (Optuna, a normál stage, patience szerint), train, predict, analyze
- [x] A hosszú lépések (search) háttérben fussanak, a Notes-ban az eredmények: valid top10_lift, train/valid lift, trial szám, futásidő
- [x] Reportok az `artifacts/<id>/reports/` alatt (sampling, feature_engineering, hyperparameter_search); `reg.*` provenance rendben (`registry_validator`)
- [x] Ha a valid lift nem pozitív/nem szignifikáns, ezt tényként a Notes-ba, ne finomhangolj túl; ez a döntés az orchestrátoré/useré
- [x] pytest (modeling), ruff, pyright zöld

## Notes

### Futtatott lépések
- `config/models.json`: `lgbm_bchusdt_l_fw60_2101_2607` + `..._s_...` (`asset_id: bchusdt`, `active: true`, a SOL champion sampling-blokkja: train 2021-01-01..2026-01-31, valid 2026-02-01..2026-07-31, embargo 240, purge 60, seed 42; csak a 2021-es kezdés, a 2019-2020 szakasz kimaradt).
- Snapshot: `05_create_snapshot.py --asset-id bchusdt --start 2021-01-01 --end 2026-07-31` (a SOL snapshot paramétereivel azonos) -> `bchusdt_fw60_2101_2607__51471332`, 2 933 281 sor, 247 s. `feature_set_hash` azonos a SOL-éval.
- setup, sample (48 884 sor: train 44 563 / valid 4 321, hourly), feature_engineering, search, train, predict, analyze mindkét modellre, `PYTHONIOENCODING=utf-8`.
- Search beallitas: `--stage` nelkul (normal), 100 trial max. A jelenlegi kodban (`lgbm_search.py`) NINCS patience/epsilon korai megallas ("no patience early-stopping"), a SOL-lal azonos futas; mindket modell mind a 100 trialt lefuttatta.

### Eredmenyek
| | LONG | SHORT |
|---|---|---|
| feature szam (FE utan, 208-bol) | 138 | 134 |
| trial szam | 100 | 100 |
| search futasido | 672 s (~11 perc) | 389 s (~6,5 perc) |
| legjobb trial | #99 | #73 |
| search objective (valid ratio p925 / p075) | 2,118 (valid p925), train 2,628, gap 0,51 | 1,339 (valid p075), train 1,404, gap 0,064 |
| spearman (search, valid) | 0,316 | 0,100 |
| train / predict ido | ~8 s / ~24 s, 258 fa | ~8 s / ~31 s, 161 fa |
| reg.models status / oos_metric | predicted / -2,118 | predicted / -1,339 |

Top10 lift (sajat szamolas a `__pred` + target tablakbol; top 10% predikcio atlag y minus teljes atlag):
| | LONG train | LONG valid | SHORT train | SHORT valid |
|---|---|---|---|---|
| top10 atlag y | 0,01304 | 0,00793 | -0,01511 | -0,00944 |
| teljes atlag y | 0,00562 | 0,00411 | -0,00592 | -0,00432 |
| lift (kulonbseg) | +0,00743 | +0,00381 | -0,00919 | -0,00512 |
| lift (arany) | 2,32 | 1,93 | 2,55 | 2,18 |
| spearman (sample) | 0,323 | 0,304 | 0,369 | 0,368 |
Valid, teljes percfelbontasu ablak (n=259 200): long top10 arany 1,98, short 2,08. Mindket irany valid lifje pozitiv es a nagysagrend a train mellett konzisztens (nincs jelentos tulillesztes). Formalis szignifikancia-tesztet nem futtattam. Megjegyzes: a search sajat objektive (p925/p075 kvantilis-arany) kulonbozik a fenti top10 lifttol, a short search-spearman (0,10) mas szamitas, mint az en sample-spearmanem (0,37); a short valid minosege a search-metrika alapjan szerenyebb (p075 1,34), a top10 mutato alapjan jo. Ezt a dontest az orchestrator/user hozza meg (nem finomhangoltam).

### Sanity tesztek BCH-n (t901 jelzes), ellenorzes
- `test_feat_close_position_differs_from_current_close`: NEM adat/feature-hiba. A teszt az `ORDER BY open_time LIMIT 500` elso 500 percet nezi, ez a 2019-12-19-i indulas ritka piaca (89/500 bar close == high vagy low, sok egymas utani azonos pozicio). Direkt lag-ellenorzes: 2021-01-tol 3 074 069 soron a `feat_close_position[t]` 100%-ban egyezik az elozo bar `close_position`-jevel (3 074 069 / 3 074 069), a same-bar egyezes 96 455 sor (egymast koveto, azonos extrem-pozicioju barok). A t-1 lag helyes. A kuszob SOL-ra hangolt es az elso 500 percre erzekeny.
- `test_target_short_mfe_range`: max 0,1525 pontosan 3 bar: 2021-05-19 13:44, 13:58, 13:59 (maj. 19-i crash outlier). Ennel nagyobb mas ertek nincs (kovetkezo 0,0866, 2024-03-05). Valos piaci esemeny, nem adathiba; a kuszob (<0,1) SOL-specifikus. Nem gyogyitottam, a tesztet nem modositottam.
- 2021-05-19 outlier: nem vagtam ki. A sampling hourly (oranként 1 sor), a 13:xx ora mintaja 13:24 (nem erintett), a 14:xx 14:13; a 13:44-13:59 outlier barok egyike sem kerult a samplebe, igy a modell tanitasat nem erinti (a snapshotban/predict range-ben benne van).

### Egyeb
- Elso search-inditasnal a ket parhuzamos folyamat egyike a `registry.duckdb` zar miatt elbukott (IOException, indulaskori `ensure_registry`); ujrainditva, eredmenyt nem veszitett. Tanulsag: a registry-erinto lepeseket (sample/FE/train/predict/analyze) sorban futtasd, parhuzamosan csak a search megy.
- `registry_validator` (`python -m src.data_handling.store.registry_validator`, repo gyokerbol): OK, config es registry konzisztens.
- Riportok: `artifacts/lgbm_bchusdt_{l,s}_fw60_2101_2607/reports/{01_sampling,02_feature_engineering,03_hyperparameter_search}`; `04_strategy` kihagyva (strategia artifact t907-ben keszul).
- `src/` kod nem valtozott. SOL DB/artifactok erintetlenek. Tesztek: `pytest src/modeling` 48 passed, ruff 0, pyright 0 hiba.
- Kovetkezo (t907): strategia session + deploy; `config/trading.json` `assets.bchusdt` blokk felvetele.
