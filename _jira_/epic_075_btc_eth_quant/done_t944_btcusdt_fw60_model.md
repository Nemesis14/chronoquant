---
epic: epic_075
id: t944
title: BTCUSDT fw60 long/short modell + stratégia
assignee: modeling_agent
status: done
blocks: [t945, t946]
blocked_by: [t939]
---

## Goal
A BCH bevezetés (epic_069, `lgbm_bchusdt_l/s_fw60_2101_2607`) mintájára: `lgbm_btcusdt_l/s_fw60_2101_2607` — snapshot, sampling, training, hyperparam search, isotonic kalibráció + Optuna stratégia sweep, artifactok, reg bejegyzések, riportok a UI-ra. Ugyanaz a train range/holdout fegyelem mint SOL/BCH. A worker-t ne állítsd le; ha DB lock van, várj/retry.

## Scope
`src/modeling/`, `src/strategy/`, `artifacts/`, lab DB, model_lifecycle_skill

## Acceptance Criteria
- [x] Modell + stratégia artifact, riportok
- [x] Eredmények összevetése SOL/BCH-val a ticketben
- [x] Doksi: 0004 érintett része (+ overview); models_doc: analyst_agent feladata (lásd lent)

## Notes

**2026-11-23 (modeling_agent): kész, validálásra vár.**

### Eredmény
- Modellek: `lgbm_btcusdt_l_fw60_2101_2607`, `lgbm_btcusdt_s_fw60_2101_2607`; `reg.models` státusz: `predicted`; feature_set `fs_btcusdt_fw60_l__b30dda63` / `fs_btcusdt_fw60_s__7234df3d`.
- Snapshot: `btcusdt_fw60_2101_2607__51471332`, 2 933 281 sor, 2021-01-01 → 2026-07-31 00:00 (a BCH-val azonos range; holdout-2 ≥ 2026-08-01 kívül esik).
- Sample: 48 884 óránkénti sor (44 563 train / 4 321 valid), train 2021-01..2026-01, valid 2026-02..2026-07, embargo 240 / purge 60 (a BCH blokk másolata).
- FE: 147 (long) / 144 (short) kiválasztott feature a 208-ból (BCH 138/134, SOL 133/137).
- Search: 100 trial/irány, stage nélkül (a BCH/SOL budget).
- Stratégia: `strat_btcusdt_fw60_long_2101_2607`, `strat_btcusdt_fw60_short_2101_2607` (`reg.strategies` candidate), 200+200 grid, kalibráció = opt ablak = 2026-02-01..2026-07-31 (`same_window`, mint a BCH). Fájlok: `artifacts/lgbm_btcusdt_{l,s}_fw60_2101_2607/strategy/`.
- Riportok: `artifacts/lgbm_btcusdt_{l,s}_fw60_2101_2607/reports/{01_sampling,02_feature_engineering,03_hyperparameter_search,04_strategy}` (4+4).
- `config/models.json`: a két bejegyzés **`active: false`**. Ok: a BTC a `trading.json` üres blokkja miatt sync-aktív, és `active: true` esetén a worker `sync_predictions` azonnal felvenné a modellt. **A t946 állítsa `true`-ra** a `trading.json` session id-kkel (`lgbm_btcusdt_l_fw60_2101_2607/strategy`, `lgbm_btcusdt_s_fw60_2101_2607/strategy`) és a `06_trigger_deploy` lépéssel együtt. A `trading.json`-hoz nem nyúltam.
- `registry_validator`: OK.

### Összevetés SOL / BCH / BTC
Regressziós modellek (nincs AUC), ezért a BCH-vs-SOL doksi (`_doc_/models_doc/bch_vs_sol_champion_2101_2607.md`) metrikáit használom. Lift/Pearson: `snap ⋈ model.__pred`, minden perc; a SOL/BCH értékeket ugyanazzal a scripttel számoltam újra, a doksival egyeznek.

| | SOL L | SOL S | BCH L | BCH S | **BTC L** | **BTC S** |
|---|---|---|---|---|---|---|
| top10 lift train / valid | 2,39 / 2,02 | 2,35 / 2,02 | 2,29 / 1,98 | 2,41 / 2,08 | **2,30 / 2,11** | **2,39 / 2,02** |
| top1% lift train / valid | 4,70 / 3,38 | 4,40 / 3,38 | 2,47 / 2,77 | 4,62 / 2,95 | **3,76 / 3,30** | **4,29 / 3,30** |
| Pearson valid | 0,39 | 0,35 | 0,32 | 0,35 | **0,39** | **0,38** |
| search valid ratio (L p925 / S p075) | 2,255 | 1,523 | 2,118 | 1,339 | **2,345** | **1,491** |
| train-valid gap | 0,39 | -0,07 | 0,51 | 0,06 | **0,10** | **0,01** |
| Spearman / decilis-monoton. | 0,303 / 0,89 | 0,112 / 0,78 | 0,316 / 1,00 | 0,100 / 0,44 | **0,326 / 0,89** | **0,083 / 0,67** |
| best_iteration | – | 72 | 235 | 146 | 161 | 833 |

**Rangsoroló erő:** a BTC a SOL szintjén van, a long irányban kicsit jobb. A valid top10 és top1% lift magasabb, mint a BCH-é, a túlillesztés kicsi (gap 0,10 / 0,01). A short search-metrika (Spearman 0,08) itt is gyenge, mint a SOL/BCH-nál.

**Stratégia (költségekkel: maker 2 / taker 5 / slip 1,5 bps + funding):**

| | best setup | n trade | gross log ret | **net log ret** | win rate | net>0 a gridben |
|---|---|---|---|---|---|---|
| BTC long | cutoff 0,99, TP 0,75x_bucket_mean, SL none | 203 | +0,144 | **-0,054** | 75,9% | 0 / 200 |
| BTC short | cutoff 0,99, TP bucket_mean_mfe, SL none | 164 | -0,007 | **-0,174** | 67,1% | 0 / 200 |
| BCH long | 0,99, bucket_p75_mfe, none | 203 | +0,012 | -0,212 | 55,2% | 0 / 200 |
| BCH short | 0,99, bucket_mean_mfe, 2,0x_tp | 232 | +0,130 | -0,115 | 63,4% | 0 / 200 |
| SOL long / short* | 0,90 / 0,92 | 2682 / 2988 | +0,470 / +0,168 | – | 36% / 34% | – |

\* A SOL artifact még a régi, gross (`total_fact_log_return`) objektívvel készült, net értéke nincs az artifactban, ezért csak gross alapon vethető össze.

A BTC long a legkevésbé negatív. A gross edge +7 bps/trade, ez a ~11 bps-os körút-költség alatt marad. A gross top-5 (pl. cutoff 0,9, 1679 trade, gross +0,31) nettóban rosszabb, mert a sok trade költsége elviszi. **Verdikt:** mint a BCH-nál, a jel használható megjelenítésre és jelfigyelésre, de az élő kereskedés nem javasolt. A korlátok is ugyanazok: `same_window`, nincs OOS, 6 hónapos valid.

### Parancs-recept (t945: `btcusdt` → `ethusdt`)
Windows / Git Bash, repo gyökér, `export PYTHONIOENCODING=utf-8`. A worker DuckDB lockja miatt minden lépést lock-hiba esetén újra kell futtatni (20 s várakozással; a lépések idempotensek). A worker-t ne állítsd le.
```bash
A=ethusdt
# 1) target rebuild (a worker nem frissíti) + quant_train  (~0,5 + ~5 perc)
uv run python src/data_handling/02_sync_pipeline.py --asset-id $A --tables targets
uv run python src/data_handling/03_build_quant_train.py --asset-id $A
# 2) snapshot (~4 perc) -> a logból a snapshot_id (várhatóan ${A}_fw60_2101_2607__51471332, lásd lent)
uv run python src/data_handling/05_create_snapshot.py --asset-id $A --start 2021-01-01 --end 2026-07-31
# 3) config/models.json: a lgbm_btcusdt_{l,s}_fw60_2101_2607 bejegyzések másolata; btcusdt->ethusdt, BTC->ETH,
#    sampling.snapshot_id = a 2) lépés id-ja, "active": false
# 4) pipeline: a registry-lépések sorban futnak, a search mehet párhuzamosan (long+short, ~10 perc)
for d in l s; do for s in setup sample feature_engineering; do
  uv run python src/modeling/pipeline.py --model lgbm_${A}_${d}_fw60_2101_2607 --step $s; done; done
uv run python src/modeling/pipeline.py --model lgbm_${A}_l_fw60_2101_2607 --step search --n-trials 100 &
uv run python src/modeling/pipeline.py --model lgbm_${A}_s_fw60_2101_2607 --step search --n-trials 100; wait
for d in l s; do for s in train predict; do
  uv run python src/modeling/pipeline.py --model lgbm_${A}_${d}_fw60_2101_2607 --step $s; done; done
# 5) stratégia: irányonként külön session (~4 perc/irány)
for dir in long short; do
  uv run python src/strategy/00_run_strategy_session.py \
    --long-model lgbm_${A}_l_fw60_2101_2607 --short-model lgbm_${A}_s_fw60_2101_2607 \
    --session-id strat_${A}_fw60_${dir}_2101_2607 \
    --calib-start 2026-02-01 --calib-end 2026-07-31 --opt-start 2026-02-01 --opt-end 2026-07-31 \
    --directions $dir; done
# 6) a session-mappák a modell strategy/ mappájába kerülnek (SOL/BCH layout), majd a temp mappa törölhető
mkdir -p artifacts/lgbm_${A}_l_fw60_2101_2607/strategy artifacts/lgbm_${A}_s_fw60_2101_2607/strategy
cp -p artifacts/strat_${A}_fw60_long_2101_2607/*  artifacts/lgbm_${A}_l_fw60_2101_2607/strategy/
cp -p artifacts/strat_${A}_fw60_short_2101_2607/* artifacts/lgbm_${A}_s_fw60_2101_2607/strategy/
rm -rf artifacts/strat_${A}_fw60_{long,short}_2101_2607
# 7) riportok; a logban NE legyen "[analyze] WARNING" (live DB lock esetén futtasd újra)
for d in l s; do uv run python src/modeling/pipeline.py --model lgbm_${A}_${d}_fw60_2101_2607 --step analyze; done
# 8) ellenőrzés
uv run python -m src.data_handling.store.registry_validator
```
Összevetéshez használható script (read-only, train/valid top10/top1 lift + Pearson): a SOL/BCH/BTC számok ezzel készültek. Logika: `snap."<snapshot>" JOIN model."<id>__pred" USING(open_time)`, short-nál előjelfordítással, lift = top q90/q99 predikció átlag-MFE-je / átlag.

### Megfigyelések
- **Snapshot hash ütközés (nem hiba):** a BTC `content_sha256` = a BCH-éval (`51471332…`). A `compute_content_sha256` csak az `open_time` hash-összegét, a sorszámot, a range-et és az oszlopkészletet ujjlenyomatozza, az értékeket nem. A reuse `(asset_id, hash)` kulcsú, így a BTC saját snapshotot kapott. Ellenőriztem: BTC átlag long MFE 35,8 bps, BCH 55,2 bps, tehát valódi BTC adat. Az ETH várhatóan ugyanezt a hash-t kapja. Ha a hash-nek értéket is kellene azonosítania, az külön database_agent ticket.
- A target rebuild során a btcusdt live DB megnyitásakor lefutott a t941 v8-as migrációja (`create_scout_signals`). Ehhez nem nyúltam, csak jelzem.
- Az `analyze` lépésben a short `02_feature_engineering` riport első futásra live DB lock miatt elbukott. Az `analyze` ezt csak WARNING-ként jelzi, ezért újrafuttattam, és tisztán lefutott.
- A `reg.artifacts` stratégia-bejegyzései az `artifacts/strat_btcusdt_…` mappára mutatnak (a BCH/SOL-nál is így van, az API a `trading.json` `<model>/strategy` id-ját használja).

### Doksi
- `0004_model_lifecycle.md`: az onboarding 8. sorában a pontos strategy parancs és a `<model>/strategy` elhelyezés; új megjegyzés a target rebuildről, az `active: false`-ról, a snapshot hash-ről és az `analyze` WARNING-ról.
- `0000_project_overview.md`: BTC a modell-listában, „11 modell”, BTC eredmény-bekezdés.
- `_doc_/models_doc/`: az analyst_agent-é. Javaslat: `btc_vs_sol_bch_champion_2101_2607.md` a fenti táblákkal (külön ticket, vagy az ETH után közös SOL/BCH/BTC/ETH doksi).

### Tesztek
- `uv run pytest src/modeling src/strategy -q`: 83 passed.
- `uv run ruff check src/modeling src/strategy`: tiszta. Kódot nem módosítottam (csak `config/models.json` + doksi), ezért pyright-futás nem szükséges.

### Validáció (validator_agent, 2026-11-23) — ELFOGADVA
- `config/models.json`: a két BTC bejegyzés a BCH-blokk pontos másolata (asset_id, snapshot_id, leírás eltér), `active: false`; 11 modell.
- Artifactok: `manifest.json`, `model.pkl` (LGBMRegressor, 147 / 144 feature = `features.json`), `params.json`, `feature_engineering/`, `search/` (100+100 trial_curves/logs), `strategy/` (7 fájl, mint BCH), 4+4 riport; mind a 8 report manifest átmegy a `reports.schema.Manifest` validáción, minden hivatkozott fájl létezik.
- Snapshot `btcusdt_fw60_2101_2607__51471332`: 2 933 281 sor, 2021-01-01 → 2026-07-31 00:00 (nincs holdout-2 adat). Sample: 44 563 / 4 321, a BCH-val azonos split / embargo 240 / purge 60.
- Search: L #79 valid_ratio_p925 2,345, gap 0,10, best_iter 161; S #8 valid_ratio_p075 1,491, gap 0,01, best_iter 833 — egyezik a táblával.
- Stratégia: `same_window`, fit 2026-02-01..2026-07-31, 200 grid, `net_log_return` objektív (mint a BCH); költségek = `config/strategies.json` (2 / 5 / 1,5 bps).
- Metrikák újraszámolva a lab DB-ből (`snap ⋈ __pred`, short előjelfordítással, q90/q99): BTC L top10 2,302 / 2,105, top1% 3,757 / 3,299, Pearson valid 0,392; BTC S 2,388 / 2,020, 4,285 / 3,296, 0,382; BCH L/S is egyezik a táblával.
- `registry_validator`: OK. `reg.models` predicted, `reg.strategies` candidate.
- `reg.artifacts` → törölt `artifacts/strat_btcusdt_*` mappa: a BCH-nál is így van (meglévő konvenció). Az API `_artifact_path` `is_file()`-lal ellenőriz, így nem tör el semmit, a `trading.json` / `registry.py` a `<model>/strategy` utat használja.
- Doksi: 0004 rendben. A 0000 overview-ban a BTC-t „modell nélkül”-ként leíró 3 sort (intro, asset-tábla, Conventions) a validátor pontosította: „modell kész, `active: false`, élő predikció nélkül”.
- Tesztek: `uv run pytest src/modeling src/strategy -q` 83 passed; `ruff check src/modeling src/strategy` tiszta.

Follow-up (nem blokkoló):
1. **Snapshot hash:** a BTC és BCH `content_sha256` azonos (`51471332…`), mert a hash nem fedi le az értékeket. Külön database_agent ticket, ha a hash-nek tartalmat is azonosítania kell (az ETH is ugyanezt fogja kapni).
2. **API fallback (ui_agent):** `src/api/services/strat.py::_artifact_path` a `reg.strategies.model_id_long` alapján old fel, ezért egy `trading.json`-ban nem szereplő *short* session (pl. BTC short a t946 előtt) a long modell `strategy_artifact.json`-ját kapná. A t946 után (aktív session) nem jelentkezik.
3. Apró eltérés: a 02_feature_engineering riport „205 vizsgált” feature-t ír, a ticket „208-ból”-t.
