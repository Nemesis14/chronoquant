
## Modellezesi feladatok

| Task | Prioritas | Reszletes leiras | Megjegyzes |
|---|---:|---|---|
| SOL modellek ujraepitese futures OHLCV adaton — FOLYAMATBAN | P1 | Step 1-8 kesz (lasd alatta). Kovetkezo: step 9 prediction sync, majd sweep + promotion. | Sample: base_solusdt_fw60_futures_v1. Modellek: lgbm_solusdt_l_fw60_q90_local_v4, lgbm_solusdt_s_fw60_q10_local_v4. Artifacts: model.pkl + features.json + params.json mindketto keszult. |

### v4 retrain elorehaladas

| Step | Allapot | Megjegyzes |
|---|---|---|
| 1. Adataudit | KESZ | 3,016,709 sor, 2020-09-14 - 2026-06-10, futures OHLCV |
| 2. Sample + split | KESZ | base_solusdt_fw60_futures_v1, 5 fold, holdout: 2025-06-10 - 2026-06-10 |
| 3. Model registry | KESZ | v4 long + short hozzaadva config/models.json-hoz (active=false) |
| 4. Parquet export + Drive | KESZ | 2.68 GB, samples/base_solusdt_fw60_futures_v1/dataset.parquet |
| 5. Colab search (smoke + explore) | KESZ | 65 trial/modell (5 smoke + 60 explore), artifacts Drive-ra mentve |
| 6. Artifacts visszamasolas | KESZ | models/lgbm_solusdt_l_fw60_q90_local_v4/search/ + short ugyanigy, 382 fajl |
| 7. Search review | KESZ | Long: trial#1 val_ll=0.2667 gap=0.029 prauc=0.359 — Short: trial#3 val_ll=0.2675 gap=0.023 prauc=0.338 |
| 8. Final fit | KESZ | long: 1961 fa / 41k sor, short: 2405 fa / 41k sor — model.pkl + features.json + params.json keszult |
| 9. Prediction sync | VARANDO | sync_predictions futtatasa a v4 modellek aktivalasa elott/utan |
| 10. Strategy sweep | VARANDO | sweep_strategy.py long + short oldalra (start 2024-01-01, end 2025-06-10) |
| 11. Config update | VARANDO | models.json v4 active=true, v3 active=false; env.json runtime model_id; strategies.json |
| 12. UI verification | VARANDO | Dashboard ellenorzese az uj modellel + strategiaval |

## Adatbazis / predikcios sema feladatok

| Task | Prioritas | Reszletes leiras | Megjegyzes |
|---|---:|---|---|
| `solusdt_1m_predictions.signal` oszlop kivezetese | P2 | Ellenorizni, hogy sem UI, sem trading runtime nem hasznalja dontesi inputkent; majd migracio/doksi frissites utan torolni vagy deprecated allapotbol eltavolitani. | Ok: a valodi trading dontes a `trading_signals.decision` + `reason` mezokben van, a prediction tabla `signal` oszlopa csak legacy threshold-label. |
| `solusdt_1m_predictions.prediction` kompatibilitasi oszlop kivezetese | P2 | UI, chart, backtest es report kodot atallitani explicit long/short probability oszlopokra (`<long_model_id>_p`, `<short_model_id>_p`, vagy normalizalt `long_probability`/`short_probability` view/output), majd a generikus `prediction` oszlopot kivezetni. | Ok: a `prediction` jelenleg csak a runtime modell probability masolata; redundans, de sok kod meg ezt varja kanonikus probability oszlopkent, ezert refaktor nelkul nem torolheto. |
