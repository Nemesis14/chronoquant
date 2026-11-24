---
epic: epic_069
id: t907
title: BCH stratégia-session + deploy + predikció backfill
assignee: modeling_agent
status: done
blocks: ["t908", "t905"]
blocked_by: ["t906", "t903"]
---

## Goal
Entry küszöbök kalibrálása és a predikciók élesítése, hogy a UI a BCH jeleket mutassa.

## Scope
`src/strategy/00_run_strategy_session.py`, `src/data_handling/06_trigger_deploy.py`, `config/trading.json` (BCH session id-k), `sync_predictions`.

## Acceptance Criteria
- [x] Stratégia-session long és short BCH-ra a meglévő globális költséggel; `strategy_artifact.json`, `strat.*` táblák, reportok
- [x] `06_trigger_deploy.py` long+short; `predictions` tábla BCH live DB-ben backfillelve és a worker/sync_predictions az új sorokat folyamatosan írja
- [x] `/api/strat/active?asset=bchusdt` és a chart végpont BCH-ra értelmes (küszöbök, jelek)
- [x] A SOL aktív stratégiája és predikciói érintetlenek
- [x] Teszt, ruff, pyright zöld

## Notes

### Session-ek (same_window: calib = opt = 2026-02-01..2026-07-31, max_hold 60, globalis koltseg: maker 2 / taker 5 / slip 1.5 bps + funding)
- `strat_bchusdt_fw60_long_2101_2607` (`--directions long`), `strat_bchusdt_fw60_short_2101_2607` (`--directions short`). Artifactok a SOL konvencioval: `artifacts/lgbm_bchusdt_{l,s}_fw60_2101_2607/strategy/` (a `strat_*` mappakat torolve); `trading.json` `assets.bchusdt.strategy_session_{long,short}_id` = `<model>/strategy`.
- A search a NET (koltseg utani) log return szerint valaszt; a grid `total_fact_log_return` gross.

| | LONG | SHORT |
|---|---|---|
| entry cutoff (kalibralt) | 0,99 | 0,99 |
| tp / sl (best net) | bucket_p75_mfe / none | bucket_mean_mfe / 2.0x_tp |
| n_trades | 203 | 232 |
| gross total log return | +0,0116 | +0,1301 |
| koltseg (fee+slip) / funding | 0,2252 / -0,0012 | 0,2431 / +0,0017 |
| NET log return | **-0,2124** | **-0,1146** |
| compounded return % (koltseg nelkul, gross) | 1,17 | 13,90 |
| win rate | 55,2% | 63,4% |

**FIGYELEM, uzleti kovetkeztetes:** mind a 200+200 setup NET NEGATIV (0 db pozitiv net a gridben). A legjobb gross setupok (pl. long cutoff 0,98 p75/1.0x_tp gross +0,128, 350 trade; short cutoff 0,9 0.75x_bucket_mean/0.5x_tp gross +0,529, 3313 trade) a koltseg miatt veszteseges. A kalibralt/telepitett setup a legkevesbe veszteseges, nem nyereseges. Az eles (live) kereskedes BCH-n nem javasolt; az `active_trade`/jelek vizualizaciora szolgalnak. Dontes: orchestrator/user.

### Deploy es predictions
- `06_trigger_deploy.py --asset-id bchusdt` long + short: 2 pending deployment. Az `assets.bchusdt` blokk felvetele utan az auto-sync azonnal bekapcsolt; `POST /api/jobs sync_predictions {asset_id: bchusdt}` job (queue, SOL sync kozben nem serult) lefutott: cutover OK, `predictions replaced rows=2 933 281`, a deployment `d1e2b6c2...` active. Worker ujrainditas nem kellett.
- `predictions` BCH live: 3 076 405 sor, latest 2026-11-07 09:24 (a backfill + a folyamatos live sync), long/short model id stampelve; auto_sync true, worker alive.
- Riportok: `pipeline --step analyze` mindket modellre -> `reports/04_strategy` elkeszult.

### Ellenorzes
- `/api/strat/active?asset=bchusdt`: cutoff 0,99/0,99, metrikak az artifactbol. `/api/market/chart?asset=bchusdt`: 1441 bar, long_pct/short_pct, thresholds {0.99, 0.99}, 24h ablakban 36 long-jel (>=0,99), 0 short-jel.
- `/api/strat/active?asset=solusdt` byte-azonos a deploy elottivel; SOL predictions folyamatosan frissul (SOL sync nem serult).
- Megfigyeles: a chart `active_trade` BCH-ra egy LONG-ot mutat (entry 268,49 = az ablak eleje) SL/TP nelkul: a journal asset-fuggetlen (t903 ismert korlat), nem BCH-jel.
- `pytest src/strategy src/api` 89 passed; ruff 0; pyright 0 hiba. Nem nyultam a SOL DB-hez/artifactokhoz.

[validator] Done — 2026-11-07. Technikailag rendben. Üzleti megjegyzés: a BCH setupok net negatívak (lásd fent), live kereskedés nem javasolt.
