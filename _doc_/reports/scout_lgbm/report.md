## Cél

**t932 (epic_073):** az 5300 módszertan **H1** elsődleges hipotézise (P1 család): egy LightGBM a kézi, `t_c`-ig ismert prefix-jellemzőkön és az order flow-n jobban rangsorolja-e az impulzust, mint a skalár logit (S0 + C). A konfiguráció **előre rögzített** (1. forduló, nincs hangolás a CV-n), a közös `scout_bench` keretben fut (azonos foldok, metrikák, permutáció, SPA, szimuláció; `_doc_/methodology_doc/5300_impulse_scout.md`, kód: `src/reports/builders/scout_models_lgbm.py`).

Adat: `research."scout_events_v1"` (a15, W4h_H8h, train split) **2170 esemény**, **308 impulzus** (14.2%), assetek: btcusdt,ethusdt,solusdt. Fold-hash `0d90521c16be85de`, run `6406880ad101`.

> **Holdout-védelem:** az eseménykeret csak a train split; az 1m árak és az order-flow sorok az OOS kezdete (2026-02-01) előtt levágva, mielőtt bármely jellemző készül; a holdout-2 sorai és címkéi nem kerültek a memóriába.

## Modell és jellemzők

- **Tanuló:** LightGBM, 3 osztályos `multiclass` cél (impulzus / hamis indulás / oldalazás); score = P(impulzus). Rögzített hiperparaméterek: `{'objective': 'multiclass', 'n_estimators': 300, 'learning_rate': 0.03, 'num_leaves': 7, 'max_depth': 3, 'min_child_samples': 40, 'subsample': 0.8, 'subsample_freq': 1, 'colsample_bytree': 0.7, 'reg_lambda': 5.0, 'min_split_gain': 0.0, 'event_variant': 'a15', 'context_asset': 'btcusdt'}`; config-hash `b35177b937e0`.
- **50 jellemző** hét csoportban (mind a `t_c`-ig zárt 1m bar-okból; címke, `τ`, más asset eseménye / címkéje nincs köztük):
  - *S0 + C* (7): `a_sigma`, `velocity_1h`, `pretrend_3d`, `log_sigma_1m`, `drop_depth`, `volume_ratio`, `drop_depth_sigma`
  - *visszapattanás-alak* (9): `reb_peak_sigma`, `reb_pullback_sigma`, `reb_peak_pos`, `reb_eff`, `reb_r2`, `reb_half_share`, `reb_ret15_sigma`, `reb_retest_sigma`, `reb_rv_ratio`
  - *esés-szakasz* (4): `dec_top_pos`, `dec_r2`, `dec_last1h_sigma`, `dec_max_drop_sigma`
  - *order flow* (20): `tb_ratio_prefix`, `tb_ratio_decline`, `tb_ratio_rebound`, `tb_ratio_diff`, `tb_ratio_slope_1h`, `tb_ratio_last1h`, `tb_ratio_low15`, `delta_decline_norm`, `delta_rebound_norm`, `trades_rel_prefix`, `trades_rel_decline`, `trades_rel_rebound`, `trade_size_rel`, `vol_decline_rel`, `vol_rebound_rel`, `vol_rebound_over_decline`, `vol_last1h_rel`, `vol_peak_rel`, `vol_peak_pos`, `qv_logz_prefix`
  - *S/R távolság* (3): `sr_hi3d_over_a`, `sr_hi7d_sigma`, `sr_low_vs_7d_sigma`
  - *napszak* (3): `tod_sin`, `tod_cos`, `weekend`
  - *BTC-kontextus* (4): `btc_ret1h_z`, `btc_ret4h_z`, `btc_ret24h_z`, `btc_rv_ratio`
- Az alak- és távolság-jellemzők `log(ár / low[t0])` `σ_W` egységben (az amplitúdó megmarad); a volumen / flow a t930 arány-jellemzői (asset-saját 7 napos bázis). A BTC-kontextus 1h / 4h / 24h log-hozam a BTC saját 24 órás 1m volatilitásával normálva (BTC-eseménynél a saját múlt).
- **Megjegyzés (5300):** a módszertan az asset-közi jellemzőt (BTC-kontextus) kísérletinek nevezi; a ticket a P1 előre rögzített készletébe sorolta. A hozzájárulását a jellemző-fontosság mutatja (lent).

## 1. Eredmény a baseline-hoz képest

A ΔAUC CI nap-klaszter bootstrap; a `perm_p` nap-csomagos permutáció **N = 1000**, teljes refit (a baseline és a modell is újratanul minden permutációban); a Holm-korrekció a futásban regisztrált elsődleges hipotézis(ek)re vonatkozik (itt csak H1; a teljes H1-H3 Holm a t936 leaderboardon). A felső 20% / 10% az **elsődleges nézet: teszt-foldon belüli rangsor**; a `_trthr` sorok a másodlagos, train-küszöbös nézet.

{{table:comparison}}

{{table:gates}}

{{chart:null_dauc}}

## 2. Blokk- és asset-bontás

A blokkonkénti ΔAUC és a felső 20% precizitás (elsődleges nézet) a go-feltételek 5. és 6. pontjához.

{{chart:blocks}}

{{table:blocks_table}}

{{table:assets_table}}

{{table:calibration_asset}}

## 3. Jellemző-fontosság

LightGBM gain, a 6 fold-modell (azonos foldok és seed) átlagos részesedése ± szórás. A fontosság a modell belső használatát mutatja, nem oksági vagy marginális hatást.

{{chart:importance}}

{{chart:importance_groups}}

{{table:importance_table}}

## 4. Triple-barrier szimuláció (díjak után)

Belépés a `t_c` utáni első 1m bar nyitóján, cél `P*`, stop `low[t0]`, időkorlát `t_c + H`; költségek a `config/strategies.json` `costs` szekciójából (`{"entry_order_type": "taker", "exit_order_type": {"sl": "taker", "timeout": "taker", "tp": "maker"}, "maker_bps": 2.0, "slippage_bps": 1.5, "taker_bps": 5.0}`). CI: nap-klaszter bootstrap.

{{table:trades}}

## Értelmezés

- **Rangsorolás:** AUC 0.6233 vs baseline 0.6361, ΔAUC **-0.0128** (95% CI [-0.0460; +0.0228]), permutációs p = 0.7203. Blokkonként ΔAUC > 0: 2 / 6; a legjobb blokk nélkül átlag -0.0213.
- **Valószínűség-minőség:** 3 osztályos log-loss 0.7758 vs 0.8009 (ΔLL -0.0251); kalibrációs meredekség 0.53.
- **Szelekció (elsődleges nézet):** felső 20% precizitás 0.221 vs 0.241 (lift 1.55; jobb blokk 1 / 6); felső 10% 0.248 vs 0.248.
- **Gazdasági előszűrő:** felső 20% nettó -6.8 bp / esemény (CI [-30.6; 16.9]) vs baseline 2.7.
- **Jellemzők:** a gain legnagyobb része: order flow 0.29, S0 + C 0.18, visszapattanás-alak 0.18.
- **SPA (validátor-javítás, 2026-11-21):** a bináris log-loss a baselinenél rosszabb (0,4048 vs 0,3938), így az SPA statisztika T = 0 és **p = 1,0**; a korábban közölt 0,021 a `spa_test` szigorú `>` összehasonlításának hibája volt (javítva `>=`-re, Hansen-konvenció). A log-loss javulás csak a 3 osztályos log-lossra igaz.
- **Döntés (5300):** **NO-GO** — bukott feltételek: `dauc,top20,stability,spa,holm,econ`. A Holm-korrekció a három elsődleges (H1-H3) p-értéken a t936 leaderboardon véglegesül; az 1. forduló konfigurációja rögzített, utólag nem módosítható (5300 iterációs szabályok).

## t936 frissítés (2026-11-22, leaderboard-univerzum)

Az SPA és a Holm értékek a **t936** együttes kiértékeléséből származnak (`_doc_/reports/scout_leaderboard/`): a teljes 1. körös univerzum (6 modell a baseline ellen, bináris log-loss, nap-összegzett) SPA p = **0.2300**, legjobb modell: `scout_hazard_r1`. Az A7 kapu csak a kiválasztott legjobb modellre vonatkozik (a többinél n/a, `not_spa_best`), a B kapu a felső 20% és 10% szimulációt is nézi. A táblákban a `spa_p` / `holm_p` / kapuk ennek megfelelően frissítve; a fenti szöveg korábbi, futásonkénti SPA-értékei elavultak.

- `scout_lgbm_r1`: SPA-legjobb: nem, Holm p (H1–H3 együtt) **0.8062**; go / no-go **NO-GO** (bukott: `dauc,top20,stability,holm,econ,not_spa_best`).
