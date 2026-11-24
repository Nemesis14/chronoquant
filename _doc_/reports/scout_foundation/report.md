## Cél

**t935 (epic_073), X2 — kísérleti, hipotézis nélkül:** hordoz-e egy előtanított idősor-alapmodell (foundation model) **befagyasztott** reprezentációja a `[t0 − B, t_c]` prefixről olyan információt, amely a skalár S0 + C baseline-on túl segít az impulzus előrejelzésében. A 5300 szerint a pénzügyi, alacsony jel/zaj arányú adaton az ilyen embeddingek eredménye irodalmilag gyenge / vegyes, ezért a modell a Holm-családon kívül, kísérleti sorként fut (SPA-belépő).

Futás `ab75270600ae`: **2170 train esemény**, ebből **308 impulzus** (14.2%), assetek: btcusdt,ethusdt,solusdt, cella W4h_H8h; közös 6 blokkos purged CV (fold-hash `0d90521c16be85de`).

> **Holdout-védelem:** csak a train split (`sb.load_scout_events`); az 1m bar-ok SQL-szűrővel `open_time < 2026-02-01`; a prefix kizárólag `t_c`-ig záruló bar-okból épül; holdout-2 sor (≥ 2026-08-01) nem került memóriába. Az embedding címke nélkül, befagyasztott modellel készül → fold-szivárgás nincs.

## Modell

**Alapmodell-választás.** A `chronos-forecasting` csomag (Chronos-Bolt, Chronos-2) CPU-n gyors és pip-ből telepíthető; a **Kronos** (K-line alapmodell) nyílt checkpointjai (HF `NeoQuasar/Kronos-*`) csak a GitHub-repó kódjával futnak (nincs pip-csomag), ezért az 1. körből kimaradt. Választás: **Chronos-Bolt-small** (T5 encoder–decoder, d_model 512, 6 encoder-réteg, 16-os patch-ek, `[REG]` token).

- **Checkpoint:** `amazon/chronos-bolt-small` @ `772f3d25d38a` (chronos-forecasting 2.3.2; letöltés ~191 MB, `model.safetensors`), float32, CPU, eval mód.
- **Prefix:** a `[t0 − B, t_c − 1m]` nyitású **1m** bar-ok (L = 480, mind `t_c`-ig zárul; a t933 `build_prefix` lépésköz 1-gyel), két csatorna: `close_sigma` = `log(close / low[t0]) / σ_W` és `log_vol_rel` = `log1p(volume / vol7d_mean_1m)`.
- **Embedding:** a Chronos-Bolt egyváltozós → csatornánként külön; a 30 patch-token encoder-állapotának **átlaga** (a `[REG]` nélkül) → 2 × 512 = 1024 dim. A Chronos-Bolt a bemenetet **példányonként normalizálja** (átlag / szórás), ami a 5300 szerint lényeges amplitúdót eldobná, ezért a normalizálás statisztikái (`loc`, `log scale`) csatornánként explicit skalárként megmaradnak (4 db).
- **Fej (rögzített, hangolatlan):** fold-train standardizálás → fold-train **PCA 64** komponens (címkefüggetlen, foldonként cache-elve) + 4 loc/scale skalár (a `+S0C` változatban + 6 S0 + C skalár) → fold-train standardizálás, ±5 vágás → **multinomiális L2 logit** (lbfgs, C = 0.1) → 3 osztály valószínűsége, score = P(impulzus). Determinisztikus.
- **Futásidő:** modell-betöltés 17 s, embedding 102 s (2170 esemény × 2 csatorna), egyszer, cache-elve (`data/scout_foundation/emb_49d19a1c27051d3e.npz`, gitignored).

Konfiguráció-hash: `2d5a3a2cc056` (embedding), `0e5c38994a0e` (embedding + S0C).

{{chart:pca_explained}}

## Az embedding leíró vizsgálata (teljes train, in-sample)

Egyváltozós AUC (impulzus vs többi) a loc/scale skalárokra és az első PCA-komponensekre, valamint a legerősebb korreláció egy S0 + C skalárral — mennyi a jelből, ami már a baseline-ban is benne van. Leíró, nem döntési alap.

{{table:univariate}}

## 1. Leaderboard (ez a futás)

ΔAUC és CI: összevont OOF, nap-klaszter bootstrap. A felső-q% elsődleges nézete a teszt-foldon belüli rangsor. A `perm. p` a ΔAUC nap-csomagos permutációs p-értéke **N = 200** permutációval (kísérleti modell → csökkentett N, minimális elérhető p = 0.0050); a permutációban az embedding (és a fold-PCA) címkefüggetlen, így azonos, csak a fej tanul újra — ez a 5300 szerinti teljes refit. Holm kísérleti modellre nem vonatkozik; az SPA itt a két X2 sorra fut (a teljes univerzum a t936-ban). A baseline sort ide nem perzisztáltuk (a t931/t932 adja).

{{table:leaderboard}}

{{chart:null_dauc}}

## 2. Blokk- és asset-bontás

{{chart:blocks_scout_chronos_r1}}

{{chart:blocks_scout_chronos_s0c_r1}}

{{table:assets}}

## 3. Kalibráció

{{chart:reliability_scout_chronos_r1}}

{{chart:reliability_scout_chronos_s0c_r1}}

## 4. Kereskedési szimuláció (triple barrier, díjak után)

Jelzés a teszt-foldon belüli rangsor szerint; költségek a `config/strategies.json`-ból (`{"entry_order_type": "taker", "exit_order_type": {"sl": "taker", "timeout": "taker", "tp": "maker"}, "maker_bps": 2.0, "slippage_bps": 1.5, "taker_bps": 5.0}`); CI nap-klaszter bootstrap. A CV-szimuláció csak előszűrő (5300 B).

{{table:trades}}

## 5. Futásidő

{{table:runtime}}

## Értelmezés

- **Csak embedding:** AUC **0.6254** vs baseline 0.6361, ΔAUC **-0.0106** (CI [-0.0470; +0.0258]), perm. p 0.692; 3-oszt. log-loss 0.8076 (Δ +0.0067); felső 20% precizitás 0.202 vs 0.241. A befagyasztott reprezentáció önmagában **nem éri el** a hat kézi skalárt.
- **Embedding + S0C:** AUC **0.6401**, ΔAUC **+0.0040** (CI [-0.0300; +0.0371]), perm. p 0.498; ΔAUC > 0 blokk 4/6; felső 20% precizitás 0.216 (lift 1.52). Az embedding a baseline-hoz adva sem ad zajtól megkülönböztethető többletet.
- **Leíró:** a legerősebb egyedi embedding-jellemző `close_sigma_loc` (max(AUC, 1−AUC) 0.618, |corr| a `volume_ratio`-vel 0.69): ami jel van, az jórészt az amplitúdó-skalárokban és a baseline-nal átfedve jelenik meg.
- **Gazdasági előszűrő:** nettó bp / esemény (felső 20%) -2.1 (embedding), -0.0 (+ S0C), baseline 2.7.
- **Go / no-go (harness):** embedding NO-GO (bukott: dauc,logloss,top20,stability,econ); embedding + S0C NO-GO (bukott: dauc,top20,stability,econ).
- **SPA (javítva, t932 validátor + t936):** a futáskor közölt 0.0134 a `stats.spa_test` szigorú `>` hibája volt; a javított (`>=`) érték a két X2 sorra 1.0 (egyik sem jobb a baseline-nál bináris log-loss-ban: napi átlagos különbség -0.0319, -0.0267). A teljes 1. körös univerzum SPA-ja (t936): p = 0.23, legjobb: `scout_hazard_r1` — az X2 sorok nem kiválasztottak (`not_spa_best`). A NO-GO változatlan.
- **Korlátok:** egy alapmodell (Chronos-Bolt-small) és egy rögzített fej (PCA 64, C = 0,1), hangolás nélkül; a Chronos-Bolt előrejelzésre (nem osztályozásra) tanult, a mean-pool az alak finom részleteit elmoshatja; a Kronos és a Chronos-2 (többváltozós) kimaradt. A permutáció csökkentett N-nel futott (kísérleti). Holdout-2 érintetlen.

Lab DB: csak a `scout_chronos_r1` és `scout_chronos_s0c_r1` sor + a futás sora íródott (`ab75270600ae`).

## t936 frissítés (2026-11-22, leaderboard-univerzum)

Az SPA és a Holm értékek a **t936** együttes kiértékeléséből származnak (`_doc_/reports/scout_leaderboard/`): a teljes 1. körös univerzum (6 modell a baseline ellen, bináris log-loss, nap-összegzett) SPA p = **0.2300**, legjobb modell: `scout_hazard_r1`. Az A7 kapu csak a kiválasztott legjobb modellre vonatkozik (a többinél n/a, `not_spa_best`), a B kapu a felső 20% és 10% szimulációt is nézi. A táblákban a `spa_p` / `holm_p` / kapuk ennek megfelelően frissítve; a fenti szöveg korábbi, futásonkénti SPA-értékei elavultak.

- `scout_chronos_r1`: SPA-legjobb: nem; go / no-go **NO-GO** (bukott: `dauc,logloss,top20,stability,econ,not_spa_best`).
- `scout_chronos_s0c_r1`: SPA-legjobb: nem; go / no-go **NO-GO** (bukott: `dauc,top20,stability,econ,not_spa_best`).
