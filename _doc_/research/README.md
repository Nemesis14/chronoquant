# Kutatások indexe

A lezárt kutatások és kísérletek rövid összefoglalói. A kutatási kód és kimenet nem része a repónak (epic_068, t886): minden md végén a „Kód a git historyban" szakasz mondja meg, hol található a törlés előtti állapot (commit `73f2156`), vagy hogy a kód sosem volt verziózva.

| Téma | Időszak | Konklúzió | Státusz |
|---|---|---|---|
| [TradingView szerző-skill](tradingview_author_skill.md) | 2026-08-17 – 20 | Nincs perzisztensen prediktív, követhető szerző | Lezárt, nemleges |
| [TradingView PoC és adatminőség](tradingview_poc.md) | 2026-08-17 | Az adat elbírja a mérést; három megkötéssel | Lezárt (collector marad) |
| [Reddit figyelem-gate](reddit_attention_gate.md) | 2026-08-21 | Valós, de gyenge és instabil jel (IC 0,02–0,10) | Lezárt, nem erőltetjük |
| [Alt-data forrásmátrix](altdata_source_matrix.md) | 2026-08-21 | Binance Vision elég, fizetős forrás nem kell | Lezárt |
| [Alt-data gate](altdata_gate.md) | 2026-08-22 | 22 derivatíva-változó: +34% él kötésenként azonos gyakoriságnál | Lezárt, GO fenntartásokkal |
| [Horizont- és target-diagnózis](horizon_and_target_diagnosis.md) | 2026-08-25 | A modell volatilitást jósol, nem irányt; a target a hiba | Lezárt |
| [Stratégia-költség hatása](strategy_cost_impact.md) | 2026-08-25 | Díj után a kontroll veszteséges, a kezelt épphogy nyereséges | Lezárt |
| [Triple-barrier gate](triple_barrier_gate.md) | 2026-08-26 | Irány-előrejelzési hipotézis: NO-GO | Lezárt, nemleges |
| [Végrehajtási költség (maker)](execution_cost_gate.md) | 2026-08-26 | Maker belépés valós: −9,38 → −4,72 bps/kötés | Lezárt, GO |
| [Vol-skálázott barrier](atr_barrier_gate.md) | 2026-08-26 | Rezsim-függés megszűnt, de nincs pozitív cella (−3,31 bps) | Lezárt, részleges |
| [Cross-sectional rangsor](cross_sectional_probe.md) | 2026-08-26 | +22,5 bps/pár, de Sharpe 0,51, t ≈ 0,7 | Lezárt, nem kereskedhető |
| [Joint feature + hiperparaméter search](joint_feature_param_search.md) | valid 2025-05 – 2026-05 | Joint K=57 nyert; az éles pipeline standardja | Lezárt, érvényesítve |
| [SOL 8 órás LightGBM](sol_8h_lgbm_gate.md) | 2026-08 | Nincs statisztikai és gazdasági jel (javított null-teszt) | Lezárt, nemleges |
| [Champion mint trendjelző](champion_streak_gate.md) | 2026-08 | Nem veri a „tartsd ugyanazt az irányt" referenciát (t = 1,19) | Lezárt, nemleges |
| [30 coinos panel, coinonkénti modellek](multi_coin_panel_models.md) | 2026-08 – 09 | Egyik modell sem ad kereskedhető irányjelet; a verdikt (t857) nem készült el | Lezáratlan, nem folytatott |
| [Klaszter-rezsim (k-means)](cluster_regime.md) | 2026-08-31 – 09-01 | Instabil klaszterek, irány 69%-ban gyengül/vált | Lezárt, nemleges |
| [EMA/SMA keresztezés](ma_cross.md) | 2026-09-05 | Nincs kereskedhető él (win rate ~50%) | Lezárt, nemleges |
| [RSI-divergencia](rsi_divergence.md) | 2026-09 | R² szűrővel nem robusztus, a hatás kicsi | Lezárt, nemleges |
| [tv_report (piaci hangulat)](tv_report.md) | 2026-09 – | A chat „piaci összkép" képességének motorja | ÉL, költözik (t896) |
| [S/R szintek placebo-teszt](sr_placebo_study.md) | 2026-11-19 | 0/36 cella GO: a valódi szintek/vonalak ≈ véletlen placebo; memória-görbe lapos | Lezárt, nemleges (t914 döntés: user) |
| [Hullám-kezdetek kép-klaszterezése](wave_image_clustering.md) | 2026-11-19 – 20 | Képkoncepció NO-GO (W4h_H8h): ΔAUC +0,029, perm. p 0,26; a skalár alap (AUC 0,665) valós, a kép nem ad többet | Lezárt, nemleges (folytatás: user) |
| [Impulse Scout (több-asset impulzus-modellek)](impulse_scout.md) | 2026-11-21 – 22 | 6 modell egyike sem veri az S0 + C logitot (univerzum SPA p 0,23, Holm 0,81); 2. kör (12 szabály): legjobb TP 3σ/SL 1,5σ +15,5 bp, CI [−5,2; +37,7], SPA p 0,44 → NO-GO | Lezárt, nemleges (holdout-2 legkorábban 2027-04 vége) |
| [Multi-skálás nagy-mozgás katalógus](move_census.md) | 2026-11-24 | Skálafüggetlen zigzag-katalógus BTC/SOL/ETH 1m-en: szerkezet, bázisráta, R-szorzós él ≈ véletlen előjelű placebo (martingál); egyetlen valós eltérés: enyhe visszafordulás, SOL-on több nagy mozgás | Lezárt, nemleges (ár-geometria önmagában) |
| [Előrejelezhető-e a nagy lökés? (jelölt + változók)](cand_pred.md) | 2026-11-24 | 48 változó / 6 csoport (zigzag, volumen, VWAP, flow, quant, alakok) forward, idő szerinti próbája a valós idejű jelölteken BTC-n, placebóval: az AUC ~0,52 a placebón is, +1R-nál 0,50; nincs használható előrejelzés | Lezárt, nemleges |
| [Kitörés-előrejelezhetőség v2 (118 változó, 4 modell, nullok)](cand_v2.md) | 2026-11-24 | Offset felett a nagy mozgásra +0,0004 log-loss (AUC +0,006, MDE 0,019), +1R-n nincs jel; a hozam-IC 0,035 rövid távú visszafordulás, 3–6 bps < 13 bps díj; mindkét null lefutott | Lezárt, nemleges |
| [Elliott-hullámok tesztje (3. és 5. hullám)](elliott_test.md) | 2026-11-24 | Szigorú, kauzális 1-2-1-2 / 1-2-3-4 setupok 7 skálán: találati arány ≈ véletlen bolyongás (legjobb többlet +0,009, CI ∋ 0, placebo-null p 0,68), díj után minden változat negatív | Lezárt, nemleges |
| [Elliott-hullámok](elliott_waves/overview.md) | 2026-06 | Algoritmikus spec + 4. hullám befejezési tanulmány (a `docs/`-ból áthozva, t890; kód nincs a repóban) | Archív kutatási spec |

A modell-szintű riportok (champion search, mintavétel, rezsim-OOS stb.) a `_doc_/models_doc/` alatt vannak, nem itt.
