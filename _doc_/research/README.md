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
| [Elliott-hullámok](elliott_waves/overview.md) | 2026-06 | Algoritmikus spec + 4. hullám befejezési tanulmány (a `docs/`-ból áthozva, t890; kód nincs a repóban) | Archív kutatási spec |

A modell-szintű riportok (champion search, mintavétel, rezsim-OOS stb.) a `_doc_/models_doc/` alatt vannak, nem itt.
