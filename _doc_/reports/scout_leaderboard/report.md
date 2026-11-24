## Cél

**t936 (epic_073):** az Impulse Scout leaderboard lezárása a `_doc_/methodology_doc/5300_impulse_scout.md` szerint: (1) a teljes 1. körös modell-univerzum **együttes** többszörös-tesztelés kontrollja (Hansen SPA minden modellre egyszerre, Holm a H1–H3 elsődleges hipotézisen), a go / no-go kapuk újraszámolásával; (2) a **2. kör** — a user döntése szerint (2026-11-22) nem új modelltípus, hanem **kereskedési szabály-iteráció a baseline S0 + C score-ján**, előre regisztrált, 12 változatos rácson; (3) a holdout-2 próba javaslata. Kód: `src/reports/builders/scout_leaderboard.py`, `scout_bench/universe.py`, `scout_bench/rules.py`.

> **Holdout-védelem:** csak a train split (2021-01 → 2026-01) purged CV-je; az OOS (2026-02 → 07) nem szerepel, a holdout-2 (2026-08-01 →) nem töltődött be. Az 1m árak a train végén vágottak.

## Protokoll

- **OOF:** mind a 7 1. körös modell OOF predikciója újraszámolva ugyanazokon a foldokon (fold-hash `0d90521c16be85de`, 2170 esemény, 308 impulzus, btcusdt,ethusdt,solusdt), tárolva: lab DB `research."scout_oof_v1"`. A tárolt leaderboard AUC-kal való egyezés: max. eltérés **0.0e+00** (a modellek determinisztikusak).
- **Univerzum SPA (statisztikai):** a 6 nem-baseline modell egyszerre, veszteség = eseményenkénti bináris log-loss (baseline − modell), nap-összegzett, stacionárius bootstrap (várható blokk 5 nap, 5000 újramintavétel). Az A7 kapu (SPA p < 0,05) **csak a kiválasztott legjobb modellre** vonatkozik; a többi modell nem lehet jelölt.
- **Holm (H1–H3 együtt):** a tárolt, N = 1000 nap-csomagos, teljes refites permutációs p-értékekből (t932 / t933 / t934).
- **B kapu:** a felső 20% **és** a felső 10% (fold-rang) szimuláció is: nettó > 0, CI alsó > 0, hit > break-even.
- **2. kör:** a baseline S0 + C OOF score-ja változatlan, a szabályok nem illesztenek paramétert. Közös: belépés a `t_c`-nél nyíló 1m bar nyitóján (taker + csúszás), same-bar = stop, költségek `config/strategies.json` (`{"entry_order_type": "taker", "exit_order_type": {"sl": "taker", "timeout": "taker", "tp": "maker"}, "maker_bps": 2.0, "slippage_bps": 1.5, "taker_bps": 5.0}`), funding nincs. **Stratégia-SPA:** 26 stratégia (7 modell × felső 20% / 10% alap szabállyal + 12 2. körös változat), nap-összegzett nettó bp, benchmark: **nincs kereskedés (0)**, másodlagosan a baseline alap szabálya (felső 20%).

## 1. Az 1. kör teljes univerzuma

Baseline (S0 + C, pooled) AUC **0.6361**. Univerzum SPA: legjobb modell **scout_hazard_r1**, **p = 0.2300** (alsó / felső változat 0.1884 / 0.3290); H1–H3 együttes Holm: scout_hazard_r1 0.8062, scout_lgbm_r1 0.8062, scout_minirocket_r1 0.8062. Egyik modell sem jelölt.

{{table:leaderboard}}

{{chart:dauc}}

{{table:gates}}

{{table:perm_sources}}

## 2. A 2. kör: kereskedési szabályok a baseline score-on

A 12 változat a ticketben **a futtatás előtt** rögzített (t936 Notes, „2. kör — ELŐZETES REGISZTRÁCIÓ”). Viszonyítás: az 1. kör baseline alap szabálya felső 20%-on **+2.7 bp** [-14.7; +21.4] (n = 435). A legnagyobb átlagú 2. körös változat **R2_03_tp3s_sl15s** +15.5 bp [-5.2; +37.7]; a rögzített szabály szerint kiválasztott (legnagyobb studentizált napi átlag) **R2_03_tp3s_sl15s** +15.5 bp [-5.2; +37.7], blokk > 0: 4/6, asset > 0: 2/3.

{{table:round2}}

{{chart:equity_round2}}

{{chart:blocks_round2}}

{{table:stability}}

## 3. Stratégia-univerzum és SPA (1. + 2. kör)

Benchmark „nincs kereskedés”: legjobb **R2_03_tp3s_sl15s**, **SPA p = 0.4416** (alsó / felső 0.2784 / 0.4664). Benchmark a baseline alap szabálya (felső 20%): legjobb **R2_03_tp3s_sl15s**, **SPA p = 0.0226**. Az 1. kör modelljeinek szimulációja az alap szabállyal az alábbi táblában (a B kapu bemenete).

{{table:round1_trades}}

{{chart:equity_round1}}

## Értelmezés

- **Modellek (1. kör):** az univerzum SPA (p = 0.2300) és a H1–H3 Holm (min. 0.8062) szerint egyik modell sem jobb a skalár S0 + C baseline-nál zajtól megkülönböztethetően; a ΔAUC CI-k mind tartalmazzák a nullát. Ahol javulás van (3 osztályos log-loss: LightGBM, MiniRocket, hazard), az a hamis indulás / oldalazás arányának becsléséből jön, nem az impulzus-rangsorból. **Mind NO-GO**, a baseline marad.
- **Szabályok (2. kör):** a kiválasztott R2_03_tp3s_sl15s nettó +15.5 bp / jelzés, CI [-5.2; +37.7]; a stratégia-SPA p = 0.4416 (vs 0) / 0.0226 (vs baseline alap szabály). **Egyik változat sem teljesíti a gazdasági jelölt feltételeit: NO-GO.**
- **Korlátok:** funding nincs modellezve; a 2. kör a baseline score-ra épül, így a szelekciós torzítást a 26 stratégiás SPA kezeli, de a szabály-rács a t930 leíró ismereteire támaszkodott (A-sáv szűrő); a CV-szimuláció csak előszűrő, a döntő a holdout-2.
