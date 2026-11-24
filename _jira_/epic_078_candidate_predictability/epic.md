# Epic 078: Előrejelezhető-e a nagy lökés az első szakaszból? (BTC long, forward változó-keresés)

## Goal
Az epic_077 megmutatta: az 1m ár geometriájából magából nincs a véletlennél jobb előrejelzés. Itt **magyarázó változókat** keresünk
(zigzag-leírók, volumen, VWAP, kereskedési adatok, quant/klasszikus, Elliott-szerű alakok), **forward módon, csoportonként** hozzáadva,
és mérjük, hogy a valós időben felismert jelölteknél (megerősített mélypont + első lökés) javítják-e a „nagy mozgás lesz" előrejelzést
a véletlen / bázisráta ellen. Cél egy egyszerű, átlátható riport: mely változócsoport mennyit ad.

## Scope
- Asset: BTCUSDT, 1m, csak long. Train ablak 2021-02 → 2024-12, validáció 2025. **OOS (2026-02 →) és holdout-2 érintetlen.**
- Skálák (T): **60, 240, 960 perc**, k = 4 (user jóváhagyta; bővítés később).
- Jelölt és cél: epic_077 t953 kauzális jelölt-állapotgépe; cél = nagy mozgás / +nR first touch a stop (mélypont) előtt.
- Kód: `src/reports/builders/` (új `cand_pred_*` modulok), táblák `research."cand_pred_v1_*"`, riport `_doc_/reports/cand_pred/`, cikk artifact.

## Key Decisions
- Változócsoportok: (1) zigzag-geometria, (2) volumen, (3) VWAP (gördülő + lehorgonyzott), (4) kereskedési adatok (taker flow, ügyletszám; funding/OI ha van), (5) quant/klasszikus (ATR-állapot, nagyobb skálájú kontextus, RSI), (6) Elliott-szerű alakok (fib-visszafutás, hullámarányok).
- Forward kiválasztás: csoportonként, csak az **új időszakon** mért javulás számít. Mérce: AUC + lift a legjobb tizedben a bázisrátához képest.
- Placebo kötelező: ugyanaz a pipeline a sign-placebo szintetikus áron; a nyertes változóknak ott nem szabad „működniük".
- Egyszerű modell: regularizált logisztikus regresszió; kis GBM csak összehasonlításul. Nincs hangolás-hajsza.
- Minden változó past-only (look-ahead teszt).
- Cikk elején időtáv-táblázat: első lökés hossza, lökés mérete, range, mozgás hossza skálánként.

## Tasks
- t960: Időtáv-táblázat + adat-felmérés (analyst_agent)
- t961: Változók számítása a jelöltekre, past-only (analyst_agent)
- t962: Forward kiválasztás + idő szerinti validáció (analyst_agent)
- t963: Placebo-ellenőrzés a nyertes változókra (analyst_agent)
- t964: Riport + cikk (analyst_agent)

Sorrend: t960 → t961 → t962 → t963 → t964
