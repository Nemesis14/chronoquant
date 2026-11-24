---
epic: epic_078
id: t962
title: Forward csoport-kiválasztás + idő szerinti validáció
assignee: analyst_agent
status: todo
blocks: [t963]
blocked_by: [t961]
---

## Goal
Csoportonként sorban hozzáadva megmérni, mennyit javít az előrejelzés az új időszakon (train 2021–2024, validáció 2025) a bázisrátához képest.

## Acceptance Criteria
- [ ] Walk-forward / idő szerinti felosztás, nincs keverés.
- [ ] AUC és lift (legjobb tized) csoportonként és kumulatívan, skálánként, nap-klaszter CI-vel.
- [ ] Regularizált logisztikus regresszió fő modellként; kis GBM csak összehasonlításul.
- [ ] Többszörös-tesztelés kezelése (előre rögzített csoportok, nincs utólagos kiválogatás).
- [ ] OOS érintetlen; ruff + pyright tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Kód: `src/reports/builders/cand_pred_model.py`; táblák `research."cand_pred_v1_metrics" / _oof / _selected / _univar`.
- Módosítás a t961-hez: az adatbázis 1m quant-változóit (`feat_ohlcv_quant`) **zárásár-alapú analógokra cseréltem** (`qd_rsi14`, `qd_bbpos140`, `qd_absret14`), mert a placebo (t963) szintetikus áron csak így számolható újra azonosan. Funding / flow / VWAP a valós adatból, a placebo árutra illesztve.
- Eltérés a tervtől: a „train 2021–24, validáció 2025" helyett **expanding-window éves fold-ok (teszt 2023, 2024, 2025)**, purge-gal (a train csak azokat a jelölteket látja, amelyek címkéje a tesztév előtt ismert) — a T=960 cella kis mintája miatt. n = 3 530 jelölt (T=60/240/960 összevonva, skála-dummyval).
- Eredmény (pooled 2023–25, logisztikus regresszió C=0,1): cél „nagy mozgás" (alapráta 0,427): AUC all = 0,523 [0,504; 0,544], base = 0,492; lift a legjobb tizedben 1,18; log-loss skill ≈ 0. Csoportok (+base): q 0,527, z 0,521, w 0,519, v 0,512, s 0,494, f 0,492. GBM 0,522. Greedy (belső idő-CV) fold-onként: ∅ / zv / q → AUC 0,514.
- Cél „+1R a stop előtt" (alapráta 0,523): minden modell ≈ 0,48–0,51; all 0,499; GBM 0,510.
- Egyváltozós top: z_risk AUC 0,543 (mechanikus: a megerősítéskor már megtett visszapattanás a 4σ célhoz közelebb visz — a placebónak ugyanezt kell mutatnia), qd_bbpos140 0,47, w_anch_low 0,526.
- Értelmezés a placebóval együtt (t963): a ~0,52 AUC a mechanikus geometriai komponens miatt várható lehet.
