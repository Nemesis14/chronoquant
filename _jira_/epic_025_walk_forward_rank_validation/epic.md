# Epic 025 — Walk-forward rank validation és 5600 modell újrafuttatás

## Goal

A jelenlegi yearly random-week validációt leváltani egy időalapú, walk-forward
validation sémára, ahol a modellválasztás fő metrikája a `Top10 Lift`
fold-stability penaltyvel:

```text
mean(top10_lift_folds) - 0.5 * std(top10_lift_folds)
```

ahol:

```text
top10_lift = mean(y_true | score in top 10%) - mean(y_true | full validation)
```

Az új metodológia baseline-ja óránkénti supervised sampling, `9 hónap train +
3 hónap validation` walk-forward foldokkal. A teljes 5600-as modellpipeline-t
újra kell futtatni samplingtól kezdve, a meglévő 5600-as elemzési anyagot
archiválni kell, és az új módszertant a `_doc_` dokumentációban is át kell vezetni.

## Context

A jelenlegi aktív sampling és search pipeline yearly random-hour mintára és
4-fold, hónapon belül randomizált validation week kiosztásra épül. Ez gyors
kutatási iterációra használható, de a production-szerű modellvalidációhoz
időkeverést enged meg.

Az új döntés:

- a target marad folytonos `fw60` regressziós target;
- a supervised sampling baseline-ban óránkénti marad;
- a validáció walk-forward foldokon fut;
- a search elsődleges objective-je nem RMSE, hanem tail-rangsorolási minőség;
- kötelező audit metrikák: `Spearman` és `decile monotonicity`;
- a 5600-as modell teljesen újrafut az új sampling/search/train logika szerint.

## Tasks

| ID | Cím | Assignee | Depends on |
|----|-----|----------|------------|
| t1 | Walk-forward sampling specifikáció és artifact redesign | modeling_agent | — |
| t2 | Sampling implementáció: 9m train + 3m validation foldok | modeling_agent | t1 |
| t3 | Search refactor: Top10 Lift objective + rank auditok | modeling_agent | t1, t2 |
| t4 | Train + calibration pipeline alignment | modeling_agent | t3 |
| t5 | 5600 modell teljes rerun samplingtól kezdve | modeling_agent | t4 |
| t6 | 5600 meglévő dokumentáció archiválása | analyst_agent | t5 |
| t7 | Módszertani dokumentáció frissítése a `_doc_` mappában | analyst_agent | t2, t3, t4 |
| t8 | 5600 analysis notebook és HTML újrakészítése | analyst_agent | t5, t7 |
| t9 | Validation és acceptance audit | validator_agent | t5, t6, t7, t8 |
