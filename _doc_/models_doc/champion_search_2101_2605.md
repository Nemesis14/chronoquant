# Hyperparameter search, champion `2101_2605` (régi 6100 notebook)

**Státusz:** elavult, a `2101_2605` search-t a `2101_2607` váltotta; a mostani search riport: `reports/03_hyperparameter_search/` a champion artifactokon. Notebook törölve.

**Mit vizsgáltunk.** A long és short LightGBM Optuna search legjobb trialjeit: valid/train top10 lift, gap, Spearman rho, decilis-monotonitás, best iteration.

**Módszer.** Trial-táblák összevetése (Long vs Short), train-valid gap a train liftre vetítve (>50% = jelentős overfit), a kiválasztott trial indoklása.

**Eredmény.** A kiválasztott trialeken a valid lift (2025-05..2026-05) a train liftnél alacsonyabb; a gap mindkét irányban mérhető, de a rangsorolás (rho, decilis-monotonitás) használható szintű. A modell score-ként/rangsorolóként erős, pontbecslőként gyenge.

**Döntés.** A champion trialek kiválasztása a valid top10 lift + gap alapján marad a protokoll; az aktuális számok a 03 riportban (nem az itteni, 2605-ös adatokból).
