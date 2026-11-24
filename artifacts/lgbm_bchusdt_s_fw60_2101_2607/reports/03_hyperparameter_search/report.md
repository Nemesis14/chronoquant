## Áttekintés

**Modell:** `lgbm_bchusdt_s_fw60_2101_2607` (Short) | **Trial-ek:** 100 | **Loss:** ratio@p07.5 (maximalizálandó) | **Target:** `short_mfe_fw60`

## Összefoglalás

{{table:best_summary}}

## Trial progressz

Bal: a valid ratio trialonként a futó maximummal; jobb: overfitting nézet (a átlóhoz közeli pontok jobban általánosítanak).

{{chart:trial_progress}}

{{chart:overfitting_view}}

## Top 10 trial

{{table:top10_trials}}

## Best hyperparaméterek

{{table:best_params}}

## Paraméter hatás (valid_ratio_p075)

Piros pont = a kiválasztott trial.

{{chart:param_effect}}

## Feature importance (post-prune)

_Pruning report hiányzik (`search/pruning_report.json`); futtasd a `run_prune()`-t a keresés után._

## Learning curve (best trial)

{{chart:learning_curve}}

## Értelmezés

- A legjobb trial (#73) valid lift-je **1.339**, a train lift 1.404 (gap 0.064): mérsékelt overfitting.
- Decilis-monotonitás 0.444, Spearman ρ 0.100: minél közelebb az 1-hez, annál következetesebb a rangsorolás a decilisekben.
- 100 trialból a futó maximum alapján a keresés konvergált (a legjobb a vége előtt megvolt).
