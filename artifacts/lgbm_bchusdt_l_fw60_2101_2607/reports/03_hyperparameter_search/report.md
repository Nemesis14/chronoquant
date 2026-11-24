## Áttekintés

**Modell:** `lgbm_bchusdt_l_fw60_2101_2607` (Long) | **Trial-ek:** 100 | **Loss:** ratio@p92.5 (maximalizálandó) | **Target:** `long_mfe_fw60`

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

## Paraméter hatás (valid_ratio_p925)

Piros pont = a kiválasztott trial.

{{chart:param_effect}}

## Feature importance (post-prune)

_Pruning report hiányzik (`search/pruning_report.json`); futtasd a `run_prune()`-t a keresés után._

## Learning curve (best trial)

{{chart:learning_curve}}

## Értelmezés

- A legjobb trial (#99) valid lift-je **2.118**, a train lift 2.628 (gap 0.510): jelentős train-valid különbség, a lift óvatosan értelmezendő.
- Decilis-monotonitás 1.000, Spearman ρ 0.316: minél közelebb az 1-hez, annál következetesebb a rangsorolás a decilisekben.
- 100 trialból a futó maximum alapján a keresés a végéig javult - érdemes több trialt futtatni.
