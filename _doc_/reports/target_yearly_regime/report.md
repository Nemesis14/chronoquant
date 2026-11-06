## Cél

A `target` tábla (`long_mfe_fw60`, `short_mfe_fw60`) éves viselkedése: használható-e a teljes history egyben tanításra / kalibrációra, vagy a target szintje rezsimenként eltér.

**Kapcsolódó dokumentáció:** módszertan `_doc_/methodology_doc/`, kód `_doc_/database_and_code_doc/3100_sync_targets.md`. Az összefoglaló régi notebook-konklúziója: `_doc_/models_doc/target_yearly_regime.md`.

## 1. Havi átlag az időben

A havi átlagos MFE a teljes időszakon; a long és a short (negatív) target együtt.

{{chart:monthly_mean}}

## 2. Éves összesítő

Megfigyelésszám és súlyozott éves átlag; a csonka évek (első/utolsó hónap) jelölve a dátumoknál.

{{table:yearly_summary}}

{{chart:yearly_mean}}

## Értelmezés

A long target éves átlaga 0.0096 (2020) értékről 0.0041 (2026) értékre változott; a short oldal -0.0103 -> -0.0043. A korai (2021-2022) évek magasabb opportunity-szintet jelentenek; a teljes history nem kizárandó, de nem kezelhető egyetlen rezsimként: expanding-window + recency-aware validáció indokolt, a kalibrációhoz a legutóbbi teljes évek az elsődleges referencia.
