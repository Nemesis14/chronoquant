---
id: t1
epic: epic_040
title: "Sampling elemzés — long champion modell (lgbm_solusdt_l_fw60_2101_2605)"
assignee: analyst_agent
status: pr
created: 2026-06-23
---

## Summary

Quarto-renderable sampling elemzési notebook elkészítve a long champion modellhez.

## Output

- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/sampling_analysis.ipynb`
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/sampling_analysis.html`

## Notes

**Adatforrás:** `lab.duckdb` → `model."lgbm_solusdt_l_fw60_2101_2605__sample"` (47,443 sor)
**Split mód:** `train_valid_split` (split TINYINT: 0=train, 1=valid)

**2026-06-24 kiegészítés:** Új `## 4. Első train nap: close és target` szekció hozzáadva.
- Close forrás: `live.duckdb` → `ohlcv` (1-perces full felbontás)
- Target: a nap óránkénti sample pontjai (secondary axis)
- P90 küszöb az aznapi sample-ből számítva; top decilis pontok sárga `axvspan` sávval jelölve (1 óra szélességű blokkok)
- Összefoglaló tábla: top decilis időpontok + a hozzájuk tartozó close érték

**Főbb eredmények:**

| Szegmens | Sorok | Időszak | Átlag long_mfe_fw60 | Pozitív arány | Szórás | IQR |
|---|---|---|---|---|---|---|
| Train | 37,939 | 2021-01-01 – 2025-04-30 | 0.00771 | 92.5% | 0.01054 | 0.00809 |
| Valid | 9,504 | 2025-05-01 – 2026-05-31 | 0.00471 | 93.0% | 0.00536 | 0.00501 |

**Kiemelt megfigyelés:** A valid periódus átlaga ~39%-kal alacsonyabb és szórása ~49%-kal kisebb
mint a trainé. Pozitív arány stabil (92–93%). A distribution shift nem a negatív irány felé tolódásból
adódik, hanem a pozitív MFE-k kisebb magnitúdójából — a valid időszak alacsonyabb volatilitású rezsimre utal.
