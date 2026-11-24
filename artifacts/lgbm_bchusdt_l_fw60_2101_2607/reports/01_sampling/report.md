## Cél

A `lgbm_bchusdt_l_fw60_2101_2607` (Long) modell **sampling periódusát** vizsgáljuk: train/valid szétválasztás, megfigyelésszámok, dátumtartományok, a `long_mfe_fw60` átlagos időbeli alakulása és a target-eloszlás stabilitása train vs valid között.

**Kapcsolódó dokumentáció:** módszertan `_doc_/methodology_doc/3000_sampling.md`, kód `_doc_/database_and_code_doc/5300_create_sample.md`.

**Asset:** BCHUSDT | **Forrás:** `model."lgbm_bchusdt_l_fw60_2101_2607__sample"` (`open_time`, `long_mfe_fw60`, `split`).

## 1. Train / Valid összesítő

A sample-sorok `split` szerint (0 = train, 1 = valid): megfigyelések száma, első és utolsó időpont, átlagos `long_mfe_fw60` és a negatív arány (target < 0 hányada).

{{table:train_valid_summary}}

## 2. Havi target alakulása

A `long_mfe_fw60` havi átlaga és ±1σ sávja a teljes periódusra; a narancs terület a valid időszak. Megmutatja, stabil-e a target szintje, van-e rezsimváltás train és valid között.

{{chart:monthly_target}}

## 3. Target eloszlása (Train vs Valid)

(a) KDE sűrűségbecslés a train és valid eloszlásra; (b) boxplot (medián, IQR, bajusz, átlag). Az eloszlások hasonlósága jelzi, hogy a valid reprezentatív-e; jelentős eltolódás distribution shiftet jelez.

{{chart:distribution_kde}}

{{chart:distribution_box}}

### Összehasonlító metrikák

{{table:interp_metrics}}

## 4. Top10 decilis havi bontásban

A top10 = a legmagasabb 10% (P90 küszöb), **szegmensenként külön** számolva. A táblában a hónapok és az éves összesítő sorok (▶); a trend-ábra a havi 'top10 / nap' értéket mutatja.

{{table:top10_reference}}

{{table:top10_monthly}}

{{chart:top10_monthly_trend}}

## 5. Top10 lift ábrák

Mikor és milyen close árfolyam mellett jelennek meg a top10 decilisbe eső megfigyelések. Szegmensenként és naptári évenként egy-egy ábra: felül a close, alul a target, piros pontok = top10 (küszöb: szegmens P90).

### Train - 2021

n = 8,756 | top10 = 1508 | lift = 4.30x | P90 = 0.01294

{{chart:top10_train_2021}}

### Train - 2022

n = 8,760 | top10 = 808 | lift = 3.71x | P90 = 0.01294

{{chart:top10_train_2022}}

### Train - 2023

n = 8,760 | top10 = 525 | lift = 4.18x | P90 = 0.01294

{{chart:top10_train_2023}}

### Train - 2024

n = 8,784 | top10 = 945 | lift = 3.95x | P90 = 0.01294

{{chart:top10_train_2024}}

### Train - 2025

n = 8,760 | top10 = 646 | lift = 3.82x | P90 = 0.01294

{{chart:top10_train_2025}}

### Train - 2026

n = 743 | top10 = 25 | lift = 3.86x | P90 = 0.01294

{{chart:top10_train_2026}}

### Valid - 2026

n = 4,321 | top10 = 433 | lift = 3.83x | P90 = 0.00950

{{chart:top10_valid_2026}}

## Értelmezés

- **Szint:** a valid átlag 0.00411 vs train 0.00562 (eltolódás -0.19 train-σ); a szórás aránya valid/train = 0.66.
- **Arány:** train 44,563 / valid 4,321 megfigyelés (8.8% valid).
- **Top10 sűrűség:** átlag 2.41 (train) vs 2.42 (valid) top10 / nap; a két érték közeli eloszlást és hasonló jelgyakoriságot jelez, nagy eltérés esetén a valid mutatók óvatosan értelmezendők.
- **Használhatóság:** a valid periódus jelentősen eltér a trainétől - rezsimhatást érdemes vizsgálni (küszöb: |eltolódás| < 0,5σ és szórásarány 0,7-1,4).
