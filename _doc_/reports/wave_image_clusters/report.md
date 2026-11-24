## Cél

**t919 (epic_072):** a hullámkezdet-események prefix-képeiből (`[t0 − B, t_c]`, `_doc_/methodology_doc/5200_wave_start_image_model.md`, v2) konvolúciós autoencoder-embeddinget tanulunk, majd kis `k`-jú klaszterezést (k ∈ 4..10); baseline a nyers, z-normalizált záróár-útvonal k-Shape / DTW k-means klaszterezése. Megnézzük, mely alakok különülnek el, hogyan néz ki a klaszterenkénti kimenet-tölcsér (p10–p90 kúp a `(t_c, t_c + H]` ablakban) a feltétel nélküli kúphoz képest, és mennyire stabil a klaszterezés.

**Adat:** `research."wave_events_v2"`, cella `W4h_H8h` (W = 4h, H = 8h), **720 train esemény** (ebből 718 nem lóg át CV-blokkhatáron), SOLUSDT 15m prefixek. Újragenerálás: `uv run --group research python -m reports.builders.wave_image_clusters`.

> ⚠️ **OOS-tisztaság:** az embedding, a klaszterezés, a k-választás, a baseline és minden itt látható szám **kizárólag a train** (2021-01-01 → 2026-01-31) eseményein fut; az OOS és a holdout-2 sorokat az adatbetöltés sem olvassa be, az 1m árfolyam a train végén le van vágva. OOS-hozzárendelés nincs (t920 dönt, hogy kell-e).

> ⚠️ **A klaszterenkénti osztályarányok leíró (in-sample) számok, nem bizonyíték.** A klaszter a train eseményeken lett illesztve, az arányokat ugyanazokon mérjük; az egyetlen bizonyíték a t920 purged CV ΔAUC-ja a skalár alap (S0 + C) fölött. Itt az out-of-fold log-loss csak a `k` kiválasztásához (5200) és a klaszterek használhatóságának előzetes jelzéséhez szerepel.

{{table:params}}

## 1. k-választás: out-of-fold log-loss (5200) és stabilitás

A 6 blokkos purged CV-ben (összesen 718 out-of-fold teszt-esemény) mindegyik `k`-ra és módszerre a klaszter → impulzus-arány prediktor log-loss-át mérjük, és a fold-train priorhoz (konstans prediktor, log-loss **0.4458**) hasonlítjuk. **Negatív ΔLL = jobb a priornál.** A kiválasztott `k` a legkisebb OOF log-loss (5200 szabály; döntetlen esetén a kisebb `k`).

{{chart:kselect_ll}}

{{chart:kselect_stability}}

{{table:kselect}}

{{table:kselect_blocks}}

**Választott k = 10** (AE + k-means; OOF log-loss 0.4366, ΔLL = -0.0092, 5/6 blokkban jobb a priornál, ARI 0.38, Jaccard 0.44). A baseline-ok saját legjobb `k`-ja: AE + k-means k = 10; k-Shape (nyers út) k = 9; DTW k-means (nyers út) k = 4.

## 2. Klaszter-centroid képek (k = 10)

A centroid-kép a klaszter tagjainak **átlag-képe** (a pixel sötétsége = a pixel gyakorisága a klaszterben). Fent az ár-panel (ATR-egységben, a `low[t0]` az alsó él), alatta a relatív volumen; az idő balról jobbra, a jobb szél a `t_c`. A medoid-kép a klaszter latens-centroidjához legközelebbi valódi esemény bináris képe.

{{chart:centroid_images}}

{{chart:medoid_images}}

## 3. Klaszter-méret és osztályarány (train, leíró)

> **Leíró, in-sample:** a klaszterek a train eseményeken illeszkednek, az arányok ugyanezeken számolódnak. Nem bizonyíték — a bizonyíték a t920 CV ΔAUC. Kis klaszternél az arány zajos.

{{chart:cluster_sizes}}

{{chart:cluster_shares}}

{{table:cluster_summary}}

## 4. Kimenet-tölcsér: klaszter vs feltétel nélküli kúp

Az útvonal a `t_c` utáni `H` ablakban `log(close / close[t_c]) / A` egységben (15 perces mintavétel); **+1 = az impulzus-szint `P*`**, **−1 = a hamis indulás szintje `low[t0]`** (szimmetrikus, mert `P* = close[t_c]·exp(A)`). Szürke sáv = az összes train esemény p10–p90 kúpja (a `H` végén medián +0.06, p10 -0.81, p90 +0.89); színes sáv = a klaszter p10–p90, vonal = medián. A tölcsér a train eseményekre vonatkozik (leíró).

{{chart:cones}}

## 5. Baseline-összehasonlítás: k-Shape és DTW k-means a nyers úton

Mindhárom módszer ugyanazokkal a foldokkal és szabályokkal fut (az AE-féle `k = 10` mellett összevetve). Az utolsó két oszlop az in-sample klaszterenkénti impulzus-arány min / max értéke (leíró).

{{table:baseline}}

{{chart:method_rates}}

{{table:baseline_clusters}}

## 6. Autoencoder: rekonstrukció-példák

Hat, időrendben egyenletesen kiválasztott train esemény eredeti képe és az AE rekonstrukciója (a tanítás végi BCE 0.1306, az első epochban 0.7028). A rekonstrukció valószínűség-kép; a lényeg, hogy a nagy alakzatok (esés, rebound, volumen-tüske) megmaradnak a 16 dimenziós latensben.

{{chart:recon}}

{{table:runtime}}

## Értelmezés

**Klaszterezés:** k = 10 (5200: legkisebb OOF log-loss a 7 jelölt között); klaszter-méretek: C0 119, C1 117, C2 112, C3 74, C4 69, C5 63, C6 62, C7 41, C8 38, C9 25. Stabilitás foldok között: ARI 0.38, párosított Jaccard 0.44 (1 = azonos partíció).

**Prediktív jel (előzetes, nem bizonyíték):** Az AE-klaszterek OOF log-loss-a jobb a priornál (ΔLL = -0.0092, 5/6 blokkban), de ez még nem a skalár alap fölötti többlet: az `A/σ_W` önmagában is erős (lásd a t925 riportot). Baseline-összevetés ugyanazon `k`-n: AE + k-means: ΔLL -0.0092, ARI 0.38; k-Shape (nyers út): ΔLL -0.0039, ARI 0.44; DTW k-means (nyers út): ΔLL +0.0057, ARI 0.39. **Figyelem a k-választásra:** a legkisebb OOF log-loss a jelölt-tartomány szélére esik, és a következő k-értékek ΔLL-je legfeljebb 0,001-gyel tér el (7, 8, 10) — ez zajszint a ~718 OOF eseményen; a legstabilabb AE-partíció k = 5 (ARI 0.71), a választott k = 10 ARI-ja 0.38. A 5200 szabály (argmin) szerint maradt a választás; a t920-nak érdemes több k-n (pl. a legstabilabb k-n) érzékenységi futást adnia.

**Leíró osztályarány (in-sample):** a klaszterenkénti impulzus-arány 2.4% … 24.6% (train átlag 16.2%); legalább 30 eseményes klaszterekben 2.4% … 24.6%. A klaszter-mediánok `A/σ_W` értéke 1.80 … 3.49: ha a klaszterek főleg a rebound méretét különítik el, a kép-modell ugyanazt az információt ismétli, amit az S0 alap már tartalmaz — ezt csak a t920 skalár-alap elleni ΔAUC-ja dönti el.

**Állásfoglalás:** a klaszterezés ezen a cellán technikailag működik (determinisztikus, per-fold illeszthető), de a bizonyíték a t920-é; ez a riport egyik eredményét sem szabad go/no-go érvként használni. A `wave_image_model.fit_fold` a t920 számára kész (fold-train-only AE + klaszter, a teszt-fold csak transzformálódik).
