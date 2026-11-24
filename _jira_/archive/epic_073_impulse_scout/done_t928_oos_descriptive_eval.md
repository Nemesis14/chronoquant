---
epic: epic_073
id: t928
title: OOS leíró kiértékelés (post-hoc, egyszeri) — A / B modell
assignee: analyst_agent
status: done
blocks: []
blocked_by: []
---

## Goal
A user kifejezett kérésére megnézni, mi történt az OOS időszakon (2026-02-01 → 2026-07-31) a t920 A (S0+C) és B (+AE latens) modelljeivel. Leíró, post-hoc, egyszeri; nem gate-eredmény (az OOS ≥ 100 egyedi t0 feltétel nem teljesül).

## Acceptance Criteria
- [x] Szabályok előre rögzítve, holdout-2 érintetlen
- [x] Osztályszámok, AUC + nap-blokk CI, kvintilisek, top 20/10% küszöb-táblák, hozamok (train OOF mellett)
- [x] Teszt: holdout sorok nem töltődnek be

## Notes
**2026-11-21 — analyst_agent.** Kód: `src/reports/builders/wave_oos_eval.py` (új; `uv run --group research python -m reports.builders.wave_oos_eval --out <dir> --cache-dir <dir>`), teszt: `src/reports/tests/test_wave_oos_eval.py` (3 teszt: az 1m betöltő nem ad vissza holdout sort, az OOS címkék nem hatnak a pontszámra, táblák konzisztenciája). A train OOF AUC reprodukálja a t920-at (A 0,6647, B 0,6934). Futásidő: W4h_H8h 135 mp (gyorsítótárból), W4h_H4h 564 mp (per-fold AE illesztéssel).

**[validator] Validálva 2026-11-21 — PASS.** ruff tiszta, pyright 0 hiba, `pytest src/reports/tests` 89 passed (nincs skip; a 3 új teszt lefutott). Holdout-2: ár SQL `open_time < ?`, események split train/oos, `assert_no_holdout`. Szivárgás: A/B csak train-en illesztve, küszöb/kvintilis train OOF-ból, OOS-címke-keverő teszt zöld. A számok egyeznek a nyers `oos_eval.json`-nal (W4h_H8h: N 70, 10 imp., AUC A 0,7167 / B 0,6950, top20 A 13/5, top10 A 8/4). A dokumentum leíró/post-hoc-ként jelöli, a t920 NO-GO változatlan. `8330_reports.md` kiegészítve.

## Előre rögzített szabályok (az OOS kimenetek megtekintése előtt rögzítve)
- **Jelleg:** leíró, post-hoc, egyszeri kiértékelés a user kifejezett kérésére. **Nem gate-eredmény**: az 5200 v2 OOS előjel-ellenőrzés minimuma (≥ 100 egyedi t0) nem teljesül (75), ezért a t920 NO-GO döntést nem módosítja.
- Elsődleges cella W4h_H8h; másodlagos W4h_H4h (azonos protokoll, a t920 döntésében nem szerepelt, ott a 66 train impulzus < 100 miatt NO-GO).
- Modell A = S0 + C (6 skalár, a TRAIN átlag/szórással standardizálva), B = A + AE latens (16 dim); L2 logisztikus, C = 0,1 (mint t920). Egyszer illesztve az összes train eseményen (AE, render-skála, standardizálás csak train), majd az OOS eseményekre alkalmazva. Hangolás, második próba nincs.
- Train összehasonlító oszlop = a t920 OOF (6 purged blokk) előrejelzések (a használható, nem határt átlépő train események).
- Szabály: "impulzust jelez" = score ≥ train-OOF 80. percentilis (top 20%); másodszor 90. percentilis (top 10%). Kvintilis-vágópontok = train-OOF 20/40/60/80%, az OOS-ra alkalmazva.
- Nap-blokk bootstrap (2000) az AUC CI-hez. Hozam: log(close[t_c+H]/close[t_c]).
- Holdout-2 (≥ 2026-08-01) nem töltődött be: az 1m ár SQL `WHERE open_time < holdout2_start`, az események `split='train'` / `'oos'`; legkésőbbi OOS label_end 2026-07-30.

## Eredmények

### W4h_H8h

Train: 720 esemény (OOF: 718 használható); OOS t0: 2026-02-02 → 2026-07-29, max label_end 2026-07-30 09:05.

**1. Esemény- és osztályszám**

| Halmaz | N | impulzus | hamis indulás | oldalazás | alap impulzus-arány |
|---|---|---|---|---|---|
| Train OOF | 718 | 117 (16,3%) | 115 (16,0%) | 486 (67,7%) | 16,3% |
| OOS | 70 | 10 (14,3%) | 8 (11,4%) | 52 (74,3%) | 14,3% |

**2. AUC (nap-blokk bootstrap 95% CI, 2000)**

| Halmaz | AUC A (S0+C) | AUC B (+AE) | ΔAUC (B−A) |
|---|---|---|---|
| Train OOF | 0,6647 [0,615; 0,716] | 0,6934 [0,644; 0,741] | +0,0287 [-0,012; 0,069] |
| OOS | 0,7167 [0,518; 0,895] | 0,6950 [0,490; 0,873] | -0,0217 [-0,142; 0,103] |

**3. Score-kvintilisek (train-OOF vágópontok)**

Modell A:

| Kvintilis | Train n | Train impulzus | Train imp.% | Train hamis% | Train oldal.% | OOS n | OOS impulzus | OOS imp.% | OOS hamis% | OOS oldal.% |
|---|---|---|---|---|---|---|---|---|---|---|
| Q1 | 144 | 5 | 3,5% | 5,6% | 91,0% | 18 | 1 | 5,6% | 11,1% | 83,3% |
| Q2 | 143 | 22 | 15,4% | 11,9% | 72,7% | 18 | 3 | 16,7% | 5,6% | 77,8% |
| Q3 | 144 | 23 | 16,0% | 16,0% | 68,1% | 8 | 0 | 0,0% | 12,5% | 87,5% |
| Q4 | 143 | 28 | 19,6% | 23,1% | 57,3% | 13 | 1 | 7,7% | 23,1% | 69,2% |
| Q5 | 144 | 39 | 27,1% | 23,6% | 49,3% | 13 | 5 | 38,5% | 7,7% | 53,8% |

Modell B:

| Kvintilis | Train n | Train impulzus | Train imp.% | Train hamis% | Train oldal.% | OOS n | OOS impulzus | OOS imp.% | OOS hamis% | OOS oldal.% |
|---|---|---|---|---|---|---|---|---|---|---|
| Q1 | 144 | 6 | 4,2% | 5,6% | 90,3% | 15 | 0 | 0,0% | 6,7% | 93,3% |
| Q2 | 143 | 14 | 9,8% | 10,5% | 79,7% | 14 | 3 | 21,4% | 0,0% | 78,6% |
| Q3 | 144 | 24 | 16,7% | 16,7% | 66,7% | 14 | 1 | 7,1% | 21,4% | 71,4% |
| Q4 | 143 | 28 | 19,6% | 21,0% | 59,4% | 10 | 0 | 0,0% | 20,0% | 80,0% |
| Q5 | 144 | 45 | 31,2% | 26,4% | 42,4% | 17 | 6 | 35,3% | 11,8% | 52,9% |

**4. Küszöb-szabályok (train-OOF 80. / 90. percentilis)**

| Modell | Szabály | Halmaz | Küszöb | Jelzések | Találat | Precizitás | Lift | Recall | Hamis ind. | Oldalazás |
|---|---|---|---|---|---|---|---|---|---|---|
| A | top 20% | Train OOF | 0,212 | 144 | 39 | 27,1% | 1,66× | 33,3% | 34 | 71 |
| A | top 20% | OOS | 0,212 | 13 | 5 | 38,5% | 2,69× | 50,0% | 1 | 7 |
| A | top 10% | Train OOF | 0,238 | 72 | 23 | 31,9% | 1,96× | 19,7% | 15 | 34 |
| A | top 10% | OOS | 0,238 | 8 | 4 | 50,0% | 3,50× | 40,0% | 1 | 3 |
| B | top 20% | Train OOF | 0,240 | 144 | 45 | 31,2% | 1,92× | 38,5% | 38 | 61 |
| B | top 20% | OOS | 0,240 | 17 | 6 | 35,3% | 2,47× | 60,0% | 2 | 9 |
| B | top 10% | Train OOF | 0,284 | 72 | 22 | 30,6% | 1,88× | 18,8% | 24 | 26 |
| B | top 10% | OOS | 0,284 | 10 | 4 | 40,0% | 2,80× | 40,0% | 0 | 6 |

**5. Előremutató hozam log(close[t_c+H]/close[t_c]), %**

| Modell | Szabály | Halmaz | Jelzés medián | Jelzés átlag | Többi medián | Többi átlag |
|---|---|---|---|---|---|---|
| A | top 20% | Train OOF | 0,11 | 0,31 | 0,31 | 0,29 |
| A | top 20% | OOS | 0,87 | 0,37 | -0,19 | 0,08 |
| A | top 10% | Train OOF | 0,13 | 0,47 | 0,30 | 0,28 |
| A | top 10% | OOS | 0,67 | -0,23 | -0,16 | 0,18 |
| B | top 20% | Train OOF | 0,20 | 0,11 | 0,26 | 0,34 |
| B | top 20% | OOS | 0,87 | 0,39 | -0,21 | 0,06 |
| B | top 10% | Train OOF | 0,11 | -0,27 | 0,30 | 0,36 |
| B | top 10% | OOS | 1,07 | 0,95 | -0,20 | 0,00 |

Minden esemény: Train OOF medián 0,25% / átlag 0,29%, OOS medián -0,09% / átlag 0,14%. Futásidő: 135 mp.

### W4h_H4h

Train: 779 esemény (OOF: 778 használható); OOS t0: 2026-02-02 → 2026-07-29, max label_end 2026-07-30 05:05.

**1. Esemény- és osztályszám**

| Halmaz | N | impulzus | hamis indulás | oldalazás | alap impulzus-arány |
|---|---|---|---|---|---|
| Train OOF | 778 | 66 (8,5%) | 58 (7,5%) | 654 (84,1%) | 8,5% |
| OOS | 73 | 6 (8,2%) | 3 (4,1%) | 64 (87,7%) | 8,2% |

**2. AUC (nap-blokk bootstrap 95% CI, 2000)**

| Halmaz | AUC A (S0+C) | AUC B (+AE) | ΔAUC (B−A) |
|---|---|---|---|
| Train OOF | 0,6788 [0,612; 0,742] | 0,7130 [0,648; 0,775] | +0,0341 [-0,022; 0,088] |
| OOS | 0,7488 [0,471; 0,971] | 0,7587 [0,542; 0,946] | +0,0100 [-0,125; 0,181] |

**3. Score-kvintilisek (train-OOF vágópontok)**

Modell A:

| Kvintilis | Train n | Train impulzus | Train imp.% | Train hamis% | Train oldal.% | OOS n | OOS impulzus | OOS imp.% | OOS hamis% | OOS oldal.% |
|---|---|---|---|---|---|---|---|---|---|---|
| Q1 | 156 | 3 | 1,9% | 2,6% | 95,5% | 16 | 0 | 0,0% | 0,0% | 100,0% |
| Q2 | 155 | 11 | 7,1% | 5,8% | 87,1% | 21 | 2 | 9,5% | 0,0% | 90,5% |
| Q3 | 156 | 8 | 5,1% | 7,7% | 87,2% | 12 | 0 | 0,0% | 16,7% | 83,3% |
| Q4 | 155 | 19 | 12,3% | 10,3% | 77,4% | 9 | 0 | 0,0% | 0,0% | 100,0% |
| Q5 | 156 | 25 | 16,0% | 10,9% | 73,1% | 15 | 4 | 26,7% | 6,7% | 66,7% |

Modell B:

| Kvintilis | Train n | Train impulzus | Train imp.% | Train hamis% | Train oldal.% | OOS n | OOS impulzus | OOS imp.% | OOS hamis% | OOS oldal.% |
|---|---|---|---|---|---|---|---|---|---|---|
| Q1 | 156 | 4 | 2,6% | 1,3% | 96,2% | 10 | 0 | 0,0% | 0,0% | 100,0% |
| Q2 | 155 | 6 | 3,9% | 9,0% | 87,1% | 15 | 1 | 6,7% | 0,0% | 93,3% |
| Q3 | 156 | 13 | 8,3% | 4,5% | 87,2% | 19 | 1 | 5,3% | 5,3% | 89,5% |
| Q4 | 155 | 13 | 8,4% | 8,4% | 83,2% | 10 | 1 | 10,0% | 0,0% | 90,0% |
| Q5 | 156 | 30 | 19,2% | 14,1% | 66,7% | 19 | 3 | 15,8% | 10,5% | 73,7% |

**4. Küszöb-szabályok (train-OOF 80. / 90. percentilis)**

| Modell | Szabály | Halmaz | Küszöb | Jelzések | Találat | Precizitás | Lift | Recall | Hamis ind. | Oldalazás |
|---|---|---|---|---|---|---|---|---|---|---|
| A | top 20% | Train OOF | 0,107 | 156 | 25 | 16,0% | 1,89× | 37,9% | 17 | 114 |
| A | top 20% | OOS | 0,107 | 15 | 4 | 26,7% | 3,24× | 66,7% | 1 | 10 |
| A | top 10% | Train OOF | 0,123 | 78 | 16 | 20,5% | 2,42× | 24,2% | 9 | 53 |
| A | top 10% | OOS | 0,123 | 5 | 1 | 20,0% | 2,43× | 16,7% | 0 | 4 |
| B | top 20% | Train OOF | 0,122 | 156 | 30 | 19,2% | 2,27× | 45,5% | 22 | 104 |
| B | top 20% | OOS | 0,122 | 19 | 3 | 15,8% | 1,92× | 50,0% | 2 | 14 |
| B | top 10% | Train OOF | 0,171 | 78 | 17 | 21,8% | 2,57× | 25,8% | 14 | 47 |
| B | top 10% | OOS | 0,171 | 7 | 3 | 42,9% | 5,21× | 50,0% | 0 | 4 |

**5. Előremutató hozam log(close[t_c+H]/close[t_c]), %**

| Modell | Szabály | Halmaz | Jelzés medián | Jelzés átlag | Többi medián | Többi átlag |
|---|---|---|---|---|---|---|
| A | top 20% | Train OOF | 0,09 | 0,21 | 0,12 | -0,02 |
| A | top 20% | OOS | 0,56 | 0,68 | -0,17 | -0,11 |
| A | top 10% | Train OOF | 0,47 | 0,65 | 0,08 | -0,04 |
| A | top 10% | OOS | 0,71 | 1,05 | -0,08 | -0,02 |
| B | top 20% | Train OOF | 0,09 | -0,11 | 0,12 | 0,06 |
| B | top 20% | OOS | 0,46 | 0,48 | -0,23 | -0,09 |
| B | top 10% | Train OOF | -0,02 | 0,27 | 0,12 | 0,00 |
| B | top 10% | OOS | 0,71 | 1,26 | -0,12 | -0,07 |

Minden esemény: Train OOF medián 0,11% / átlag 0,03%, OOS medián -0,05% / átlag 0,06%. Futásidő: 564 mp.


## Értelmezés (óvatosan, kis minta)
- W4h_H8h: 70 OOS esemény, 10 impulzus (14,3%; a train 16,3%). Egy impulzus 10 százalékpont a recallban, az arányok CI-i nagyon szélesek.
- AUC A = 0,717 (CI [0,52; 0,90]), B = 0,695 ([0,49; 0,87]): a skalár modell OOS-on nem rosszabb, mint train OOF-ban (0,665), de a CI széles. ΔAUC (B−A) = −0,022 ([−0,14; +0,10]): a kép OOS-on sem ad többet, iránya ellentétes a train-nel; összhangban a t920 NO-GO-val, de zajszinten.
- Top 20% szabály: A 13 jelzésből 5 találat (38,5%, lift 2,7×, recall 50%); B 17-ből 6 (35,3%, lift 2,5×, recall 60%). A train OOF precizitás 27% / 31% volt: az irány megegyezik és OOS-on jobb, de 5–6 találat mellett ez a különbség nem megkülönböztethető a zajtól.
- A felső kvintilis mindkét modellnél a legmagasabb impulzus-arányú (A 38%, B 35%); a középső kvintilisek nem monotonok (n = 8–18).
- A jelzett csoport medián hozama OOS-on +0,9% (a többi −0,2%), de az átlagok közel azonosak (A top 20%: +0,37% vs +0,08%) és 10 jelzés alatt (A top 10%) az átlag negatív: outlier-érzékeny.
- W4h_H4h (másodlagos): 73 esemény, 6 impulzus; AUC A 0,749, B 0,759, ΔAUC +0,010 (CI [−0,13; +0,18]); top 20%: A 4/15 (lift 3,2×), B 3/19 (lift 1,9×). Még kisebb minta; a két cella eseményei időben átfedőek, így nem független megerősítés.
- Összkép: a skalár visszapattanás-jel (A) OOS-on irányban megtartja a rangsorolást (felső kvintilis ~2–3× lift), a kép-komponens nem ad többet; mindez leíró, nem bizonyító. A t920 döntés (NO-GO a képre) változatlan.
