# Epic 071: Támasz/ellenállás + trendvonal feature-ök challenger modellben (SOLUSDT)

**Created:** 2026-11-19
**Status:** CLOSED (user döntés, 2026-11-19) — negatív eredmény

## Final Conclusion (olvasd ezt először)

> **A pivot-alapú vízszintes S/R szintek és trendvonalak (detektor config v1, s/m/l lépték) SOLUSDT 1m-en
> nem mutatnak a véletlen placebo szinteknél/vonalaknál nagyobb reakciót: 0/36 cella GO**
> (touch→bounce, break→follow-through, retest→hold; Δ valódi − placebo 0–2 pp, Holm p = 1,0 a train-en).
> Memória-hatás (szint kora szerint) nincs. A t911 nyers arányai (58–66% bounce) a volatilitás és a címkeküszöb
> következményei. A `feat_levels` oszlopok gyenge egyváltozós kapcsolata (ρ ~0,04–0,09) ár-pozíció jellegű, nem S/R-memória.
> **Döntés: (A) megállás** — a challenger modell (t914) és stratégia (t915) nem futott.

Megmarad (újrahasznosítható): detektor könyvtár (`_levels_polars.py`), `feat_levels` tábla + sync, `quant_train__<profile>`
challenger-infrastruktúra, riportok `_doc_/reports/sr_trendline_detection/` és `sr_placebo_study/`, research md
`_doc_/research/sr_placebo_study.md`. Nyitott hipotézisek (Holm után nem szignifikáns): s vonal 7–30d (+4,5/+5,5 pp).

**Javítás:** a t913 OOS határát (2025-05-01) tévesen indokoltuk "champion valid kezdeté"-vel — a champion
(`config/models.json`) train 2021-01 → 2026-01, valid 2026-02 → 2026-07. A konklúziót nem érinti (a train-en is 0 GO).

## Goal
Vízszintes támasz/ellenállás szintek (több memória-léptékkel), trendvonalak, kitörések és
visszatesztek algoritmikus detektálása SOLUSDT 1m adatból, majd ezek feature-ként való
kipróbálása egy **challenger** LGBM fw60 modellpárban a meglévő champion
(`lgbm_solusdt_{l,s}_fw60_2101_2607`) mellett. Tiszta A/B: ugyanaz a snapshot, split és
feature-készlet + az új S/R/trendvonal feature-ök.

## ⛔ Ellenőrzőpont (user kérés)
A **t911 után megállunk**: a user az UI Elemzések oldalán (`_doc_/reports/sr_trendline_detection/`)
vizuálisan ellenőrzi a detektált alakzatokat és az összefoglaló statisztikát. A t912+ (feature tábla,
modell) **csak user jóváhagyás után** indul.

## Scope
- `src/data_handling/sync_tables/` — detektor könyvtár (tiszta függvények) + később feature-sync
- `config/features.json` — detektor paraméterek (utils.py-n át olvasva)
- `src/reports/builders/` — detektálási riport builder
- `_doc_/reports/sr_trendline_detection/` — általános riport (UI Elemzések)
- `_doc_/methodology_doc/` — módszertani rationale
- `config/models.json`, `artifacts/` — challenger modellpár (t914+)

## Tasks
- t909: Módszertan — S/R, trendvonal, kitörés/visszateszt definíciók (methodology_agent)
- t910: S/R + trendvonal detektor könyvtár, look-ahead-mentes, tesztekkel (database_agent)
- t911: Detektálási képes riport + összefoglaló statisztika az UI-ra (analyst_agent) ⛔ ellenőrzőpont
- t912: `feat_levels` feature tábla + sync (database_agent) — user jóváhagyás után
- t913: Placebo eseménytanulmány + memória-görbe (analyst_agent)
- t914: Challenger modellpár pipeline (modeling_agent)
- t915: Challenger stratégia-session + összehasonlító riport (modeling_agent)

Sorrend: t909 ∥ t910 → t911 → ⛔ user review → t912 → t913 ∥ t914 → t915

## Key Decisions
- Memória nem egyetlen ablak: 3 lépték (s=1h/7d, m=4h/30d, l=1d/180d); a valódi memóriát a t913 memória-görbe méri.
- Áttört szint nem szűnik meg, szerepet vált (role flip).
- Minden objektum (pivot, szint, vonal) csak a `confirmed_at` időponttól létezik — look-ahead tilos.
- A t911-ig nincs DB írás: a detektor tiszta könyvtár, a riport read-only olvassa az `ohlcv`-t.
- Az új feature-ök külön táblába (`feat_levels`) kerülnek, a champion `feat_ohlcv_quant` érintetlen.
- **2026-11-19 — ⛔ ellenőrzőpont lezárva:** a user átnézte a t911 riportot, jóváhagyta ("remek munka, mehetünk tovább"). Paraméter-hangolás nem kellett → a `levels_detector` config **v1-ként befagyasztva** (a t913 csak ezzel fut; bármely változás = új verzió + t913 újrafuttatás).
- t913 OOS határ: **2025-05-01** (champion valid kezdete); többszörös teszt: **Holm-korrekció** a cellákon (lépték × irány × eseménytípus). A 2025-05→ időszakot a t911 riport megmutatta → a riportban "vizuálisan látott" jelölés.
- Feature-höz csak a `levels_as_of` / `trendlines_as_of` kimenete használható (a `levels`/`trendlines` tábla több mezője az objektum élete végi állapota = jövő információ) — validator észrevétel.
- Kapcsolódik: epic_065 (klaszter tanulságok), epic_058 (triple barrier), `_doc_/research/elliott_waves/`.
