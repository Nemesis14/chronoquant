---
epic: epic_072
id: t924
title: Rögzítsd a hullámesemény-módszertan v2-t (5200)
assignee: methodology_agent
status: done
blocks: [t925]
blocked_by: []
---

## Goal
A t918 eredményei (cellánként 21–73 OOS esemény, 8h/12h a train-minimum alatt, ~85% oldalazás) alapján
a user 2026-11-20-án az A opciót (v2) választotta. Frissítsd a `_doc_/methodology_doc/5200_wave_start_image_model.md`-t:

- **Rács:** csak W=4h, H ∈ {4h, 8h}. A (8h,8h) és (12h,12h) cella kikerül (indoklás: eseményszám).
- **Címke (háromosztályos, first-touch):** t_c után H-n belül mit üt meg előbb az ár:
  P* = close[t_c]·exp(A) → **impulzus**; low[t0] → **hamis indulás**; egyiket sem → **oldalazás**.
  Ha ugyanazon a baron mindkettő: konzervatív szabály (hamis indulás), dokumentálva.
  **Fő teszt bináris marad:** impulzus vs nem-impulzus. A 3 osztály másodlagos/leíró.
- **Bizonyítás:** elsődlegesen purged, blokkolt idősoros CV a train időszakon (6 időbeli blokk, purge+embargo a
  teljes [t0−B, t_c+H] intervallumra). Skalár baseline vs kép-modell; Holm a 2 cellán.
- **OOS (2026-02-01→2026-07-31):** csak megerősítő előjel-ellenőrzés, feltétel: ≥100 esemény a két 4h-s cellában együtt.
  Holdout-2 (2026-08-01→) érintetlen.
- **Nem-átfedés javítása:** a doksi 85. és 192. sor körül még `t0' ≥ t_c + H` szabály szerepel; a kód és az
  epic döntés a teljes `[t0 − B, t_c + H]` intervallum nem-átfedését követi — ezt írd át.
- Minimum eseményszámok és go/no-go küszöbök frissítése a fentiek szerint.

## Acceptance Criteria
- [ ] 5200 v2 szekció / verziójelölés (v1 → v2 változásnapló)
- [ ] Nem-átfedési szabály egyezik a kóddal
- [ ] Go/no-go: CV-alapú elsődleges, OOS megerősítő

## Notes
- `5200_wave_start_image_model.md` v2-re frissítve (v1 → v2 változásnapló a doksi elején, indoklással a t918 számaival).
- Rács: W=4h, H∈{4h,8h}. Címke: 3 osztályos first-touch; same-bar kettős érintés = hamis indulás (konzervatív); fő teszt bináris.
- Bizonyítás: 6 blokkos purged CV a train-en (purge a teljes [t0−B, t_c+H] intervallumra + embargo H), Holm 2 cellán; OOS csak megerősítő előjel (≥100 esemény a két cellában); holdout-2 érintetlen.
- Nem-átfedés mindenhol: t0' − B ≥ t_c + H (teljes intervallum). Min. eseményszámok és go/no-go átírva (CV-alapú elsődleges, OOS megerősítő).
- Új feltételezések (jelölve a doksiban): blokkonként ≥40 teszt-esemény, összevontan ≥100 pozitív/≥100 negatív; blokk-előjel ≥4/6; embargo = H. WRC kikerült a döntési szabályból.
- [validator] Validated (doc-only) 2026-11-20: Goal/AC and epic Key Decisions (2026-11-20) consistent. 4h-only grid H in {4h,8h}; 3-class first-touch with same-bar -> false start; binary main test; 6-block purged CV (purge on [t0-B,t_c+H] + embargo H) primary; OOS confirmatory sign only (>=100 events, both cells); holdout-2 untouched. Old `t0' >= t_c + H` appears only as v1 reference (changelog, explanation). go/no-go, tables, mermaid, checklist consistent; no look-ahead; doc code-free. Rule matches code (`i - B >= t_c(prev) + H`, overlap_rule="full_interval").
- [validator] t925 must implement (code is still v1): CELLS only W4h_H4h/W4h_H8h (drop W8h_H8h, W12h_H12h); label = true first-touch (code: impulse = any high>P* in window regardless of order; must compare bar order, same-bar => false start; rename classes t0_below/sideways -> false start/sideways, drop "korrekció" naming); main binary target = impulse vs rest; 6-block purged CV split + embargo H (builder only has train/oos/holdout2 split); report same-bar frequency.
