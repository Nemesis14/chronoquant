---
epic: epic_079
id: t970
title: Mintakészlet, offset, célok, erő-számítás
assignee: analyst_agent
status: todo
blocks: [t971]
blocked_by: []
---

## Goal
Minden skála (15–960) k=4 jelöltjei, klaszterezés (azonos mélypont), súly, kauzális kizárások, analitikus offset, célok (big, +1R, előre vett hozam 0,5T/1T/2T), erő-számítás.

## Acceptance Criteria
- [ ] Jelöltek, klaszterek, effektív mintaszám skálánként.
- [ ] Offset p_rw ellenőrzése (kalibráció a valós és a placebo adaton).
- [ ] Erő-számítás (MDE) a placebo-szórásból.
- [ ] OOS érintetlen; ruff + pyright tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Kód: `src/reports/builders/cand_v2_sample.py`; táblák `research."cand_v2_sample"` (30 087 jelölt, T = 15/30/60/120/240/480/960, k = 4), `_sample_stats`, `_offset_calibration`, `_power` (btcusdt lab DB).
- Jelöltek skálánként: 13 630 / 7 720 / 4 227 / 2 298 / 1 221 / 645 / 346. Klaszterek (azonos mélypont ±max(3, 0,1·T_kicsi) perc): **13 604**, azaz minden nagyobb skálájú mélypont egyben T = 15-ös jelölt is → az effektív minta ≈ a T = 15 jelöltjei; a súly 1/klaszterméret. A nagy mozgások több skálán is látszanak, ezért a súlyozott prevalencia alacsonyabb (0,34 vs 0,43) — a pooled értékelés a klaszter-szintű populáción fut, emellett skálánként súlyozatlanul is közlendő.
- Kauzális kizárások: flash 1 567 (V-alakú, a visszapattanás ≤ max(2, T/20) bar), gap 11, trivial 243 (risk_z ≥ 3,8; a címke ~biztos).
- Analitikus offset `p_rw = exp(−(k − risk_z)/(ρ·k))` **jól kalibrált**: risk_z szerint p_big vs p_rw: 0,399 vs 0,383 (2,0–2,2), 0,435 vs 0,430 (2,2–2,5), 0,492 vs 0,516 (2,5–3,0), 0,650 vs 0,712 (3,0–4,0); enyhe túlbecslés magas risk_z-nél → M0 (slope + intercept rekalibráció) kezeli. A +1R alapráta 0,49–0,51 (martingál).
- Erő-számítás (teszt 2023–25, tiszta jelöltek, klaszter-súlyozva): n_eff ≈ 7 960; az AUC standard hibája null alatt 0,0068, az MDE (80% erő, α = 0,05) ≈ **0,019 AUC** (az epic_078 ~0,028-hoz képest; a nyereség mérsékelt, mert a klaszterezés után a független minta nem nő arányosan).

[analyst] 2026-11-24 — **javítás (t972 közben)**: a klaszter-súly (1/klaszterméret) **nem használható** sem a tanításban, sem a kiértékelésben. A klaszterméret hátrafelé ismert mennyiség, és a címkével korrelál (a nagy mozgások több skálán is látszanak, ezért a pozitívak kisebb súlyt kapnak: súlyozott prevalencia 0,34 vs 0,42). A súlyozott AUC emiatt 0,606 volt, a súlyozatlan 0,526 (az offset 0,531) — ez a szivárgás volt a „jó" eredmény. A `w` oszlop a táblában marad leíró célra (effektív minta), a modellezés súlyozatlan; a függést a nap-klaszteres bootstrap és a skálánkénti riport kezeli. A t970 effektív-mintaszám és az MDE-számítás ennek megfelelően konzervatívabb olvasatú (n_eff ≈ 8 000 klaszter a tesztben, de a sorok 17 387).
