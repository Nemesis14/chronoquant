---
epic: epic_079
id: t971
title: Új változók + look-ahead teszt
assignee: analyst_agent
status: todo
blocks: [t972]
blocked_by: [t970]
---

## Goal
Az epic_078 változói + dinamika, zigzag-sorozat, 4T skála-állapot, Wyckoff-szerű, rezsim, funding-változás; csak a megerősítésig ismert adatból.

## Acceptance Criteria
- [ ] Változócsoportok és darabszám.
- [ ] Look-ahead teszt (levágott adaton azonos), a 4T zigzag megerősítései is kauzálisak.
- [ ] ruff + pyright tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Kód: `src/reports/builders/cand_v2_features.py`; tábla `research."cand_v2_features"` (30 066 sor, 118 változó; 21 jelölt kimarad, mert nincs 6 megelőző pivot).
- Csoportok: z 11, v 6, w 6, f 7, q 11 (az epic_078 záróár-alapú 1m quant-analógjaival), s 7 (az epic_078 48 változója) + újak: **d** dinamika 22 (RSI_T, VWAP-távolság, taker-egyenleg, hozam változása 0,25T/0,5T/1T alatt; volumen / ügyletszám / rv gyorsulás; a visszapattanás alakja), **g** zigzag-sorozat 21 (utolsó 3 felfelé és lefelé lábának mérete σ-ban, időtartama T-ben, arányok, magasabb mélypont / csúcs sorozat, mozgástér az előző csúcsokig), **m** 4T-skála állapota 8 (a 4T zigzag megerősített állapota a megerősítéskor, pozíció az 1T/2T/4T sávban), **k** Wyckoff-szerű 4 (spring, mélypont-újratesztelés volumene, erőfeszítés/eredmény, kiárusítási csúcsvolumen), **r** rezsim 15 (7/30 napos trend, hatékonysági arány, vol-rezsim, távolság a 30 napos csúcstól/mélypont, napszak/hét napja, idő és darab a legutóbbi nagy mozgás óta, funding-változás, 7 napos funding).
- Look-ahead teszt: 84 jelöltre (12 / skála) a levágott adaton újraszámolva — zigzag, 4T-állapot és a „legutóbbi nagy mozgás" lista is — a változók azonosak.
- NaN-ek: 4T-állapot 0,05% (az első pivotok), r_since_big 0,007%; inf nincs.
- Előjel-hibák javítva menet közben: a Wyckoff-csoport prefixe `y_` ütközött a célváltozók nevével → `k_`.
