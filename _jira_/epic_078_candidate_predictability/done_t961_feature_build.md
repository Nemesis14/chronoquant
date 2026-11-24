---
epic: epic_078
id: t961
title: Változók számítása a jelöltekre (past-only)
assignee: analyst_agent
status: todo
blocks: [t962]
blocked_by: [t960]
---

## Goal
Minden jelölthöz (T=60/240/960, k=4) a hat változócsoport jellemzői, a megerősítés időpontjáig ismert adatból.

## Acceptance Criteria
- [ ] Hat csoport (zigzag, volumen, VWAP, kereskedési adatok, quant/klasszikus, alakok) oszlopai a jelölt-táblán.
- [ ] Look-ahead teszt: a jellemzők a megerősítésig ismert adatból, ellenőrzött.
- [ ] Hiányzó adat csoportonként jelezve.
- [ ] ruff + pyright tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Kód: `src/reports/builders/cand_pred_features.py`; tábla `research."cand_pred_v1_features"` (btcusdt lab DB), 5 788 jelölt (T=60: 4225, 240: 1219, 960: 344; 2 + 2 + 2 kimarad, mert nincs 4 megelőző pivot).
- 48 változó, 6 csoport: z (11 zigzag-geometria), v (6 volumen), w (6 VWAP: gördülő 1d/7d, lehorgonyzott a megelőző csúcstól / mélyponttól / előző mélyponttól), f (7 kereskedési adat: taker buy arány-eltérés 3 ablakon, ügylet-méret, ügyletszám, funding), q (11: 1T/3T/10T/30T hozam σ-ban, pozíció a 10T/30T sávban, RSI_T, vol-arány + 3 záróár-alapú 1m quant-analóg), s (7 alakok: fib-távolságok, hullámarányok).
- Past-only teszt: 75 véletlen jelöltre (25 / skála) a változókat és a zigzag-pivotokat a megerősítő baron elvágott adaton újraszámolva azonos eredmény (`lookahead_check`).
- Nincs NaN/inf. Nehéz farkú változók (w_7d, q_ret_30T) → t962-ben rang-/winsor-standardizálás.
- Funding 8 órás, a megerősítő baron már ismert utolsó elszámolás; OI nincs az adatbázisban.
