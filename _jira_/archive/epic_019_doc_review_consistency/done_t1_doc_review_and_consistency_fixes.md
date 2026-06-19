---
epic: epic_019
id: t1
title: Doc review — konzisztencia javítások és rendező elv rögzítése
assignee: code_doc_agent
status: done
blocks: []
blocked_by: []
---

## Goal

Átvizsgálni a `_doc_/` könyvtár összes fájlját (0000-tól 3300-ig), összevetni a kóddal
és az overviewval, majd javítani az inkonzisztenciákat. A kód a frissebb — a dokuk
frissültek hozzá. Az elvégzett munkát rögzíteni az agent manifestekben és a skill-ben.

## Scope

Érintett dokumentumok:
- `_doc_/1000_database.md` — target + predictions tábla schema javítva
- `_doc_/3200_features.md` — path javítva, feature count 202→208, Session Relative csoport hozzáadva
- `_doc_/3300_targets.md` — path javítva (database→data_handling)
- `_doc_/1260_quant_train.md` — path javítva (database→data_handling)
- `_doc_/1250_features_polars.md` — path javítva fejlécben
- `_doc_/3400_quant_train.md` — **ÚJ** X100 metodológiai dok a quant_train táblához
- `_doc_/3000_modelling.md` — fejezet-tábla frissítve (3400 bejegyzés, szint oszlop)
- `.agent/skills/docs_skill.md` — rendező elv hozzáadva, könyvtárstruktúra frissítve
- `.agent/agents/code_doc_agent.md` — scope + rendező elv dokumentálva
- `.agent/agents/methodology_agent.md` — rendező elv + mermaid-first szabályok bővítve

## Acceptance Criteria

- [x] `1000_database.md` target tábla: 10 fw60 DOUBLE oszlop (nem legacy BOOLEAN)
- [x] `1000_database.md` predictions tábla: legacy `trg_l/s_fw60_q90/q10` eltávolítva
- [x] `3200_features.md`: path = `src/data_handling/`, feature count = 208, 25 csoport, Session Relative dokumentálva
- [x] `3300_targets.md`: path = `src/data_handling/sync_tables/sync_targets.py`
- [x] `1260_quant_train.md` + `1250_features_polars.md`: minden path `data_handling`-ra javítva
- [x] `3400_quant_train.md`: 6 kötelező szekció, 4+ Mermaid diagram, INNER JOIN szemantika dokumentálva
- [x] `docs_skill.md`: Overview→Metodológia→Technikai rendező elv rögzítve, struktúra naprakész
- [x] Agent manifestek: X000/X100/X110 szinthatárok és rendező elv egyértelmű

## Notes

**Konzisztencia-problémák azonosítva:**

1. `1000_database.md` target tábla ELAVULT: `trg_l_fw60_q90 BOOLEAN` + `trg_s_fw60_q10 BOOLEAN`
   → A valós kód (epic_011 óta) 10 db fw60 DOUBLE outcome oszlopot ír, nem binary label-t

2. `1000_database.md` predictions tábla: legacy `trg_*` BOOLEAN oszlopokat tartalmazott
   → Ezek `ensure_tables()` migrációban törlődnek; nem részei a tényleges schema-nak

3. `3200_features.md` path elavult: `src/database/` → `src/data_handling/` (epic_006 óta)
   Feature count elavult: 202 → 208 (Session Relative csoport 4 új feature-rel bővítette)
   Csoport 25 (`_add_session_relative_pl`) nem volt dokumentálva

4. `3300_targets.md` + `1260_quant_train.md` + `1250_features_polars.md`: path elavult

5. `quant_train` tábla: 1260-as X110 technikai referencia létezett, de X100 metodológiai
   dok nem volt. Létrehozva: `3400_quant_train.md`

6. Rendező elv nem volt rögzítve: Overview→Metodológia→Technikai. Rögzítve
   `docs_skill.md`-be és mindkét agent manifestbe.
