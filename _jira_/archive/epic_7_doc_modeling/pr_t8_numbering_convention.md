---
epic: epic_7
id: t8
title: Docs skill és doc_agent manifest frissítése — 1000/1100/1110 számozási séma
assignee: doc_agent
status: pr
blocked_by: [t7]
---

## Goal

Beépíteni az új hierarchikus számozási sémát a `docs_skill.md`-be és a `doc_agent.md`
manifestbe, hogy minden jövőbeli dokumentáció ebből a definícióból dolgozzon.

## Scope

- Módosítandó: `.agent/skills/docs_skill.md`
- Módosítandó: `.agent/agents/doc_agent.md` (scope tábla: `_doc_/` hozzáadása)

## Acceptance Criteria

- [ ] `docs_skill.md` "Doc File Naming" szekciója frissítve:
  - Régi: "lowercase, underscores, no date prefixes"
  - Új: hierarchikus számozás szabályai + chapter assignment táblázat
- [ ] Számozási szabályok:
  - `X000` = főfejezet (pl. 3000 = Modeling)
  - `X100` = alfejezet (pl. 3100 = Sampling)
  - `X110` = részletes fájl (pl. 3110 = config.py)
  - `0000` = globális project overview (kivétel)
- [ ] Chapter assignment táblázat `docs_skill.md`-ben:
  - 0000: project overview (reserved)
  - 1000: database
  - 3000: modeling
  - (2000, 4000+: future)
- [ ] `doc_agent.md` Scope táblájába bekerül: `_doc_/` | Documentation files (numbered scheme)
- [ ] Backward-compatible: a meglévő `0001–0232` fájlok átnevezése epic_8 feladata (erre utalni)
