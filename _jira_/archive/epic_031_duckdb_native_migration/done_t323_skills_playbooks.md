---
epic: epic_031
id: t323
title: Skillek — model_lifecycle_skill + deploy_skill
assignee: code_doc_agent
status: todo
blocks: []
blocked_by: [t317]
---

## Goal
Playbook-réteg: utasító checklistek az agenteknek. Új modell / részleges retrain
(mit hol updatelj, mit NEM kell újra), és élesítés (backfill+swap+flip, pointer-átírás,
rollback). A megvalósított folyamatot tükrözve, a referencia-docokra linkelve.

## Scope
- `.agent/skills/model_lifecycle_skill.md` — új modell + részleges-retrain döntési tábla (13.3)
- `.agent/skills/deploy_skill.md` — élesítés checklist + rollback
- delegation/skill-referenciák frissítése szükség szerint (CLAUDE.md, 0001)
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 13.2, 13.3

## Acceptance Criteria
- [ ] model_lifecycle_skill tartalmazza a részleges-retrain döntési táblát
- [ ] deploy_skill: validáció → pending → backfill+swap → flip → rollback lépések
- [ ] a skillek a `_doc_`-ra linkelnek a „miért"-ért (nem ismétlik)
- [ ] rövid, utasító, checklist-formátum

## Notes

### Elvégezve (code_doc_agent)

**Létrehozott fájlok:**
- `.agent/skills/model_lifecycle_skill.md` — új skill, playbook az új modell build-hez és részleges retrain döntésekhez
- `.agent/skills/deploy_skill.md` — új skill, élesítési checklist és rollback

**model_lifecycle_skill döntési tábla struktúrája (plan 13.3 alapján):**

| Mi változott | Snapshot | Sample | FE | Search | Train | Predict | Deploy |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Csak hyperparam | — | — | — | Futtasd | Futtasd | Futtasd | Futtasd |
| Új feature_set (ugyanaz a range) | — | — | Futtasd | Futtasd | Futtasd | Futtasd | Futtasd |
| Új range | Futtasd | Futtasd | Futtasd | Futtasd | Futtasd | Futtasd | Futtasd |
| Csak újra-élesítés (kész modell) | — | — | — | — | — | — | Futtasd |

Pipeline lépések sorrendben: 0. setup → 1. snapshot → 2. sample → 3. FE → 4. search → 5. train → 6. predict → deploy_skill

**deploy_skill checklist lépései:**
1. Validáció — `reg.strategies` + `reg.models` status=`predicted` ellenőrzés
2. Deploy trigger — `06_trigger_deploy.py --strategy-session-id <session_id>` → `reg.deployments` pending sor
3. Cutover (automatikus, a live sync loop végzi) — BEGIN/DELETE/INSERT/COMMIT egyetlen tranzakcióban
4. Registry aktiválás (automatikus) — `pending → active`, `previous_strategy_id` rögzítve
5. Smoke ellenőrzések — predictions tábla, UI, trading service
6. Rollback — `06_trigger_deploy.py --strategy-session-id <previous_strategy_id>`

**CLAUDE.md frissítés:** nem volt szükséges — a delegation table helyes és teljes; a skill-referenciák az agent manifestekben és a `0001_agentic_system.md`-ben vannak.

**Frissített fájlok:**
- `.agent/agents/modeling_agent.md` — `model_lifecycle_skill` + `deploy_skill` on-demand referencia hozzáadva
- `.agent/agents/database_agent.md` — `deploy_skill` on-demand referencia hozzáadva
- `_doc_/0001_agentic_system.md` — skill összefoglaló tábla és Mermaid diagram frissítve az új skill-ekkel

**Acceptance Criteria:**
- [x] model_lifecycle_skill tartalmazza a részleges-retrain döntési táblát (plan 13.3)
- [x] deploy_skill: validáció → pending → backfill+swap → flip → rollback lépések
- [x] a skillek a `_doc_`-ra linkelnek a „miért"-ért (0003, 0004 — nem ismétlik a tartalmat)
- [x] rövid, utasító, checklist-formátum
