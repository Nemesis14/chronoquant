---
epic: epic_031
id: t322
title: Referencia docok — 0002 / 0003 / 0004
assignee: methodology_agent
status: todo
blocks: []
blocked_by: [t317]
---

## Goal
A cross-cutting referencia-docok kibontása a plan-docból, a megvalósított folyamatot
tükrözve: tárolási architektúra, éles folyamat, modell-lifecycle (deploy-jal).

## Scope
- `_doc_/0002_data_architecture.md` — tárolási topológia (3-fájl, sémák, snapshot, registry)
- `_doc_/0003_runtime_flow.md` — éles folyamat: sync → live predict → trade
- `_doc_/0004_model_lifecycle.md` — snapshot → … → train → predict → deploy/cutover
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` (teljes), 13.1

## Acceptance Criteria
- [ ] 0002/0003/0004 megfelel a megvalósított implementációnak (nem a tervnek, ha eltért)
- [ ] minden doc Overview flowchart + módszertani szekciók
- [ ] deploy/cutover a 0004 része (5.2 alapján)
- [ ] 0000_project_overview.md frissítve/hivatkozva az új rétegekre
- [ ] docs_skill szabályok betartva

## Notes

### Elvégezve (methodology_agent)

**Létrehozott fájlok:**

- `_doc_/0002_data_architecture.md` — Tárolási topológia
  - Sections: Overview flowchart (4 réteg + ATTACH), A 3-fájlos topológia (sémák táblázat + fizikai elrendezés + ATTACH minta), Sémák részletezése (main / snap / model / strat / reg), Snapshot és immutability (stateDiagram), Miért 3 fájl (flowchart), Kapcsolódó dokumentumok
  - Mermaid diagramok: flowchart TD (4-réteg overview), flowchart LR (3-fájl ATTACH), stateDiagram-v2 (snapshot állapotok), flowchart LR (3-fájl írás-szétválasztás)
  - Cross-reference: 1400_snapshots.md, 1500_registry.md, 0003_runtime_flow.md, 0004_model_lifecycle.md — lefelé NEM hivatkozik code_doc_agent zónára

- `_doc_/0003_runtime_flow.md` — Éles folyamat (sync → predict → trade → deploy)
  - Sections: Overview sequenceDiagram, Sync ciklus (flowchart), Live predict / sync_predictions (sequenceDiagram), Deploy észlelése és cutover (sequenceDiagram + atomicitás flowchart), Trading service (stateDiagram state machine), Rollback, Kapcsolódó dokumentumok
  - Mermaid diagramok: sequenceDiagram (teljes éles loop), flowchart TD (sync lépések), sequenceDiagram (predict + cutover detect), sequenceDiagram (deploy/cutover), flowchart TD (atomicitás tranzakció), stateDiagram-v2 (trading state machine)

- `_doc_/0004_model_lifecycle.md` — Modell életciklus (snapshot → deploy)
  - Sections: Overview flowchart (teljes pipeline), 1. Snapshot, 2. Sample, 3. Feature Engineering, 4. Hyperparameter Search, 5. Train, 6. Offline Predict, 7. Strategy Kalibráció, 8. Deploy és Cutover (backfill+swap), reg.models státusz-lánc (stateDiagram), Részleges retrain döntési tábla, Kapcsolódó dokumentumok
  - Mermaid diagramok: flowchart TD (teljes pipeline overview), flowchart TD (snapshot create), flowchart TD (sampling), flowchart LR (FE logikai feature_set), flowchart TD (search), flowchart TD (train), flowchart TD (offline predict + hash verif), flowchart TD (strategy), flowchart TD (deploy cutover tranzakció), stateDiagram-v2 (reg.models státuszok)
  - deploy/cutover a 0004 részeként szerepel (5.2 alapján)

**Frissített fájl:**

- `_doc_/0000_project_overview.md` — a következő szekciók frissültek:
  - Header után: hivatkozások az új 0002/0003/0004 docokra
  - `## Data Flow`: teljes újraírás DuckDB-natív, 3-fájlos architektúrára
  - `## Database (DuckDB)`: 3-fájlos topológia táblázat hozzáadva; "Live táblák" szekció; persistence rules frissítve (parquet → DuckDB táblák ahol releváns)
  - `## Module Architecture`: modeling és strategy leírás frissítve (snapshot-natív pipeline, strat.* táblák)
  - `## Modeling Pipeline`: pipeline lépések + CLI frissítve (snapshot CLI, predict lépés, deploy trigger)
  - `## Key Conventions`: 3 új szabály (DuckDB-natív, 3-fájlos, snapshot immutability)
  - `## Repository Layout`: database/ szekció frissítve lab + registry fájllal

**Döntések:**
- A 0002/0003/0004 fájlok a `_doc_/` gyökérben maradtak (nem methodology_doc/ alkönyvtárban) — globális cross-cutting docs, nem séma-specifikusak, összhangban a docs_skill "global root" elvével
- A methodology_doc/1400 és 1500 fájlok cross-referálnak az új 0002-re (már tartalmazzák ezt a hivatkozást is), a 0002/0003/0004 pedig felfelé hivatkoznak az X100 metodológiai docokra
- A 0000 frissítése tömör: csak a szekciók releváns részeit érintette, nem teljes újraírás
