# Epic 031: DuckDB-natív adat- és folyamat-architektúra átállás

## Goal
A `_doc_/_plans_/data_process_architecture.md` rendszerterv megvalósítása: a modellezési
adatfolyam átállítása DuckDB-natív, reprodukálható, registry-vezérelt architektúrára.
Két új réteg (immutable snapshot + központi registry), 3-fájlos DuckDB topológia
(live / lab / registry, ATTACH-csal), regisztrált deploy/cutover folyamat, valamint
a teljes referencia- és playbook-dokumentáció.

## Scope
- `src/data_handling/store/` — `registry.py`, `snapshots.py`, migrations framework, cleanup
- `src/data_handling/` — `05_create_snapshot.py` CLI, deploy/cutover (predictions stamp + backfill+swap)
- `src/modeling/` — sampling, pipeline provenance, offline predict table
- `src/strategy/` — strat.* táblák, registry-regisztráció, strategies.json normalizálás
- `src/ui/` — trades/equity olvasás strat.* táblákból
- `src/utils.py` — registry/ATTACH hozzáférési API
- `_doc_/` — 0002–0004 referencia, 1400/1410, 1500/1510
- `.agent/skills/` — model_lifecycle_skill, deploy_skill

## Tasks
- t311: Registry foundation + migrations framework (database_agent)
- t312: Snapshot réteg + CLI (database_agent)
- t313: Sampling refactor — snap forrás + model.__sample tábla + reg.feature_sets (modeling_agent)
- t314: Pipeline provenance + registry integráció (modeling_agent)
- t315: Offline predict tábla model.__pred (modeling_agent)
- t316: Strategy kimenetek → strat.* táblák + reg.strategies (modeling_agent)
- t317: Deploy / cutover folyamat (database_agent)
- t318: Cleanup + strategies.json normalizálás + registry-validátor (database_agent)
- t319: UI átállítás strat.* táblákra (ui_agent)
- t320: Metodológia X100 — 1400_snapshots + 1500_registry (methodology_agent)
- t321: Kód-referencia X110 — 1410_snapshots_code + 1510_registry_code (code_doc_agent)
- t322: Referencia docok — 0002_data_architecture, 0003_runtime_flow, 0004_model_lifecycle (methodology_agent)
- t323: Skillek — model_lifecycle_skill, deploy_skill (code_doc_agent)
- t324: Validációs kör — ruff + pyright + pytest az érintett modulokon (validator_agent)
- t325: Artifacts wipe + teljes pipeline újrafuttatás a 2 champion modellre (modeling_agent)
- t326: Régi objektumok/refek eldobása + trading/UI kompatibilitás zárás (database_agent)

## Execution waves (blocked_by alapján)
- 1. hullám: t311
- 2. hullám: t312
- 3. hullám: t313, t320
- 4. hullám: t314, t321
- 5. hullám: t315
- 6. hullám: t316
- 7. hullám: t317, t318, t319
- 8. hullám: t322, t323
- 9. hullám: t324
- 10. hullám: t325 (operatív — artifacts wipe + champion újrafuttatás új struktúrában)
- 11. hullám: t326 (operatív — legacy purge + trading/UI kompat zárás)

## Key Decisions
- Snapshot = DuckDB immutable tábla (nem parquet), content-hash-sel — reprodukálhatóság.
- 3-fájlos topológia (live / lab / registry) ATTACH-csal — végleges; az újrabecslés nem érinti az élő `main.predictions`-t.
- Feature-szűrés logikai feature_set (registry), nem fizikai tábla-törlés.
- Offline predikció külön tábla (`model.__pred`), NEM a snapshotba fúzva.
- Deploy: a live író végzi registry-szándék alapján, atomi backfill+swap; predikciók model_id stamppel.
- Dokumentáció kétrétegű: `_doc_` referencia (leíró) + `.agent/skills/` playbook (utasító).

## References
- `_doc_/_plans_/data_process_architecture.md` — a forrás-rendszerterv (minden szakasz)
