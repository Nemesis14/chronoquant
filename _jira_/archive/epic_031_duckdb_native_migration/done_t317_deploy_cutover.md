---
epic: epic_031
id: t317
title: Deploy / cutover folyamat
assignee: database_agent
status: todo
blocks: [t322, t323, t324]
blocked_by: [t315, t316]
---

## Goal
Élesítési folyamat: az új modell/stratégia élesbe léptetése úgy, hogy az élő
`main.predictions` atomikusan cserélődik (backfill), és a live scoring az új modellre vált —
a trading sosem lát részleges táblát. A cserét a live író végzi registry-szándék alapján.

## Scope
- `main.predictions` séma: + `long_model_id`, `short_model_id` stamp oszlopok
- `src/data_handling/sync_tables/sync_predictions.py` — pending deploy észlelése, atomi backfill+swap, pointer-flip
- `reg.deployments` — pending → active, previous_strategy_id (rollback)
- deploy trigger (CLI vagy registry-szándék)
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 5.2 (deploy/cutover), 4.2 döntés

## Acceptance Criteria
- [ ] predictions kap long_model_id / short_model_id stampet
- [ ] backfill+swap egyetlen tranzakcióban (trading nem lát fél táblát)
- [ ] live író registry pending alapján váltja az aktív modellt
- [ ] reg.deployments: active=true, activated_at, previous_strategy_id
- [ ] rollback: előző strategy_id újra-deployolható (lab adat megvan)
- [ ] integration/smoke teszt a cutoverre; ruff + pyright tiszta

## Notes

### Elvégezve (database_agent)

**Módosított / létrehozott fájlok:**
- `src/data_handling/store/duckdb_store.py` — `ensure_tables` átírva a migrations framework-re (LIVE_DB_MIGRATIONS v1��v5 lista); v5 migration: `_m005_add_model_stamp_cols` adja hozzá a `long_model_id VARCHAR` és `short_model_id VARCHAR` oszlopokat a predictions táblához. Inline ALTER + try/except mintát kiváltotta a Migration dataclass-os framework. Import: `from data_handling.store.migrations import Migration, run_migrations`.
- `src/data_handling/store/registry.py` — v2 migration (`_migration_002_deployments_lifecycle`) hozzáadva: `activated_at TIMESTAMP` és `previous_strategy_id VARCHAR` oszlopok a `reg.deployments`-hez (nullable; rollback-re). `REG_MIGRATIONS` lista: [v1, v2].
- `src/data_handling/sync_tables/sync_predictions.py` — Deploy/cutover logika hozzáadva. `sync_predictions` most a normál inference + stamp oszlop írás mellett ellenőrzi a registry pending deploymentet (`_detect_pending_deployment`). Ha talál, `_execute_cutover`-t hív. Stamp oszlopok (`long_model_id`, `short_model_id`) a normál sync esetén is bekerülnek a `df_out`-ba.
- `src/data_handling/06_trigger_deploy.py` (**új**) — CLI: `--strategy-session-id` kötelező, `--asset-id` opcionális. Insertál egy `status='pending'` sort a `reg.deployments`-be. Validálja, hogy a strategy létezik a registryben (`reg.strategies`).
- `src/data_handling/tests/sync_tables/smoke/test_deploy_cutover.py` (**új**) — 7 smoke teszt a cutover flow-ra (lásd lent).
- `src/data_handling/tests/store/smoke/test_registry_migrations.py` — 2 meglévő teszt frissítve: `applied == [1]` → `set(applied) == {m.version for m in registry.REG_MIGRATIONS}` (v2 migration hozzáadása miatt).

**`main.predictions` séma-bővítés:**
- `long_model_id VARCHAR` — az inferenciát végző long modell ID-je (melyik modell generálta a sort)
- `short_model_id VARCHAR` — az inferenciát végző short modell ID-je
- Meglévő DB-ken: `ALTER TABLE predictions ADD COLUMN IF NOT EXISTS` migration (v5) fut le idempotensen.

**Backfill+swap tranzakció logikája:**
- `_execute_cutover(conn, db_path, deployment, asset_id, reg_path=None, lab_path=None)`:
  1. Feloldja a strategy → model_id_long/model_id_short-ot a `reg.strategies`-ből.
  2. Betölti a `model."<long_id>__pred"` és `model."<short_id>__pred"` táblákat a lab DB-ből, join-olja `open_time`-on, hozzáadja a stamp oszlopokat + OHLCV close-t (live RO attach, best-effort).
  3. `BEGIN; DELETE FROM predictions; INSERT INTO predictions ...; COMMIT;` — egyetlen tranzakcióban. DuckDB MVCC biztosítja, hogy a trading service sosem lát részleges táblát.
  4. ROLLBACK ha hiba (predictions változatlan marad).
  5. Tranzakció után: `_activate_deployment` → pending ��� active.

**Deploy trigger módja: registry-szándék alapú (pending-detect)**
- Döntés: a CLI (`06_trigger_deploy.py`) insertál egy `status='pending'` sort; a live sync loop következő futásakor detektálja és végrehajtja az atomikus cserét. Ez elkerüli a second-writer versenyhelyzetet (a live sync az egyetlen writer a predictions táblán). A CLI csak registry-t ír, nem a predictions-t — írás-izoláltan fut.

**`reg.deployments` életciklus:**
- `pending` (CLI inserti) → `active` (sync végrehajtja a cutover-t: `active=True`, `activated_at=now()`, `previous_strategy_id=<régi strategy_id>`)
- Párhuzamosan: az előző aktív deployment(ek) `active=FALSE, status='archived'`-re kerülnek ugyanabban az `_activate_deployment` hívásban.

**Rollback:**
- Az előző `strategy_id` a `reg.deployments.previous_strategy_id`-ben tárol��dik.
- Rollback: `06_trigger_deploy.py --strategy-session-id <previous_strategy_id>` → következő sync cycle csinálja meg a visszacserét. A lab adat (`model.__pred` táblák) megmarad a lab DB-ben.

**Teszt eredmény (7 passed):**
- `TestPredictionsStampColumns::test_ensure_tables_creates_stamp_columns` — long/model_id oszlopok jelen vannak
- `TestAtomicCutover::test_cutover_replaces_all_rows` — 10 új sor, régi 5 törlődött
- `TestAtomicCutover::test_cutover_is_atomic_rollback_on_error` — hiba esetén predictions változatlan marad (5 sor)
- `TestDeploymentLifecycle::test_activation_sets_active_and_timestamps` — active=True, status='active', activated_at not None
- `TestDeploymentLifecycle::test_previous_strategy_id_recorded` — második deploy rögzíti az előző strategy_id-t
- `TestTriggerDeployCLI::test_trigger_deploy_inserts_pending_row` — pending sor keletkezik
- `TestTriggerDeployCLI::test_trigger_deploy_rejects_unknown_strategy` — ismeretlen strategy_id ValueError

**ruff + pyright:** tiszta (0 error, 0 warning).
**pytest src/data_handling/tests/ (smoke, összes nem-sanity/perf):** **76 passed**.

