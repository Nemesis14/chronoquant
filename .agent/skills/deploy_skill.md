# Deploy Skill

Playbook az élesítési (cutover) folyamathoz és rollback-hez.
A „miértek" és az élő rendszer end-to-end leírása: `_doc_/database_and_code_doc/0003_runtime_flow.md`.

---

## Mikor kell ezt a skill-t betölteni

- Új stratégia élesítése (`reg.deployments` pending → active)
- Rollback (előző strategy_id visszaállítása)
- Deploy státusz ellenőrzése

---

## Előfeltételek (deploy előtt kötelező)

- [ ] A modell(ek) `reg.models` status=`predicted` — offline pred tábla megvan
- [ ] `model."<model_id_long>__pred"` és `model."<model_id_short>__pred"` táblák elérhetők a lab DB-ben
- [ ] `reg.strategies` bejegyzés létezik a `strategy_session_id`-hoz (`model_id_long` + `model_id_short` link korrekt)
- [ ] `artifacts/<session_id>/strategy_artifact.json` megvan (isotonic + rank_lookup betöltve)
- [ ] OOS metrika elfogadható (lásd `metrics.json`, `reg.models.oos_metric`)

---

## Deploy checklist

### 1. Validáció

- [ ] `reg.strategies` bejegyzés ellenőrzése:
  ```sql
  SELECT * FROM reg.strategies WHERE session_id = '<session_id>';
  ```
- [ ] `reg.models` status=`predicted` mindkét irányhoz (long + short):
  ```sql
  SELECT model_id, status FROM reg.models WHERE model_id IN ('<long_id>', '<short_id>');
  ```

### 2. Deploy trigger (pending bejegyzés)

- [ ] `uv run python src/data_handling/06_trigger_deploy.py --strategy-session-id <session_id>`
- [ ] Ellenőrizd: `reg.deployments` új sor, `status='pending'`, `active=FALSE`
  ```sql
  SELECT * FROM reg.deployments WHERE strategy_id = '<session_id>' ORDER BY created_at DESC LIMIT 1;
  ```

### 3. Cutover (a live sync loop végzi — automatikus)

A sync cycle következő futásakor (`sync_predictions.py`) detektálja a pending deploymentet és
egyetlen tranzakcióban hajtja végre:

- `BEGIN` — `DELETE FROM main.predictions` → `INSERT INTO main.predictions` (backfill a pred táblákból) → `COMMIT`
- DuckDB MVCC garantálja: a trading service sosem lát részleges táblát
- Stamp oszlopok (`long_model_id`, `short_model_id`) az új predikciós sorokban kitöltve

### 4. Registry aktiválás (automatikus, a cutover végén)

- [ ] `reg.deployments` frissítve: `active=TRUE`, `status='active'`, `activated_at=now()`, `previous_strategy_id=<régi_id>`
- [ ] Előző deployment: `active=FALSE`, `status='archived'`

### 5. Smoke ellenőrzések (manuális, cutover után)

- [ ] `main.predictions` sorok száma helyes (az új pred tábla range-je szerint):
  ```sql
  SELECT COUNT(*), MIN(open_time), MAX(open_time), model_id FROM main.predictions GROUP BY model_id;
  ```
- [ ] UI: predictions tábla frissül (Streamlit dashboard — live data tab)
- [ ] Trading service: következő döntési ciklus az új stratégia `strategy_artifact.json`-ját tölti be
- [ ] `reg.deployments` aktív bejegyzés ellenőrzése

---

## Rollback

Ha a cutover után problémát találsz:

- [ ] Az előző `strategy_id` a `reg.deployments.previous_strategy_id` mezőben van:
  ```sql
  SELECT previous_strategy_id FROM reg.deployments WHERE active = TRUE AND asset_id = 'solusdt';
  ```
- [ ] Rollback trigger: `uv run python src/data_handling/06_trigger_deploy.py --strategy-session-id <previous_strategy_id>`
- [ ] A következő sync cycle visszaállítja a predictions táblát az előző modell pred adataira
- [ ] A lab DB adatok (`model.__pred` táblák) megmaradnak — rollback bármikor lehetséges

---

## Hivatkozás

- Élő rendszer end-to-end flow: `_doc_/database_and_code_doc/0003_runtime_flow.md`
- Model lifecycle (predict lépés): `.agent/skills/model_lifecycle_skill.md`
- CLI referencia (`06_trigger_deploy.py`): `_doc_/database_and_code_doc/` — data_handling kód-ref
- Registry séma és státusz-lánc: `_doc_/database_and_code_doc/1510_registry_code.md`
