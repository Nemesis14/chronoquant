---
epic: epic_075
id: t946
title: BTC predikciók élesben + dashboard (asset sorrend: BTC, BCH, SOL)
assignee: ui_agent
status: done
blocks: []
blocked_by: [t944, t943]
---

## Goal
`config/trading.json` stratégia-blokkok BTC/ETH-ra, `sync_predictions` bekapcsolása, dashboard asset-váltó + predikciós chartok BTC/ETH-ra. A worker-t ne állítsd le.

## Scope
`config/trading.json`, `src/worker/`, `frontend/`

## Acceptance Criteria
- [x] Predikciók folyamatosan frissülnek BTC-re, UI mutatja (ETH: nincs modell, scope-ból kiesett)
- [x] Doksi: 0000 overview asset tábla, 8350 (+ 8300, 8310, 0004)

## Notes
- (orchestrátor, t944 validátor) Bug: `src/api/services/strat.py::_artifact_path` trading.json-on kívüli sessionnél a `reg.strategies.model_id_long`-on keres → a short session a long modell strategy_artifact.json-ját kapja. t946-ban javítani (teszttel), a trading.json BTC/ETH blokkok kitöltése előtt.
- (orchestrátor, 2026-11-24) **CANCELLED** — user döntés: ebben a fejlesztésben nincs quant modell implementáció; a quant modellek újragondolása a Scout után külön epicben.
- (orchestrátor, 2026-11-24) **ÚJRANYITVA, szűkített scope — user:** a kész BTC modellpár (t944) kerüljön a dashboardra. Asset sorrend a UI-ban: **BTC elöl, mellette BCH, utána SOL** (ETH csak adat, modell nélkül — a sor végén vagy rejtve a predikciós nézetekből). ETH modell nincs (t945 cancelled). Teendők: `config/models.json` BTC l/s `active: true`, `config/trading.json` btcusdt session ids (`lgbm_btcusdt_{l,s}_fw60_2101_2607/strategy`), predikció backfill/sync, `strat.py::_artifact_path` bug javítása teszttel, asset-váltó sorrend. Stratégia net negatív (t944) → csak jelzés-megjelenítés, nincs éles végrehajtás. blocked_by t943 (frontend ütközés elkerülése).

### 2026-11-23 (ui_agent): kész, validálásra vár

**1. `strat.py::_artifact_path` bug javítva.** A `reg.strategies` fallback most a modellpár mindkét `artifacts/<model>/strategy/strategy_artifact.json` fájlját nézi, és azt adja vissza, amelyiknek a saját `session_id`-ja egyezik a kérttel. Ha egyik sem egyezik, a következő lépés (`artifacts/<session>/`), majd 404 jön, nem a másik irány fájlja. Teszt: `test_api_smoke.py::test_strat_artifact_inactive_session_per_direction` (long/short pár a `<model>/strategy` layoutban, plusz egy illeszkedő artifact nélküli session → 404).

**2. BTC élesítés** (0004 onboarding 9–11. lépés, a BCH mintájára):
- `06_trigger_deploy.py --asset-id btcusdt` → `strat_btcusdt_fw60_long_2101_2607`, majd `…short…`: 2 pending deployment (`86001c68…`, `f7950606…`).
- `config/models.json`: `lgbm_btcusdt_{l,s}_fw60_2101_2607` `active: true`. `config/trading.json` `assets.btcusdt`: `lgbm_btcusdt_{l,s}_fw60_2101_2607/strategy`.
- A futó worker (nem állítottam le) `sync_full` ciklusai csinálták a többit. 20:39 cutover long (2 933 281 sor = a lab `__pred` 2021-01-01 → 2026-07-31), 20:41 cutover short (a long `archived`, a short `active`, ugyanúgy, mint a BCH-nál). 20:42 gap-fill 2026-08-01 → most, 166 780 sor egy inferenciában. Külön backfill CLI vagy job nem kellett. Szándékosan nem indítottam `sync_predictions` jobot: üres táblánál `INITIAL_SYNC_START`-tól töltené be a teljes feature-történetet a cutover előtt. Lock-ütközés nem volt.
- `reg.models` (`predicted`) és `reg.strategies` (`candidate`) státusz változatlan, mint SOL/BCH-nál. `registry_validator`: OK.
- **Lefedettség:** `live.predictions` 3 100 061 sor, 2021-01-01 00:00 → 2026-11-23 19:40. Ez pontosan a percek száma, `lag` gap = 0, 0 NULL pred, egyetlen stamp-pár (`lgbm_btcusdt_l/s_fw60_2101_2607`). A worker azóta percenként írja tovább. A target oszlopok a friss sorokban NULL-ok („Nincs target adat” warning), mert a worker a BTC `target`-et nem frissíti (t944 megjegyzés, mint a BCH-nál). Ez nem blokkol.
- **API (TestClient, valódi config, rövid futás):** `predictions/latest?asset=btcusdt` 19:41-es sort ad, BTC stampekkel. `strat/active` long/short BTC session, cutoff 0,99 / 0,99. `registry/active`: BTC modellek + sessionök, 0 pending. Mindkét `strat/<session>/artifact` a saját sessionjét adja. `market/chart` 4h: 241/241 bar `long_pct`-vel, thresholds 0,99 / 0,99. ETH `status.models` = null, `scout/latest?asset=ethusdt` available.

**3. Asset sorrend (config-vezérelt).** `config/assets.json` új kulcs: `"display_order": ["btcusdt", "bchusdt", "solusdt", "ethusdt"]`. `worker.assets.display_asset_ids()` alkalmazza, és csak a `list_assets` (`GET /api/assets`, MCP `list_assets`) használja. A scheduler és a Scout sorrendje (`configured_asset_ids`, default elöl) nem változott. A frontend `AssetTabs` az API sorrendjét követi, ehhez frontend-módosítás nem kellett. Tesztek: `test_assets_listing` (API = `display_asset_ids()`), `test_display_order` (ismeretlen és duplikált id kiszűrve, a nem listázott id a végére kerül, `display_order` nélkül a régi sorrend).
- **ETH (modell nélkül):** a `DashboardPage` `noQuant` (mindkét `model_id` null) esetén „Nincs quant modell …” sávot mutat. Ilyenkor a `PredictionPriceChart` `quant={false}`, vagyis csak gyertya, a long/short pct sor nélkül; a `StrategyPanel` és az `ActiveTradeCard` rejtve. Scout, Sync, Modellek/adat és TradeTabs marad.
- **Default asset NEM változott** (`default_asset_id: solusdt`): a `Dashboard` link és a `/` továbbra is SOL-ra visz, ha nincs utolsó asset (`cq_last_asset`). **User döntés:** ha a BTC legyen a default, elég átírni a `config/assets.json` `default_asset_id`-t `btcusdt`-re. Ehhez API/worker restart kell. Ilyenkor a `test_assets_api.py::test_assets_listing` `default_asset_id == "solusdt"` asszertjét is igazítani kell, mert a valódi `assets.json`-t olvassa.

**4. Ellenőrzés**
- `uv run pytest src/api src/worker -q`: 107 passed. `src/cq_mcp` + `test_trading_sessions.py`: 17 passed.
- `ruff check src/api src/worker`: tiszta. `pyright` a módosított Python fájlokon: 0 hiba.
- `cd frontend && npx tsc --noEmit`: tiszta. `npm run build`: OK (`dist/` frissült).

**5. Doksi:** `0000_project_overview.md` (intro, asset-tábla BTC/ETH sor, Funding, BTC modell `active: true`, Conventions/Assets fülsorrend), `8350_frontend.md` (fülsorrend, noQuant viselkedés), `8300_api.md` (`/api/assets` sorrend, `strat/{session}/artifact` feloldás), `8310_worker.md` (`display_asset_ids`), `0004_model_lifecycle.md` (élesítés sync-aktív assetre: lépések, a worker végzi a cutovert és a gap-fillt, ellenőrzés; 12. sor: `display_order`).

**Módosított fájlok:** `src/api/services/strat.py`, `src/worker/assets.py`, `src/api/tests/test_api_smoke.py`, `src/api/tests/test_assets_api.py`, `config/models.json`, `config/trading.json`, `config/assets.json`, `frontend/src/pages/DashboardPage.tsx`, `frontend/src/components/charts/PredictionPriceChart.tsx`, `frontend/src/components/AssetTabs.tsx` (komment), docs (lásd fent).

**Restart:** a worker már élesben futtatja a BTC predikciót, nem kell újraindítani. Az **API (:8600) újraindítása kell** a `strat.py` / `worker/assets.py` kódváltozás (fülsorrend, artifact-fix) és az új `frontend/dist` kiszolgálásához. A config-változásokat (`models.json`, `trading.json`) a futó API is látja, mert minden hívásnál újraolvassa.

Megjegyzés: a `general_principles.md` „Aktív asset: SOLUSDT — más párok ne kapjanak fejlesztési munkát” szabálya ütközik ezzel az epickel (user-döntésre készült). Érdemes frissíteni (code_doc_agent).

### 2026-11-23 (validator_agent): ELFOGADVA → done

**1. `strat.py::_artifact_path`:** a javítás helyes. A `reg.strategies` fallback a modellpár mindkét `<model>/strategy/strategy_artifact.json` fájlja közül csak azt adja vissza, amelyiknek a saját `session_id`-ja egyezik. Irányok közötti átcsúszás nincs, egyezés hiányában `artifacts/<session>/`, majd 404 jön. A `test_strat_artifact_inactive_session_per_direction` mindkét irányt és a 404-es esetet is lefedi. A valódi BTC artifactok session_id-ja: l → `strat_btcusdt_fw60_long_2101_2607`, s → `strat_btcusdt_fw60_short_2101_2607`.

**2. BTC élesítés (csak olvasó ellenőrzés):**
- `config/models.json`: a BTC l/s `active: true`. `trading.json` `btcusdt`: a SOL/BCH formátumával azonos (`lgbm_btcusdt_{l,s}_fw60_2101_2607/strategy`).
- `reg.deployments`: BTC long `archived`, short `active` (previous = long), ugyanúgy, mint a BCH-nál. `reg.models` `predicted`, `reg.strategies` `candidate`, ez is BCH-konzisztens.
- BTC `live.predictions`: 3 100 065 sor, 2021-01-01 00:00 → 2026-11-23 19:44. A sorok száma pontosan a percek száma, 0 gap, 0 NULL pred, egyetlen stamp-pár (`lgbm_btcusdt_l/s_fw60_2101_2607`).
- SOL/BCH érintetlen: a stampek csak a saját modellpárjukat tartalmazzák. A SOL/BCH predikciókban szórványos 1 perces gapek vannak (BCH: 4 db, az utolsó 2026-11-23 07:15 UTC). Ezek **régebbiek**, mint a BTC cutover (~19:39 UTC), és nem t946-ból erednek. Az OHLCV-ben ugyanott nincs gap, ez a live predikció-sync korábbról meglévő jelensége (külön ticketet érdemelhet).

**3. Asset sorrend / ETH:** a `display_asset_ids` csak a `list_assets`-ben szerepel (API `/api/assets`, MCP). A scheduler (`sync_asset_ids`) és a Scout (`scout_asset_ids`) továbbra is a `configured_asset_ids` sorrendet használja. A fülsorrend BTC, BCH, SOL, ETH, a default `solusdt` maradt. Az ETH nézet: `status.models` mindig objektum (`model_id: null`), ezért nincs null-crash. A `quant={false}` mellett csak gyertya látszik, a threshold-vonalak null thresholdnál kimaradnak.
- **Validátor-javítás (kicsi):** `DashboardPage.tsx` `noQuant` → `st.live !== null` feltétel került bele. DB busy esetén a status `live: null` és null model id-kat ad, emiatt SOL/BTC/BCH-n a „Nincs quant modell” sáv villant fel, és a Strategy/ActiveTrade panelek eltűntek. A 8350 doksi is frissítve.

**4. Tesztek:** `uv run pytest src/api src/worker src/cq_mcp -q`: 122 passed. `ruff check src/api src/worker src/cq_mcp`: tiszta. `pyright` (strat.py, assets.py, a 2 teszt): 0 hiba. `npx tsc --noEmit`: tiszta. `npm run build`: OK (a `dist/` a javítás után újraépítve).

**5. Doksi:** a 0000, 8350, 8300, 8310 és 0004 pontos (asset-tábla, fülsorrend, noQuant, artifact-feloldás, `display_asset_ids`, onboarding 12. sor).

**Szükséges restartok (a validátor nem indított újra semmit):**
- **API (:8600) — kell.** 05:22 óta fut. A `strat.py` (artifact-fix) és a `worker/assets.py` (`display_order`, amit az API importál) 20:38/20:39-kor módosult, és az új `frontend/dist` is ekkor készült.
- **Worker — kell.** 05:23 óta fut, a handlerek in-process futnak, ezért a régi kódot tartja. Ezek a módosítások utána jöttek: `worker/queue.py` (08:57, `sync_scout` job kind), `worker/scout_runner.py` (09:04), `sync_scout.py` (08:37), `sync_funding.py` (07:51). Bizonyíték: BTC/ETH `funding_rate` max 2026-11-23 00:00 (a 08:00 és 16:00 settlement hiányzik), SOL `funding_rate` max 2026-07-31; az utolsó `scout_signals` BTC/ETH-n 11-22 09:45, SOL-on 11-23 06:42. A t946 BTC-predikcióhoz a worker restart nem kell (már élesben fut), de a t939 funding hookhoz és a t942 sync_scouthoz igen. Megjegyzés: két worker és két API folyamat látszik (venv + rendszer python, egy-egy PID-pár), ez valószínűleg a launcher/child pár.
- A `general_principles.md` „Aktív asset: SOLUSDT” sora elavult → code_doc_agent.
