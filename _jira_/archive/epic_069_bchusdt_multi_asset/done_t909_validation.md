---
epic: epic_069
id: t909
title: Validálás: ruff + pyright + pytest + tsc
assignee: validator_agent
status: done
blocks: []
blocked_by: ["t900", "t901", "t902", "t903", "t904", "t905", "t906", "t907", "t908"]
---

## Goal
Az epic összes `pr_` ticketjének validálása.

## Scope
Az epic összes érintett modulja.

## Acceptance Criteria
- [ ] ruff, pyright, pytest (data_handling, modeling, strategy, api, worker, cq_mcp, reports), `npx tsc --noEmit` + `npm run build`
- [ ] `registry_validator` rendben, a SOL pipeline és UI regressziómentes
- [ ] Minden task `done_` vagy visszakerül `todo_`-ba okkal

## Notes
[validator] 2026-11-07
- `ruff check src/`: All checks passed.
- pyright (api worker cq_mcp modeling strategy data_handling reports utils.py): 0 errors, 0 warnings.
- pytest (api worker cq_mcp sentiment reports modeling strategy data_handling/tests): 418 passed, 4 failed. 3 = ismert SOL live-data sanity (no_time_gap, target_row_count, predictions_score_range, nem blokkoló). 1 = `worker/tests/test_queue.py::test_scheduler_dedup_does_not_mark_minute`: nem hermetikus teszt, mert t907 óta a BCH is sync-aktív (trading.json assets.bchusdt), így a tick BCH jobot is enqueue-olt. Javítva: monkeypatch `scheduler.assets.sync_asset_ids -> ["solusdt"]`. src/worker: 30 passed, ruff tiszta.
- frontend: `npx tsc --noEmit` 0 hiba; `npm run build` OK.
- `registry_validator`: OK (config és registry konzisztens).
- Regresszió (BFF 8600): /api/health ok; /api/assets (solusdt+bchusdt, mindkettő has_data+active); /api/market/chart?asset=solusdt 1441 bar, thresholds {long 0.9, short 0.92}; /api/strat/active?asset=solusdt rendben; /api/status?asset=bchusdt live+modellek+worker alive.
- `rg -i solusdt src` (nem teszt): csak docstring/usage példák, `utils.FALLBACK_ASSET_ID`, costs.py komment, TV ticker map, persona prompt/MCP docstringek "default solusdt" — mind a t902 Notes szerint indokolt.
- git status: nincs DB/nagy artifact. Jelzések: `logs/` (2,1 MB) untracked és nincs gitignore-olva; `frontend/vite.config.ts.timestamp-*.mjs` ideiglenes vite fájl (09:12, nem commitolandó); `artifacts/lgbm_bchusdt_*` 27 MB (a SOL artifactok is trackeltek, 2-4 MB rank_lookup parquetek) — konzisztens.
