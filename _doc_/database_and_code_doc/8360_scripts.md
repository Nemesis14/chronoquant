# 8360 — `scripts/` (PowerShell indítók)

Windows, PowerShell. Egy paranccsal indítható mind a fejlesztői, mind az éles mód.

| Script | Mód | Mit csinál |
|--------|-----|-----------|
| `dev.ps1` | dev | Leállítja az éles példányt (`Stop-Api`, `Stop-Worker`), indít: `uvicorn api.main:app --app-dir src --reload --reload-dir src` (:8600, külön ablak), a workert (rejtett), `npm run dev` (Vite :5173, külön `cmd` ablak), 4 s után megnyitja `http://localhost:5173`. Kilépés: a két ablak bezárása, `stop.ps1`, majd `start.ps1` |
| `start.ps1 [-NoBrowser] [-Build]` | prod | Ha nincs `frontend/dist/index.html` vagy `-Build`: `npm ci && npm run build`. Idempotens: ha az API már válaszol (`/api/health`), nem indítja újra; ha a porton más folyamat fut, hiba. Rejtett `uvicorn` (:8600; log `logs/api.out.log`, `logs/api.err.log`), 90 s-ig várja a health-et, indítja a workert (`logs/worker.out.log`, `worker.err.log`, PID: `logs/worker.pid`), megnyitja a böngészőt (Chrome új fül, fallback alapértelmezett böngésző) |
| `stop.ps1` | prod/dev | Leállítja az API-t (a figyelő port folyamata és gyerekei, `taskkill /T`) és a workert (PID fájl) |
| `install_autostart.ps1 [-Uninstall]` | autostart | Task Scheduler feladat `ChronoQuantAutostart`: belépéskor (30 s késleltetéssel) `start.ps1 -NoBrowser`; újraindítás hibánál (3x/1 perc), `MultipleInstances IgnoreNew`. A régi Streamlit-os `ChronoQuantUiAutostart` feladatot törli |
| `_common.ps1` | segéd | Közös beállítások (dot-source): `$Root`, `$Port` (alap 8600, felülírható `CQ_PORT`), bind `127.0.0.1` (nem `localhost`: az IPv6 próba lassítaná a health-et), `$LogDir`, `$Python` = `.venv\Scripts\python.exe`; `PYTHONPATH=src`; függvények: `Test-ApiHealthy`, `Get-ApiListenerPid`, `Stop-Api`, `Get-WorkerPid`, `Start-Worker`, `Stop-Worker`, `Open-InChrome` |

Logok: `logs/api.*.log`, `logs/worker.*.log` (a worker saját `logs/worker.log`-ot is ír, rotating), job logok `logs/jobs/<id>.log`.

Előfeltételek: `uv sync` (`.venv`), `frontend/node_modules` (`npm ci`), Node.js. A korábbi `start_ui.ps1` / `stop_ui.ps1` megszűnt.
