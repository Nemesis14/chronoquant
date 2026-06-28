---
epic: epic_037
id: t254
title: Utils cleanup + ruff/pyright fixes + exchange config
assignee: code_doc_agent
status: done
blocks: []
blocked_by: []
---

## Goal

Alacsony kockázatú, izolált javítások: dead function törlés utils-ból,
auto-fixable ruff hibák, test pyright hibák, és egy hardcoded per-asset konstans
config-driven átalakítása.

## Scope

- `src/utils.py`
- `src/ui/main.py`
- `src/trading/live/exchange.py`
- `config/assets.json`
- `src/data_handling/tests/store/sanity/test_quant_train.py`

## Acceptance Criteria

### 1. `signal_cutoffs_from_config` törlése (utils.py)

- [ ] `utils.py:498` — `signal_cutoffs_from_config` függvény törölve
- [ ] Nem létező config kulcsot (`model_cfg.get("trading_strategy")`) olvasna — nincs caller
- [ ] Grep megerősíti: 0 caller a `src/`-ban

### 2. Ruff auto-fixes (3 hiba)

- [ ] `src/ui/main.py:1` — I001: import blokk rendezve (stdlib a third-party előtt)
- [ ] `src/utils.py:41` — SIM108: if/else → ternary operator
- [ ] `src/utils.py:575` — SIM108: if/else → ternary operator
- [ ] `uv run ruff check src/ --output-format=text` = 0 hiba a javítások után

### 3. Pyright test fixes (test_quant_train.py)

- [ ] `test_quant_train.py` — 4 hely (sorok: 94, 95, 106, 130): `assert row is not None` hozzáadva fetchone() után, subscript előtt
- [ ] `uv run pyright src/data_handling/tests/store/sanity/test_quant_train.py` = 0 hiba

### 4. `_SOL_QTY_STEP` config-driven (exchange.py)

- [ ] `exchange.py:18` — `_SOL_QTY_STEP = 0.1` hardcoded konstans törölve
- [ ] `config/assets.json` solusdt entrybe kerül: `"qty_step": 0.1`
- [ ] `exchange.py` betölti `utils.load_asset_config("solusdt")["qty_step"]` (vagy az asset config megfelelő helyéről)
- [ ] Az exchange osztály inicializálásakor olvassa be, ne module-levelre hardcode-olva

## Notes

A `_resolve_path` / `_repo_root` → public rename (20+ caller érintett) NINCS ebben a taskban.
Cross-module ripple effect miatt külön epice halasztva — ez a task csak izolált, kockázatmentes
javításokat tartalmaz.

A 3 ruff hiba auto-fixable (`ruff check --fix`), de manuálisan is elvégezhető.
A test pyright fixek standard pytest pattern: `assert result is not None`.

[code_doc_agent] Implementálva 2026-06-23
- signal_cutoffs_from_config törölve (0 caller volt src/-ban, grep megerősítette)
- SIM108 fixek: _load_json és utc_str_to_ms ternary operatorra alakítva
- I001: main.py import sorrend javítva (html stdlib a third-party előtt)
- test_quant_train.py: 4 fetchone() hívás split + assert row is not None hozzáadva
- _SOL_QTY_STEP konstans törölve; qty_step=0.1 bekerült assets.json solusdt entrybe;
  BinanceFuturesClient.__init__ betölti utils.load_assets_config()-ból szimbólum szerint
- ruff check src/ = 0 hiba; pyright = 0 hiba, 0 warning
