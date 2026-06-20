# Coding Skill

ChronoQuant Python coding standards. Read this before writing or reviewing code.

---

## Quality Gate

During coding — single file check (gyors, ~3s):
```powershell
uv run pyright src/foo.py
```

Before committing — full gate from repo root:
```powershell
ruff check . --fix
uv run pyright
uv run pytest _tests/
```

Ruff always CLI (`--fix` auto-applies; MCP cannot do this).

---

## Language Rules

- All public functions in `src/` must have type annotations on parameters and return values.
- Use Pydantic v2 schemas (`src/schemas/`) for data contracts crossing module boundaries — do not pass raw `dict` between layers.
- Do not replace existing `dict`-based internal logic with Pydantic unless explicitly asked.

---

## Core Project Rules

- Run commands from the repo root.
- Use `src/utils.py` as the config-loading entry point — never read JSON config directly from business logic.
- Keep scripts thin; reusable logic belongs under `src/`.
- Store timestamps as UTC strings: `YYYY-MM-DD HH:MM:SS`.
- Keep generated model artifacts under `artifacts/<model_id>/`.
- Keep candidate model evaluation output separate from the live predictions table.

---

## Data Processing Defaults

- DuckDB is the default engine for analytical data access and manipulation.
- Prefer native DuckDB SQL for filtering, joining, aggregation, window functions, sampling, and large scans.
- Use Polars when transformations are large, in-memory, or awkward to express clearly in SQL.
- Use pandas only for small final display tables, seaborn/matplotlib plotting inputs, or cases where DuckDB/Polars are impractical.
- Analysis helpers under `_doc_/analysis/src/` follow the same typing, docstring, logging, and organization rules as other Python modules.
- Analysis helpers must not write to DuckDB, mutate production tables, or change model artifacts unless explicitly requested.

---

## Documentation Style

### File Header
Module-level docstring at the top of each file:
```python
"""Sync OHLCV data and compute derived features for a given asset.

Reads from Binance, writes to DuckDB via the shared store layer.
Idempotent by open_time — safe to re-run.
"""
```

### Function / Class Docstrings
Google-style:
```python
def sync_features(start_time: str, lookback_bars: int = 240) -> None:
    """Fetch OHLCV and compute all configured features from start_time.

    Args:
        start_time    : UTC timestamp in YYYY-MM-DD HH:MM:SS format.
        lookback_bars : Minutes to look back for feature computation.

    Returns:
        None. Inserts rows directly into the features table.
    """
```

### Section Markers
File-level major blocks — `# %%` (VS Code recognizes as interactive cells):
```python
# %% Load configuration
# %% Fetch OHLCV
# %% Compute features
```

Within-function sections — short dash separators:
```python
# --- load config ---
# --- fetch data ---
```

---

## Alignment Conventions

### Variable Assignment
Align `=` for short related blocks:
```python
db_cfg      = utils.load_db_config()
feat_cfg    = utils.load_features_config()
db_path     = db_cfg["database"]["db_path"]
```

### Multi-line Calls
Align keyword argument `=`:
```python
rebuild_derived_tables(
    start            = args.start,
    end              = args.end,
    features_only    = args.features_only,
    predictions_only = args.predictions_only,
)
```

### Function Signatures
Align `:` and `|` across parameters:
```python
def ensure_table_columns(
    db_path    : str,
    table_name : str,
    df         : pd.DataFrame,
) -> None:
```

### Docstring Args
Align `:` to longest parameter name:
```python
    Args:
        conn       : Open DuckDB connection.
        table_name : Target table name.
        df         : DataFrame with an open_time column.
```

---

## Naming Conventions

- **snake_case** for all variables and functions
- Config dicts: `db_cfg`, `feat_cfg`, `model_cfg`, `env_cfg`
- Features: `feat_rsi_14`, `feat_roc_140`
- Targets: `long_mfe_fw60`, `short_mfe_fw60`
- Verb-first functions: `sync_features()`, `load_db_config()`
- Private helpers: leading underscore `_resolve_path()`

---

## Code Organization

### Within a file

```python
# 1. Header docstring
# 2. Imports (stdlib → third-party → internal)
# 3. Constants / Config
# 4. Helper functions
# 5. Main classes / functions
```

### Module / Directory Layout

Each domain module lives in its own folder. Scripts at the module root call into
submodule folders; library code never lives loose at the root.

```
src/<domain>/               ← module root
  <submodule_a>/            ← library code, grouped by topic
    __init__.py
    foo.py
    bar.py
  <submodule_b>/
    __init__.py
    baz.py
  tests/                    ← all tests for this module
    <submodule_a>/
      smoke/
        test_foo.py
    <submodule_b>/
      smoke/
        test_baz.py
  00_first_step.py          ← numbered entry-point scripts (if order matters)
  01_second_step.py
  __init__.py
```

**Rules:**
- The module root contains **only** scripts (`NN_name.py`) and `__init__.py` — no loose library `.py` files.
- Scripts are thin CLI wrappers; all reusable logic belongs in a submodule folder.
- Scripts are numbered (`00_`, `01_`, …) when execution order is defined.
- Submodule folders group files **by topic** (e.g., `sampling/`, `training/`, `search/`, `evaluation/`).
- Tests mirror the submodule structure: `tests/<submodule>/<type>/test_*.py`.

**Reference implementation:** `src/database/` and `src/modeling/quantitative/`.

---

## Logging

Every module uses the standard `logging` module. No `print()` for operational messages.

```python
import logging
logger = logging.getLogger(__name__)
```

Log levels:
- `logger.debug(...)` — detailed internal state, dev only
- `logger.info(...)` — normal operation: row counts, batch progress
- `logger.warning(...)` — non-fatal, needs attention
- `logger.error(...)` — operation failed
- `logger.exception(...)` — error + automatic stack trace (in except blocks)

Handler configured only at entry points (`__main__`) — never in library code.

---

## Error Handling

At boundaries (Binance API, DuckDB, file I/O) use `try/except`, log with `logger.exception`:
```python
try:
    rows = client.get_klines(...)
except Exception:
    logger.exception("Binance klines fetch failed")
    raise
```

Internal logic: let exceptions propagate — do not catch them.

---

## Console Output

`print()` allowed only in interactive CLI scripts (`__main__` block). Always ASCII, no emojis:
```python
print("OK: Computed 1,234 feature rows into 'FEATURES'")
print("INFO: Fetching SOLUSDT klines from Binance...")
print("ERROR: No feature rows found since 2026-05-16")
```

---

## Checklist

- [ ] Module docstring at file top
- [ ] Google-style docstring on every public function and class
- [ ] `# %%` markers for major file-level sections
- [ ] `# --- ... ---` for within-function sections
- [ ] Alignment: assignment blocks, multi-line calls, function signatures
- [ ] snake_case, correct prefixes (`*_cfg`, `feat_*`, `trg_*`)
- [ ] ASCII console output, no emojis
- [ ] Type annotations on all public functions
