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
- Keep generated model artifacts under `models/<model_id>/`.
- Keep candidate model evaluation output separate from the live predictions table.

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
- Targets: `trg_l_fw60_q90`, `trg_s_fw60_q10`
- Verb-first functions: `sync_features()`, `load_db_config()`
- Private helpers: leading underscore `_resolve_path()`

---

## Code Organization

```python
# 1. Header docstring
# 2. Imports (stdlib → third-party → internal)
# 3. Constants / Config
# 4. Helper functions
# 5. Main classes / functions
```

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
