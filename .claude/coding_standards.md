# ChronoQuant Coding Standards

## Quality Gate

Before committing, run:

```bash
ruff check . --fix
pyright
pytest
```

## Language Rules

- All public functions in `src/` must have type annotations on parameters and return values.
- Use Pydantic v2 schemas (`schemas/`) for any data contract that crosses a module boundary — do not pass raw `dict` between layers.
- Do not replace existing `dict`-based internal logic with Pydantic unless explicitly asked.
- New tools in `tools/` and skills in `skills/` must be fully annotated.

## Core Project Rules

- Run commands from the repo root.
- Use `src/utils.py` as the config-loading entry point. Do not read JSON config files directly from business logic.
- Keep scripts thin; reusable logic belongs under `src/`.
- Store timestamps as UTC strings in `YYYY-MM-DD HH:MM:SS`.
- Keep generated model artifacts under `models/<model_id>/`.
- Keep candidate model evaluation output separate from the live predictions table.

---

## Documentation Style

### File Header
Module-level docstring at the top of each file:

```python
"""Sync OHLCV data and compute derived features for a given asset.

Reads from Binance, writes to SQLite via the shared maintenance workflow.
Idempotent by open_time — safe to re-run.
"""
```

### Function / Class Docstrings
Google-style. Pyright and IDEs use these for hover info and type inference:

```python
def sync_features(start_time: str, lookback_bars: int = 240) -> None:
    """Fetch OHLCV and compute all configured features from start_time.

    Args:
        start_time: UTC timestamp in YYYY-MM-DD HH:MM:SS format.
        lookback_bars: Minutes to look back for feature computation.

    Returns:
        None. Inserts rows directly into the features table.
    """
```

For simple one-liners, a single-line docstring is enough:

```python
def now_utc_str() -> str:
    """Return current UTC time as YYYY-MM-DD HH:MM:SS string."""
```

### File-level Section Separators
Use `# %%` markers to separate major logical blocks within a file.
VS Code recognizes these as interactive cells (runnable with Ctrl+Enter):

```python
# %% Load configuration
db_cfg = utils.load_db_config()

# %% Fetch OHLCV
df = fetch_ohlcv(start_time)

# %% Compute features
```

### Within-function Section Separators
Use short dash separators inside longer functions:

```python
def sync_features(start_time: str) -> None:
    """..."""
    # --- load config ---
    db_cfg = utils.load_db_config()

    # --- fetch data ---
    df = fetch_ohlcv(start_time)
```

### Inline Comments
Use sparingly. Explain *why*, not what:

```python
rolling_max = df["close"][::-1].rolling(rolling_win).max()[::-1]  # Reverse to look forward
```

---

## Alignment Conventions

### Variable Assignment Alignment
Align `=` signs for short, related assignment blocks when it improves scanability:

```python
db_cfg      = utils.load_db_config()
feat_cfg    = utils.load_features_config()
db_path     = db_cfg["database"]["db_path"]
table_ohlcv = db_cfg["database"]["tables"]["ohlcv"]
```

### Function and Call Parameter Alignment
For multi-line calls, align keyword argument `=` signs. This is a deliberate local
exception to strict PEP 8 keyword-call spacing:

```python
parser.add_argument(
    "--start",
    default = INIT_START_DATE,
    help    = "Start time, format: YYYY-MM-DD HH:MM:SS",
)

rebuild_derived_tables(
    start            = args.start,
    end              = args.end,
    features_only    = args.features_only,
    predictions_only = args.predictions_only,
)
```

---

## Naming Conventions

- **snake_case** for all variables and functions
- **Prefix config dicts**   : `db_cfg`, `feat_cfg`, `model_cfg`, `env_cfg`
- **Prefix features**       : `feat_rsi_14`, `feat_roc_140`
- **Prefix targets**        : `trg_l_fw60_q90`, `trg_s_fw60_q10`
- **Verb-first functions**  : `sync_features()`, `load_db_config()`
- **Private helpers**       : leading underscore `_resolve_path()`

---

## Code Organization

### Module Structure
```python
# 1. Header comments
# 2. Imports (stdlib → third-party → internal)
# 3. Constants / Config
# 4. Helper functions
# 5. Main classes / functions
```

### Function Structure
```python
def sync_features(start_time: str, lookback_bars: int = 240) -> None:
    # 1. Load configuration
    # 2. Validate inputs (optional)
    # 3. Main logic (clearly sectioned)
    # 4. Error handling
    # 5. Return / Print summary
```

---

## Error Handling

```python
try:
    with sqlite3.connect(db_path) as conn:
        result = pd.read_sql_query(query, conn)
    return result["max_time"].iloc[0]
except Exception as e:
    print(f"ERROR in sync_features: {str(e)}")
    return None
```

---

## Console Output

Plain ASCII prefixes only — no emojis (Windows encoding issues):

```python
print("OK: Computed 1,234 feature rows into 'FEATURES'")
print("INFO: Fetching SOLUSDT klines from Binance...")
print("ERROR: No feature rows found since 2026-05-16")
```

---

## Key Patterns

### Config Loading
```python
db_cfg     = utils.load_db_config()
feat_cfg   = utils.load_features_config()
db_path    = db_cfg["database"]["db_path"]
table_name = db_cfg["database"]["tables"]["features"]
```

### Database Query
```python
with sqlite3.connect(db_path) as conn:
    df = pd.read_sql_query(
        f"SELECT col1, col2 FROM {table_name} WHERE time >= ?",
        conn,
        params=(start_time,),
    )
```

### Feature / Target Columns
```python
feat_cols   = [c for c in df.columns if c.startswith("feat_")]
target_cols = utils.target_columns_from_config(feat_cfg)
cols_final  = ["open_time", "close"] + target_cols + feat_cols
```

---

## Checklist

- [ ] Module docstring at file top
- [ ] Google-style docstring on every public function and class
- [ ] `# %%` markers for major file-level sections
- [ ] `# --- ... ---` for within-function sections
- [ ] Align assignment blocks and multi-line keyword calls per guide
- [ ] snake_case, correct prefixes (`*_cfg`, `feat_*`, `trg_*`)
- [ ] ASCII console prefixes, no emojis
- [ ] config → logic → error flow
