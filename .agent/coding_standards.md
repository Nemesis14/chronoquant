# ChronoQuant Coding Standards

## Quality Gate

During coding — check a single file with CLI (fast, ~3s):

```bash
pyright src/foo.py
```

Before committing — run the full gate from repo root:

```bash
ruff check . --fix
pyright
pytest
```

Ruff always uses CLI (`--fix` auto-applies; MCP cannot do this).

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

### Function Definition Parameter Alignment
For multi-line function signatures, align `:` and `|` across parameters:

```python
def ensure_table_columns(
    db_path    : str,
    table_name : str,
    df         : pd.DataFrame,
) -> None:

def live_model_meta(
    model_cfg : dict | None = None,
    env_cfg   : dict | None = None,
    asset_id  : str  | None = None,
) -> tuple[str, dict]:
```

Align so that `:` and `|` form vertical columns. Pad with spaces before `:` as needed.

### Docstring Args Alignment
In Google-style docstrings, align `:` across all parameter names in the Args block:

```python
def upsert_by_open_time(
    conn       : sqlite3.Connection,
    table_name : str,
    df         : pd.DataFrame,
) -> int:
    """Insert rows, updating non-key columns on open_time conflict.

    Args:
        conn       : Open SQLite connection.
        table_name : Target table with a unique open_time column.
        df         : DataFrame with an open_time column as the conflict key.

    Returns:
        rowcount from executemany.
    """
```

Align to the longest parameter name in the block. Single-param Args blocks need no padding.

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

## Logging

Every module uses the standard `logging` module. No `print()` for operational messages.

```python
import logging

logger = logging.getLogger(__name__)
```

Log levels:
- `logger.debug(...)` — részletes belső állapot, csak fejlesztéskor
- `logger.info(...)` — normál működés: sorok száma, batch progress
- `logger.warning(...)` — nem fatális, de figyelmet igényel
- `logger.error(...)` — hiba, a művelet nem sikerült
- `logger.exception(...)` — hiba + automatikus stack trace (except blokkban)

```python
logger.info("OHLCV batches=%d, inserted=%d, latest=%s", batch_count, inserted_total, ts)
logger.error("Binance API hiba: %s", e)
logger.exception("Varatlan hiba sync_ohlcv kozben")
```

Handler-t csak a belépési pontban (CLI script, `__main__`) konfigurálunk — library kódban soha.

---

## Error Handling

Határfelületen (Binance API, SQLite, fájl I/O) `try/except`, a hibát `logger.exception`-nel jelezzük:

```python
try:
    rows = client.get_klines(...)
except Exception:
    logger.exception("Binance klines lehivas sikertelen")
    raise
```

Belső logikában (saját kód) nem kapjuk el a kivételeket — hagyjuk propagálni.

---

## Console Output

`print()` csak interaktív CLI scriptekben megengedett (`__main__` blokk). Library kódban mindig `logger.*`.

Plain ASCII prefixes only — no emojis (Windows encoding issues):

```python
print("OK: Computed 1,234 feature rows into 'FEATURES'")
print("INFO: Fetching SOLUSDT klines from Binance...")
print("ERROR: No feature rows found since 2026-05-16")
```

---

## Code Navigation Tools

### Eszköz-prioritás kereséshez

| Feladat | Eszköz |
|---------|--------|
| Szimbólum definíciója, hivatkozások | `mcp__language-server__definition` / `__references` |
| Típus, docstring lekérése | `mcp__language-server__hover` |
| Típushibák egy fájlban | `mcp__language-server__diagnostics` |
| Szintaxisfa-alapú mintakeresés | `ast-grep` CLI (`sg run`) |
| Egyszerű string/regex keresés | `Grep` tool |
| Fájlok listázása | `Glob` tool |

### ast-grep (sg) — mikor és hogyan

Használd `sg run` CLI-t (Bash toolon keresztül), ha:
- Strukturális mintát keresel, nem szöveg-egyezést (pl. "minden `with sqlite3.connect(...)` blokk")
- Refaktor előtt fel kell térképezni, hol van egy adott hívásforma
- `Grep` túl sok zajt adna vissza

Szintaxis:
```bash
# Minden függvényhívás adott névvel
sg run --pattern 'utils.$METHOD($$$)' --lang python src/

# Összes függvény-definíció
sg run --pattern 'def $FUNC($$$): $$$' --lang python src/

# Adott with-blokk
sg run --pattern 'with sqlite3.connect($PATH) as $CONN: $$$' --lang python src/

# Csak fájlnév lista
sg run --pattern '...' --lang python src/ --json | python -c "import sys,json; [print(m['file']) for m in json.load(sys.stdin)['matches']]"
```

`sg lsp` az ast-grep rule-alapú szerkesztőhöz való (VS Code) — nem használjuk MCP-ként.

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
