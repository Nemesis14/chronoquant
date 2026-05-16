# ChronoQuant Code Style Guide

## Overview
This document codifies the code style conventions used throughout the ChronoQuant project. Follow these guidelines when writing or modifying code to maintain consistency and readability.

---

## Comment Style

### File Header Comments
Use heavy separator blocks at the top of each module to document purpose:

```python
# =============================================================================
# Module Title / Brief Description
# =============================================================================
# Purpose:
#  - Bullet point 1
#  - Bullet point 2
#  - Bullet point 3
# =============================================================================
```

**Examples:**
- `sync_ohlcv.py`: "Fetch and sync OHLCV data from Binance"
- `features.py`: "Compute technical indicators and target variable for feature engineering"
- `worker.py`: "WORKER LOOP: runs the sync/predict cycle and streams output via a queue"

### Function Documentation
Document function purpose, parameters, and behavior:

```python
# =============================================================================
# function_name(param1: type, param2: type) -> return_type
# =============================================================================
# Purpose:
#  - What the function does (verb-first)
#  - How it processes data
#  - What it returns/stores
# Parameters:
#  - param1: description and units/format if applicable
#  - param2: description and units/format if applicable
# =============================================================================
```

**Example:**
```python
# =============================================================================
# sync_features(start_time: str, lookback_bars: int = 240) -> None
# =============================================================================
# Purpose:
#  - Fetch raw OHLCV data from [start_time - lookback, end]
#  - Compute target variable (ratio >= percentile)
#  - Generate all configured technical indicators with 'feat_' prefix
#  - Insert rows into features table
# Parameters:
#  - start_time: "YYYY-MM-DD HH:MM:SS" (UTC)
#  - lookback_bars: minutes to look back for feature computation
# =============================================================================
```

### Section Separators
Break code logic into labeled sections with dashes (79 chars):

```python
# -------------------------------------------------------------------------
# Section Name
# -------------------------------------------------------------------------
```

**Examples:**
- `# --------- Load configuration ---------`
- `# --------- Fetch raw OHLCV data ---------`
- `# --------- Compute target variables ---------`
- `# --------- Generate technical indicators with 'feat_' prefix ---------`

### Inline Comments
Use sparingly. When needed, explain *why*, not what:

```python
# ✅ Good: explains intent
rolling_max = df["close"][::-1].rolling(rolling_win).max()[::-1]  # Reverse to look forward

# ❌ Avoid: describes obvious code
rolling_max = df["close"].rolling(rolling_win).max()  # Get rolling max
```

---

## Indentation & Alignment

### Indentation Style
- **Use TABS** (not spaces)
- Indentation level = logical nesting depth
- Continuation lines aligned with tabs + spaces for readability

### Variable Assignment Alignment
Group related assignments and align with tabs:

```python
# Align simple assignments
db_cfg      = utils.load_db_config()
feat_cfg    = utils.load_features_config()
db_path     = db_cfg["database"]["db_path"]
table_ohlcv = db_cfg["database"]["tables"]["ohlcv"]
table_feat  = db_cfg["database"]["tables"]["features"]

# Longer names on separate lines
with sqlite3.connect(db_path) as conn:
	result = pd.read_sql_query(
		f"SELECT MAX(open_time) as max_time FROM {table_name}",
		conn
	)
```

### Multi-line Dictionary/Config Access
Maintain clear alignment:

```python
# Config loading pattern
paths              = model_meta["paths"]
model_dir          = paths["model_dir"]
feat_file          = paths["features_file"]
resolved_model_dir = utils._resolve_path(model_dir)
features_path      = os.path.join(resolved_model_dir, feat_file)
```

---

## Naming Conventions

### Variable Names
- **snake_case** for all variables and functions
- **Prefix config dicts** with domain: `db_cfg`, `feat_cfg`, `model_cfg`, `env_cfg`
- **Prefix features** with `feat_`: `feat_rsi_14`, `feat_roc_140`, `feat_sma_ratio_14`
- **Prefix targets** with `trg_`: `trg_l_rw_240_prc_09`, `trg_s_rw_240_prc_01`

**Examples:**
```python
db_cfg          # Database config dict
feat_cfg        # Features config dict
model_cfg       # Models config dict
df_ohlcv        # DataFrame of OHLCV data
df_reset        # Reset/transformed DataFrame
table_ohlcv     # Table name constant
col_to_keep     # Column filtering variable
feat_cols       # List of feature columns
target_cols     # List of target columns
feat_name       # Individual feature name
rolling_win     # Rolling window size (integer)
```

### Function Names
- **snake_case** with descriptive verb-first action:
  - `sync_features()` — fetch and sync
  - `fetch_predictions_df()` — retrieve data
  - `get_last_timestamp()` — retrieve single value
  - `load_db_config()` — load configuration
  - `_resolve_path()` — private helper (leading underscore)

---

## Code Organization

### Module Structure (standard order)
```python
# 1. Header comments (purpose, overview)

# 2. Imports
import os
import sys
import sqlite3
import pandas as pd

import utils                                  # Local imports after stdlib

# 3. Constants / Config
DEFAULT_LOOKBACK = 240

# 4. Helper functions (optional)
def get_last_timestamp(...): ...

# 5. Main classes / functions
class Worker: ...

def sync_features(...): ...
```

### Function Structure
```python
def sync_features(start_time: str, lookback_bars: int = 240) -> None:
	# 1. Load configuration
	db_cfg = utils.load_db_config()
	
	# 2. Validate inputs (optional)
	
	# 3. Main logic (clearly sectioned)
	
	# 4. Error handling (try/except blocks)
	
	# 5. Return / Print summary
```

---

## Type Hints

Use **PEP 484 type hints** on all function signatures:

```python
# ✅ Good
def sync_features(start_time: str, lookback_bars: int = 240) -> None:
	...

def get_last_timestamp(db_path: str, table_name: str) -> str:
	...

def load_db_config() -> dict:
	...

# ✅ Optional for complex types
def fetch_predictions_df(db_path: str, table: str) -> pd.DataFrame:
	...
```

---

## String Formatting

Use **f-strings** (Python 3.6+):

```python
# ✅ Good
print(f"Computed {len(df_final)} feature rows into '{table_feat}'")
print(f"   Last: {max_ohlcv}")
print(f"Cycle #{cycle} at {utils.now_utc_str()}")

# ❌ Avoid
print("Computed {} rows into '{}'".format(len(df_final), table_feat))
print("Computed " + str(len(df_final)) + " rows")
```

---

## Error Handling

Use explicit try/except blocks with clear error context:

```python
try:
	with sqlite3.connect(db_path) as conn:
		result = pd.read_sql_query(query, conn)
	return result["max_time"].iloc[0]
except Exception:
	return None

# Or with logging
try:
	...
except Exception as e:
	print(f"❌ Error in sync_features: {str(e)}")
	traceback.print_exc()
```

---

## Emoji Usage (Console Output)

Use semantic emojis for log clarity:

| Emoji | Usage |
|-------|-------|
| ✅ | Success/completion |
| ❌ | Error/failure |
| 🌐 | API/network operation |
| 💾 | Database operation |
| ⚙️ | Configuration/setup |
| 🤖 | ML model operation |
| 📊 | Data/analytics |
| 🖥️ | UI/display |
| ⚡ | Performance/speed |
| 🔴 | Critical signal |
| 🟢 | Positive signal |
| ⚪ | Neutral state |

**Example:**
```python
print("✅ Computed 1,234 feature rows into 'FEATURES'")
print("🌐 Fetching BTCUSDT klines from Binance...")
print("❌ No feature rows found since 2026-05-16")
```

---

## Key Patterns

### Config Loading Pattern
```python
db_cfg      = utils.load_db_config()
feat_cfg    = utils.load_features_config()
db_path     = db_cfg["database"]["db_path"]
table_name  = db_cfg["database"]["tables"]["features"]
```

### Database Query Pattern
```python
with sqlite3.connect(db_path) as conn:
	df = pd.read_sql_query(
		f"SELECT col1, col2 FROM {table_name} WHERE time >= ?",
		conn,
		params=(start_time,)
	)
```

### Feature Naming Pattern
```python
feat_prefix = "feat_"
feat_name = f"{feat_prefix}rsi_{window}"
df[feat_name] = ta.momentum.RSIIndicator(close=df["close"], window=window).rsi()
```

### DataFrame Preparation Pattern
```python
feat_cols   = [c for c in df.columns if c.startswith("feat_")]
target_cols = utils.target_columns_from_config(feat_cfg)
cols_final  = ["open_time", "close"] + target_cols + feat_cols
df_result   = df[cols_final].copy()
```

---

## Summary Checklist

When writing code for ChronoQuant:

- [ ] File header with `# ===...===` block + Purpose
- [ ] Function docs with Purpose + Parameters
- [ ] Section dividers for logic blocks: `# -----...-----`
- [ ] Use TABS for indentation
- [ ] Align variable assignments with tabs
- [ ] Use snake_case for all names
- [ ] Prefix configs: `*_cfg`
- [ ] Prefix features: `feat_*`
- [ ] Prefix targets: `trg_*`
- [ ] Add type hints on all functions
- [ ] Use f-strings for formatting
- [ ] Use emoji in console output for clarity
- [ ] Follow config → logic → error flow
- [ ] Keep main_app.py minimal for PyInstaller

