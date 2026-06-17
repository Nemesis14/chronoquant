"""Table formatting helpers for _doc_/analysis/ notebooks.

Import in the Setup cell:
    import sys
    sys.path.insert(0, "src")
    from table_formatting import format_analysis_table, display_analysis_table
"""

from __future__ import annotations

import pandas as pd
from IPython.display import display

_COUNT_NAMES = {"count", "count(*)", "n", "row_count", "rows", "trades", "volume", "violations"}
_PCT_TOKENS = ("rate", "ratio", "share", "pct", "percent")

_TABLE_STYLES = [
    {"selector": "",         "props": [("border-collapse", "collapse"), ("border", "1px solid black")]},
    {"selector": "th",       "props": [("border", "1px solid black"), ("padding", "4px 8px"), ("text-align", "center")]},
    {"selector": "td",       "props": [("border", "1px solid black"), ("padding", "4px 8px"), ("text-align", "right")]},
    {"selector": "thead th", "props": [("border-bottom", "2px solid black")]},
]


def format_analysis_table(df: pd.DataFrame) -> pd.DataFrame:
    """Return a display-ready copy of df with consistent numeric formatting.

    Rules applied per column (case-insensitive name match):
    - ``year``                          → plain integer string, never ``2024.0``
    - count/integer-named columns       → Int64, zero decimals
    - percentage-named columns          → ``xx.xx%`` (auto-scaled from [0,1] if needed)
    - all other float columns           → 3 decimal string
    """
    out = df.copy()

    for col in out.columns:
        lower = col.lower()

        if lower == "year":
            out[col] = pd.array(out[col], dtype="Int64").astype(str)

        elif (
            lower in _COUNT_NAMES
            or lower.endswith("_count")
            or lower.endswith("_n")
        ):
            out[col] = pd.array(out[col], dtype="Int64")

        elif any(t in lower for t in _PCT_TOKENS):
            values = out[col]
            if pd.api.types.is_numeric_dtype(values):
                max_val = values.max(skipna=True)
                if pd.notna(max_val) and max_val <= 1.0:
                    values = values * 100
                out[col] = values.map(lambda x: f"{x:.2f}%" if pd.notna(x) else "—")

        elif pd.api.types.is_float_dtype(out[col]):
            out[col] = out[col].map(lambda x: f"{x:.3f}" if pd.notna(x) else "—")

    return out


def display_analysis_table(df: pd.DataFrame) -> None:
    """Format and display df with the pandas index hidden and consistent border/alignment styling."""
    display(
        format_analysis_table(df)
        .style
        .hide(axis="index")
        .set_table_styles(_TABLE_STYLES)
    )
