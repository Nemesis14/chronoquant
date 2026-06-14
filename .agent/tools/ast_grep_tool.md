# ast-grep Tool — Structural Code Search

Syntax-aware Python search. Use when pattern matters, not just text.

---

## When to Use

- Structural pattern search before refactoring (e.g. "every `with duckdb.connect(...)` block")
- Finding all call sites of a function signature
- When `rg` returns too much noise due to text-only matching

Use `rg` for simple string/regex search. Use `sg run` when the syntax tree matters.

---

## Installation

```powershell
# Already installed: ast-grep 0.43.0 via winget
# Command: sg
```

---

## Syntax

```bash
# All calls to a specific method
sg run --pattern 'utils.$METHOD($$$)' --lang python src/

# All function definitions
sg run --pattern 'def $FUNC($$$):' --lang python src/

# Specific with-block
sg run --pattern 'with duckdb.connect($PATH) as $CONN: $$$' --lang python src/

# File list only (no match content)
sg run --pattern '...' --lang python src/ --json \
  | python -c "import sys,json; [print(m['file']) for m in json.load(sys.stdin)['matches']]"
```

---

## Pattern Variables

| Variable | Matches |
|----------|---------|
| `$VAR` | Single node (expression, identifier) |
| `$$$` | Zero or more nodes (variadic) |

---

## Notes

- No MCP server — CLI only via Bash tool
- `sg lsp` is for VS Code rule-based editing — not used here
