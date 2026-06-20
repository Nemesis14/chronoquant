# uv Tool — Python Environment and Package Management

`uv` manages the `.venv`. `pip` does not exist in this project.

---

## Core Commands

```powershell
# Add a package (updates pyproject.toml and uv.lock automatically)
uv add <package>

# Remove a package
uv remove <package>

# Run a script in the venv
uv run python script.py

# Run a tool (pyright, ruff, pytest)
uv run pyright
uv run pytest src/<module>/tests/
```

---

## Rules

- Never edit `pyproject.toml` dependency versions manually — use `uv add`/`uv remove`
- Never use `pip install` — it does not exist in `.venv/Scripts/`
- `uv add` updates both `pyproject.toml` and `uv.lock` atomically
- Always run commands from the repo root

---

## Environment Info

- Python: 3.12
- Venv: `.venv/` at repo root
- Pyright binary: `.venv/Scripts/pyright-langserver.exe`
