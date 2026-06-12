# AI fejlesztői eszközök — beállítás és használat

Rendszeres felülvizsgálatra szánt összefoglaló. Frissítsd, ha új eszköz kerül be vagy konfiguráció változik.

---

## Eszközök áttekintése

| Eszköz | Hozzáférés | Mire való |
|--------|-----------|-----------|
| **pyright** | MCP (`mcp__language-server__*`) | Szemantikus navigáció, típusellenőrzés |
| **ruff** | CLI (Bash) | Linting, auto-fix (`--fix`) |
| **ast-grep** (`sg`) | CLI (Bash) | Strukturális kódkeresés, mintaillesztés |
| **uv** | CLI (PowerShell) | `.venv` kezelője — `pip` nincs a venv-ben |

## Csomagkezelés — uv

A `.venv`-et `uv` kezeli, a `pip.exe` **nem létezik** a Scripts mappában.

```powershell
# Csomag hozzáadása (pyproject.toml-t is frissíti):
uv add <csomag>

# Csomag eltávolítása:
uv remove <csomag>

# Szkript futtatása a venv-ben:
uv run python script.py
```

`uv add` automatikusan frissíti a `pyproject.toml`-t és a `uv.lock`-ot — nem kell kézzel szerkeszteni.

---

## Pyright — MCP Language Server

**Konfig:** `.agent/.mcp.json`

```json
"language-server": {
  "command": "mcp-language-server.exe",
  "args": ["--workspace", "d:/repos/chronoquant",
           "--lsp", "d:/repos/chronoquant/.venv/Scripts/pyright-langserver.exe",
           "--", "--stdio"]
}
```

**Pyright konfig:** `pyrightconfig.json` (repo gyökér), Python 3.12, `.venv`, `typeCheckingMode: basic`

**Elérhető toolok:**

| Tool | Mire |
|------|------|
| `diagnostics` | Típushibák és warningok egy fájlban — kódolás közben, CLI pyright helyett |
| `hover` | Szimbólum típusa és docstringe pozíció szerint |
| `definition` | Hol van definiálva egy szimbólum |
| `references` | Hol hivatkoznak egy szimbólumra — refaktor előtt kötelező |
| `rename_symbol` | Projekt-szintű átnevezés |
| `edit_file` | LSP text edit alkalmazása |

**Mikor használd:**
- Fájl szerkesztése után: `diagnostics`
- Refaktor / átnevezés előtt: `references`
- Ismeretlen szimbólum esetén: `hover` vagy `definition`

**Mandatory priority:** if the active agent runtime exposes the language-server
MCP tools, every agent must use them first for definitions, references,
hover/type inspection, and file-level diagnostics. CLI `pyright` is the fallback
for full-project validation, missing MCP tools, or MCP timeout.

---

## Ruff — CLI

**Parancs:** `ruff check . --fix` (repo gyökérből)

Nincs MCP szerver — a `--fix` auto-alkalmazás csak CLI-n lehetséges.

Quality gate részeként: `ruff check . --fix && pyright && pytest`

---

## ast-grep — CLI

**Telepítve:** winget (`ast-grep.ast-grep 0.43.0`), parancs: `sg`

Nincs MCP szerver — CLI Bash toolon keresztül.

**Mikor használd:** strukturális mintakereséshez grep helyett, ha a szintaxis számít, nem a szöveg.

**Szintaxis minták:**

```bash
# Függvényhívás adott névvel
sg run --pattern 'utils.$METHOD($$$)' --lang python src/

# Összes függvény-definíció
sg run --pattern 'def $FUNC($$$):' --lang python src/

# with-blokk mintára
sg run --pattern 'with sqlite3.connect($PATH) as $CONN: $$$' --lang python src/
```

---

## Navigation Priority

All agents must follow this order when the relevant tool is available:

| Task | First choice | Fallback |
|------|--------------|----------|
| Symbol lookup | Pyright MCP `references` / `definition` | `rg` only if MCP is unavailable or times out |
| Type/docstring lookup | Pyright MCP `hover` | read the smallest relevant source file |
| File diagnostics | Pyright MCP `diagnostics` | CLI `uv run pyright <file>` |
| Structural code pattern | `sg run` | `rg` only for simple text patterns |
| Simple text search | `rg` | runtime-specific grep/search tool |
| File listing | `rg --files` or runtime glob tool | shell directory listing |


---

## Felülvizsgálati szempontok

- Van-e új MCP szerver, ami hasznos lenne (pl. ast-grep MCP, ha megjelenik)?
- Directory-level documentation: check whether important `src/` areas have enough context.
- Pyright verzió és `typeCheckingMode` megfelelő-e?
