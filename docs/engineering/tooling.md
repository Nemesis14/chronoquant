# ChronoQuant Tooling

## MCP

The shared agent MCP reference configuration lives in `.agent/.mcp.json`.

Current configured server:

- `language-server`: wraps Pyright through `mcp-language-server`.

Agents should use MCP/LSP tools when they help inspect definitions, references,
or diagnostics. If an LSP call times out or is unavailable, fall back to local
repo inspection with fast shell tools such as `rg`.

## LSP

The language server is configured for the repo workspace:

```text
d:/repos/chronoquant
```

Expected use cases:

- inspect symbol definitions;
- find references;
- check diagnostics after code edits;
- avoid guessing cross-file behavior when LSP can answer directly.

## Session Permissions

This project expects coding agents to be able to:

- read and write files in the repository;
- edit source, config, docs, tests, and scripts;
- run tests and project scripts;
- install Python packages when needed for the task.

Permission enforcement is controlled by the active tool/runtime, not only by
repository docs. If a tool asks for approval, update that tool's runtime settings
or session launch options.

Shared agent allow rules and local runtime references live under `.agent/`.
Codex receives its active MCP tools from the current session environment; in
this repo it should use the exposed language-server tools when available.

## Package Management

Prefer the existing project tooling and lock files. This repo currently includes
`pyproject.toml` and `uv.lock`, so package changes should be made in a way that
keeps those files consistent.

## Remote Training (Google Colab)

Use Google Colab for compute-heavy phases. See `docs/engineering/workflow.md` for
which phases require it.

### How It Works

The workflow is fully agent-driven — the user only presses Run All in Colab:

1. Claude generates or updates the notebook under `notebooks/`.
2. Claude exports the required data (parquet) locally and copies it to
   `F:\My Drive\chronoquant\` (Google Drive desktop app at `F:\`).
3. Claude commits and pushes the notebook to GitHub.
4. Claude opens the Colab URL in the browser:
   `https://colab.research.google.com/github/Nemesis14/chronoquant/blob/main/notebooks/<notebook>.ipynb`
5. User presses **Ctrl+F9** (Run All). No other manual steps needed.

### Notebook Responsibilities

Each Colab notebook must:

- Mount Google Drive (`/content/drive`).
- Clone or update the repo from GitHub into `/content/chronoquant`.
- Read input data from `/content/drive/My Drive/chronoquant/`.
- Install dependencies via `pip` (not `uv`).
- Write output artifacts (models, search results) back to Drive.

### Data Flow

```
Parquet/DuckDB data layer -> sample export path -> samples/<id>/dataset.parquet
                                                        ↓
                                            F:\My Drive\chronoquant\samples\
                                                        ↓
                                            Colab reads from /content/drive/...
                                                        ↓
                                            Artifacts written back to Drive
                                                        ↓
                                            Claude copies artifacts back to repo
```

### Drive Path Convention

| Local path | Colab path |
|---|---|
| `F:\My Drive\chronoquant\samples\` | `/content/drive/My Drive/chronoquant/samples/` |
| `F:\My Drive\chronoquant\models\` | `/content/drive/My Drive/chronoquant/models/` |

### Local Preparation Script

The legacy SQLite sample export entry point was removed. Use the current
Parquet/DuckDB sample export path once it is redefined.

### Notebooks

| Notebook | Purpose |
|---|---|
| `notebooks/colab_hello.ipynb` | Workflow connectivity test |
| `notebooks/colab_training.ipynb` | LightGBM hyperparameter search (smoke + explore, long + short) |
