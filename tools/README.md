# tools/

Agent-callable tool implementations for ChronoQuant.

Each file exposes one or more functions that an AI agent can call directly.
Tools must be stateless, side-effect-free where possible, and operate only
through the project's public API (`src/utils.py`, `src/db/`, etc.).

## Adding a tool

1. Create `tools/<domain>.py`.
2. Define functions with full type annotations.
3. Import them in `tools/__init__.py` if they should be auto-discovered.
