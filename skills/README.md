# skills/

Reusable agent skill definitions for ChronoQuant.

Skills are higher-level capabilities composed from tools. They encode domain
knowledge (e.g. "how to evaluate a model candidate") as callable workflows
rather than one-shot tool calls.

## Adding a skill

1. Create `skills/<name>.py`.
2. Compose tool calls and validation logic.
3. Document preconditions and expected outputs in a module-level docstring.
