# evals/

Evaluation harnesses for agent and model output quality.

Evals verify that tools, skills, and schema contracts behave correctly.
They are separate from `tests/` (which covers source modules) — evals
focus on agent-observable behaviour and end-to-end correctness.

## Running evals

```bash
pytest evals/
```
