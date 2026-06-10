# Modeling Guide

ChronoQuant model development follows a time-series ML workflow:

```text
data audit -> sample/split -> model registry entry -> feature audit
-> search/training -> validation -> model card -> strategy evaluation handoff
-> promotion
```

## Core Rules

- New production candidates should use LightGBM unless a documented experiment
  says otherwise.
- Use chronological splits only.
- Keep final holdout untouched during feature/model/trigger selection.
- Reuse `sample_id` for comparable long/short or candidate variants.
- Keep generated artifacts under `models/<model_id>/`.
- Document serious candidates with a model card.

## Documents

| Document | Purpose |
|---|---|
| `sampling.md` | Sample, split, embargo, holdout policy |
| `lightgbm_development.md` | LightGBM search and promotion workflow |
| `model_validation.md` | Validation checklist and acceptance criteria |
| `model_cards/_template.md` | Model card template |

