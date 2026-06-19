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
- Keep generated artifacts under `artifacts/<model_id>/`.
- Document serious candidates with a model card.

## Documents

| Document | Purpose |
|---|---|
| `_doc_/5010_sampling_yearly.md` | Active yearly sample methodology |
| `_doc_/5500_hyper_param_search.md` | Active LightGBM search workflow |
| `model_validation.md` | Validation checklist and acceptance criteria |
| `model_cards/_template.md` | Model card template |
