# Target Concepts

ChronoQuant target columns define forward-looking events. They are labels for
training and evaluation, not live features.

## Naming

```text
trg_<side>_fw<horizon_minutes>_q<percentile>
```

Examples:

| Target | Meaning |
|---|---|
| `trg_l_fw60_q90` | Long-side forward 60-minute high-percentile event |
| `trg_s_fw60_q10` | Short-side forward 60-minute low-percentile event |

## Value Semantics

| Value | Meaning |
|---|---|
| `1` | Forward event confirmed |
| `0` | Forward event not confirmed |
| `NULL` | Forward window is incomplete/unknown |

## Modeling Rule

Targets are removed from model inputs. They may be used only as labels or
evaluation outcomes.

