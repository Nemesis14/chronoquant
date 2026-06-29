---
epic: epic_047
id: t2
title: Közös kód kiemelése analyst/lib/-be
assignee: analyst_agent
status: todo
blocks: [t3]
blocked_by: [t1]
---

## Goal
A long model 4 notebookjából azonosított közös kódot kiemelni az `analyst/lib/` modulba
úgy, hogy a notebooks-ban csak az import + paraméterek maradjanak. Cél: artifact notebookok
ne tartalmazzanak src kódot, csak eredményeket és minimális betöltési logikát.

## Scope
- `analyst/lib/plot_utils.py` — CQ_COLORS, sns_setup(), közös plot wrappers
- `analyst/lib/db_utils.py` — DB loading patterns (sample betöltés, ohlcv betöltés)
- Esetlegesen `analyst/lib/sampling_analysis.py` ha a logika elég nagy
- `analyst/__init__.py` — export ellenőrzés

## Acceptance Criteria
- [ ] CQ_COLORS és sns_setup() `analyst/lib/plot_utils.py`-ban van, notebookból importálható
- [ ] DB loading helper (sample tábla betöltés) `analyst/lib/db_utils.py`-ban van
- [ ] Nem kerül src kód az artifact notebookokba (csak analyst.lib import)
- [ ] Meglévő `analyst/lib/` kód nem törik el (backward compatible extension)

## Notes
t1 audit azonosítja pontosan mi kerüljön ide.
