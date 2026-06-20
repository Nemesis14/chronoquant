---
epic: epic_029
id: t136
title: data.py comment block cleanup
assignee: ui_agent
status: done
blocks: [t137]
blocked_by: []
---

## Goal

A `src/ui/data.py` minden függvényét `# ===...===` fejléc-blokkok veszik körül, amelyek megismétlik a függvény nevét és célját. Ez ütközik a coding standarddel ("no comments unless WHY is non-obvious").

## Scope

- `src/ui/data.py` — összes `# ===...===` comment blokk eltávolítása

## Acceptance Criteria

- [ ] Minden `# =============...` + `# Purpose:` + `# =============...` blokk eltávolítva (25+ ilyen blokk van)
- [ ] Maguk a függvények, logikájuk nem változnak (pure cleanup task)
- [ ] Megtartandó: inline kommentek ahol a WHY nem nyilvánvaló (pl. `# Stable schema: always look for long_pred / short_pred first.`)
- [ ] `uv run ruff check src/ui/data.py --fix` tisztán fut
- [ ] `uv run pyright src/ui/data.py` tisztán fut

## Notes

Ez a legegyszerűbb task az epicben — pure delete, nincs logikai változás. Elvégezhető t130–t135-tel párhuzamosan, független tőlük.

A fájl ~595 sorából kb. 80-90 sor puszta comment header. Törlés után ~500 sor lesz, ami már kezelhető.
