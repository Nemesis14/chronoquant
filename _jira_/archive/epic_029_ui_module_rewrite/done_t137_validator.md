---
epic: epic_029
id: t137
title: Validator session — ui module rewrite
assignee: validator_agent
status: done
blocks: []
blocked_by: [t130, t131, t132, t133, t134, t135, t136]
---

## Goal

Validálni az epic_029 összes pr_ taskját: ruff + pyright, majd done_ státuszra emelni.

## Scope

- `src/ui/`
- `src/ui/components/`

## Acceptance Criteria

- [ ] `uv run ruff check src/ui/ --fix` — 0 hiba
- [ ] `uv run pyright src/ui/` — 0 hiba
- [ ] Manuális smoke test: `STREAMLIT_CONFIG_DIR=src/ui uv run streamlit run src/ui/main.py` — dashboard betöltődik, nincs Python traceback a terminalban
- [ ] Minden pr_t130–pr_t136 → done_ átnevezve

## Notes

Az UI modulnak nincs automatizált pytest suite-ja — a validáció manuális smoke test (dashboard betöltés + funkció ellenőrzés). Ha bármely lépés fail-el: a releváns pr_ ticket visszakerül todo_-ba, Notes szekcióba a hiba leírva.

Ellenőrizendő funkciók:
- Chart betöltődik (prediction + OHLCV adat)
- Sidebar sync gomb működik
- Trading start/stop gomb elérhető
- Trade panel kártyák nem dobnak exception-t
- Log panel megjelenik
