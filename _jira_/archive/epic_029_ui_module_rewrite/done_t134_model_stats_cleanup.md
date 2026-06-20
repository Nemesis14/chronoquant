---
epic: epic_029
id: t134
title: Model stats cleanup — _load_sol_model_stats + _render_model_stats_panel eltávolítás
assignee: ui_agent
status: done
blocks: [t137]
blocked_by: []
---

## Goal

A `main.py`-ban (t130 után `components/trade_panel.py`-ban) lévő `_load_sol_model_stats()` és `_render_model_stats_panel()` elavult:

- `_load_sol_model_stats()` a `artifacts/<model_id>/model_card.json`-t keresi — ez a fájl nem létezik az új artifact struktúrában (csak `manifest.json`, `features.json`, `params.json` van)
- A metrikák rosszak: `train_prauc`, `valid_prauc` — de a modellek folytonos regressziósak, nem bináris osztályozók
- A `render_trade_panel()` végén `_render_model_stats_panel()` hívás van a "Champion" kártyákhoz — ezek soha nem töltik be az adatot (KeyError / üres dict), és csendesen eltűnnek

## Scope

- `src/ui/components/trade_panel.py` (t130 után) — `_load_sol_model_stats()` + `_render_model_stats_panel()` + a hívási helyek
- `render_trade_panel()` vége — eltávolítani a model stats kártyák renderelését

## Acceptance Criteria

- [ ] `_load_sol_model_stats()` eltávolítva
- [ ] `_render_model_stats_panel()` eltávolítva
- [ ] `_SOL_MODEL_STATS` modul-szintű betöltés eltávolítva
- [ ] `render_trade_panel()` nem hívja model stats rendert
- [ ] A dashboard nem dob KeyError-t / silent exception-t a model stats betöltésekor
- [ ] `uv run pyright src/ui/` tisztán fut

## Notes

Ha a jövőben model teljesítmény kártyát akarunk mutatni, az alapja a `manifest.json` (`model_id`, `status`, `fit_date`, stb.) és a strategy artifact `summary.json` (trades, win_rate, gross_return) lesz — nem az egyes modell CV metrikái. De ez külön task, most csak a stale kód kerül ki.
