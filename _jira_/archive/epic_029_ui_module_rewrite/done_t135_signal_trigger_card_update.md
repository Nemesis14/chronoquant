---
epic: epic_029
id: t135
title: Signal trigger card frissítés — journal döntés alapú megjelenítés
assignee: ui_agent
status: done
blocks: [t137]
blocked_by: [t130]
---

## Goal

A `_render_signal_trigger_card()` jelenleg raw prediction score-t hasonlít raw entry threshold-hoz:
- `long_pred` (0.0–1.0 raw MFE score) vs `entry_threshold` (régi config mező, most nem létezik)

Az új strategy percentile-alapú: a trading service `np.interp` segítségével konvertálja a raw score-t percentilre, majd ezt hasonlítja `long_entry_pct`-hez. A raw score önmagában nem jelzi az entry-t.

A card-ban már van egy "Legutóbbi döntés" szekció a trading journal `get_recent_signals()`-ból — ez az egész döntési logikát lefedi (decision + reason + score_pct). Ez a helyes forrás.

## Scope

- `src/ui/components/trade_panel.py` (t130 után) — `_render_signal_trigger_card()`

## Acceptance Criteria

- [ ] A raw prediction bar (progress bar + threshold marker) eltávolítva, ha nincs értelmes raw vs threshold összehasonlítás
- [ ] A card fókusza: **jelenlegi state** (FLAT / LONG / SHORT / COOLDOWN) + **utolsó döntés** (decision, reason, bar_open_time) a trading journal-ból
- [ ] Ha a trading service nem fut (nincs journal adat), a card "Nincs aktív kereskedési service" üzenettel jelenik meg
- [ ] Ha `load_long_short_strategies()` (t131 után) visszaad entry_pct-t, opcionálisan mutatható egy "entry küszöb: X%" szöveg — de ne legyen progress bar raw score-ral
- [ ] A kártyában lévő "Trigger állapot" fejléc lecserélhető "Trading State"-re
- [ ] `uv run pyright src/ui/` tisztán fut

## Notes

A trading service `trading_signals` táblában tárolja: `bar_open_time`, `pred_long`, `pred_short`, `state_before`, `decision`, `reason`. A percentile nem kerül a journal-ba (csak a service logba). Ezért a raw score megjelenítése megtartható informatív célból (`pred_long`, `pred_short` értékként), de ne úgy tünjön fel mintha a döntési logika alapján mutatná az "AKTÍV/VÁRAKOZIK" állapotot — mert az a percentile-n alapul.

Legegyszerűbb helyes megközelítés: a card mutatja a legutóbbi signal `decision` + `reason` + `state_before` értékeit, és a jelenlegi state-t (FLAT/LONG/SHORT/COOLDOWN) a `get_trading_status()`-ból.
