---
epic: epic_027
title: Strategy methodology and runtime contract
status: active
---

## Cél

Véglegesíteni a strategy domain metodológiáját úgy, hogy:

1. a `src/strategy/` a modellscore-okból ténylegesen kereskedhető stratégiát képezzen;
2. a logika illeszkedjen a meglévő `strategy_table` + `strategy_artifact` irányhoz;
3. a kimenet a `src/trading/` számára közvetlenül alkalmazható runtime contract legyen;
4. a dokumentációs számozás és felelősségi határ tiszta legyen:
   - `6000_strategy.md` = strategy metodológia
   - a trading domain később külön fejezetet kapjon, mint a strategy alkalmazó rétege.

## Motiváció

- A jelenlegi 6xxx dokumentáció összemossa a strategy és trading felelősségeket.
- A modellek a jelenlegi evidence alapján inkább rangsorolásra, mint abszolút értékbecslésre erősek.
- Emiatt a strategy réteg elsődleges feladata nem egyszerű score-thresholdolás, hanem
  rank-alapú jelalkotás, konfliktuskezelés, artifact-perzisztencia és runtime alkalmazhatóság.
- A live trading csak akkor validálható, ha ugyanazt a signal contractot kapja, amit az offline
  strategy build és optimize lépés előállított.
- A fejlesztési default működés egyablakos: a user által megadott időszakon történik
  a calibration, a strategy search és a riportálás is; ezt az artifactnak explicit jelölnie kell.
- A strategy artifactnek nem csak paramétereket, hanem értelmezhető eredmény-outputot is kell adnia:
  realized trade ledger, equity-szerű összegzés, és analyst által generált bemutató notebook/HTML report.

## Taskok

| ID | Cím | Assignee | Blokkolja | Blokkolt általa |
|----|-----|----------|-----------|-----------------|
| t116 | Strategy methodology és 6xxx doc reset | methodology_agent | - | t117, t118, t119, t120 |
| t117 | Strategy artifact runtime contract | modeling_agent | t116 | t119, t120 |
| t118 | Rank calibration pipeline a strategy table-re | modeling_agent | t116 | t119 |
| t119 | Strategy optimizer és backtest-live alignment | modeling_agent | t117, t118 | t120, t121 |
| t120 | Trading runtime strategy application | ui_agent | t117, t119 | t121 |
| t121 | Validator session | validator_agent | t119, t120 | - |
| t122 | Strategy realized backtest outputs | modeling_agent | t119 | t123, t124 |
| t123 | Strategy artifact analyst notebook and Quarto report | analyst_agent | t122 | t124 |
| t124 | Validator session for strategy reporting | validator_agent | t122, t123 | - |
