---
epic: epic_063
id: t843
title: Kockázat-kiegyenlített súlyozás és realisztikusabb szimuláció
assignee: modeling_agent
status: todo
blocks: [t844]
blocked_by: [t842]
---

## Goal

A mostani könyv **egyenlő súlyú** — a volatilis párok dominálják a P&L-t,
és a kockázatot is. Ez nem az, amit valaki élesben csinálna.

## Scope

- inverz-volatilitás súlyozás a két lábon
- a long és short láb **notional-egyeztetése** (ma implicit egyenlő)
- margin- és likvidációs modell: mennyi tőkeáttétel bírja el a legrosszabb
  drawdownt (a direkciós ágon ez 5x-nél tetőzött, `horizon_and_target_diagnosis.md` §5)
- a legrosszabb hónap és a leghosszabb vesztes sorozat

## Acceptance Criteria

- [ ] Inverz-vol súlyozás lemérve az egyenlő súly ellen
- [ ] Equity görbe, max drawdown, leghosszabb vesztes sorozat
- [ ] Tőkeáttétel-érzékenység, mint a direkciós ágon
- [ ] Kimondva: mekkora tőkével és mekkora áttétellel lenne értelme

## Notes
