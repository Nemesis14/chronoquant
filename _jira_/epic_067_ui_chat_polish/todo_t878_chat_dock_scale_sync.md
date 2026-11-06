---
epic: epic_067
id: t878
title: Chat ikon + jobb oldali panel, méretezés, sync alatti elérhetőség
assignee: ui_agent
status: todo
blocks: []
blocked_by: []
---

## Goal
Három user-visszajelzés javítása (2026-11-06):
1. Chat: az oldal alján van; legyen egy ikon (lebegő gomb), amire kattintva jobb oldalon kicsúszó panel nyílik (life_plan frontend/src/components/chat/ChatPanel.tsx mintájára: d:\repos\life_plan\frontend\src\components\chat\). A panel nyitva tartása mellett az oldal használható marad; az állapot (nyitva/zárva, history, session) megmarad rerun-ok között.
2. Méretezés: az oldal "nagyítottnak" tűnik, a user kb. 80% böngészőzoomot használ, hogy értelmes legyen. Deríts ki okot (main.py CSS: html font-size 15px, sok fix px méret a komponensekben, layout=wide; scripts/start_ui.ps1 Chrome indítása — device scale / ablakméret / --app mód?), és állítsd úgy az alapértelmezett méretezést, hogy 100% zoomnál is az eddigi "80%-os" kép látszódjon. Ne törd el a chart/oszlop elrendezést.
3. Sync alatti elérhetőség: amikor az adatbázis szinkronizál (src/ui/sync.py, sync_runner.py, main.py sidebar fragment run_every=2s), az oldal nem görgethető/nem elérhető. Találd meg az okot (blokkoló hívás a script threadben, fragment/rerun, DB lock, nagy rerender?) és oldd meg úgy, hogy sync közben is lehessen görgetni, használni a chatet és látni a képet. A sync maga ne sérüljön, a kereskedési logika ne változzon.

## Scope
src/ui/main.py, src/ui/components/chat_panel.py, src/ui/chat/, src/ui/sync*.py, src/ui/.streamlit/config.toml, scripts/start_ui.ps1 (csak ha a méretezés oka ott van).

## Acceptance Criteria
- [ ] Chat ikon lebegő gombként (jobb alsó/felső sarok), kattintásra jobb oldali panel; bezárható; a futó turn nem szakad meg panel nyitás/zárásra
- [ ] 100% böngészőzoomnál az oldal a korábbi ~80%-os nézetnek megfelelő méretű
- [ ] Sync közben az oldal görgethető és a chat használható; az ok a Notes-ban dokumentálva
- [ ] meglévő UI tesztek zöldek, ruff + pyright tiszta, új/érintett logikára teszt
- [ ] valódi böngészős ellenőrzés nem az éles :8501 UI-n, hanem másik porton (utána állítsd le)

## Notes
