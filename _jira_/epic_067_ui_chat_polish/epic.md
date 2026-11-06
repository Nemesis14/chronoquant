# Epic 067: UI chat finomhangolás (lebegő chat, méretezés, sync alatti elérhetőség)

## Goal
Az epic_066 chat user-visszajelzései: (1) a chat ne az oldal alján legyen, hanem ikon + jobb oldali kicsúszó panel, mint a life_plan-ben; (2) az oldal méretezése "nagyítottnak" tűnik (user ~80% böngészőzoomra kényszerül); (3) DB-sync alatt az oldal nem görgethető/elérhető, a chat sem.

## Scope
`src/ui/` (main.py, components/chat_panel.py, sync*.py, .streamlit), esetleg `scripts/start_ui.ps1` (Chrome indítás).

## Tasks
- t878: chat ikon+jobb oldali panel, méretezés, sync alatti reszponzivitás (ui_agent)
