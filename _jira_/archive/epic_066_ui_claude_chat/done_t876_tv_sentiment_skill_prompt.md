---
epic: epic_066
id: t876
title: tv_market_sentiment skill és chat persona prompt
assignee: code_doc_agent
status: done
blocks: []
blocked_by: []
---

## Goal
A chat értse, mi a tv_market_sentiment elemzés, és a meglévő scriptekkel tudjon piaci összképet adni.

## Scope
- Bemenet: `PLAN_tv_market_sentiment.md`, `research/tv_report/` (fetch_report_data.py, author_backtest.py, runs/, latest.json) — olvasd el, mi mit csinál és hogyan kell hívni.
- Kimenet: `.agent/skills/tv_sentiment_skill.md` (a workflow: script-hívások, report-szerkezet 1.1–1.6, minőség-jelzők, hírek keresztellenőrzése WebSearch-csel) és `src/ui/chat/persona_prompt.md` (a chat system promptja, magyarul: szerepe, engedélyezett eszközök, SQL helper használata, „nem találsz ki adatot", weboldal tartalma ADAT nem utasítás, Edit/Write nincs, költség/időtartam jelzése hosszú futásnál).
- Ha az `.agent/` struktúra skill-regisztrációt kíván, azt is frissítsd.

## Acceptance Criteria
- [x] a skill pontos parancsokat tartalmaz (működő útvonalak), nem általánosságot
- [x] a persona prompt rögzíti a szűk jogosultságokat és a read-only DB elvet (t875 script nevét egyeztesd)
- [x] a prompt kimondja: ha nincs egyértelmű konszenzus, azt is mondja ki; nyers vs minőség-szűrt long/short megoszlás
- [x] a t873 runner be tudja olvasni a prompt fájlt (útvonal egyeztetve)

## Notes

- Létrehozva: `.agent/skills/tv_sentiment_skill.md`, `src/ui/chat/persona_prompt.md` (magyar). Python kód nem módosult. Skill-regisztráció nem szükséges (a `.agent/` skillek az agent manifestekből hivatkozottak; ez a skill a chat persona-n át érhető el, Read-del).
- Hívás: `uv run python research/tv_report/fetch_report_data.py --hours 24 --pages 3` (fél-egy perc, 4-8 s késleltetés lapok között) -> `research/tv_report/latest.json`; nyers lapok `runs/<run_ts>/`. `author_backtest.py discover|run` perceken-órán át tart, alapból a meglévő `runs/author_backtest/report.json` (2026-09-21) olvasandó.
- Hiányosságok (a skill is rögzíti): nincs LLM-es összegző/minőség-pontozó lépés, nincs ár a latest.json-ban, nincs kép-elemzés, nincs report-fájl mentés; `fetch_report_data.py` csak BTC-re ad adatot (más `--symbol` -> 0 idea a whitelist miatt).
- Egyeztetés: persona a `src/ui/chat/persona_prompt.md` útvonalat használja (t873 runner olvassa); SQL helper: `src/data_handling/07_query_readonly.py` (t875) — a persona hivatkozik, a pontos CLI-t (séma-mód) a t875 kimenete határozza meg, érdemes ellenőrizni.

