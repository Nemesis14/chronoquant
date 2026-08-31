---
epic: epic_053
id: t206
title: Riport és go/no-go döntés
assignee: analyst_agent
status: done
blocks: []
blocked_by: [t205]
---

## Goal

A gate eredményét emberi döntéssé fordítani: folytatódik-e a Reddit track
szentiment-réteggel, vagy véglegesen lezárul.

## Scope

- `_doc_/research/reddit_attention_gate.md` — riport
- minta és szerkezet:
  [tradingview_author_skill.md](../../_doc_/research/tradingview_author_skill.md)

## A riportnak ki kell mondania

- [ ] Mekkora minta jött össze (órák, subredditek, tartomány) és mennyi a
      tényleges Reddit-volumen óránként — sűrű-e egyáltalán perces
      predikcióhoz
- [ ] Az (a) magnitúdó-ág eredménye, FDR után
- [ ] A (b) irány-ág eredménye, kontrollként
- [ ] A frekvencia-eltérés őszinte kezelése: órás feature 60 perces targetre
      → ez rezsim/kontextus-feature, nem per-bar alfa. Ha van hatás, az
      valószínűleg vol-rezsimet mér, nem belépési jelet
- [ ] **Limitációk szekció** — mit NEM mond ki a mérés. Az `epic_051` riport
      erős pontja volt, hogy megkülönböztette a "nincs képesség"-et a
      "nem azonosítható"-tól. Itt ugyanez kell
- [ ] **Döntés**: MEGY (→ szentiment-réteg külön epicben) vagy NEM MEGY
      (→ a Reddit track lezárul, `plan.md` és az `epic_052` tombstone
      frissítendő az aggregált ágra is)

## Notes

Ha NEM MEGY: az eredmény akkor is érték. Két hipotézis lesz kizárva olcsón —
a szerző-skill (`epic_051`) és az aggregált figyelem —, és a `btcusdt` store
plusz a `social_hourly` idősor bármilyen későbbi BTC-munkához megmarad.

[orchestrator] ELKESZULT — 2026-08-21

Riport: `_doc_/research/reddit_attention_gate.md`

Dontes: **NEM EROLTETJUK.** A jel valos, de gyenge (IC 0,02-0,10), nem stabil
subredditek kozott (amelyik korszakban egy subreddit mukodik, a masikban nem),
es az inkrementalis erteke a meglevo volatilitas-feature-ok felett nincs
bizonyitva. A varhato hozadek nem indokolja a tovabbi munkat.

Az irany-kontroll mindket rezsimben tiszta nulla maradt — ez a meres
ervenyesseget igazolja, es azt is, hogy a pipeline nem gyart jelet a semmibol.
