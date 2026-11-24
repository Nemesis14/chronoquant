---
epic: epic_073
id: t929
title: Impulse Scout módszertan (5300)
assignee: methodology_agent
status: done
blocks: [t930, t931]
blocked_by: []
---

## Goal
`_doc_/methodology_doc/5300_impulse_scout.md`: az 5200 v2 eseménydefinícióra építve rögzítsd:
- Esemény és címke (first-touch 3 osztály + az első érintés ideje), W4h/H8h elsődleges cella; több asset összevonása
  (asset-normalizálás ATR/σ_W egységekben), az asset-azonosító szerepe.
- Közös protokoll: 6 blokkos purged CV a train-en (purge teljes intervallum + embargo H), azonos foldok minden modellnek,
  baseline = skalár logit (S0+C).
- Metrikák: AUC (impulzus vs többi), 3-osztályos log-loss, felső 20%/10% precizitás + lift, kalibráció,
  triple-barrier kereskedési szimuláció költséggel (díjak a `config/trading.json` szerint).
- Többszörös tesztelés: sok modell → White Reality Check / Hansen SPA vagy Holm; előre regisztrált elsődleges modellek.
- Holdout-2 szabály (érintetlen, egyszeri végső próba, minimum eseményszám); a SOL OOS már látott (t928).
- Modellcsaládok rövid indoklása (szakirodalom): meta-labeling/triple barrier, MiniRocket/InceptionTime,
  versengő kockázatok (Fine-Gray/DeepHit), order flow, idősor-alapmodellek (Chronos/Kronos).
- Go/no-go: mikor „jobb” egy modell a baseline-nál.

## Acceptance Criteria
- [ ] 5300 doksi, kódmentes, döntési szabályokkal

## Notes
- Elkeszult: `_doc_/methodology_doc/5300_impulse_scout.md` (v1, 2026-11-21), kodmentes, 11 Mermaid diagram, a ticket Goal minden pontjara.
- Fo szabalyok: W4h_H8h elsodleges cella; pooling SOL+BTC+ETH sigma_W/ATR egysegben, asset-id nelkul (X3 kiserleti); kozos naptari 6 blokkos purged CV + embargo H; baseline S0+C; elsodleges H1 LightGBM+order flow, H2 MiniRocket, H3 hazard (Holm, p<0,01); SPA/WRC a teljes leaderboardra; nap-klaszter (1 nap = 3 asset) bootstrap/permutacio; 1. forduló minden csalad, 2. forduló legjobb 1-2; go/no-go 7 statisztikai lepes + gazdasagi szint; holdout-2 egyszeri, befagyasztott jelolt, min. esemenyszam.
- Megjegyzes: a dijak NEM a `config/trading.json`-ban vannak, hanem a `config/strategies.json` `costs` szekciojaban (maker/taker/slippage, kilepes-tipus); a doksi ezt hivatkozza, szamot nem kodol be.
- Feltetelezesek (validalasra varo) a doksi vegen listazva (minimum esemenyszamok, CI-feltetel, asset-elojel szabaly, belepo ar, holdout-2 minimum stb.). Nem commitolva, ticket nem nevezve at.
- [validator] Validated (doc-only) — 2026-11-21: Goal minden pontja lefedett; kodmentes; 11 Mermaid diagram, idezesi stilus az 5200-zal egyezik; a `config/strategies.json` `costs` (maker 2,0 / taker 5,0 / slippage 1,5 bps, kilepes-tipus) megvan, a `config/trading.json`-ban nincs dij-mezo; look-ahead szabalyok (jellemzo [t0-B,t_c], cimke (t_c,t_c+H], belepo a t_c utani elso bar nyitoja, kozos naptari blokk purge/embargo) konzisztensek az 5200 v2-vel es az epic_072 tanulsagaival.
- Javitva: SOL holdout-2 szam (41 W4h_H8h esemeny, 44 egyedi t0 a ket 4h cellaban); go/no-go eredmenysor + diagram a B) CV-szimulacios eloszuroval; kiserleti jelolt holdout-2 sikerenek statusza; D3 diagram felirat; nyelvtani javitas.
- Nyitott (nem blokkolo, user/t931 dontes): H1-H3 melyik valtozatra (1. fordulo rogzitett vs. 2. fordulo legjobb) vonatkozik; holdout-2 minimum eleresi ideje ~2027 marcius kozepe (becsles, 3 asset ~0,37 esemeny/nap/asset).
