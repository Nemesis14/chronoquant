---
epic: epic_058
id: t705
title: Összevetés a regressziós megközelítéssel, go/no-go
assignee: analyst_agent
status: pr
blocks: []
blocked_by: [t704]
---

## Goal

Eldönteni, hogy a triple-barrier megközelítés jobb-e, és éles tőkére
alkalmas-e.

## Az összevetés

Azonos ablakon (2025-08 → 2026-07), azonos feature-készlettel:

| | Regresszió (fw240 MFE) | Triple barrier |
|---|---|---|
| legjobb nettó | +10,52% (in-sample szelekcio) | ? |
| nyereséges setup | 4/200 | ? / 8 |
| kötésszám | 242 | ? |
| out-of-sample allitas | nincs | ? |

## A döntési szabály — előre rögzítve

| Kimenet | Verdikt |
|---|---|
| A felső sáv +1 aránya > 55% ÉS a nettó pozitív 12 hónapon | **GO** — deploy tervezés indulhat |
| 55% fölött, de a nettó nulla körül | a korlátokat kell újrahangolni (150/150), nem deploy |
| 55% alatt | **A megközelítés lezárul.** A modell nem tud irányt jósolni ezen a horizonton. |

Ezt **most** rögzítjük, nem az eredmények ismeretében.

## Ha a verdikt negatív

Akkor ki kell mondani, hogy a jelenlegi feature-készlettel és 4 órás
horizonton a SOLUSDT irány-előrejelzés nem oldható meg. Az infrastruktúra
(adat, pipeline, költség-modell, validációs fegyelem) megmarad és értékes —
de a kereskedési hipotézis lezárul.

## Acceptance Criteria

- [ ] `_doc_/research/triple_barrier_gate.md` riport
- [ ] A fenti döntési szabály alkalmazva, az eredmény kimondva
- [ ] Ha negatív: ugyanolyan egyértelműen leírva, mint egy GO

## Notes

[orchestrator] Elvegezve — 2026-08-26

Riport: `_doc_/research/triple_barrier_gate.md`

**VERDIKT: NO-GO** — az elore rogzitett szabaly szerint.

- A nyero irany a trenddel fordul: eso piacon SHORT p95 = 59,2%,
  emelkedon LONG p99 = 61,8%. Egyik konfiguracio sem teljesit MINDKET rezsimben.
- A lift valos (~20pp mindket rezsimben), de nem eleg a dijfedezethez.
- Trend-szuro: ROSSZABB lett (44,8% vs a csak-modell 59,2%-a).
- "A modell valassza az iranyt": nem mukodik, mert a P(+1) szisztematikusan
  magasabb a P(-1)-nel — beepitett long-elfogultsag, a ket osztaly nincs
  egymashoz kalibralva.

t704 (egyszerusitett strategy reteg) TOROLVE: nincs ertelme strategiat epiteni
egy meg nem erositett jelre.

**A rezsim a rendszer hianyzo dimenzioja** — ma haromszor futottunk bele
ugyanabba. De szurokent nem potolhato; ha valaha folytatodik, a rezsimnek a
modell BEMENETENEK kell lennie, nem utolagos kapunak.

[orchestrator] Kiegeszites — 2026-08-26 (tisztesseges teszt)

A NO-GO megerositve, most mar AZONOS MERCEVEL:
100 Optuna trial + havi ujratanitas + 24 oras korlat (kiegyensulyozott
osztalyok, 48,5/51,1/0,4) + realisztikus koltseg.

Eredmeny: 1612 kotes, -11,03 bps/kotes, -178 USD / 1000 notional.
Egyetlen rezsim-irany cella sem eri el az 55,3%-os fedezeti pontot;
a short eso piacon 42,7% az 51,1%-os alappal szemben (a veletlennel is rosszabb).

**A havi ujratanitas NEM valtoztatott az eloljelen** — a user hipotezise
(hogy a meres tul szigoru volt) tesztelve es elvetve.

A korabbi 4 oras +20pp lift az kiegyensulyozatlan osztalyeloszlas muveke volt:
ott a modell azt tanulta, MIKOR van nagy mozgas, nem azt, hogy MERRE.

[orchestrator] Vegleges meres — 2026-08-26 (4 oras cimke, napi kalibracio)

A 24 oras verzio mas feladat volt (alap ~48,5%, tisztan irany). A 4 orasra
visszaterve, 100 trial + havi ujratanitas + NAPI kalibracio:

  1986 kotes, -9,12 bps/kotes, -181 USD / 1000 notional

MIND A NEGY rezsim x irany cellaban POZITIV a lift:
  emelkedo LONG +12,9pp | emelkedo SHORT +9,0pp
  eso LONG +5,1pp       | eso SHORT +10,2pp
  osszesen: LONG +9,3pp, SHORT +9,6pp

**A modell a szukseges el ~55%-at hozza.** Alap 37,7%, fedezeti pont 55,3%,
szukseges lift +17,6pp, a modell +9,3pp-t ad.

Nem nyereseges, de nem is reménytelen — es ez lenyegesen jobb kep, mint a
6. szakasz 24 oras merese (ott ~0 lift volt).

Ket konkret irany a szakadek zarasara (egyik sem parameterezés):
- bookDepth orderbook-melyseg (2023-tol megvan, soha nem hasznaltuk)
- maker belepes: a fedezeti pont 55,3% -> 51,8%, a szukseges lift -> 14,1pp
