---
epic: epic_044_methodology_doc_restructure
id: t2
title: Feature alfejezetek létrehozása (1100–1600)
assignee: methodology_agent
status: pr
---

## Goal

A `1000_features.md` áttekintő szinten maradt — szükség volt részletes szegmensenként felosztott alfejezet-fájlokra, amelyek tartalmazzák a módszertant, képleteket, időintervallum ábrákat és értelmezést.

## Elvégzett munka

6 új fájl létrehozva a `_doc_/methodology_doc/` könyvtárban:

| Fájl | Tartalom | Feature db |
|---|---|---|
| `1100_features_ar_struktura.md` | Price Action, Market Structure, Gap, Pattern Flags | ~27 |
| `1200_features_lendület_trend.md` | Momentum, Trend, Interaction, Autocorrelation | ~32 |
| `1300_features_volatilitas.md` | Volatility, Tail Risk, Drawdown & Timing | ~34 |
| `1400_features_volume_aktivitas.md` | Volume, Activity, Regime Rank | ~33 |
| `1500_features_kontextus.md` | Return Distance, SR Levels, Time/Session, Session Relative | ~35 |
| `1600_features_specialis.md` | Candle Shape, Ichimoku, Donchian, LinReg, Efficiency, Trend Slope, Extended Accel | ~47 |

Minden fájlban:
- Módszertani leírás: eredet, piaci intuíció, miért prediktív
- Matematikai formulák (LaTeX jelöléssel, nem Python kóddal)
- Ablak méretek táblázata
- Mermaid időintervallum ábrák (t-visszatekintési ablak vizualizációja)
- Értelmezési rész: range, extrém értékek jelentése
- Pontos feature-lista tábla (nevekkel, ablakokkal)

Forrás: `src/data_handling/sync_tables/_features_polars.py`

A `1000_features.md` alfejezet-index táblával bővítve.

## Acceptance Criteria

- [ ] 6 fájl létezik a methodology_doc-ban
- [ ] Minden fájlban legalább 3 Mermaid diagram
- [ ] Minden alcsoporthoz van feature-lista táblázat pontos nevekkel
- [ ] T_MINUS_1_SKIP az 1500-as fájlban explicit dokumentálva
- [ ] `1000_features.md` tartalmaz linket az alfejezetekre
