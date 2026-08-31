# Epic 062: Cross-sectional előmérés — piac-semleges rangsor

> **A logika:** a modell dokumentáltan **jól rangsorol** (2,77× lift a top
> 1%-on, stabil kalibrációval), és dokumentáltan **bukik az abszolút irányon**,
> mert *„a nyerő irány a trenddel fordul"*. Egy cross-sectional felállás
> pontosan a bizonyított képességet használja, és **kiejti a bétát** —
> vagyis magát a rezsim-függést, ami háromszor megölt minket.

## Goal

**Ez egy előmérés, nem építés.** Egyetlen kérdésre válaszol, mielőtt bárki
egy sor stratégia-kódot írna:

> Elég nagy-e a felső és alsó decilis közti keresztmetszeti hozamkülönbség
> ahhoz, hogy **kétszeres** kör-költséget fedezzen?

## ⚠️ Az ellenérv, amit előre kimondunk

Két láb = **kétszeres díj**. A jelenlegi struktúrában 2 × 13 bps = 26 bps
kötésenként. Ha a decilis-spread ezt nem hozza fényévvel, az irány rossz és
egy nap alatt lezárul. **Nem építünk semmit, amíg ez nem dől el.**

## Miért olcsó

- Az adat ingyenes (Binance Vision, ugyanaz a csatorna, ami már fut)
- A feature-készlet változatlan — eszközönként ugyanaz a 235
- Nem kell modell az előméréshez: a kérdés első körben tisztán
  **leíró statisztika** a hozamok keresztmetszeti szórásáról

## Scope

- `src/data_handling/collectors/derivatives/vision.py` — multi-symbol OHLCV
- `src/modeling/experiments/cross_sectional_probe.py` — új, kísérleti
- `_doc_/research/cross_sectional_probe.md`

Nem érinti: `src/strategy/`, `src/trading/` — nulla stratégia-kód.

## Tasks

- t831: 30 alt-perp 1 perces OHLCV backfill
- t832: Keresztmetszeti decilis-spread mérés
- t833: Go/no-go

Sorrend: t831 → t832 → t833

## A döntési szabály — előre rögzítve

Mérték: a 4 órás hozamok keresztmetszeti felső-alsó decilis **átlagos
spreadje**, minden órában, az egész 2024-07 → 2026-07 ablakon.

| Kimenet | Verdikt |
|---|---|
| Átlagos decilis-spread > **80 bps** | **GO** — van mit elosztani, jöhet a modell-ág |
| 40–80 bps | határeset — csak maker belépéssel (epic_059) érdemes |
| < 40 bps | **NO-GO** — a kétszeres díj megeszi, az irány lezárul |

A 80 bps küszöb indoklása: 26 bps kör-költség két lábon, és a modell
történetileg a rendelkezésre álló él **~55%-át** fogja meg — tehát a nyers
spreadnek nagyjából háromszorosnak kell lennie a költségnek.
