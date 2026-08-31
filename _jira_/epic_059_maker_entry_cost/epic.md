# Epic 059: Maker belépés — a fedezeti pont leszállítása

> **A kiváltó szám:** a triple-barrier modell +9,3pp liftet hoz, a fedezeti
> ponthoz +17,6pp kellene. A rés fele **nem a modellen**, hanem a
> költségstruktúrán múlik.

## Goal

Megmérni — nem feltételezni —, hogy a limit (maker) belépés mennyit ér.

```
taker belepes  6,5 bps  ->  fedezeti pont 55,3%   szukseges lift +17,6pp
maker belepes  2,0 bps  ->  fedezeti pont 51,8%   szukseges lift +14,1pp
```

## ⚠️ A csapda, amit mérni kell

A 4,5 bps megtakarítás **papíron** van. Valójában:

1. A pihenő limit **nem mindig telik** — a kimaradt kötés nem nulla, hanem
   elveszett lehetőség (vagy épp a megmenekülés).
2. **Adverse selection:** egy long limit csak akkor telik, ha az ár előbb
   LEFELÉ tickel. A túlélő belépések tehát szisztematikusan rosszabbak.

Ha a teljesülési arány magas ÉS a találati arány nem romlik → a megtakarítás
valós. Ha a találati arány a fill-szűrés után esik → a maker belépés illúzió.

## Scope

- `src/modeling/experiments/walk_forward_tb.py` — a `--maker-entry` /
  `--maker-timeout` ág **már implementálva van, de sosem futott le**
- `config/strategies.json` → `costs` — díjszint felülvizsgálat
- `_doc_/research/execution_cost_gate.md` — új riport

## Tasks

- t801: Maker belépés mérése — fill rate és adverse selection
- t802: Díjszint-optimalizálás (BNB-kedvezmény, VIP tier, maker timeout kilépés)
- t803: Verdikt és a fedezeti pont újraszámolása

Sorrend: t801 → t802 → t803

## A döntési szabály — előre rögzítve

| Kimenet | Verdikt |
|---|---|
| Fill rate > 60% ÉS a találati arány romlása < 1,5pp | **GO** — a maker belépés a rendszer alapértelmezése lesz |
| Fill rate > 60% de a találat > 3pp-t romlik | az adverse selection megeszi — marad taker |
| Fill rate < 60% | a jel túl gyorsan fut el; marad taker |
