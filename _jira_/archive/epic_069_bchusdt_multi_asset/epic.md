---
id: epic_069
title: BCHUSDT long+short modell + multi-asset UI (SOL | BCH fülek)
status: open
---

## Cél
A SOLUSDT mellé BCHUSDT (Binance futures, 1m) long és short LightGBM modell a teljes pipeline-on végigvíve
(snapshot -> sample -> FE -> search -> train -> predict -> analyze -> strategy -> deploy), és a UI-n
asset-fülek (SOL | BCH): a mostani SOL dashboard klónja, BCH adattal. A UI-n a zöld/piros jel-háromszögek
(percentil >= entry küszöb) legyenek láthatók BCH-ra is.

## Döntések (user, 2026-11-07)
- Csak BCHUSDT (a MASK kimarad). Futures, 1m, mint a SOL.
- Költségmodell: a meglévő globális (`strategy/costs.py`, `config/strategies.json`) marad, asset-specifikus nem kell.
- Összesítő nézet nem kell. Az asset-fül a SOL dashboard klónja.
- A `btcusdt` spot csonk (config + `database/btcusdt/`) törölhető (a user jóváhagyta).
- Train/valid ablak alapból a SOL-lal azonos (train 2021-01 -> 2026-01, valid 2026-02 -> 2026-07); a t901 adatellenőrzés után
  (onboardDate) pontosítandó. Default asset marad `solusdt`.

## Taskok / sorrend
t900 -> (t901 || t902) -> (t903 || t906) -> (t904 || t907) -> (t905, t908) -> t909
