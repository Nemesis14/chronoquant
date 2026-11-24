---
epic: epic_064
id: t856
title: Tanítómintavétel és gördülő mérés szelektív belépéssel
assignee: modeling_agent
status: todo
blocks: [t857]
blocked_by: [t853, t854, t855]
---

## Goal

A tényleges mérés: van-e a N-coinos merítésben olyan szigorúan szelektált
halmaz, ami fedezi a díjat.

## Mintavétel

⚠️ 24h targetnél két szomszédos perc címkéje **1439 percben átfed**. Minden
percet betenni a tanításba hamis biztonságot ad.

```
tanitas:  orankent 1 veletlen perc COINONKENT   -> ~N x 22 000 sor
purge:    24 ora a train vegerol
embargo:  a leghosszabb feature-ablak a train elejerol
```

## Protokoll — változatlan az eddigi fegyelem

- Hiperparaméter-keresés **egyszer**, a legkorábbi ablakon, cache-elve
- **Havi újratanítás**, minden addigi adaton
- A küszöb a train predikcióiból, **változatlanul** alkalmazva a valid hónapra

## Belépés

- **Abszolút küszöb** a score-on. Ha egy coin sem éri el: **nincs kötés.**
- **Long ÉS short**, amit a modell mond.
- **Maker limit** a jel utáni percben, fill az `ohlcv_1m`-ből ellenőrizve
  (az `epic_059` fill-modellje: 98,2% teljesülés, −0,35pp adverse selection)
- Stop **market** marad — a nem teljesülő stop-limit nyitva hagyja a pozíciót
  abban a mozgásban, ami elől menekülne.

## Kimenet — bontásokkal

- **Likviditási sávok** szerint (a döntési szabály két független sávot kér)
- **Rezsim** szerint (emelkedő / eső)
- **Jel-klaszterezés**: hány különböző napra esnek a jelek, és egyszerre hány
  coinon szólalnak meg? Ha tömörülnek, akkor nem szelektivitást vettünk,
  hanem rezsim-detektort építettünk.

## Acceptance Criteria

- [ ] Több küszöbszint (p99 / p99,5 / p99,9) lemérve
- [ ] Találat vs. **vol-normalizált** fedezeti pont, sávonként és rezsimenként
- [ ] Jel-klaszterezési diagnosztika
- [ ] Kötésszám, bps/kötés, havi bontás

## Notes
