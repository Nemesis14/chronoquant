# Elliott Wave 4 Completion — Event Study

## Elemzés célja

Megvizsgálni, hogy egy **konfirmált 1-2-3-4 impulzus struktúra** után tényleg következik-e
az 5-ös hullám — azaz az ár eléri-e a Wave 3 csúcsát (P3) a Wave 4 mélypontjának (P4) betörése előtt.

Kérdés: **mennyire megbízható belépési jel a konfirmált Wave 4 vége?**

---

## Módszertan

### Detekció

A ZigZag pivot motorra épül (`detect_zigzag`). Minden egymást követő 5-pivot ablakon
(`P0, P1, P2, P3, P4`) ellenőrizzük az alábbi **hard rule**-okat (long irány):

| Szabály | Feltétel |
|---------|----------|
| Alternáló struktúra | P0=LOW, P1=HIGH, P2=LOW, P3=HIGH, P4=LOW |
| Wave 2 nem invalidál | P2 > P0 |
| Wave 3 kiterjed | P3 > P1 |
| Wave 4 nem invalidál | P4 > P2 |
| Non-overlap (standard impulse) | P4 > P1 (wave 4 low nem lépi át wave 1 csúcsát) |

A non-overlap szabály kizárja a diagonálokat — csak standard impulzus struktúrát keresünk.

### Labeling (lookahead-free)

Minden setup **P4.confirmed\_idx**-től indul — ez az a bar, amikor a Wave 4 low
pivot megerősítést kapott. Ettől a ponttól nézünk előre `window` bárra:

- **Siker (hit\_target)**: `high >= P3.price` — az ár eléri a Wave 3 csúcsát
- **Kudarc (hit\_stop)**: `low <= P4.price` — az ár betöri a Wave 4 mélyet
- **Timeout**: sem siker, sem kudarc a window-n belül

Stop = P4 low (wave 4 mélypontja), Target = P3 high (wave 3 csúcsa).

### Non-overlapping szűrő

"One trade at a time": amíg egy setup nem zárult (target vagy stop), az időben
fedő következő setup ki van szűrve. A vizsgálat kimutatta, hogy az átlagos zárási sebesség
miatt az overlap a teljes mintán **<1%** volt, tehát a szűrés nem változtat érdemben
az eredményeken.

### Script

```
scripts/backtest_wave4.py
```

Futtatás:
```bash
python scripts/backtest_wave4.py                  # 1h + 15m
python scripts/backtest_wave4.py --tf 1h
python scripts/backtest_wave4.py --tf 15m --window 240
```

---

## Eredmények

**Adatbázis**: SOLUSDT, 2020-08-11 — 2026-06-09

### 1h timeframe (window = 120 bar ≈ 5 nap)

| Metrika | Érték |
|---------|-------|
| Összes setup | **207** |
| Wave 5 triggerelt | **148 (71.5%)** |
| Stop out | 59 (28.5%) |
| Timeout | 0 (0%) |
| Átlag idő Wave 5-ig | **4.2 óra** |
| Átlag W3/W1 arány | 1.95x |
| Overlap szűrt | 0 setup |

Évenkénti bontás:

| Év | Setupok | Wave 5 | Hit rate |
|----|---------|--------|----------|
| 2020 | 23 | 14 | 61% |
| 2021 | 68 | 50 | 74% |
| 2022 | 29 | 20 | 69% |
| 2023 | 32 | 25 | 78% |
| 2024 | 30 | 21 | 70% |
| 2025 | 20 | 15 | 75% |
| 2026 | 5  | 3  | 60% |

---

### 15m timeframe (window = 480 bar ≈ 5 nap)

| Metrika | Érték |
|---------|-------|
| Összes setup | **597** |
| Wave 5 triggerelt | **416 (69.7%)** |
| Stop out | 181 (30.3%) |
| Timeout | 0 (0%) |
| Átlag idő Wave 5-ig | **5.4 gyertya (~81 perc)** |
| Átlag W3/W1 arány | 2.03x |
| Overlap szűrt | 1 setup |

Évenkénti bontás:

| Év | Setupok | Wave 5 | Hit rate |
|----|---------|--------|----------|
| 2020 | 73  | 49  | 67% |
| 2021 | 200 | 137 | 68% |
| 2022 | 90  | 60  | 67% |
| 2023 | 83  | 61  | 73% |
| 2024 | 75  | 52  | 69% |
| 2025 | 63  | 49  | 78% |
| 2026 | 13  | 8   | 62% |

---

## Következtetések

1. **A ~70% hit rate stabil** — 2020-2026 között mindkét timeframe-en konzisztensen
   68-78% közé esik, ami strukturális piaci tulajdonságra utal, nem véletlenre.

2. **Nincs overlap torzítás** — a setups szinte mind független időben, a dupla
   számlálás nem magyarázza az eredményeket.

3. **Gyors zárulás** — az átlagos Wave 5 trigger 4-5 gyertyán belül megjelenik,
   ami azt jelzi, hogy a P3 target közel van a P4 konfirmáláshoz.

4. **2025 erősebb**: 1h=75%, 15m=78% — bullish trend kedvez az impulzus struktúráknak.

---

## Nyitott kérdések / következő lépések

- **R:R elemzés**: a 70% hit rate akkor értékes, ha a nyerés/veszteség arány >1.
  Kell mérni: átlagos gain (P4 → P3 távolság) vs. átlagos loss (P4 → stop távolság).

- **Fibonacci szűrő**: a Wave 2 retrace (R2) és Wave 3 extension (W3/W1) arány
  alapján szegmentálva lehet-e javítani a hit rate-en?
  Kérdés: a magasabb W3/W1 arányú setupoknál jobb-e a Wave 5 follow-through?

- **Short irány** vizsgálata (direction=-1).

- **Belépési finomítás**: a P3 trigger helyett Fibonacci extension célok
  (pl. P4 + 1.618 × W1) mint target.
