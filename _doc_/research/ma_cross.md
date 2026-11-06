# EMA25/EMA99 és SMA25/SMA99 keresztezés — SOLUSDT

> **Verdikt: nincs kereskedhető él.** Az önálló MA-cross semelyik timeframe-en nem ad sem statisztikailag megbízható, sem gazdaságilag releváns jelet; a win rate mindenhol ~49–51%.
>
> Futtatva: 2026-09-05 · SOLUSDT, 2020-09 → 2026-09 (~3,14 M 1 perces bar)

## Mit néztünk
Van-e információtartalma az EMA25/EMA99 golden/death crossnak (long/short) 1m, 15m, 1h, 4h, 1d timeframe-en; plusz egy 15m-es SMA25/SMA99 + irány-helyes MFE variáns.

## Módszer
Timeframe-enként ~1 napos tartási horizont (1m: 60 bar, 15m: 96, 1h: 24, 4h: 30, 1d: 10), 297 bar warmup. Két mérés: (1) *signal edge* — a cross-eseményekre a forward log-return a feltétel nélküli bázishoz képest (átfedő ablakok, t-stat), (2) szekvenciális, nem átfedő backtest. **Díj és csúszás nincs modellezve.**

## Fő számok
| tf | n/irány | long t | short t | backtest hozam | Sharpe-szerű |
|---|---|---|---|---|---|
| 1m | 20 487 | +0,21 | +1,06 | −76,8% | −0,33 |
| 15m | ~1 260 | +2,50 | −1,36 | −88,6% | −0,47 |
| 1h | 300 | +1,05 | −1,26 | +13,2% | +0,05 |
| 4h | ~77 | +1,10 | +0,09 | +19,4% | +0,06 |
| 1d | 10 | +1,12 | 0,00 | +62,0% | +0,35 (n túl kicsi) |

A 15m long t = +2,50 az átfedő ablakok autokorrelációja miatt felfelé torzít, a short oldal ellenkező előjelű, a backtest −88,6%. Az 1h/4h pozitív számok költség nélküliek; egy 4h trade átlaga +16,6 bps, ami egy taker oda-vissza költséget (8–20 bps) alig fedez. Az SMA/MFE variáns (`sma_mfe_15m.py`) eredménye nem lett rögzítve.

## Döntés / következmény
A legegyszerűbb MA-cross modell nem kereskedhető; nem építünk rá. Nem indított lehetséges folytatások: rezsim-szűrő (ATR, lásd `atr_barrier_gate.md`), hosszabb EMA-pár, díjmodell.

## Kód a git historyban
**Nincs.** A `research/ma_cross/` git-ignorált volt (`.gitignore`), sosem került commitba; a scriptek (`ema_cross_research.py`, `sma_mfe_15m.py`) és a kimenetek (`out/summary.csv`, equity PNG-k, 260 KB) törlése után ez az md az egyetlen megmaradó forrás.
