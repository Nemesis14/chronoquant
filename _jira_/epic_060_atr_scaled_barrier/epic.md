# Epic 060: ATR-skálázott triple-barrier — a rezsim mint a feladat része

> **A diagnózis, ami ide vezet:** háromszor futottunk bele ugyanabba —
> a júniusi hónap (0/200 nyereséges setup), az ablakonként változó optimum,
> és a trenddel forduló nyerő irány. *„A rezsim a rendszer hiányzó
> dimenziója... szűrőként nem pótolható; a modell BEMENETÉNEK kell lennie."*
> — `epic_058` / t705

## Goal

Megszüntetni azt, hogy a címke jelentése korszakonként elcsúszik.

## A probléma, számmal

Fix ±100 bps korlát mellett a `tb_label_240_100` osztályeloszlása:

| Év | timeout arány |
|---|---|
| 2021 | **1,6%** |
| 2026 | **39,3%** |

Ugyanaz a címke 2021-ben „mindig elérünk valamelyik korlátot", 2026-ban
„általában egyiket sem". A modell egy **korszak-proxyt** tanul, nem egy
piaci mechanizmust. A train 2023-tól, a valid 2024-07-től — két különböző
feladat.

## A megoldás

A korlát a **t−1 volatilitáshoz** skálázódjon:

```
barrier_bps(t) = k * ATR_pct(t-1)        k Optuna-parameter vagy fix racs
```

Két következmény, és a második a fontosabb:

1. **A címke rezsim-invariáns lesz** — az osztályeloszlás korszakok között
   stabil, tehát a modell irányt tanul, nem rezsimet.
2. **A TP/SL is ugyanezzel skálázódik** → a nyerő/vesztes bps is skálázódik,
   miközben a **díj fix**. Ezért a fedezeti pont **maga is rezsim-függő** lesz:
   magas volatilitásban tágabb korlát → arányosan kisebb díjteher.

⚠️ A 2. pont a rejtett veszély is: alacsony volatilitásban a korlát szűk lesz,
és a fix díj felemészti. Ezért kell **alsó korlát** a barrier-szélességre
(pl. min 60 bps), és ezt előre rögzítjük, nem utólag hangoljuk.

## Scope

- `src/data_handling/sync_tables/_triple_barrier.py` — per-soros korlát
- `src/data_handling/sync_tables/sync_triple_barrier.py` — ATR-változat
- `src/modeling/experiments/walk_forward_tb.py` — soronkénti gazdaságtan
- `_doc_/research/atr_barrier_gate.md` — riport

## Tasks

- t811: Per-soros (ATR-skálázott) korlát a címkézőben
- t812: Soronkénti gazdaságtan a walk-forward mérésben
- t813: Long/short osztályok egymáshoz kalibrálása
- t814: Verdikt

Sorrend: t811 → t812 → t813 → t814

## A döntési szabály — előre rögzítve

A mérték **nem** a nyers találati arány (az a korláttal együtt mozog), hanem:

| Kimenet | Verdikt |
|---|---|
| `talalat − fedezeti_pont` **mind a négy** rezsim×irány cellában > 0 | **GO** |
| Az összesített `nettó bps/kötés` > 0, de cellánként ingadozik | részleges — második eszközön ismételni |
| Nettó bps/kötés ≤ 0 | **A skálázott korlát sem oldja meg. Az irány-hipotézis végleg zárul.** |

Az osztályeloszlás korszak-stabilitása **belépési feltétel**, nem eredmény:
ha 2021 és 2026 timeout-aránya nem kerül 10pp-en belülre, a címke rossz és
a mérésnek nincs értelme.
