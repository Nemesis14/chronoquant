---
epic: epic_064
id: t860
title: Teljes magyarázó oldal 15 perces bar-on, 8 órás target
assignee: modeling_agent
status: pr
blocks: [t856]
blocked_by: [t858, t859]
---

## Goal

Az órás rács elsimítja mindazt, ami órán belül történik. Egy húsz perc alatt
felépülő és az óra záróárára már elhalt mozgás nyomtalan az órás barban, és
semmilyen azon számolt feature nem látja. 8 órás barrier-target mellett épp egy
ilyen órán belüli felépülés lenne értékes korai jel.

Ezért **nem átmintavételezés kell, hanem 15 perces bar a teljes magyarázó
oldalon**.

## Terv

**Ablakok átskálázva ugyanazokra a falióra-hosszakra.** 4 bar = 1 óra, 32 bar =
8 óra. Enélkül a modell egyórás történetet hasonlítana az órás futás nyolcórás
történetéhez, és az összevetés semmit nem érne.

| | órás rács | 15 perces rács |
|---|---|---|
| ablakok | 1, 2, 4, 8 bar = 1–8h | 2, 4, 8, 16, 32 bar = **30p**, 1, 2, 4, 8h |
| max visszatekintés | 8h | 8h (változatlan) |
| gördülő percentilis | 720 bar = 30 nap | 2880 bar = 30 nap |
| purge | 8 bar | 32 bar |
| macd | 1/2/1, 2/4/2, 4/8/3 | 4/8/4, 8/16/8, 16/32/12 |

**Az egy óránál rövidebb ablak most válik kifejezhetővé.** A 2 bar = 30 perc az
órás rácson semmilyen beállítással nem létezett. Ez az — nem a többletsor —,
ami érdemben segíthet.

## ⚠️ Négyszer annyi sor nem négyszer annyi bizonyíték

Az órás rácson a 8 órás címke 8 barból 7-et megoszt a szomszédjával; itt 32-ből
31-et. A **független** 8 órás ablakok száma változatlan. A szignifikancia
továbbra is a 24 havi blokkon dől el, soha nem a sorszámon. Ha a számok
javulnak, az azért lesz, mert a feature-ök élesebbek, nem mert nagyobb a minta.

## Protokoll — változatlan

- Regularizáció-keresés purge-olt CV-vel, kizárólag a train ablakon belül
- Havi újratanítás bővülő ablakon, 2024-08 … 2026-07
- Véletlen 5-feature baseline ugyanazzal a modellel
- `--null-shift` a null-teszthez

## Eredmény

`feat_sol_short_15m`: **163 104 sor, 355 feature** (órás: 40 776 / 235). A
regularizáció-keresés ugyanazt a konfigurációt választotta, mint az órás rácson.

| | órás rács | **15 perces** |
|---|--:|--:|
| IC valid | +0,0240 | **+0,0312** |
| AUC valid | 0,5157 | **0,5209** |
| p90 lift | +2,22pp | **+3,11pp** |
| IC train | +0,1019 | +0,1008 |

A train IC gyakorlatilag változatlan — a modell kapacitása ugyanaz maradt, csak
a valid oldal javult. Ez a helyes szignatúra: túlillesztésnél a train oldalnak
is emelkednie kellett volna.

**Null-eloszlás, 24 rotáció, fix konfiguráció** (`t861`):

| | éles | null átl. | null szórás | ≥ éles | **p** | **z** |
|---|--:|--:|--:|--:|--:|--:|
| IC | +0,0312 | +0,0046 | 0,0135 | 1/24 | 0,080 | 1,98 |
| AUC | 0,5209 | 0,5027 | 0,0084 | **0/24** | **0,040** | **2,17** |
| p90 lift | +3,11 | +0,64 | 1,69 | 2/24 | 0,120 | 1,46 |

Órás rács ugyanezen: 0,120 / 0,120 / 0,200 (z = 1,31 / 1,34 / 1,17).

**A 15 perces rács következetesen jobb**, mindhárom mérőszámon. Az órán belüli
hipotézis nem dőlt meg. A sűrűbb rács tényleg gyárt több látszatliftet
(null-átlag +0,64 vs +0,03), de nem annyit, hogy az éles eredményt megmagyarázza.

⚠️ **A p = 0,040 nem felfedezés** — lásd `t861`. Amit eldönt, az a
rácsválasztás, nem a jel megléte.

## Acceptance Criteria

- [x] `feat_sol_short_15m` tábla felépítve, ablakok falióra-ekvivalensek
- [x] Regularizáció-keresés lefuttatva
- [x] Gördülő mérés IC / AUC / p90 lift, havi t-vel
- [x] Véletlen baseline, és a **többlet** összevetve az órás rács többletével
- [x] Null-teszt — null-eloszlásként, 24 rotációval (`t861`)

## Notes

Az első null-teszt egyetlen rotációval futott és úgy látszott, az ág artefakt.
Ez téves volt: a használt 4380 órás eltolás szélsőségesen magas húzás. Lásd
`t861`.

A [sol_15m_windows.py](../../src/modeling/experiments/sol_15m_windows.py)
(30 perces vs hosszabb ablakok szétválasztása) megírva, de nem futott — nincs
elég erős kimutatott jel ahhoz, hogy szétbontása értelmes legyen. A panelen
kerül elő újra, ha ott megáll az eredmény.
