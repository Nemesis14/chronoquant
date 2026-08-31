---
epic: epic_051
id: t5
title: Szerző-skill scoring — empirical Bayes shrinkage, rolling out-of-sample
assignee: —          # főszálon fut, agent nélkül (a t1-t4 folytatása)
status: done
blocks: [t6]
blocked_by: []
---

## Goal

A `call_returns` per-call hozamaiból szerző-szintű skill-becslés, és — ez az
epic tényleges kérdése — annak mérése, hogy a becslés **előrejelzi-e a szerző
jövőbeli** calljait.

## Scope

```
src/data_handling/store/analyst_signal_store.py  ← migráció v3
src/modeling/analyst_signal/score_authors.py     ← a scoring
   -> author_scores   (as_of_date, horizon_min, author_id)
   -> score_params    (fit-diagnosztika: tau2, sigma2, k)
```

## Miért shrinkage

A medián szerzőnek **7 használható callja** van. Nyers átlagra rangsorolva a
szerencsét rangsoroljuk: ezen az adaton a naiv top-10 **kizárólag 1-3 callos
szerzőkből áll** (`massguy66` egyetlen calljával vezet).

Empirical Bayes:

```
shrunk_i = mu + w_i (mean_i - mu),   w_i = n_i/(n_i + k),   k = sigma2/tau2
```

`sigma2` a szerzőn belüli call-szórás, `tau2` a szerzők **közötti** valódi
skill-szórás. **Egyiket sem feltételezzük — mindkettőt becsüljük.** Ha `tau2`
nullára esik, nincs kimutatható szerző-közti különbség, minden `w_i = 0`, és a
modell ezt úgy mondja meg, hogy mindenkit azonosra rangsorol.

## Miért rolling out-of-sample

Nem az a kérdés, ki volt jó, hanem hogy **aki jó volt, jó marad-e**. Minden
`as_of` időpontban a pontszám kizárólag korábbi callokból számol, a kimenetet
pedig az utána következő callokon mérjük. Az értékelési ablakok
**nem átfedők** (havi lépés, havi ablak), így az IC-idősor nem autokorrelált,
és a T6 tisztán tesztelheti.

## Acceptance Criteria

- [x] `author_scores` + `score_params` tábla migrációval
- [x] `tau2` és `sigma2` becsülve, nem feltételezve
- [x] Pontszám kizárólag `as_of` előtti callokból (nincs leak)
- [x] Nem átfedő értékelési ablakok
- [x] Elsődleges horizont kijelölve (1d), a többi robusztussági ellenőrzés
- [x] `ruff check` + `pyright` tiszta

---

## EREDMÉNY — 2026-08-20 — **nem találtunk jelet**

`author_scores`: 39 995 sor · `score_params`: 275 fit · 55 havi időpont,
2022-01-tól.

### IC — a pontszám és a következő havi teljesítmény rangkorrelációja

| h | értékelhető hónap | IC átlag | szórás | t | p |
|---|---:|---:|---:|---:|---:|
| 1h | **1 / 55** | — | — | — | — |
| 4h | 28 / 55 | +0,0286 | 0,1225 | 1,23 | 0,217 |
| **1d (elsődleges)** | 28 / 55 | **+0,0286** | 0,1577 | **0,96** | **0,338** |
| 3d | 50 / 55 | −0,0189 | 0,1979 | −0,68 | 0,499 |
| 7d | 34 / 55 | +0,0045 | 0,1839 | 0,14 | 0,886 |

**Egyik horizonton sem szignifikáns** — és ez még a többszörös tesztelés
korrekciója *előtt* van. Az elsődleges horizonton `p = 0,338`.

### A mélyebb ok: `tau2` gyakorlatilag nulla

| h | `tau2 > 0` | medián `tau2` | medián `sigma2` | implikált `k` |
|---|---:|---:|---:|---:|
| 1h | **1/55** | 0,00000 | 1,117 | ∞ |
| 4h | 28/55 | 0,00018 | 1,071 | ~5 900 |
| 1d | 28/55 | 0,00023 | 1,081 | ~4 700 |
| 3d | 50/55 | 0,01014 | 0,979 | ~97 |
| 7d | 34/55 | 0,00816 | 0,898 | ~110 |

Az 1 napos horizonton `k ≈ 4 700`: **egy szerzőnek ~4 700 callra lenne
szüksége, hogy a saját átlaga feleakkora súlyt kapjon, mint a populációé.**
A legtermékenyebb szerzőnek 716 van.

És a hónapok felében a `tau2` nullára esik — vagyis **az adat szerint nincs
kimutatható szórás a szerzők valódi képessége között**, a rangsor ott
definiálatlan. Ez nem a módszer hibája, hanem a mérés eredménye.

### Bucket-teszt

Az elsődleges horizonton a sorrend a helyes irányba mutat
(top20 −0,009 > mezőny −0,016 > bottom20 −0,033), de a különbségek
nagyságrendekkel a zajszint alatt vannak, és 3d/7d-n a top20 **rosszabb**,
mint a mezőny. Nincs konzisztens kép.

## Notes

**Amit a T6-nak még ellenőriznie kell, mielőtt kimondjuk a nemet:**

1. **Statisztikai erő.** A havi ablak szerzőnként csak ~2 callt ad — nagyon
   zajos. Egy negyedéves ablak több erőt adna, cserébe átfedő ablakokat és
   autokorrelált IC-sorozatot. Érdemes megnézni, hogy a következtetés
   megfordul-e.
2. **`n`-hatás.** Erős shrinkage mellett a rangsort részben a call-szám
   vezérli. A placebónak ezt kontrollálnia kell (n-megőrző permutáció),
   különben nem azt teszteli, amit gondolunk.
3. **Placebo + FDR** a tervezett módon.

**Előzetes olvasat:** ez a „nincs jel" ág felé mutat, ami az `epic.md` gate-je
szerint az `epic_052` törlését jelentené. **De a döntést a T6 mondja ki**, nem ez.
