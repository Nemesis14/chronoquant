# Epic 058: Triple-barrier target — irány tanítása volatilitás helyett

> **A diagnózis, ami ide vezetett:** a modell nem irányt jósol, hanem
> volatilitást. Az out-of-sample mérés a top 1%-ra: max felfelé **+258,8 bps**,
> max lefelé **−350,6 bps**, tényleges 4 órás záró **+20,7 bps**.
> A lefelé kilengés **nagyobb**, mint a felfelé.

## Goal

Olyan targetre váltani, ami azt a kérdést teszi fel, amit a kereskedés:
**melyik korlátot éri el előbb az ár — a take profitot vagy a stop losst?**

## Miért nem működött az eddigi

A `long_mfe_fw240` **csak a felfelé kilengést** méri. A modell pontosan ezt
tanulta meg — és mivel kriptóban a nagy felfelé kilengés együtt jár nagy
lefelé kilengéssel, a modell valójában volatilitás-előrejelző lett.

Ezt a FE is végig mutatta: a top 10 feature mind volatilitás
(`gk_vol_*`, `natr_14`, `hist_vol_20`, `returns_std_14`).

Következmények, amiket végigmértünk:
- A lift (2,28×) **valós**, de az MFE-re igaz, nem a hozamra
- A kalibráció **stabil** (A-n illesztett küszöbök B-n működnek) — nem ez a hiba
- **Nulla díjjal is** a 200 setupból csak 59 nyereséges, a medián bruttó él
  kötésenként **−0,53 bps**
- Éves out-of-sample: a korábbi győztes setup **−14,15%**

A hiba tehát nem a modellben, nem a feature-ökben, nem a kalibrációban és
csak részben a díjban van — **a target rossz kérdést tesz fel**.

## A címke

Három korlát a belépési ár körül, a címke az, amelyiket előbb eléri:

```
+1  felso korlat eloszor  (+100 bps)  -> LONG
-1  also korlat eloszor   (-100 bps)  -> SHORT
 0  egyik sem 240 perc alatt          -> NE KERESKEDJ
```

**Szimmetrikus korlát: TP = SL = 100 bps, függőleges korlát 240 perc.**

Mért osztályeloszlás (2025-08 → 2026-07, óránkénti minta):

| | +1 | −1 | 0 |
|---|---|---|---|
| 100/100 | 32,9% | 36,0% | 31,0% |

## Miért 100/100

| TP/SL | Osztályok | Kell nyerő% | Alap-arány | Hézag |
|---|---|---|---|---|
| 50/50 | 46/49/5 | 60,0% | 48,6% | 11,4pp |
| **100/100** | **33/36/31** | **55,0%** | 47,8% | **7,2pp** |
| 150/150 | 21/24/55 | 53,3% | 47,7% | 5,6pp |
| 200/100 | 12/40/48 | 36,7% | 23,6% | 13,1pp |

- **Szimmetrikus jobb, mint aszimmetrikus.** A TP=2×SL csak 37%-os nyerő
  arányt kér, de az alap-arány ott 23,6% — 13pp lift kellene. A 100/100 csak 7,2pp-t.
- **100 bps TP a ~10 bps kör-költség tízszerese** — a díjfedezet garantált.
- A 150/150 kevesebb liftet kér (5,6pp), de a minták **55%-a timeout**, tehát
  fele annyi tanulható ±1 példa. A 100/100 a jobb kompromisszum.
- A 7,2pp elérhetőnek tűnik: a mért `>p99` csoportban a záró hozam
  **63,6%-ban pozitív** a 48,3%-os alappal szemben — az 15pp.

## Ami eltűnik a rendszerből

A TP és SL **konstans**, nem a predikcióból származik. Ezért feleslegessé válik:

- `rank_lookup_long/short.parquet`
- `isotonic_long/short.pkl`
- `bucket_mean/median/p75_mfe` kalibráció
- a `tp_spec` × `sl_spec` rácskeresés

**A rács 200 setupról 8 cutoff értékre zsugorodik.** Kevesebb szabadsági fok,
kevesebb túlillesztés — ez ma a fő gyanúsított volt.

**Egy modell váltja ki a kettőt.** Szimmetrikus korlátnál a long és short
címke egymás tükörképe; két modell ugyanazt tanulná kétszer.

## Scope

- `src/data_handling/sync_tables/sync_targets.py` — triple-barrier címke
- `src/modeling/training/` — multiclass trainer
- `src/modeling/search/` — osztályozási objective
- `src/strategy/` — fix TP/SL, egyszerűsített rács
- `config/models.json` — új modell bejegyzés

## Tasks

- t701: Triple-barrier címke a target táblába → database_agent
- t702: Multiclass trainer + search objective → modeling_agent
- t703: Modell tanítás és out-of-sample kiértékelés → modeling_agent
- t704: Egyszerűsített strategy réteg (fix TP/SL) → modeling_agent
- t705: Összevetés a regressziós megközelítéssel, go/no-go → analyst_agent

Sorrend: t701 → t702 → t703 → t704 → t705

## Key Decisions

- **Fix bázispontos korlát, nem volatilitás-skálázott** — először. A user
  kézzel kereskedik, tehát az egyszerűség és a garantált díjfedezet számít.
  A volatilitás-skálázás (TP = 1,0 × trailing 4h szórás) későbbi finomítás,
  ha a fix változat beválik.
- **A kiértékelés out-of-sample ablakon történik, nem in-sample optimumon.**
  Ma háromszor buktunk el azon, hogy in-sample szelekciót mértünk.
- **A minimális elvárás: a `>küszöb` csoport nyerő aránya tartósan 55% fölött**
  egy legalább 12 hónapos ablakon. Ez alatt nincs deploy.

## Notes

Előzmény: `epic_054` (alt-data — GO visszavonva), `epic_055` (költség-modell),
`epic_057` (4 órás horizont — az éves teszten megbukott).

Riportok: `_doc_/research/altdata_gate.md`, `strategy_cost_impact.md`
