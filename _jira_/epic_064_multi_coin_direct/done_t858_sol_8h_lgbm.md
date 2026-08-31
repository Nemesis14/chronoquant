---
epic: epic_064
id: t858
title: SOL 8 órás LightGBM — regularizáció-keresés, gördülő mérés, gazdasági kapu
assignee: modeling_agent
status: done
blocks: [t856]
blocked_by: []
---

## Goal

Egycoinos referencia-mérés a panel előtt: SOLUSDT, 8 órás triple-barrier
célváltozó, rövid (1–8h) feature-ablakok, regularizált LightGBM havi
újratanítással. A kérdés nem az volt, hogy „van-e IC", hanem hogy **fedezi-e a
díjat**.

## Mit csináltunk

- Regularizáció-keresés 8 rácsponton, purge-olt idősoros CV a train ablakon
  belül (2024-07-31-ig). A keresés soha nem látta a gördülő hónapokat.
- Havi újratanítás bővülő ablakon, 2024-08 … 2026-07, 24 hónap.
- **Véletlen 5-feature baseline** minden mérés mellé — ez az a mérce, ami a
  korábbi ágakat megölte.
- **Null-teszt**: címke 4380 órával elforgatva.
- Gazdasági kiértékelés: train-eloszlásból vett p90/p10 küszöb, maker költség,
  30 napos blokk-bootstrap.

## ⚠️ JAVÍTVA (t861) — az eredeti következtetés téves volt

Az alábbi „statisztikailag átment" állítás **egyetlen** null-rotációra épült,
ami szerencsés húzás volt. 24 rotációval, fix konfigurációval a null-eloszlás
nem nullára centrált (IC átlag +0,0051, p95 +0,0255), és az éles +0,0240 egyik
mérőszámon sem éri el a 95. percentilist: **p = 0,12 (IC) / 0,12 (AUC) / 0,20
(lift)**. Ezen az ágon **nem mutattunk ki jelet**. Részletek: `t861` és a
kapu-jelentés §10.

A ticket többi része az eredeti állapotot tükrözi.

## Eredmény (eredeti, JAVÍTVA — lásd fent)

**Statisztikailag átment, gazdaságilag nem.**

| | valid | baseline | null |
|---|--:|--:|--:|
| IC | +0,0240 (t 2,00) | +0,0134 | −0,0041 |
| AUC | 0,5157 | 0,5084 | 0,4958 |
| p90 lift | +2,22pp (t 1,03) | +1,25pp | −0,31pp |

Ez az **első ág, ami a null-tesztet és a véletlen baseline-t is átlépi**.

De a küszöb-szabály nettója nem különböztethető meg a nullától:

| szabály | kötés | találat | nettó bps | 95% CI | p(≤0) |
|---|--:|--:|--:|---|--:|
| long (≥ p90) | 1187 | 52,9% | +3,37 | [−16,54, +19,31] | 0,338 |
| short (≤ p10) | 1252 | 53,2% | +0,37 | [−11,05, +10,33] | 0,484 |

Havi bontásban 12/24 pozitív hónap, a havi átlagok t-je −0,07 — a poololt
pozitívum néhány sok-kötéses hónapból jön.

## Két tanulság, ami a `t856`-ot köti

1. **Mintaméret, nem modellezés a korlát.** *(A javítás után pontosabban: a
   mintaméret is korlát, de elsősorban jelet nem mutattunk ki.)* A mért hatás és szórás mellett
   ~10 400 kötés kell t = 2-höz: egy coinon ~18 év, vagy ~9 coin két év alatt.
   Az egycoinos ágon nincs mit tovább próbálni. A ~9 coin felső korlát, mert az
   egyidejű jelek korreláltak — ezért kell a `t856` jel-klaszterezési
   diagnosztikája.
2. **A modell magnitúdót tanult, nem irányt.** A top 10 feature-ből öt tiszta
   volatilitás (`natr` ×2, `hist_vol`, `bb_width`, `returns_std`), az első
   helyezett `adx_neg` trenderősség. A `direction_features.py` irány-blokkja
   (funding, OI, crowding, taker-imbalance) **nem szerepelt** ebben a futásban,
   mert a `feat_sol_short_1h` tábla nem tartalmazza. A panelen a két blokkot
   együtt kell mérni.

## Kimenet

- [src/modeling/experiments/sol_lgbm_8h.py](../../src/modeling/experiments/sol_lgbm_8h.py)
  — regularizáció-keresés, gördülő mérés, baseline, `--null-shift`
- [src/modeling/experiments/sol_lgbm_8h_econ.py](../../src/modeling/experiments/sol_lgbm_8h_econ.py)
  — küszöb-szabály, blokk-bootstrap, feature-fontosság
- [_doc_/research/sol_8h_lgbm_gate.md](../../_doc_/research/sol_8h_lgbm_gate.md)
  — teljes kapu-jelentés

## Acceptance Criteria

- [x] Regularizáció-keresés purge-olt CV-vel, a mérési időszakon kívül
- [x] Havi gördülő újratanítás, 24 hónap
- [x] Véletlen baseline minden mérőszám mellett
- [x] Null-teszt elforgatott címkével
- [x] Nettó bps/kötés blokk-bootstrap konfidencia-intervallummal
- [x] Feature-fontosság megnézve

## Notes

A `_metrics` konstans predikció esetén 0,0-t ad vissza, nem `nan`-t. A
korábbi `sol_lasso.py` `nan`-ként kezelte, ami kiejtette a hónapot az
átlagból — épp azokat, amelyekben a modell semmit nem talált, tehát felfelé
torzított.
