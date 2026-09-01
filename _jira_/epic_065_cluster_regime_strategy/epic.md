# Epic 065 — Klaszter-alapú Setup-felismerés

**Status:** **CLOSED** (user jóváhagyta a végleges lezárást, 2026-09-01)

**Tracker:** d:\repos\chronoquant\_jira_\epic_065_cluster_regime_strategy\

**Created:** 2026-08-31
**Closed:** 2026-09-01

---

## Final Conclusion (olvasd ezt először)

> **A klaszter-alapú regime-felismerés (K-means a 360-perces ablakos technikai indikátor
> summary-statisztikákon) nem termel kereskedésre alkalmas, stabil edge-et — sem fix k-val
> (k=3, k=10), sem CV-optimalizált k-val (k=13), semmilyen irány-klasszifikációs módszertannal,
> TP/SL kalibrációval, vagy cross-fold robusztussági ellenőrzéssel sem.**

Az epic három nekifutáson és négy fő vizsgálaton át jutott erre a következtetésre (lásd lent
a részletes történetet). **Nincs további klaszter-alapú trading validáció javasolva ez alatt az
epic alatt.** Javasolt következő irány: visszatérés a LightGBM-alapú directional modeling
pipeline-hoz (`src/modeling/`), explicit fókusszal a tranzakciós-költség-tudatos strategy
kalibrációra (`src/strategy/`) — ott az isotonic kalibráció és Optuna threshold search
közvetlenül tudja kezelni a realisztikus költségstruktúrát, amit ez az epic ismételten a fő
akadálynak talált, és amit a klaszterezési megközelítés nem tud kezelni.

---

## Summary

Kutatási epik az unsupervised klaszterezésen alapuló setup-felismeréshez.
360 perces ablak, alacsony-redundanciájú feature-készlet, K-means klaszterezés, out-of-sample validáció.

**⚠️ A legkorábbi "Final Result" (t870-ből, 98.3% win rate) ELAVULT és FÉLREVEZETŐ — t871 megcáfolta.**

### Iteráció történet

**1. t863–t870 (fix k=10, post-hoc irány-hozzárendelés):** k=10 K-means, 4 "tradeable" setup
(2 SHORT + 2 LONG), 1-éves idealizált OOS backtest: 2,235 trades, $63.3k nettó profit, 98.3%
win rate (MFE-alapú, zero-cost, **nem realisztikus**).

**2. t871 (realisztikus validáció + hibajavítás):** Az idealizált 98.3% win rate **mérési
artifact** volt, nem valódi edge. Két kritikus hibát találtunk és javítottunk (LONG-only bug,
majd egy matematikailag hibás t-test az irány-klasszifikációban), és egy kalibrált TP/SL
tesztet is futtattunk. **Öt realisztikus konfiguráció mindegyike ugyanarra jutott: nincs
kihasználható trading edge realisztikus költségek mellett** (0.3% costs + 0.4% slippage →
szükséges win rate ~67%, elért ~35-38%). Részletek: `_doc_/models_doc/6501_k10_validation_realistic_backtest.md`.

**3. t872 core (CV-optimalizált klaszter-szeparáció):** A fix-k megközelítés helyett
walk-forward CV-vel (4 fold: 2021-2022→2023, 2021-2023→2024, 2021-2024→2025H1, 2021-2025H1→2025H2-2026H1)
optimalizáltuk magát a klaszterezést (ANOVA F-score a long+short MFE irányjelen, k=2..30
keresés, mindig validation-only mérve). Optimális **k=13** (cv_score=17.70, stabil — nem egy
fold dominálja). A Fold-4 out-of-fold direction report: 4/13 klaszter szignifikánsan SHORT,
**0/13 szignifikánsan LONG**, 7 NEUTRAL, 2 üres OOS-ban. Ez a SHORT-only aszimmetria megismétli
a k=10-ből ismert mintázatot. **Ez a task NEM tartalmazott trading backtestet** — csak a
klaszterezés minőségét validálta.

**4. t872 follow-up vizsgálatok (3 db, a lezárás előtti végső stressz-teszt):**
   - **4a. Méret-stabilitás** (train vs. valid n_obs, mind a 4 fold): az eltűnő/gyenge
     klaszterek már a train mintán is a legkisebbek közé tartoztak (1.1-1.9% train share) —
     base-rate jelenség, nem meglepetésszerű összeomlás.
   - **4b. Irány-stabilitás** (train vs. valid, ugyanazon fold-on belül, mind a 13 klaszterre):
     csak **30.77%** (16/52) kombináció egyezik train↔valid között. **6 valódi FLIP eset**
     (LONG↔SHORT megfordulás), 10 gyengülés (szignifikáns→NEUTRAL), és — meglepő módon a
     legnagyobb kategória — **14 "megjelenő" jel** (NEUTRAL→szignifikáns validation-ön, ami
     inkább zajra utal, mint valódi, perzisztens signalra).
   - **4c. Cross-fold cluster identity** (centroid cosine similarity, mutual-nearest-neighbor
     matching, union-find): mivel minden fold külön K-means fit, a "Cluster 9" azonossága
     fold-ok között nem garantált. Validity check-kel (within-fold baseline vs. cross-fold
     match) igazoltuk, hogy a matching módszertanilag RELIABLE, de szerény szeparációval
     (within-fold baseline medián 0.963 vs. cross-fold match medián 0.998, erősen 1.0-hoz
     komprimált skálán). **Eredmény: mindössze EGY valóban robusztus, cross-fold-azonosított,
     irány-konzisztens klaszter-mintázat van (SHORT, ~620-790 megfigyelés/fold, ~1-2% az
     adatból) — ez túl kicsi ahhoz, hogy önmagában kereskedésre alkalmas populáció legyen.
     Hasonlóan robusztus LONG mintázat NINCS.**

   Részletek mindhárom follow-up-ra és a végleges szintézisre: `_doc_/models_doc/6502_cv_cluster_separation_optimization.md` (Section 8-9).

Ticket: `done_t872_cv_optimized_cluster_separation.md` — **user maga validálta menet közben,
validator_agent session nélkül lezárva.**

---

## Validation Summary (korábbi, t870-re vonatkozott — ELAVULT, csak történeti referencia)

**t863–t869: done_ status.** t870 validálva volt saját (idealizált) scope-jában, de az eredményt
t871 felülírta realisztikus feltételek mellett. t871 és t872 is a user explicit jóváhagyásával
lett lezárva (t871: validator_agent session; t872: user közvetlen validáció, lásd fent).

---

## Notes

- **Ne a t870 "98.3% win rate" eredményt idézd döntéshez** — az idealizált, zero-cost scenario,
  amit t871 megcáfolt.
- **t871 lezárt következtetése:** fix-k klaszterezés (k=3 vagy k=10) nem életképes trading
  stratégia realisztikus végrehajtás mellett, semmilyen irány-klasszifikációs módszertannal vagy
  TP/SL kalibrációval sem.
- **t872 lezárt következtetése:** a CV-optimalizált klaszterezés (k=13) objektíven jobb
  szeparációt ad, mint a fix k=10, DE ez a javulás nem stabil — sem méretben, sem irányban, sem
  cross-fold azonosságban. Legfeljebb egyetlen kicsi (~1-2%), valóban megbízható SHORT jel
  azonosítható; LONG oldalon semmi hasonló nincs. **Nem indokolt egy jövőbeli trading backtest
  task ez alapján** — az elvárt eredmény, t871 mintájára, valószínűleg szintén negatív lenne, és
  a jel mérete is valószínűleg túl kicsi lenne érdemi P&L-hez.
- **Epic 065 lezárva.** Minden jövőbeli klaszter-regime kutatás ehhez az epichez kellene
  kapcsolódjon (ne új epic), és explicit hivatkozzon rá, miért gondolja, hogy más eredményt
  hozna, mint amit itt találtunk.
