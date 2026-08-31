# Epic 065: Klaszter-alapú setup-felismerés (unsupervised regime strategy)

## Goal

Az eddigi LGBM-regressziós megközelítéstől független kutatási irányként megvizsgálni,
hogy egy 6 órás (360 perces) visszatekintő ablakon számított, alacsony-redundanciájú
indikátor-készletre épülő **klaszterezés** azonosít-e olyan piaci "setup"-okat, amik
statisztikailag megbízható long/short irányú előretekintő MFE-torzítást mutatnak.

Teljes módszertani terv: [`_doc_/_plans_/plan.md`](../../_doc_/_plans_/plan.md)

## Scope

- `research/cluster_regime/` — új sandbox modul (célváltozó, ablak-reprezentáció,
  klaszterezés, OOS validáció). Nem érinti az élő pipeline-t.
- `_doc_/models_doc/` — validációs eredmény interpretáló notebook (analyst_agent).
- `_doc_/methodology_doc/` — rationale dokumentáció a döntésről.
- **Nem érinti**: `src/modeling/`, `src/strategy/`, `src/trading/`, `src/ui/` — amíg
  nincs Fázis 3 döntés a bevezetésről, semmi nem kerül production kódba.

## Key Decisions

Lezárva a tervezés során (`plan.md` fejléce):
1. Mintavételi sűrűség a klaszterezéshez: **ritkítva** (15-30 percenként) — nem
   minden perc, az erős autokorreláció és számításigény miatt.
2. Normalizálás: **kizárólag alak-felismerés** — ablakon belüli z-score/rang,
   ár-szint és volatilitás-rezsim független.
3. Hatókör: **önálló kutatási irány**, LGBM-től függetlenül validálva
   `research/` sandboxban; bevezetési döntés csak Fázis 3 után.
4. Globális vs. évenkénti klaszterezés: nyitva marad, Fázis 2 OOS eredménye dönti el
   (lásd `plan.md` 5. szakasz).

**Agent-routing eltérés a CLAUDE.md delegation table-től:** a táblázat szerint
`research/` → `analyst_agent`, de az analyst_agent manifestje explicit kizárja a
produkciós jellegű kód írását és a jira-ticket alapú munkát (kizárólag interaktív,
user-célból induló notebook session). Mivel ez az epic ténylegesen új
feature-engineering/klaszterezési **kódot** igényel (célváltozó számítás,
ablak-transzformáció, klaszter-illesztés, OOS validáció), a kód-taskok
**modeling_agent**-hez kerültek — ez illeszkedik a modeling_agent meglévő
kompetenciájához (feature computation, model artifacts, evaluation), csak a
kimenet helye `research/` a `src/modeling/` helyett, amíg nincs bizonyított edge.
Az **analyst_agent** egyetlen taskot kap: a validált eredmények interpretáló
Quarto notebookját, ami pontosan illeszkedik a szerepéhez (valós futási
eredményekből döntés-orientált értelmezés).

## Tasks

- t863: Feature-redundancia analízis és dimenziónkénti kiválasztás (modeling_agent)
- t864: `fw360` long/short MFE célváltozó számítása (modeling_agent)
- t865: Ablak-reprezentáció — normalizálás, ritkítás, összegző statisztikák (modeling_agent)
- t866: Klaszterezés implementálása és profilozás (modeling_agent)
- t867: Multi-year OOS validáció és multiple-testing korrekció (modeling_agent)
- t868: Validációs eredmények interpretáló notebook (analyst_agent)
- t869: Módszertani rationale dokumentáció és döntési javaslat (methodology_agent)

Sorrend: t863 ∥ t864 → t865 (blocked_by t863) → t866 (blocked_by t864, t865) →
t867 (blocked_by t866) → t868, t869 (blocked_by t867)
