---
epic: epic_065
id: t868
title: Validációs eredmények interpretáló notebook
assignee: analyst_agent
status: todo
blocks: []
blocked_by: [t867]
---

## Goal

A t867 OOS validációs eredményeiből (klaszter-profilok, train vs. OOS bias,
effektív N, szignifikancia) döntés-orientált Quarto notebookot készíteni: van-e
kereskedésre alkalmas klaszter, és ha igen, melyik/mennyi.

## Scope

- Output hely: `_doc_/models_doc/` (általános adatelemzés — nem kötött konkrét
  model/session ID-hoz), a routing szabály szerinti következő sorszámmal.
- A notebook a `research/cluster_regime/` valós futási eredményeit olvassa be
  (nem újraszámol semmit) — a t864-t867 taskok kimeneteire épít.
- Az analyst_agent saját munkamódja szerint készül (interaktív session, valós
  futtatás, visszaolvasott eredmények, döntés-orientált záró értelmezés).

## Acceptance Criteria

- [ ] Notebook létrehozva `_doc_/models_doc/`-ban, Quarto-renderelve (.ipynb + .html)
- [ ] Bemutatja a szignifikáns (OOS-stabil) klasztereket, ha vannak, jellemzésükkel
      (méret, irány, effektív N, MFE statisztika)
- [ ] Ha nincs szignifikáns klaszter, ezt explicit, döntés-orientáltan dokumentálja
- [ ] Záró szekció állást foglal: van-e gyakorlati edge, érdemes-e Fázis 3 döntésre
      (bevezetés vagy lezárás) továbblépni

## Notes
