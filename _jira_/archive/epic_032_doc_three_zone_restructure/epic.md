# Epic 032: _doc_ háromzónás átszervezése (implementation / methodology / results)

## Goal
A `_doc_/` dokumentáció átszervezése flat számozott gyűjteményből **három globális
zónára**, a három doc-tulajdonos agentre leképezve:

1. **`database_and_code_doc/`** — DB, séma, kód-referencia (X110); `.md`; mermaid/UML, minden függvény alábontva → **kizárólag** code_doc_agent
2. **`methodology_doc/`** — rationale, döntések, módszertan; `.md`; sok rajz, KÓDRÓL SEMMIT → **kizárólag** methodology_agent
3. **`models_doc/`** — modellenként külön doksi, hivatkozás a methodra; `.ipynb` → Quarto (CSS, plot-paletta) → **kizárólag** analyst_agent (a modeling_agent a forrás: registry + artifact)

A téma-számozás (1xxx db, 5xxx modelling …) mindhárom zónában megmarad, hogy a
kereszthivatkozás kiszámítható maradjon.

## Scope
- `.agent/skills/docs_skill.md` — flat→3-zóna séma; alkönyvtár-engedés
- `.agent/agents/code_doc_agent.md`, `methodology_agent.md`, `analyst_agent.md` — scope path-ok
- `CLAUDE.md` — delegation table doc-hivatkozások
- `_doc_/0001_agentic_system.md` — 9. szakasz (dokumentációs rendszer) frissítés
- `_doc_/*.md`, `_doc_/*.ipynb` — fizikai átmozgatás a három zónába
- `src/analyst/_quarto.yml`, doc-renderer, CSS — útvonalak

## Tasks
- t325: docs_skill átírása flat→3-zóna (code_doc_agent)
- t326: Manifestek + CLAUDE.md + 0001 scope-frissítés a 3-zónára (code_doc_agent)
- t327: Meglévő _doc_ fájlok átmozgatása a három zónába (code_doc_agent)
- t328: Doc-renderer + _quarto.yml + CSS útvonalak frissítése (analyst_agent)
- t329: Validáció — linkek, Quarto render, TOC az új struktúrában (validator_agent)

## Execution waves
- 1. hullám: t325
- 2. hullám: t326, t327
- 3. hullám: t328
- 4. hullám: t329

## Key Decisions
- Globális típus-szerinti bontás (3 zóna), nem topic-szerinti — a doc-agent tulajdonlással egyezik.
- A 3. zóna (results) elsősorban **registry-lekérdezés + rendered report** (.ipynb/.html),
  nem kézzel karbantartott markdown — per-példány adat ne nőjön a `_doc_`-ban.
- **Formátum zónánként kötött**: implementation + methodology = `.md`; results = `.ipynb` (+ `.html`).
  A 3. zóna adat-nehéz, elemzéseket tartalmaz → futtatható notebook való hozzá, nem statikus markdown.
- Téma-számozás megtartva mindhárom zónában a kereszthivatkozásért.
- **Kereszthivatkozás egy-irányú**: `database_and_code_doc` → `methodology_doc` KÖTELEZŐ
  (a kód a „miért"-re linkel); a `methodology_doc` kód-mentes és NEM hivatkozik lefelé a kódra
  (stabil marad refaktoráláskor). Ez az eddigi Entry Gate elv mappák közti változata.
- **`models_doc` owner = analyst_agent** (notebook/Quarto/CSS/plot domain); a modeling_agent
  a forrás (model.pkl, metrikák, reg.models). Termelő–fogyasztó szétválasztás.
- Mappanév: `models_doc` (NEM `model's_doc` — aposztróf path/Quarto problémát okoz).
- Nincs külön TOC-index: a beszédes, számozott fájlnevek + Glob/Grep elég a navigációhoz.

## Cross-epic koordináció
- Az `epic_031` doc-taskjai (t320–t323) **az új zóna-struktúrába** írjanak. Ezért a t325
  (docs_skill átírás, a célstruktúra definíciója) ideálisan **az epic_031 doc-taskjai előtt**
  landoljon. Ha mégis flat-be kerülnek, a t327 mozgatja át őket.

## References
- `_doc_/_plans_/data_process_architecture.md` 13. szakasz (dokumentációs terv)
