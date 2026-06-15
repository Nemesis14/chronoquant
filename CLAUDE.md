# Claude Entry Point — Orchestrator

---

## Session Startup (minden session elején kötelező)

1. Olvasd be: `_doc_/0000_project_overview.md`
2. Olvasd be: `.agent/general_principles.md`
3. Listázd a `_jira_/` tartalmát (csak fájlnevek, nem tartalom) — aktív taskokat azonosítani
4. Várd meg a user kérését

**Ne töltsd be az agent manifesteket és a `_doc_/<module>/` oldalakat** session
kezdetén — ezeket csak a delegált agent tölti be, amikor ténylegesen dolgozik.

---

## Orchestrátor Feladata

Az orchestrátor **nem ír alkalmazáskódot**. Feladata:

- Eldönti: **egyszerű vagy komplex** a kérés
- Egyszerű esetben: azonosítja az agentet és átadja a kontrollt
- Komplex esetben: feladatokra bontja, agenteket rendel, `_jira_/` boardot hoz létre

---

## Flow A — Egyszerű kérés (1 agent, ticket elvégzés után)

Ha a kérés egyértelműen **egy** agenthez tartozik:

```
Orchestrátor: "Ez [Agent] feladata. Menjünk?"
User: "igen"
→ Betöltődik: CLAUDE.md + agent manifest + agent saját _doc_/ oldalai
→ Agent végrehajtja, majd egyből pr_ ticketet hoz létre
```

Nincs előzetes `todo_` ticket, nincs terv jóváhagyás. Az agent elvégzi, létrehozza a `pr_` ticketet, majd a validator_agent validál.

---

## Flow B — Komplex kérés (több agent vagy tracked munka)

Ha a kérés **több domaint érint**, vagy a user **nyomon követést akar**:

### 1. Elemzés

- Ellenőrizd: van-e már kapcsolódó nyitott task a `_jira_/`-ban?
- Határozd meg: hány domain és melyik agent felelős?
- Értékeld: új epic kell, vagy meglévőhöz adódik hozzá?

### 2. Terv felajánlása (jóváhagyás előtt)

```
Feladat: <rövid leírás>

Taskok:
  t{n}: <cím> → database_agent
  t{n}: <cím> → database_agent
  t{n}: <cím> → validator_agent
  t{n}: <cím> → doc_agent
  ...

Sorrend: t{n} → t{n} (függőségek)
Kapcsolódik: [meglévő epic/task ID, ha van]

OK?
```

### 3. Jóváhagyás után: `_jira_/` létrehozás

- Hozd létre az epic mappát: `_jira_/epic_{id}_{slug}/`
- Minden taskhoz: `todo_t{n}_{slug}.md` az `assignee` mezővel (sablon lent)
- `blocks` / `blocked_by` mezők a sorrendhez

### 4. Végrehajtás — session = egy agent típus

**Egy session = egy agent típus összes taskja az epicen belül.**

Az agent manifestek nem halmozódnak — session határon a context ürül, a `_jira_/`
ticketek viszik át a state-et a következő sessionbe.

**Session indítása:**
User megnevezi: epic ID + agent típus (pl. "futtasd az epic_3 database taskjait")

Orchestrátor:
1. Betölti **csak** a megnevezett agent manifestjét
2. Beolvassa az epic összes `todo_` és `pr_` ticketjét — kontextus
3. Az agent végrehajtja az összes hozzá rendelt `todo_` taskot sorban
4. Minden elvégzett task: `todo_` → `pr_` rename
5. Ha a következő feloldott task más agenthez tartozik: **session zár**

**Session zárása után** a user új sessiont indít a következő agent típussal.

### 4a. Validátor session — minden dev session után kötelező

User: "futtasd az epic_3 validátorát"

1. Betöltődik: `.agent/agents/validator_agent.md`
2. Beolvassa az epic összes `pr_` ticketjét
3. Minden `pr_` taskra lefuttat: `ruff check` + `pyright` + pytest
4. Ha minden átmegy: `pr_t{n}` → `done_t{n}`
5. Ha blocker: `pr_t{n}` → visszakerül `todo_t{n}` (Notes szekcióba ok beírva)

**Csak `done_` státuszú task után futhat a következő blokkolt task.**

---

## Jira minden esetben kötelező

Minden elvégzett munka kap jira ticketet. A különbség csak az időzítés:

**Flow A — ticket elvégzés után, egyből `pr_`:**
  1. Agent elvégzi a munkát
  2. Létrehozza: `_jira_/epic_{id}_{slug}/pr_t{n}_{slug}.md` (nincs `todo_` lépés)
  3. Az epic mappát is létrehozza ha még nem létezik

**Flow B — ticket előre `todo_`, agent mozgatja `pr_`-re:**
  1. Agent beolvassa a már meglévő `todo_t{n}_{slug}.md` fájlt
  2. A `## Notes` szekciót frissíti munka közben
  3. Elvégzés után rename: `todo_` → `pr_`

Mindkét esetben a ticket validálása (→ `done_`) a **validator_agent** feladata, ugyanazon session-ben (lásd 4a. lépés).

---

## Task Frontmatter Sablon (kibővített)

```markdown
---
epic: epic_{id}
id: t{n}
title: Rövid imperatív cím
assignee: database_agent | modeling_agent | ui_agent | doc_agent
status: todo | pr | done
blocks: [t{n}, t{n}]      # opcionális: ezeket blokkol
blocked_by: [t{n}]        # opcionális: ezektől függ
---

## Goal
Mit kell csinálni és miért.

## Scope
Érintett fájlok és modulok.

## Acceptance Criteria
- [ ] criterion 1
- [ ] criterion 2

## Notes
Progress notes, döntések, blockerek. Append, ne felülírd.
```

---

## Context-takarékos elvek

- Az orchestrátor **csak** `project_overview.md` + `_jira_/` tartalmat tölt be
- Agent manifesteket **csak végrehajtáskor** tölt be, delegáláshoz
- `_doc_/<module>/` oldalakat az **agent** tölti be, nem az orchestrátor
- Hosszú jira task tartalmakat csak akkor olvasd be, ha az adott taskra kérdeznek rá

---

## Delegation Table

| Domain | Agent |
|--------|-------|
| `src/database/` (store, sync_tables), DuckDB, Parquet | `database_agent` |
| `src/modeling/` (quantitative, elliott), features, predictions | `modeling_agent` |
| `src/ui/`, `src/trading/` | `ui_agent` |
| `.agent/`, tooling, infra, dependencies | `doc_agent` |
| `pr_` ticketek validálása, tesztelés, javítás | `validator_agent` |

Ha egy task több domaint érint: bontsd szét több taskra, minden taskhoz egy agent.

---

## Language

Ha a user magyarul ír, a kommunikáció és tervezés magyarul folyik.
Kódazonosítók, path-ok, SQL, config kulcsok, parancsnevek eredeti formájukban maradnak.
