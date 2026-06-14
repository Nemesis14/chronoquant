# Claude Entry Point — Orchestrator

---

## Session Startup (minden session elején kötelező)

1. Olvasd be: `_docs/project_overview.md`
2. Olvasd be: `.agent/general_principles.md`
3. Listázd a `_jira/` tartalmát (csak fájlnevek, nem tartalom) — aktív taskokat azonosítani
4. Várd meg a user kérését

**Ne töltsd be az agent manifesteket és a `_docs/<module>/` oldalakat** session
kezdetén — ezeket csak a delegált agent tölti be, amikor ténylegesen dolgozik.

---

## Orchestrátor Feladata

Az orchestrátor **nem ír alkalmazáskódot**. Feladata:

- Eldönti: **egyszerű vagy komplex** a kérés
- Egyszerű esetben: azonosítja az agentet és átadja a kontrollt
- Komplex esetben: feladatokra bontja, agenteket rendel, `_jira/` boardot hoz létre

---

## Flow A — Egyszerű kérés (1 agent, nincs jira)

Ha a kérés egyértelműen **egy** agenthez tartozik:

```
Orchestrátor: "Ez [Agent] feladata. Menjünk?"
User: "igen"
→ Betöltődik: CLAUDE.md + agent manifest + agent saját _docs/ oldalai
→ Agent végrehajtja — nincs jira ticket, nincs state tracking
```

Nincs jira, nincs epic, nincs ticket mozgatás. Az agent elvégzi és kész.

---

## Flow B — Komplex kérés (több agent vagy tracked munka)

Ha a kérés **több domaint érint**, vagy a user **nyomon követést akar**:

### 1. Elemzés

- Ellenőrizd: van-e már kapcsolódó nyitott task a `_jira/`-ban?
- Határozd meg: hány domain és melyik agent felelős?
- Értékeld: új epic kell, vagy meglévőhöz adódik hozzá?

### 2. Terv felajánlása (jóváhagyás előtt)

```
Feladat: <rövid leírás>

Taskok:
  t{n}: <cím> → database_agent
  t{n}: <cím> → modeling_agent
  ...

Sorrend: t{n} → t{n} (függőségek)
Kapcsolódik: [meglévő epic/task ID, ha van]

OK?
```

### 3. Jóváhagyás után: `_jira/` létrehozás

- Hozd létre az epic mappát: `_jira/epic_{id}_{slug}/`
- Minden taskhoz: `todo_t{n}_{slug}.md` az `assignee` mezővel (sablon lent)
- `blocks` / `blocked_by` mezők a sorrendhez

### 4. Végrehajtás ticket alapján

Ha a user azt mondja: "futtasd t{n}-t" vagy "hajtsd végre epic_X-et":

- Olvasd be a task fájlt: agent, scope, acceptance criteria
- Töltsd be **csak** azt az agent manifestet: `.agent/agents/<agent>.md`
- Az agent betölti saját `_docs/` oldalait és végrehajtja
- Elvégzés után az agent rename-eli: `todo_` → `pr_`
- Az orchestrátor csak akkor avatkozik be, ha a következő task feloldásához kell

---

## Jira minden esetben kötelező

Minden elvégzett munka kap jira ticketet. A különbség csak az időzítés:

**Flow A — ticket elvégzés után, egyből `pr_`:**
  1. Agent elvégzi a munkát
  2. Létrehozza: `_jira/epic_{id}_{slug}/pr_t{n}_{slug}.md` (nincs `todo_` lépés)
  3. Az epic mappát is létrehozza ha még nem létezik

**Flow B — ticket előre `todo_`, agent mozgatja `pr_`-re:**
  1. Agent beolvassa a már meglévő `todo_t{n}_{slug}.md` fájlt
  2. A `## Notes` szekciót frissíti munka közben
  3. Elvégzés után rename: `todo_` → `pr_`

Mindkét esetben a ticket validálása (→ `done_`) egy **következő session** feladata.

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

- Az orchestrátor **csak** `project_overview.md` + `_jira/` tartalmat tölt be
- Agent manifesteket **csak végrehajtáskor** tölt be, delegáláshoz
- `_docs/<module>/` oldalakat az **agent** tölti be, nem az orchestrátor
- Hosszú jira task tartalmakat csak akkor olvasd be, ha az adott taskra kérdeznek rá

---

## Session végi kötelező lépés: project_overview frissítése

Minden session végén — mielőtt a user lezárja a munkát — ellenőrizd:

> Történt-e olyasmi, ami a `_docs/project_overview.md`-t érinti?

Frissíteni kell ha:
- Új DB tábla, oszlop, vagy sémaváltozás történt
- Új vagy inaktívvá tett ML modell
- Trading strategy logika változott
- Új modul, agent, vagy fő konvenció jött létre
- Asset config változott (új asset, interval, stb.)

Ha igen: frissítsd az érintett szekciót. Ha nem: nem kell semmit csinálni.
Ez a szabály minden agentre vonatkozik, nem csak az orchestrátorra.

---

## Delegation Table

| Domain | Agent |
|--------|-------|
| `src/store/`, `src/data_pipeline/`, DuckDB, Parquet | `database_agent` |
| `src/modeling/`, `src/evaluation/`, features, predictions | `modeling_agent` |
| `src/streamlit_app/`, `src/trading/service.py`, UI | `ui_agent` |
| `.agent/`, tooling, infra, dependencies | `doc_agent` |
| `pr_` ticketek validálása, tesztelés, javítás | `validator_agent` |

Ha egy task több domaint érint: bontsd szét több taskra, minden taskhoz egy agent.

---

## Language

Ha a user magyarul ír, a kommunikáció és tervezés magyarul folyik.
Kódazonosítók, path-ok, SQL, config kulcsok, parancsnevek eredeti formájukban maradnak.
