# Claude Entry Point — Orchestrator

---

## Session Startup (minden session elején kötelező)

1. Olvasd be: `_doc_/0000_project_overview.md`
2. Olvasd be: `.agent/general_principles.md`
3. Listázd a `_jira_/` tartalmát (csak fájlnevek, nem tartalom) — aktív taskokat azonosítani
   ⚠️ **`_jira_/archive/` TILOS** — soha ne nyisd meg, ne listázd, ne olvass belőle
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

Nincs előzetes `todo_` ticket, nincs terv jóváhagyás. Az agent elvégzi, létrehozza a `pr_` ticketet, majd a validator_agent validál **ugyanabban a sessionben** — nem igényel külön session indítást.

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
  t{n}: <cím> → code_doc_agent
  ...

Sorrend: t{n} → t{n} (függőségek)
Kapcsolódik: [meglévő epic/task ID, ha van]

OK?
```

### 3. Jóváhagyás után: `_jira_/` létrehozás

- Hozd létre az epic mappát: `_jira_/epic_{id}_{slug}/`
- Hozd létre az `epic.md` fájlt a mappában (lásd sablon a jira_skill-ben)
- Minden taskhoz: `todo_t{n}_{slug}.md` az `assignee` mezővel (sablon lent)
- `blocks` / `blocked_by` mezők a sorrendhez

### 4. Végrehajtás — két mód

#### Mód A — Szegmentált (alacsony kontextus, az eredeti flow)

User megnevezi: epic ID + agent típus (pl. "futtasd az epic_3 database taskjait")

1. Betölti **csak** a megnevezett agent manifestjét
2. Beolvassa az epic összes `todo_` és `pr_` ticketjét
3. Agent végrehajtja az összes hozzá rendelt taskot sorban
4. Minden elvégzett task: `todo_` → `pr_` rename
5. Ha a következő feloldott task más agenthez tartozik: **session zár**

Session zárása után a user új sessiont indít a következő agent típussal.

#### Mód B — Orchestrált (főszál koordinál, párhuzamos subagent spawn)

User: "futtasd az epic_{n}-et orchestráltan"

Az orchestrátor **aktív marad** és subagenteket hív. Lépések:

1. Olvassa be az epic összes `todo_` ticketjét + `blocks`/`blocked_by` mezőket
2. Csoportosítja a taskokat végrehajtási hullámokba:
   - **1. hullám**: minden task, amelynek `blocked_by` listája üres
   - **2. hullám**: azok, amelyek `blocked_by`-ja kizárólag 1. hullám taskjaiból áll
   - stb.
3. Indítja párhuzamosan az aktuális hullám összes taskját (**max 3 egyidejű Agent spawn**):
   - Keresés / read-only lookup → `subagent_type: "Explore"`, `model: "haiku"`
   - Rövid / izolált implementáció (1-2 fájl) → névvel ellátott agent, `model: "haiku"`
   - Komplex / cross-file implementáció → névvel ellátott agent, `model: "sonnet"` (default)
4. Minden visszatérő subagent után:
   - `todo_` → `pr_` rename az elvégzett taskra
   - Ellenőrzi: feloldódott-e valamely blokkolt task → ha igen, indítja a következő hullámot
5. Minden task `pr_` állapotban → validator session következik (4a. pont)

**Mikor ajánlott Mód A?**
- Epic > 6 task esetén (orchestrátor kontextusa túlnő a subagent eredményektől)
- Ha a user közte review-zni szeretne az agent-típusok között

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

## Task Sablon

→ `.agent/skills/jira_skill.md` — "Task File Template" szekció

---

## Context-takarékos elvek

- Az orchestrátor **csak** `project_overview.md` + `_jira_/` tartalmat tölt be
- Agent manifesteket **csak végrehajtáskor** tölt be, delegáláshoz
- `_doc_/<module>/` oldalakat az **agent** tölti be, nem az orchestrátor
- Hosszú jira task tartalmakat csak akkor olvasd be, ha az adott taskra kérdeznek rá
- Keresésnél / fájl-lookup-nál: `Explore` agent `model: "haiku"` — **soha nem `general-purpose`**
- **`_jira_/archive/` olvasása szigorúan tilos** — archivált ticketek nem relevánsak aktív munkához

---

## Agent + Model Routing — Kötelező

⚠️ **`general-purpose` agent TILOS.** Minden keresési/lookup task `Explore + haiku`-val fut.

| Task típus | Agent | Model |
|---|---|---|
| Fájl keresés, szimbólum lookup, read-only olvasás | `Explore` | `haiku` |
| 1-2 fájl izolált implementáció | névvel ellátott agent | `haiku` |
| Cross-file / architektúra-szintű implementáció | névvel ellátott agent | sonnet (default) |
| Validáció, tesztelés | `validator_agent` | sonnet (default) |

**Ökölszabály: ha az agent nem ír fájlt → Haiku.**

---

## Session Zárás

Zárd le a sessiont, ha az adott fázis kész — ne tarts orchestrátort nyitva órákon át.

- **Flow A:** session lezárul a `pr_` ticket és validálás után.
- **Flow B Szegmentált:** session lezárul minden agent-típus váltásnál.
- **Flow B Orchestrált:** session lezárul, miután az összes task `pr_` státuszban van és a validator futott.

---

## Delegation Table

| Domain | Agent |
|--------|-------|
| `src/data_handling/` (store, sync_tables), DuckDB, Parquet | `database_agent` |
| `src/modeling/` (sampling, training, evaluation, feature_engineering), model artifacts | `modeling_agent` |
| `src/strategy/` (isotonic calibration, Optuna sweep, strategy artifacts) | `modeling_agent` |
| `src/trading/` (live service, journal, exchange client) | `ui_agent` |
| `src/ui/` (Streamlit dashboard, pages, components) | `ui_agent` |
| `.agent/`, tooling, infra, dependencies; `_doc_/database_and_code_doc/` (kód-referencia zóna) | `code_doc_agent` |
| `_doc_/models_doc/` (modell-report notebookok, .ipynb→Quarto), ML EDA | `analyst_agent` |
| `_doc_/methodology_doc/` (X000/X100 módszertani háttér és üzleti rationale, kód-mentes) | `methodology_agent` |
| `research/` | `analyst_agent` |
| `pr_` ticketek validálása, tesztelés, javítás | `validator_agent` |

Ha egy task több domaint érint: bontsd szét több taskra, minden taskhoz egy agent.

---

## Language

Ha a user magyarul ír, a kommunikáció és tervezés magyarul folyik.
Kódazonosítók, path-ok, SQL, config kulcsok, parancsnevek eredeti formájukban maradnak.
