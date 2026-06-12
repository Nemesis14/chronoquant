# Documentation Standards

## Mikor kell .md dokumentációt írni?

- Uj modul vagy reteg kerul a projektbe, es a kapcsolatok nem derulnek ki a kodbol
- Egy kód-csoport összefüggései nem derülnek ki a kódból
- A user kéri, hogy legyen doc a mappában

Soha ne hozz létre .md-t automatikusan — csak ha a user kéri, vagy ha az aktív agent entry point explicit előírja.

---

## Dokumentum-fájl elhelyezése

| Hatókör | Hova kerül |
|---------|-----------|
| Egy modul-csoport | Az erintett mappa melle vagy kapcsolodo `docs/` oldalra |
| Egesz projekt | `docs/` mappa |
| AI-belso referencia | `.agent/` mappa |

---

## .md fájl felépítése

```
# Cím

Rövid, egy-mondatos leírás miről szól a doc.

---

## 1. Szekció — szöveges leírás

...

## 2. Szekció — diagram (ha szükséges)

```mermaid
...
```

## 3. Szekció — táblázat (ha szükséges)

...
```

- Szekciókat `---` választja el
- Táblázatokat használj szöveges összefoglalóhoz, ne folyó szöveget
- Ne írj felesleges bevezető bekezdést — a cím + első sor elég

---

## Mermaid szabályok

### Diagram típus választása

| Cél | Típus |
|-----|-------|
| Modul-függőségek, hívási lánc | `graph TD` vagy `flowchart TD` |
| Időbeli sorrend, API hívások | `sequenceDiagram` |
| Tábla/osztály séma | `erDiagram` |

### graph TD / flowchart TD szabályok

**Node ID-k:**
- Csak ASCII betű, szám, underscore — ékezetes karakter TILOS node ID-ban
- Helyes: `load_asset_config`, `tbl_ohlcv`
- Hibás: `tábla_ohlcv`

**Node labelek (a `["..."]` belseje):**
- `\n` sortörést ad
- `()` zárójelek megengedettek
- Ékezetes karakter megengedett
- `'` aposztróf megengedett idézőjeles labelben
- TILOS karakterek — parser-crasht okoznak, a diagram fekete lesz:
  - `{` és `}` — Mermaid alakzatszintaxis
  - `→` Unicode nyíl — TILOS, még idézőjelben is
  - `->` ASCII nyíl — TILOS node labelben, Mermaid edge-ként értelmezi
  - `—` em-dash és `–` en-dash — TILOS, helyette `-`
  - `/` perjel edge labelben idézőjel nélkül — TILOS

**Subgraph:**
- Subgraph neve NEM használható edge-forrásként vagy céljaként
  - Hibás: `mysubgraph --> some_node`
  - Helyes: `specific_node_inside --> some_node`
- Subgraph label: csak ASCII, ékezet és speciális karakter nélkül
  - Hibás: `subgraph foo["Cím — leírás"]`
  - Helyes: `subgraph foo["Cim - leiras"]`
- Ha sok node és edge van: **ne használj subgraphot** — inkább írd le szövegesen a csoportosítást, és a node labelbe tedd bele a kontextust

**Edge-ek:**
- Szóköz nélküli egyszerű label: `A -->|INSERT| B`
- Szóközös label: `A -->|"sqlite3 direkt"| B`
- `→`, `—`, `–`, `->`, `/` TILOS edge labelben is

**Méret:**
- Maximum ~25 node és ~35 él egy diagramban
- Ha több kell: bontsd több kisebb diagramra külön szekcióba
- Subgraph csak kis (<10 node) egyszerű diagramban biztonságos

### sequenceDiagram szabályok

- Participant ID: csak ASCII, szóköz nélkül — alias szabad: `participant SyncOHLCV as sync_ohlcv`
- Note szintaxis: `Note over X: szoveg`
- `<br/>` sortörés megengedett Note-ban
- Ékezetes karakter megengedett üzenet-szövegekben

### erDiagram szabályok

- Táblanév csak ASCII, szóköz nélkül
- Oszlop típus: `string`, `int`, `float`, `datetime`

### Ellenőrzőlista írás előtt

Mielőtt Mermaid blokkot írsz, ellenőrizd:

- [ ] Első sor: `%%{init: {'theme': 'neutral'}}%%`
- [ ] Node ID-k csak ASCII karakterek
- [ ] Subgraph labelek csak ASCII (ékezet nélkül)
- [ ] Nincs `→`, `—`, `–` sehol a Mermaid blokkban
- [ ] Nincs `{` vagy `}` node labelben
- [ ] Subgraph neve nem szerepel edge-forrásként
- [ ] Edge label: szóköz esetén idézőjelben van

---

## Nyelv

- A dokumentáció nyelve **magyar**, ha a user magyarul kommunikál
- Kódban szereplő dolgok (függvénynevek, fájlnevek, SQL) mindig az eredeti formájukban szerepelnek

---

## Hivatkozások a .md-ben

Fájlra vagy kódhelyre mutató linkek mindig relatív útvonallal, VS Code-ban kattintható formában:

```markdown
[sync_ohlcv.py:20](src/data_pipeline/sync_ohlcv.py#L20)
[table_ops.py](src/db/table_ops.py)
```
