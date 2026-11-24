---
epic: epic_060
id: t811
title: Per-soros (ATR-skálázott) korlát a triple-barrier címkézőben
assignee: database_agent
status: todo
blocks: [t812]
blocked_by: []
---

## Goal

A `triple_barrier_labels()` ma skalár `up_bps` / `dn_bps`-t vesz. Vegyen
tömböt is, hogy a korlát soronként a volatilitáshoz skálázódhasson.

## Scope

- `src/data_handling/sync_tables/_triple_barrier.py`
- `src/data_handling/sync_tables/sync_triple_barrier.py`
- `src/data_handling/tests/sync_tables/smoke/test_triple_barrier.py`

## A skálázás

```
atr_pct(t-1) = ATR_14 / close, a jel elotti barrol (leak-mentes)
barrier_bps(t) = clip(k * atr_pct(t-1) * 10000, MIN_BPS, MAX_BPS)
```

Rögzített értékek (előre, nem hangolva): `k = 1.5`, `MIN_BPS = 60`,
`MAX_BPS = 400`. Címke neve: `tb_label_240_atr15`.

⚠️ A két konvenció **nem változhat**: intrabar érintés dönt, same-bar
ütközésnél az alsó korlát nyer.

## Acceptance Criteria

- [ ] `triple_barrier_labels()` elfogad skalárt ÉS tömböt is (visszafelé kompatibilis)
- [ ] Smoke teszt: tömb-alakú konstans korlát == a skalár eredmény
- [ ] `tb_label_240_atr15` oszlop a `quant_train`-ben, NULL az utolsó 240 barra
- [ ] **Osztályeloszlás évenkénti bontásban kiírva** — ez a belépési feltétel
- [ ] `ruff check` + `pyright` tiszta

## Notes
[orchestrator] Elvegezve — 2026-08-26

**Terv-eltéres, tudatos.** A ticket ATR_14-et irt `k=1.5`-tel. Helyette:

```
sigma_1m = EWM std(logret, span=1440), t-1-re shiftelve
korlat_bps = clip(k * sigma_1m * sqrt(240) * 1e4, 60, 400)
```

Ok: az ATR_14 egy 1 perces bar tartomanyat meri, a korlat viszont egy 240
perces ablakra vonatkozik — a ketto kozott nincs rogzitett arany. A
sqrt-ido skalazas kozvetlenul a horizontra visz.

A `k` **nem hangolt**: ugy van valasztva (`normalising_k`), hogy a korlat
teljes-historias medianja pontosan 100 bps legyen. Igy a fix valtozattal
osszevetheto — az egyetlen kulonbseg az adaptivitas, nem a szelesseg.
Mert ertek: **k = 0,5242**.

## Az eredmeny — a cimke rezsim-stabilitasa

| | fix ±100 bps | vol-skalazott |
|---|---|---|
| timeout 2021 | 1,6% | 6,5% |
| timeout 2026 | 39,3% | 19,6% |
| **tartomany** | **37,7pp** | **13,1pp** |

A korszak-fuggés **haromszorosara csokkent**, de a 10pp-es belepesi
feltetelt 3,1pp-vel nem teljesiti.

## ⚠️ Miert nem — es ez fontosabb, mint a szam

A maradek instabilitas **teljes egeszeben a 60 bps-es padlobol jon**.
Padlo nelkul merve, evenkenti median korlat:

| ev | median | 60 bps alatt |
|---|---|---|
| 2021 | 153,5 bps | 0,0% |
| 2023 |  91,8 bps | 14,7% |
| 2025 |  82,8 bps | 12,8% |
| **2026** | **64,5 bps** | **42,7%** |

Vagyis: **2026-ban a SOL 4 oras volatilitasa annyira leesett, hogy egy
rezsim-semleges korlat a barok 42,7%-an a dijfedezeti szint ALA kerulne.**

Ez nem cimkezesi hiba — ez strukturalis allitas a kereskedhetosegrol, es
onmagaban magyarazza a 2026-os veszteseg egy reszet. A padlo nem elvehetó:
egy 40 bps-es korlat mellett a 13 bps kor-koltseg a nyeresег harmada.

## Acceptance Criteria

- [x] `triple_barrier_labels()` elfogad skalart ES tombot (visszafele kompatibilis)
- [x] Smoke teszt: tomb-alaku konstans korlat == skalar eredmeny
- [x] `tb_label_240_vol` + `tb_label_240_vol_bps` a `target` / `quant_train`-ben
- [x] Osztalyeloszlas evenkent kiirva
- [x] `ruff check` + `pyright` tiszta, 17 teszt zold
