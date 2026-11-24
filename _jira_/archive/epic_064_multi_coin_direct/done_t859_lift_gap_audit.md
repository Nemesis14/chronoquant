---
epic: epic_064
id: t859
title: A train/valid lift-szakadék auditja (+12,31pp vs +2,22pp)
assignee: modeling_agent
status: done
blocks: [t856]
blocked_by: [t858]
---

## Goal

A `t858` gördülő mérésében a p90 lift a trainen +12,31pp, a validon +2,22pp.
Az 5,6-szoros szakadékot nem elég in-sample optimizmusnak nevezni: ellenőrizni
kellett, hogy a két oldal **ugyanazt a számot számolja-e**, és hogy a maradék
különbség rezsimváltásból jön-e.

Négy magyarázat adja ugyanezt a képet: (1) nem ugyanaz a mennyiség, (2) nem
ugyanaz a becslő, (3) tiszta in-sample optimizmus, (4) rezsimváltás.

## Eredmény — nincs számítási inkonzisztencia

**1. Ugyanaz a mennyiség.** A legkomolyabb gyanú a holtverseny volt: a modell
szándékosan durva (3 levél, `min_child_samples=4000`), tehát lépcsős a
predikciója, és a `pred >= quantile(0,90)` a tizednél többet választhat ki —
25 000 train soron másképp, mint 600 valid soron.

| | kiválasztott hányad | n_sel | alaparány (+1) | timeout |
|---|--:|--:|--:|--:|
| train | 0,100 | 1892 | 49,05% | 17,38% |
| valid | 0,101 | 50 | 49,56% | 17,80% |

Nem ez történik. A hányad, az alaparány és a timeout-arány is egyezik.

**2. Ugyanaz a becslő.** Havi átlag +2,22pp, poololt +2,77pp — 0,55pp
különbség, az aggregáció sem magyarázza a szakadékot.

**3. A döntő mérés.** A gördülő mérés visszanyújtva 2023-10-ig, így a tanítási
korszak hónapjai is out-of-sample pontot kapnak:

| korszak | hónap | lift IN-SAMPLE | lift OUT-OF-SAMPLE | szórás |
|---|--:|--:|--:|--:|
| train-kor | 10 | +11,49pp | +5,13pp | 6,05 |
| valid-kor | 24 | +12,31pp | +2,22pp | 10,60 |

Az in-sample lift a **két korszakban gyakorlatilag azonos**. A +12pp tehát nem
a tanítási időszak tulajdonsága, hanem a modell kapacitásáé: ennyit ér el
bármilyen adaton, amit már látott.

- in-sample → OOS ugyanazon a korszakon: **−6,36pp** (tiszta optimizmus)
- train-kor OOS → valid-kor OOS: **−2,91pp, Welch t = −1,01** — nem válik el
  a zajtól

**4. Ami tényleg elmozdult: a barrier.** A címke eloszlása stabil (+1 arány
45–55%, timeout 14–24%), de a barrier szélessége **205 bps → 93 bps**-re esett.
A liftet ez nem torzítja (címke-arány, nem hozam), a gazdaságtant viszont igen:
205 bps mellett a 4,72 bps költség a kifizetés 2,3%-a, 93 bps mellett 5,1%-a.

**5. Havi zaj.** A havi OOS liftek szórása 10,60pp, az egyedi hónapok −15,64 és
+21,39 közt szórnak. Egyetlen hónap liftje gyakorlatilag zaj.

## Kimenet

- [src/modeling/experiments/sol_lgbm_8h_audit.py](../../src/modeling/experiments/sol_lgbm_8h_audit.py)
- [_doc_/research/sol_8h_lgbm_gate.md](../../_doc_/research/sol_8h_lgbm_gate.md) §9

## Acceptance Criteria

- [x] Realizált kiválasztási hányad és alaparány mindkét oldalon
- [x] Poololt vs havi átlag lift összevetés
- [x] OOS lift a tanítási korszakban is (visszanyújtott gördülő mérés)
- [x] Korszakok közti különbség formális próbája
- [x] Címke- és barrier-drift negyedévente
- [x] Értékelési blokkhossz érzékenysége (1/3/6/12 hónap, poololt)

## Notes

⚠️ A visszanyújtás diagnosztika, nem tiszta eredmény: a regularizációs rácsot
2024-07-31-ig terjedő adaton választottuk, tehát a train-kori hónapok ettől
kontamináltak. Arra jó, hogy „változott-e a világ", nem arra, hogy „mennyi pénz
van benne".

Hosszabb értékelési blokk nem segít (§9.7): a pontbecslés +2,2 és +2,8pp
kozott marad, a standard hiba pedig nem csökken, mert kevesebb blokk lesz.
A mintaméret a korlát, és azt csak több coin oldja meg.

A csökkenő barrier-szélesség új megkötés a `t856`-ra: fix bps-ben mért költség
mellett ugyanaz a találati arány egyre kevesebbet ér. A `t856` vol-normalizált
fedezeti pontja pontosan ezt kezeli — ez a mérés megmutatta, miért nem
elhagyható.
