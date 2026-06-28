---
epic: epic_041
id: s2
title: "Top10 lift metrika: differencia helyett ráta"
---

## Goal

A jelenlegi `top10_lift` keresési objektív differenciát számít:
`mean(y_true | top 10% score) − mean(y_true | all valid)`

Ezt érdemes rátára cserélni:
`mean(y_true | top 10% score) / mean(y_true | all valid)`

**Indok:** A ratio skála-invariáns — train és valid periódus összehasonlítása robusztusabb,
mert nem függ a baseline abszolút szintjétől. A valid átlag ~39%-kal alacsonyabb mint a trainé
(0.00471 vs 0.00771); differencia alapú metrikánál ez optikai torzítást okoz train/valid lift
összevetésnél. Ratio esetén a „kétszeres átlag" mindkét perióduson ugyanazt jelenti.

**Kockázat:** Alacsony — a target 92-93%-ban pozitív, a nevező stabilan > 0.

## Tasks

- [ ] t330: Search kód módosítása — `top10_lift` differencia → ratio (`modeling_agent`)
- [ ] t331: Metodológia dokumentáció frissítése — `5500_hyper_param_search.md` (`methodology_agent`)

## Notes

Felvetés forrása: sampling analysis notebook review (2026-06-24).
Egyéb alternatíva megvizsgálva: normalizált lift `(mean_top10 - mean_all) / std_all`
— erősebb, de over-engineered; egyelőre nem prioritás.
