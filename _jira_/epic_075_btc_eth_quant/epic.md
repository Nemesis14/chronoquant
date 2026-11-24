# Epic 075: BTCUSDT + ETHUSDT quant fw60 modellek

**Created:** 2026-11-23
**Status:** DONE (2026-11-24) — BTC élesben; ETH modell cancelled; utómunka: epic_076

## Goal
A SOL/BCH mintájára (epic_069) fw60 LightGBM long/short quant modell + stratégia-kalibráció BTC-re és ETH-ra,
és megjelenítés a dashboardon. Tanítási időszak és beállítás a SOL/BCH-val azonos (2021-01 → 2026-07, fw60).

## Tasks
- t944: BTC fw60 modell (sampling, training, search) + stratégia (modeling_agent)
- t945: ETH fw60 modell + stratégia (modeling_agent)
- t946: BTC/ETH a dashboardon + trading.json stratégia-blokkok, predikció sync (ui_agent)

Sorrend: t939 (epic_074) → t944 → t945 → t946. A t944/t945 CPU-igényes, egymás után futnak.

## Key Decisions
- **2026-11-23 (user):** BTC + ETH quant modell; azonos train range és fw60 beállítás, mint SOL/BCH. Orchestrált végrehajtás.
