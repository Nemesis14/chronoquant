# 30 coinos panel és coinonkénti irány-modellek — epic_064 kísérletsor

> **Státusz: LEZÁRATLAN, de nem folytatott.** A t856 (walk-forward) és t857 (verdikt) sosem készült el; az egyedi kísérletek egyike sem talált kereskedhető irányjelet. A SOL 8 órás ág lezárt jelentése: [sol_8h_lgbm_gate.md](sol_8h_lgbm_gate.md), [champion_streak_gate.md](champion_streak_gate.md).
>
> Időszak: 2026-08-26 – 09 · 30 coin (görgetett top-30 univerzum), 24 órás vol-skálázott triple-barrier cél

## Mit néztünk
Megoldja-e a „merítés" (30 coin, ~21-szer több tanítósor) azt, amit egy coinon nem lehetett: szigorúbb (p99,9) küszöb mellett a szelektált jelek találati aránya a vol-normalizált fedezeti pont fölött marad-e két likviditási sávban és mindkét rezsimben.

## Módszer és lépések
- **Panel:** görgetett univerzum, 5 perces feature-rács, részarány-blokk, coinonként azonos vol-többszörösű korlát (közös k = 0,648; az első két build hibás volt: 1,0x korlátnál timeout 0,1% és 56,9% kétoldali érintés, a coinonkénti 300 bps célérték coin-identitást tanított volna). 12,9 M `target_5m` sor.
- **Alapmodellek** (`baseline_features`, `baseline_regression`, `woe_models`): a hibás első modell a hét napját memorizálta (train találat 92–98%, valid 43–58%). Poolozott 30 coinos OLS: IC 0,0253 → 0,0016 (a train IC 6%-át tartotta meg); BTC egyedül 0,0649 → 0,0585; XRP 3 változós OLS 0,041 → 0,038.
- **Coinonkénti modellek** (`single_coin_regression`, `single_coin_lgbm`, `forward_selection`): 79 feature-ös LightGBM a train IC-t teljesen elvesztette (0,219 → 0,000) még 3 levélnél is; az effektív minta ~2350 független megfigyelés (13,9-szeres variancia-inflálás).
- **Irány-feature-ök** (`direction_features`, `direction_models`): minden tiszta magnitúdó-feature (natr, atr, hist_vol, parkinson_vol…) kidobva, mert a modellek folyton a volatilitást találták meg; deriváns-pozicionálás (funding, OI, long/short ratio, taker split) hozzáadva.
- **Horizont:** `horizon_sweep` — az irányított lift +9,5 pp 4 órán, ~0 24 órán; a horizontot a díj miatt tolták ki, nem a jel miatt.
- **SOL egy coin:** `sol_lasso` (Lasso + véletlen baseline), `sol_short_horizon` (havi újratanítás, rövid feature-ök), `sol_15m_grid` (15 perces rács — minden mutatón jobb az órásnál, de az első null-teszt egyetlen szerencsés rotáción alapult, lásd [sol_8h_lgbm_gate.md](sol_8h_lgbm_gate.md) §10), `sol_15m_windows` (megírva, nem futott).
- `multi_coin_walkforward` (t856): megírva, a panelen nem futott le a verdiktig.

## Konklúzió
Az irány-előrejelzés a panelen sem hozott robusztus jelet: a nagy modellek memorizálnak, az egyszerű lineárisak kb. a train IC-jüket tartják, de abszolút szinten ~0,04 IC-vel, ami díj után nem kereskedhető. A null-eloszlás-teszthez mindig több rotáció kell (24 rotációnál a legkisebb p-érték 0,040).

## Döntés / következmény
A direkciós ág nem folytatódik; az egyetlen pozitív nyom a piac-semleges rangsor ([cross_sectional_probe.md](cross_sectional_probe.md), nem szignifikáns). A panel kísérleti kódja (`src/modeling/experiments/`) a t894-ben törlődik.

## Kód a git historyban
Commit `73f2156`: `src/modeling/experiments/{baseline_features,baseline_regression,woe_models,single_coin_regression,single_coin_lgbm,forward_selection,direction_features,direction_models,horizon_sweep,sol_lasso,sol_short_horizon,sol_15m_grid,sol_15m_windows,multi_coin_walkforward}.py`. A panel adatai (`database/`) nem a repo részei.
