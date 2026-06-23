# Epic 035: execution-aware strategy search a jelenlegi strategy optimizer helyére

## Goal
A jelenlegi strategy optimizer lecserélése egy olyan execution-aware strategy
keresőre, amely a modell rangsorolási erejét nem közvetlenül headline proxy
metrikává, hanem tényleges backtest execution szabállyá fordítja.

A fő cél nem új predikciós modell készítése, hanem annak keresése, hogy a felső
score-régióból milyen belépési cutoff, take profit és stop loss szabály mellett
maximalizálódik a teljes mintán a realizált összesített log return.

## Scope
- `src/strategy/`
- `analyst/`
- `config/strategies.json` és kapcsolódó strategy config contract
- `artifacts/<strategy_session>/`
- kapcsolódó strategy report kontraktus
- szükséges strategy smoke / validation tesztek

## Out of Scope
- új modell target vagy új modell-tréning
- feature engineering refaktor
- live trading service döntési logika finomhangolása ezen epicen túl
- trade-level UI dashboard redesign

## Search Objective
- Optimalizációs cél: **maximális total realized log return** a fit window-n.
- A nyertes setup az, amelyik a teljes backtest mintán a legnagyobb összesített
  `fact_log_return`-t adja.
- A `1h timeout` fix fallback exit marad.
- Korai TP vagy SL zárás után a következő bartól újranyitás engedett.

## Candidate Search Space

### Entry cutoff
A decilis túl durva; az optimizer elsődleges belépési egysége ne bucket, hanem
score-percentilis cutoff legyen.

Javasolt első keresési rács:
- `0.90`
- `0.92`
- `0.94`
- `0.95`
- `0.96`
- `0.97`
- `0.98`
- `0.99`

Megjegyzés: a report ettől még mutathat decilis / band szintű összesítőt, de a
kereső belső paramétere cutoff legyen.

### Take profit candidates
A TP a kiválasztott felső score-régió 1h target statisztikáiból származzon.

Első iterációban javasolt opciók:
- `bucket_mean_mfe`
- `bucket_median_mfe`
- `bucket_p75_mfe`
- `0.75 * bucket_mean_mfe`
- `0.50 * bucket_mean_mfe`

### Stop loss candidates
Az SL log return mértékben értendő, longnál negatív előjellel. A `0` külön érték,
tehát stop nélküli verzió is keresendő.

Első iterációban javasolt opciók:
- `0`  (nincs stop)
- `0.5 * TP`
- `1.0 * TP`
- `1.5 * TP`
- `2.0 * TP`

## Required Strategy Rules
- csak a kiválasztott direction(ök)re fusson; első iterációban a long-oldali
  keresés is legyen támogatott külön
- belépés, ha `score_pct >= entry_cutoff`
- TP és SL intrabar szinten `high/low` érintés alapján aktiválható
- ha ugyanazon baron TP és SL is elérhető lenne, a szabály legyen explicit és
  konzervatív módon dokumentált
- ha egyik sem aktiválódik, zárás `60` perc után `close` áron
- korai TP/SL exit után a következő bartól új entry vizsgálható

## Reporting Contract
Az új report ne tartalmazzon kötelező trade-level kimutatást.

Szükséges output:
- `summary` tábla a **nyertes setupra**
- `entry cutoff / TP / SL / exit_reason` szintű aggregált összesítő
- `entry band` vagy decilis szerinti összesítő a nyertes vagy top setupokra
- opcionálisan top-N setup összehasonlító tábla

A summary csak aggregálható mezőket tartalmazzon, például:
- `n_trades`
- `avg_entry_price`
- `avg_exit_price`
- `avg_profit_pct`
- `avg_expected_log_return`
- `avg_fact_log_return`
- `avg_fact_1h_max_range_log_return`
- `total_fact_log_return`
- `compounded_return_pct`
- `realized_directional_win_rate`
- `avg_hold_minutes`
- `take_profit_spec`
- `stop_loss_spec`

## Deliverables
- új strategy search implementáció a régi optimizer helyére
- explicit param grid / search config
- best setup artifact
- summary-only report
- top candidate setup összehasonlítás
- frissített strategy artifact contract
- tesztek a fő execution szabályokra

## Tasks
- t49: execution-aware strategy search spec és search-space kontraktus véglegesítése
- t50: új strategy search engine implementáció percentile cutoff + TP/SL grid alapján
- t51: report contract átállítása summary-only / setup-comparison outputokra
- t52: strategy artifact és config contract frissítése az új optimizerhez
- t53: smoke és szabálytesztek az új TP/SL/timeout/re-entry logikára
- t54: teljes újrafuttatás a két champion modellen és top setup eredmények mentése
- t55: végső validáció és konzisztencia-ellenőrzés

## Key Decisions
- A stratégia-keresés célfüggvénye realized total log return, nem proxy hit rate.
- A decilis csak riport-szintű interpretációs egység; a tényleges optimalizációs
  tengely score-percentilis cutoff.
- A `1h timeout` fix marad, hogy az execution tér kezelhető legyen.
- A `stop_loss = 0` legitim opció, tehát a stop nélküli baseline is a keresési tér része.
- A report fókusza setup-szintű összefoglaló, nem trade ledger.

## Risks
- A TP/SL intrabar-touch backtest egyszerűsített; a fill-logika torzíthat, ha a
  baron belüli sorrend nem ismert.
- A túl sűrű search grid könnyen overfit-elhet a fit window-ra.
- A percentilis cutoff és a bucket-statisztikák közti mappingot világosan kell
  definiálni, különben a report nehezen lesz olvasható.
- A meglévő optimizer és artifact contract részleges cseréje több downstream
  komponenst érinthet.
