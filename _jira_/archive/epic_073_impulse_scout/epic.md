# Epic 073: Impulse Scout — önálló impulzus-előrejelző modellcsalád

**Created:** 2026-11-21
**Status:** DONE (2026-11-22) — eredmény: minden modell és szabály NO-GO, S0+C baseline marad; összefoglaló `_doc_/research/impulse_scout.md`

## Goal
Önálló modellcsalád (**Impulse Scout**, model ID prefix `scout_`), amely egy megerősített lokális mélypont után
(t_c = t0 + W) megkülönbözteti: **impulzus** / **hamis indulás** / **oldalazás** (first-touch címke, epic_072 v2).
Nem a fw60 quant modell feature-e: hosszú távon a kereskedési döntés három független forrásból áll
(quant fw60 + Impulse Scout + hírek/sentiment); ez az epic csak a Scoutra fókuszál.

Kiinduló mérce (epic_072, skalár logit S0+C, W4h_H8h, SOL): train CV AUC 0,665; OOS (post-hoc) AUC 0,717;
felső 20% precizitás train 27% / OOS 38% (alap 16% / 14%).

## Scope
- Új assetek: BTCUSDT, ETHUSDT (SOL-hoz hasonló DB, 1m futures) — database_agent
- Több asset összevont eseményadatkészlet + order flow mezők — analyst_agent
- Több modelltípus közös benchmark kereten, iterációval — analyst_agent
- Módszertan `_doc_/methodology_doc/5300_impulse_scout.md` — methodology_agent

## Tasks
- t928: Kiinduló OOS leíró mérés (post-hoc, epic_072 modellek) — áthozva az epic_072-ből (analyst_agent)
- t929: Módszertan — Impulse Scout spec, protokoll, leaderboard, többszörös tesztelés, kereskedési szimuláció (methodology_agent)
- t937: BTCUSDT + ETHUSDT asset felvétele, DB + 1m OHLCV backfill (database_agent)
- t930: Több-asset eseményadatkészlet + order flow mezők (analyst_agent)
- t931: Közös benchmark keret (CV, metrikák, PnL-szimuláció) (analyst_agent)
- t932: Modell 1 — LightGBM kézi feature-ökön (analyst_agent)
- t933: Modell 2 — MiniRocket + InceptionTime nyers prefixen (analyst_agent)
- t934: Modell 3 — versengő kockázatok / diszkrét idejű hazard (analyst_agent)
- t935: Modell 4 — Chronos/Kronos embedding (kísérleti) (analyst_agent)
- t936: Leaderboard + 2. iteráció + összefoglaló (analyst_agent)

Sorrend: t928 (validálás) ; t929 ∥ t937 → t930 ∥ t931 → t932 ∥ t933 ∥ t934 (∥ t935) → t936

## Key Decisions
- **2026-11-21 (orchestrátor default, user felülbírálhatja):** a H1–H3 elsődleges (Holm) tesztek az **1. kör előre rögzített konfigurációin** futnak; a 2. kör győztesei csak SPA-val (teljes modell-univerzum) értékelhetők. Holdout-2 becslés (t929 validátor): pooled ≥250 esemény kb. 2027 márc. (sáv febr.–máj.), a t930 BTC/ETH rátáitól függ.
- **2026-11-21 (orchestrátor, t931 validátor javaslat):** a leaderboard elsődleges top-q nézete a **teszt-foldon belüli rangsor** (felső 20%/10%, címke nélkül); a train-küszöbös nézet másodlagos oszlop (`n_top`-pal). A harness-módosítást a t932 végzi; a t933/t934/t935 nem nyúl a `scout_bench` meglévő fájljaihoz.
- **2026-11-22 (user):** 1. kör eredménye: mind az 5 modell NO-GO, az S0+C baseline marad. A t936 2. köre **nem új modelltípus**, hanem a baseline-ra épülő kereskedési szabály (TP/SL σ_W/ATR-többszörös, részleges kilépés, belépési szűrő) — SPA-val a teljes univerzumon.
- **2026-11-21 (user):** scout_events vizuális ellenőrzőpont jóváhagyva → t932–t934 párhuzamosan indul.
- **2026-11-21 (user):** név: Impulse Scout; önálló modell, NEM kerül a quant modellbe. BTC + ETH DB felépítése a SOL
  mintájára, több-asset összevonás a mintaméret miatt. A t928 az epic_072-ből ide került.
- **Értékelési fegyelem:** iteráció csak train-en belüli purged CV-vel; a SOL OOS (2026-02→07) már egyszer meg lett nézve
  (t928) → nem tiszta; végső tiszta próba a holdout-2 (2026-08-01→), érintetlen. BTC/ETH-ra ugyanazok a split-határok.
- Kapcsolódik: epic_072 (archivált; v2 eseménydefiníció, `research."wave_events_v2"`, 5200), epic_071 (S/R feature-ök),
  epic_064 (multi-coin tanulságok), epic_061 (microstructure flow).
