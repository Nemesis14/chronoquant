## Cél

**Ellenőrzőpont (epic_071 / t911):** mielőtt a detektált alakzatok feature-ként a modellbe kerülnek (t912), vizuálisan és számokkal ellenőrizzük, hogy a detektor *jó alakzatokat* talál-e: értelmes vízszintes támasz/ellenállás szinteket és trendvonalakat, ésszerű darabszámmal és időben (look-ahead nélkül) megerősítve.

**Kapcsolódó dokumentáció:** módszertan `_doc_/methodology_doc/1700_features_sr_trendline.md` (definíciók, paraméter-indoklás, placebo és go/no-go), kód `src/data_handling/sync_tables/_levels_polars.py` (t910, sémák a docstringekben).

**Adat:** SOLUSDT `ohlcv` 1m, 2020-09-14 07:00 → 2026-11-19 13:39 UTC (3,250,480 sor), read-only. Generálás: `uv run python -m reports.builders.sr_detection`.

## Módszer röviden

- **Léptékek:** az 1m adatból epoch-igazított scale-bar (`s` = 1h / 7 nap, `m` = 4h / 30 nap, `l` = 1d / 180 nap), ATR14 a scale-baron.
- **Pivot:** fraktál, n = 3 bar mindkét oldalon, prominencia ≥ 1·ATR; `confirmed_at` = az i+n. bar *záróideje*.
- **Szint:** megerősített pivotok online klaszterezése 0.25·ATR tűréssel, ≥ 2 érintéstől szint; az ár az első két érintés átlaga, utána fagyasztva. Elévül az utolsó érintés megerősítése + lookback után. A törést túléli (szerepcsere).
- **Trendvonal:** emelkedő mélypontok (támasz) / csökkenő csúcsok (ellenállás), ≥ 3 érintés ε = 0.25·ATR-en belül, a horgonyok között nincs ε-nál nagyobb vonalon túli záróár; duplikátumszűrés. A törés után a 12 bar retest-ablak végén meghal.
- **Események:** `touch` (bar-szélsőérték ε-on belül a megközelítés oldaláról), `break` (záróár ≥ 0.5·ATR-rel túl), `retest` (break után 12 baron belül vissza ε-on belülre).
- **Erő:** `strength = Σ exp(−kor/τ)`, τ = lookback/3 — kb. a „friss érintések száma”.

{{table:params}}

## 1. Összefoglaló léptékenként

Teljes SOL 1m történet. A szerep (támasz/ellenállás) a szint megerősítésekor érvényes oldal (záróár ≥ szint → támasz). Élettartam = `confirmed_at` → elévülés; a még élő szinteknél az adat végéig (cenzorált). Kerek szám: 0.25·ATR-en belül 10 (ill. csak 5) többszöröséhez.

{{table:scale_summary}}

{{table:level_touch_dist}}

### Trendvonalak

Hossz = első horgony → utolsó érintés scale-barban; meredekség ATR/bar a megerősítéskori ATR-rel normálva; „törve ért véget” = a vonal élete kitöréssel zárult (nem elévüléssel).

{{table:line_summary}}

{{table:line_touch_dist}}

### Események

{{table:event_counts}}

{{table:event_yearly}}

{{chart:event_monthly}}

### Nyers kimenet-arányok — ⚠️ NYERS, placebo nélkül, NEM bizonyíték

Az alábbi arányok a `forward_label_events` címkéi (24 scale-bar horizont, 1·ATR küszöb) **kontroll nélkül**. Nem mondanak semmit arról, hogy a szintek *többet* érnek-e véletlen áraknál: ugyanilyen arány véletlen szinteken is kijöhet (pl. a bounce 1·ATR-es küszöbe egy volatilis piacon gyakran teljesül magától). A szignifikancia kérdése a t913 placebo event study feladata; itt csak a címkék eloszlásának józanságát nézzük.

Pozitív címke: touch → `bounce`, break → `follow_through`, retest → `hold`. Timeout = a horizonton belül nem dőlt el (negatív címkét kap). A retest `hold` szigorú: bármely 24 baron belüli záróár a régi oldalon már `fail`.

{{table:forward_raw}}

## 2. Áttekintő képek

**Jelölés:** zöld = támasz, piros = ellenállás (a *pillanatnyi* szerep: egy szint színe a kitörésnél vált). A vonalvastagság és átlátszóság az erő: szintnél az adott pillanatban ismert `strength` (< 0.5 gyenge, < 1.2 közepes, különben erős), trendvonalnál az addig ismert érintésszám (3 / 4 / ≥ 5). Minden objektum **csak a `confirmed_at`-tól** az elévülésig (vonalnál a törésig) látszik — amit a detektor abban a pillanatban tudott. Sárga háromszög = pivot (▼ csúcs, ▲ mélypont). A jelmagyarázatra kattintva egy réteg ki-/bekapcsolható; hover a szakaszok végein és közepén adja a paramétereket. Az y-tengely a gyertyákhoz igazodik, a képen kívül eső szintek nem látszanak.

### Utolsó 90 nap, 4h gyertya — `l` (folytonos) és `m` (szaggatott) szintek

Látható objektumok: 16 `l` és 41 `m` szint (képen kívül: 0 / 0).

{{chart:overview_4h_m_l}}

### Utolsó 30 nap, 1h gyertya — `s` szintek (folytonos) és trendvonalak (szaggatott)

Látható objektumok: 42 szint, 51 trendvonal (képen kívül: 0 / 0).

{{chart:overview_1h_s}}

### Jelenleg aktív szintek (adat vége)

A `levels_as_of(adat vége)` pillanatkép — összevethető a saját chartoddal.

{{table:active_levels_now}}

## 3. Kinagyított példák

Az utolsó 365 nap eseményeiből, **determinisztikus** kiválasztással (nem kézi): kategóriánként léptékenként rangsor — az esemény pillanatában már ismert érintésszám csökkenő, utána a kimenet nagysága (MFE ATR-ben; hamis kitörésnél a visszatérés gyorsasága), végül a frissebb esemény; a léptékek körforgásban (`l` → `m` → `s`), kategóriánként 3 példa, egy objektum legfeljebb egyszer, és két példa kulcseseménye legalább 7 nap távolságra (egy piaci epizód egyszer szerepel).

**Jelölés a példákon:** a gyertyák a példa saját léptékének barjai. Pontozott szürke = a kialakulás szakasza (első érintés → `confirmed_at`, a detektor ekkor még *nem* ismeri); fehér csillag + függőleges pontozott vonal = `confirmed_at` (hover: az objektum minden paramétere); nagy sárga gyűrű = az objektum érintés-pivotjai, kis háromszög = a lépték többi pivotja; ○ = touch, ✕ = break, ◆ = retest esemény (hover: nyers forward címke, MFE/MAE); kék sáv = a kulcsesemény 24 bar-os forward horizontja; nyíl = kulcsesemény a nyers címkéjével.

### Erős, többszörösen érintett szint visszapattanással

**Példa 1** — `l-L000064` (l, 1d), kulcsesemény 2025-12-12 00:00 UTC; érintés az eseménykor: 7; támasz-érintés, +1.4 ATR max. elmozdulás.

{{chart:ex01_bounce_l}}

**Példa 2** — `m-L000574` (m, 4h), kulcsesemény 2026-05-24 00:00 UTC; érintés az eseménykor: 8; ellenállás-érintés, +3.7 ATR max. elmozdulás.

{{chart:ex02_bounce_m}}

**Példa 3** — `s-L002582` (s, 1h), kulcsesemény 2026-10-28 21:00 UTC; érintés az eseménykor: 9; ellenállás-érintés, +10.0 ATR max. elmozdulás.

{{chart:ex03_bounce_s}}

### Szerepcsere (áttört szint visszatesztje az új oldalról, tart)

**Példa 4** — `l-L000091` (l, 1d), kulcsesemény 2026-07-09 00:00 UTC; érintés az eseménykor: 4; támasz áttörve, visszateszt ellenállásként, +2.4 ATR.

{{chart:ex04_role_flip_l}}

**Példa 5** — `m-L000566` (m, 4h), kulcsesemény 2026-05-05 20:00 UTC; érintés az eseménykor: 8; ellenállás áttörve, visszateszt támaszként, +8.1 ATR.

{{chart:ex05_role_flip_m}}

**Példa 6** — `s-L002502` (s, 1h), kulcsesemény 2026-08-18 07:00 UTC; érintés az eseménykor: 7; ellenállás áttörve, visszateszt támaszként, +5.7 ATR.

{{chart:ex06_role_flip_s}}

### Trendvonal-kitörés + sikeres visszateszt

**Példa 7** — `l-T000119` (l, 1d), kulcsesemény 2026-03-28 00:00 UTC; érintés az eseménykor: 3; támasz áttörve, visszateszt ellenállásként, +2.2 ATR.

{{chart:ex07_line_break_retest_l}}

**Példa 8** — `m-T000818` (m, 4h), kulcsesemény 2026-04-27 16:00 UTC; érintés az eseménykor: 4; támasz áttörve, visszateszt ellenállásként, +4.3 ATR.

{{chart:ex08_line_break_retest_m}}

**Példa 9** — `s-T003012` (s, 1h), kulcsesemény 2026-01-27 16:00 UTC; érintés az eseménykor: 5; ellenállás áttörve, visszateszt támaszként, +4.4 ATR.

{{chart:ex09_line_break_retest_s}}

### Hamis kitörés (záróár visszatér ε-on belülre)

**Példa 10** — `l-L000058` (l, 1d), kulcsesemény 2025-12-04 00:00 UTC; érintés az eseménykor: 12; ellenállás szint átütve, 1 bar múlva vissza.

{{chart:ex10_false_break_l}}

**Példa 11** — `m-L000572` (m, 4h), kulcsesemény 2026-05-16 12:00 UTC; érintés az eseménykor: 7; támasz szint átütve, 2 bar múlva vissza.

{{chart:ex11_false_break_m}}

**Példa 12** — `s-L002297` (s, 1h), kulcsesemény 2026-03-12 15:00 UTC; érintés az eseménykor: 9; támasz szint átütve, 1 bar múlva vissza.

{{chart:ex12_false_break_s}}

{{table:examples}}

## 4. Mire figyelj az ellenőrzésnél

1. **Túl sok vonal?** Az áttekintő képeken egyszerre hány objektum él, és hány közülük „semmi” (egyetlen gyenge, szinte érintetlen szakasz)? Ha zavaró a sűrűség, a min. érintésszám vagy a prominencia emelése a fő eszköz.
2. **Hiányzik nyilvánvaló szint?** Van-e szemmel jól látható, többször tesztelt ár, amit a detektor nem jelöl? (Gyakori ok: a pivot prominenciája < 1·ATR, vagy az érintések 0.25·ATR-nél jobban szórnak.)
3. **Késői megerősítés?** A csillag (`confirmed_at`) és az első érintés közötti távolság mutatja, mennyit kell várni egy szintre (min. a 2. pivot + 3 bar). Ez a look-ahead-mentesség ára — elfogadható-e?
4. **Szint ára:** az első két érintés átlaga fagyasztva — illeszkedik-e a későbbi érintésekhez, vagy elcsúszik?
5. **Trendvonalak:** a 3+ érintéses vonalak „rajzolná-e így egy trader”? Nem túl laposak/meredekek? A dedup nem hagy-e szinte párhuzamos duplikátumokat?
6. **Események:** a touch / break / retest jelölések ott vannak-e, ahol a képen is érintést/kitörést látsz? A hamis kitörések tényleg visszatérnek-e? Oldalazásban egy sokszor érintett szint többször is oda-vissza „törik” (szerepcsere-sorozat), mert a 0.5·ATR-es záróár-küszöb a kis léptéken könnyen teljesül — zaj-e ez?

## Nyitott kérdések a paraméterhangoláshoz (user döntés)

- **Szelekciós torzítás:** a módszertan (1700) szerint a paraméterek a t911 előtt fagyasztva vannak; ha e riport alapján változtatunk, az új verzió, és a t913 placebo-tesztet a módosított paraméterekkel kell futtatni.
- **OOS-szennyeződés:** a riport (és a példák) a 2025–2026-os időszakot is mutatják, így a t913 OOS része ezzel „látottá” vált — kontaminált jelölés vagy train-re korlátozott riport (1700: nyitott döntés).
- `level_min_touches` = 2 elég-e, vagy a 3 érintéses szint a tisztább? (a szintek érintés-eloszlása fent)
- `pivot_min_prominence_atr` = 1.0 és `level_tolerance_atr` = 0.25: sok-e a közeli, párhuzamos szint?
- A retest `hold` definíciója nagyon szigorú (bármely régi oldali záróár = fail) — ezért ritka a `hold`.
- A kerek szám flag (10 / 5 USDT) SOL árszintjén mennyire informatív? (aránya fent)
- A 2 érintéses trendvonal-jelöltek nincsenek a kimenetben (t910 döntés) — kell-e flaggel?

## Értelmezés

A darabszámok léptékenként arányosak (`s`: 2622 szint, medián élettartam 7.5 nap; `m`: 642 szint, medián élettartam 31.8 nap; `l`: 104 szint, medián élettartam 180.0 nap), a szintek többsége 2–3 érintéses. A szint élettartama konstrukció szerint ≥ lookback (elévülés = utolsó érintés + lookback), ezért a medián a lookback körül van.

**Feltűnő mintázatok a képek mellé:**

- A szintek nagy része legalább egyszer szerepet cserél (`s` 78%, `m` 78%, `l` 77%): a szint a törést túléli, és oldalazásban többször oda-vissza „törik”. Ez a szerepcsere-feature-öket zajossá teheti.
- A trendvonalak szinte mindig kitöréssel érnek véget, rövid élettel (törve arány / medián élet: `s` 93% / 0.5 nap, `m` 93% / 1.9 nap, `l` 96% / 11.2 nap): a 3. érintés megerősítése (pivot + 3 bar) gyakran már a vonal végéhez közel jön — lásd a vonal-példákat.
- A 10-es kerek szám flag aránya (`s` 7%, `m` 14%, `l` 32%) a nagy léptéken a 0.25·ATR tűrés miatt közel triviális — 1d ATR mellett a ±tűrés a 10 USDT-s rács jelentős részét lefedi.

A számok és a képek alapján a detektor *strukturálisan* működik (élettartam-korlátos, időben megerősített objektumok, értelmes eseményszámok); a **döntés a vizuális ellenőrzésé**: ha a képek a fenti 1–6. szempont szerint rendben vannak, a paraméterek fagyasztva maradnak és jöhet a t912/t913. A nyers kimenet-arányok nem használhatók a hasznosság megítélésére — erre csak a placebo-kontrollált t913 alkalmas.
