# Elliott Wave Detection System — Implementation Plan

**Státusz:** Kész — 2026-06-09  
**Cél:** Moduláris, multi-timeframe Elliott-hullám detektáló rendszer SOLUSDT adatokon,  
1m-től tetszőleges TF-ig paraméterezhető pivot és threshold beállítással.

---

## Áttekintés

A rendszer jelölt-alapú: nem egyetlen "helyes" számozást kényszerít ki, hanem
`hard_rule_pass + confidence_score` párost rendel minden detektált mintához.
A meglévő `elliott_1212.py` ZigZag+scoring logikája az M1 pivot motorba épül be,
a 1212 scanner pedig M4-ben kap végleges helyet.

---

## Könyvtárstruktúra

```
src/analysis/elliott/
    __init__.py
    config.py               # ElliottConfig, timeframe preset-ek
    data.py                 # Candle, Pivot, WaveSegment, PatternCandidate

    pivots/
        __init__.py
        zigzag.py           # ZigZag threshold motor (átdolgozott 1212 motor)
        fractal.py          # Williams Fractal motor
        multi_degree.py     # Több fokozatú pivot builder (degree_0/1/2)

    indicators/
        __init__.py
        atr.py              # ATR-14 Wilder (átdolgozott 1212 _atr14)
        momentum.py         # EMA slope, range expansion, volume

    validators/
        __init__.py
        base.py             # ValidationResult, PatternValidator ABC
        impulse.py          # ImpulseValidator (1-2-3-4-5)
        diagonal.py         # DiagonalValidator (ending / leading)
        zigzag_abc.py       # ZigZagValidator (5-3-5 correction)
        flat.py             # FlatValidator (regular / expanded / running)
        triangle.py         # TriangleValidator (contracting / barrier / expanding)
        combination.py      # WXY / WXYXZ combination correctives
        double_zigzag.py    # DoubleZigZagValidator
        full_cycle.py       # FullCycleValidator (motive + corrective)

    scoring/
        __init__.py
        ratios.py           # retracement, extension, band_score, fib_score
        geometry.py         # channel lines, wedge shape, alternation
        momentum_ctx.py     # volume score, momentum score, shallow pullback

    parser/
        __init__.py
        candidate_store.py  # CandidateStore (top-K cache per interval)
        dynamic_parser.py   # sliding window parser (kombinatorikus robbanás ellen)
        state_machine.py    # OnlineStateMachine élő adathoz

    scanners/
        __init__.py
        setup_1212.py       # 1212 scanner (refaktorált elliott_1212.detect_1212)
        wave3.py            # Wave3 active / complete scanner
        wave4.py            # Wave4 corrective scanner
        wave5.py            # Wave5 scanner
        abc.py              # ABC correction after impulse

    viz/
        __init__.py
        candle_chart.py     # Újrahasználható dark-theme japángyertya chart
        wave_plot.py        # Pivot annotation, összekötő vonalak, zónák
        multi_wave.py       # Több jelölt egyszerre, score-alapú színezés

    backtest/
        __init__.py
        labels.py           # Wave3 / Wave5 / ABC setup label generátor
        evaluator.py        # win rate, R, drawdown metrikák
        param_sweep.py      # pivot threshold × fib_tol × score sweep
        walkforward.py      # out-of-sample walk-forward kiértékelő
```

### Megjegyzés a jelenlegi draft anyagokhoz

| Jelenlegi fájl | Sors az új struktúrában |
|---|---|
| `src/analysis/elliott_1212.py` | `detect_pivots` → `pivots/zigzag.py`; `_atr14` → `indicators/atr.py`; `detect_1212` → `scanners/setup_1212.py`; `load_ohlcv`, `resample_ohlcv` → `data.py` |
| `scripts/plot_1212_example.py` | Viz logika → `viz/candle_chart.py` + `viz/wave_plot.py`; script marad vékony wrapper |
| `scripts/scan_1212.py` | Script marad, de importál az új modulokból |

---

## Skálázás: multi-timeframe paraméterzés

Az `ElliottConfig`-ban minden pivot és threshold paraméter timeframe-hez kötött presetek
formájában kerül meghatározásra. A `resample_ohlcv` függvény az 1m forrásból buildel
bármilyen TF-et.

```python
@dataclass
class ElliottConfig:
    # Timeframe (pl. "1m", "5m", "15m", "1h")
    timeframe: str = "1m"
    candle_seconds: int = 60       # automatikus számítás timeframe-ből

    # Pivot paraméterek — timeframe-arányos presettel
    fractal_left: int = 3
    fractal_right: int = 3
    zigzag_threshold: float = 0.010   # 1m → 0.005; 5m → 0.010; 15m → 0.015

    min_reversal_atr: float = 0.75    # ATR-szorzó — TF-független, mert ATR skálázódik

    # Toleranciák
    fib_tol: float = 0.10
    overlap_atr: float = 0.10
    eps_atr: float = 0.05
    shortest_tol: float = 0.03

    # Pattern engedélyek
    allow_truncation: bool = True
    allow_expanding_triangle: bool = False
    allow_running_flat: bool = True

    # Parser
    min_score: float = 55.0
    emit_score: float = 70.0
    top_k_per_interval: int = 5
    max_window_pivots: int = 20

    @classmethod
    def for_timeframe(cls, tf: str) -> "ElliottConfig":
        """Ajánlott preset per timeframe."""
        presets = {
            "1m":  dict(zigzag_threshold=0.005, fractal_left=2, fractal_right=2),
            "5m":  dict(zigzag_threshold=0.010, fractal_left=3, fractal_right=3),
            "15m": dict(zigzag_threshold=0.015, fractal_left=3, fractal_right=3),
            "1h":  dict(zigzag_threshold=0.025, fractal_left=5, fractal_right=5),
        }
        return cls(timeframe=tf, **presets.get(tf, {}))
```

A timeframe-hez igazodó kulcs döntés: **a ZigZag threshold és az ATR-multiplikátor
határozza meg, mit lát az algoritmus hullámnak**. 1m-en kis threshold, 1h-n nagy.
Az ATR önmagában skálázódik, ezért az ATR-alapú küszöbök (`min_reversal_atr`)
timeframe-függetlenek maradhatnak.

---

## Mérföldkövek és feladatok

---

### M1 — Pivot motor és adatmodellek

**Cél:** Stabil, tesztelt pivot réteg, amelyre az összes többi modul épít.

| # | Feladat | Modul | Megjegyzés |
|---|---------|-------|-----------|
| 1.1 | `Candle`, `Pivot`, `WaveSegment`, `PatternCandidate` dataclass-ok | `data.py` | `Pivot.y(direction)` transzformációval |
| 1.2 | `ElliottConfig` + `for_timeframe()` preset | `config.py` | Lásd fent |
| 1.3 | `load_ohlcv`, `resample_ohlcv` áthelyezés | `data.py` | Forrás: `elliott_1212.py` |
| 1.4 | `_atr14` → `atr.py` (Wilder ATR) | `indicators/atr.py` | Forrás: `elliott_1212._atr14` |
| 1.5 | EMA, range expansion, volume helpers | `indicators/momentum.py` | Wave 3 trigger-hez kell |
| 1.6 | ZigZag pivot motor refaktorálása | `pivots/zigzag.py` | Forrás: `detect_pivots` az 1212-ből; `conf_bar` megmarad |
| 1.7 | Williams Fractal pivot motor | `pivots/fractal.py` | L/R + min_reversal_atr szűrő; `confirmed_idx = i + R` |
| 1.8 | Multi-degree pivot builder | `pivots/multi_degree.py` | degree_0/1/2; erősebb szűrő = magasabb degree |
| 1.9 | Unit tesztek: pivot alternation, conf_bar, ATR szűrés | `tests/` | |

**Elfogadási kritérium:** `pivots.detect_zigzag(df_1m, cfg)` visszaad alternáló,
megerősített Pivot listát; bármely TF-en működik resampled DF-ből.

---

### M2 — Alap validator framework + Impulse

**Cél:** Az impulse 1-2-3-4-5 validator teljesen működik hard rule + soft score szinttel.

| # | Feladat | Modul | Megjegyzés |
|---|---------|-------|-----------|
| 2.1 | `ValidationResult`, `PatternValidator` ABC | `validators/base.py` | `valid`, `pattern_type`, `score`, `diagnostics`, `subpatterns` |
| 2.2 | `retracement`, `extension`, `band_score`, `fib_score` | `scoring/ratios.py` | Spec §2.2 és §12.5 alapján |
| 2.3 | `channel_score`, `alternation_score`, `wedge_geometry_score` | `scoring/geometry.py` | Channel line számítás Wave 2/4 alapján |
| 2.4 | `volume_score`, `momentum_score`, `shallow_pullback_score` | `scoring/momentum_ctx.py` | Opcionális komponensek; alapból 0.0 ha nincs adat |
| 2.5 | `ImpulseValidator` hard rule-ok + soft scoring | `validators/impulse.py` | Spec §3.1, §12.7 alapján; direction transzformáció |
| 2.6 | `DiagonalValidator` (ending + leading) | `validators/diagonal.py` | Spec §7; overlap elvárás, wedge geometry |
| 2.7 | Unit tesztek: hard rule pass/fail esetek, score range | `tests/` | Szintetikus pivot listákkal |

**Elfogadási kritérium:** `ImpulseValidator` visszaadja a spec §17.1 összes hard rule-ját
helyesen; score 0–100 tartományban van; truncation penalty működik.

---

### M3 — Korrekciós pattern validátorok

**Cél:** Zigzag, Flat, Triangle, WXY kombinációk detektálhatók.

| # | Feladat | Modul | Megjegyzés |
|---|---------|-------|-----------|
| 3.1 | `ZigZagValidator` (5-3-5, ABC) | `validators/zigzag_abc.py` | Spec §8.1, §12.8; internal substructure score |
| 3.2 | `FlatValidator` (regular / expanded / running) | `validators/flat.py` | Spec §8.2; 3 altípus automatikus felismerése |
| 3.3 | `TriangleValidator` (contracting / barrier / expanding) | `validators/triangle.py` | Spec §8.3; geometry score, 33333 internal |
| 3.4 | `DoubleZigZagValidator` (W-X-Y) | `validators/double_zigzag.py` | Spec §8.4; iterál lehetséges split pontokon |
| 3.5 | `CombinationValidator` (W-X-Y, W-X-Y-X-Z) | `validators/combination.py` | Spec §8.5; sideways shape bonus |
| 3.6 | `validate_any_corrective` segédfüggvény | `validators/__init__.py` | Iterál az összes corrective validátoron, best-et adja vissza |
| 3.7 | Unit tesztek: minden altípus 1-2 pass + 1-2 fail esettel | `tests/` | |

**Elfogadási kritérium:** Minden validator csak `valid=True`-t ad, ha a spec hard rule-ok
teljesülnek; a 3 flat altípus automatikusan azonosítódik a B retrace arány alapján.

---

### M4 — Full Cycle + Scanner réteg

**Cél:** Teljes 1-2-3-4-5-A-B-C ciklus detektálható; 1212 scanner az új architektúrában fut.

| # | Feladat | Modul | Megjegyzés |
|---|---------|-------|-----------|
| 4.1 | `FullCycleValidator` (motive + corrective kompozíció) | `validators/full_cycle.py` | Spec §9, §12.9; best motive × best corrective |
| 4.2 | `CandidateStore` (top-K cache) | `parser/candidate_store.py` | Spec §13.2; key = (start_idx, end_idx, dir, degree) |
| 4.3 | `DynamicParser` (sliding window) | `parser/dynamic_parser.py` | Spec §13.3; max_window_pivots limit a kombinatorikus robbanás ellen |
| 4.4 | `setup_1212` scanner (refaktorált 1212 detektor) | `scanners/setup_1212.py` | Az `elliott_1212.detect_1212` logika az új Pivot objektumokra átírva |
| 4.5 | `Wave3Scanner` (active + complete) | `scanners/wave3.py` | Spec §4; trigger: close > P1 + buffer |
| 4.6 | `Wave4Scanner` (zigzag/flat/triangle felismerése) | `scanners/wave4.py` | Spec §5; corrective pattern matcher P3→P4 intervallumra |
| 4.7 | `Wave5Scanner` (normal / ending diagonal / truncated) | `scanners/wave5.py` | Spec §6 |
| 4.8 | `ABCScanner` (impulse utáni korrekció) | `scanners/abc.py` | Spec §8 |
| 4.9 | Integrációs teszt SOLUSDT 1m → 5m adaton | `tests/` | |

**Elfogadási kritérium:** `DynamicParser(pivots, cfg)` visszaad `PatternCandidate` listát;
a legfrissebb 1212 setup egyezik az `elliott_1212.detect_1212` outputjával.

---

### M5 — Vizualizáció

**Cél:** Bármely detektált minta és teljes ciklus plotolható SOLUSDT chartán.

| # | Feladat | Modul | Megjegyzés |
|---|---------|-------|-----------|
| 5.1 | `CandleChart` osztály (dark theme, újrahasználható) | `viz/candle_chart.py` | Forrás: `plot_1212_example.py` candlestick logikája; `add_candles(df)` metódus |
| 5.2 | `WavePlot`: pivot annotáció, összekötő vonalak, zónák | `viz/wave_plot.py` | `add_pivots(pivots, labels)`, `add_confirmation_zone()`, `add_trigger_line()` |
| 5.3 | `MultiWavePlot`: több jelölt egyszerre | `viz/multi_wave.py` | Score-alapú színezés (zöld = erős, sárga = gyenge); overlapping candidates |
| 5.4 | Channel lines overlay | `viz/wave_plot.py` | Wave 2/4 és Wave 3/5 csatorna vonalak |
| 5.5 | `scripts/plot_elliott.py` — generikus plot script | `scripts/` | CLI: `--tf 5m --pattern IMPULSE --top 3` |
| 5.6 | A meglévő `plot_1212_example.py` migrációja | `scripts/` | Az új viz modulokat hívja |

**Elfogadási kritérium:** `plot_elliott.py --tf 5m --pattern IMPULSE --top 3` kimenetel
mentéssel és képernyőn megjelenítéssel; vizuálisan megkülönböztethető multi-score display.

---

### M6 — Online State Machine

**Cél:** Élő 1m bar feed-en futtatható állapotgép, amely valós időben trackeli
a wave formálódást és invalidációkat.

| # | Feladat | Modul | Megjegyzés |
|---|---------|-------|-----------|
| 6.1 | `OnlineStateMachine` state definíciók | `parser/state_machine.py` | Spec §14: IDLE → WAVE2 → WAVE3_ACTIVE → WAVE3_DONE → WAVE4 → WAVE5 → ABC → FULL_CYCLE |
| 6.2 | `update(candle)` metódus: bar-onként hív | `parser/state_machine.py` | Pivot megerősítés figyelése, trigger vizsgálat |
| 6.3 | Invalidáció tracking: `invalidation_level` per state | `parser/state_machine.py` | Ha ár töri a stop szintet → reset megfelelő state-re |
| 6.4 | Emit: `PatternCandidate` kibocsátása state transition-nél | `parser/state_machine.py` | `on_pattern(callback)` hook |
| 6.5 | Integráció `Wave3Scanner`-rel és `Wave4Scanner`-rel | `parser/state_machine.py` | State machine a scanner eredményeit validálja |

**Elfogadási kritérium:** Visszajátszott 1m adaton az SM ugyanazokat a pattern timestamp-eket
adja vissza, mint az offline `DynamicParser`, `confirmed_at` késleltetéssel.

---

### M7 — Backtest és kalibráció

**Cél:** Paraméterhalmaz optimalizálás, out-of-sample validáció, multi-TF összehasonlítás.

| # | Feladat | Modul | Megjegyzés |
|---|---------|-------|-----------|
| 7.1 | `LabelGenerator`: Wave3/5/ABC setup kimenetel labeling | `backtest/labels.py` | Spec §16; entry/stop/target meghatározás per setup |
| 7.2 | `BacktestEvaluator`: win rate, R-többszörös, max drawdown | `backtest/evaluator.py` | `evaluate(setups_df, ohlcv_df)` |
| 7.3 | `ParamSweep`: `zigzag_threshold × fib_tol × min_score` grid | `backtest/param_sweep.py` | Minden TF-en külön futtatva |
| 7.4 | `WalkForwardEvaluator`: train/test window rolling | `backtest/walkforward.py` | Repaint check: csak `confirmed_at` utáni bar-okon belépés |
| 7.5 | Multi-TF összehasonlító riport script | `scripts/backtest_elliott.py` | CLI output: TF × threshold × pattern típus × win rate tábla |

**Elfogadási kritérium:** `WalkForwardEvaluator` nem tartalmaz lookahead; a `confirmed_at`
barrier kötelező; 1m és 5m eredmények összehasonlíthatók egységes metrikával.

---

## Implementációs sorrend (ajánlott)

```
M1 (pivot + adat)
    ↓
M2 (impulse validator + scoring)
    ↓
M3 (corrective validátorok)
    ↓
M4 (full cycle + parser + 1212 scanner migráció)
    ↓
M5 (viz — párhuzamosítható M4-gyel)
    ↓
M6 (online SM — M4 után)
    ↓
M7 (backtest — M4 + M5 után)
```

M5 vizualizáció legalább részben párhuzamosítható M4-gyel: mihelyt az impulse validator
kész (M2 vége), már plotolható néhány kézi pivot lista.

---

## Kulcsdöntések és indoklás

### 1. Jelölt-alapú rendszer, nem binary detektor

A spec §0 elvét követve: `hard_rule_pass = True/False`, `confidence_score = 0–100`.
Több lehetséges számozás megtartható, a backtest dönti el, melyik paraméterhalmaz működik.

### 2. ZigZag pivot az alap, Fractal opcionális

A ZigZag threshold motor (meglévő `detect_pivots`) konfirmált, visszafestés-mentes.
A Williams Fractal `L/R` gyertyás motor alternatív degree_0 forrás, de lassabb konfirmáció.
Mindkettő megmarad; a konfig mondja meg, melyiket használja a rendszer.

### 3. `direction` transzformáció a validátorokon belül

Spec §2 elvét követve: minden validator `y = direction * price` koordinátában dolgozik.
Így az összes hard rule `<` / `>` reláció egyszer írható meg, bearish és bullish irányra egységes.

### 4. Multi-degree pivotok fraktál-validációhoz

A spec §1.2 szerint legalább két fokozat kell. A `multi_degree.py` builder degree_0 és
degree_1 pivot listát épít ugyanabból az OHLCV-ből. A Wave 3 belső validáció
(spec §4.3 A jelű eset) degree_0 pivotokat kap bemenetként, a külső impulse degree_1-et.

### 5. `confirmed_at` barrier — repaint prevention

Minden pivot és PatternCandidate tartalmaz `confirmed_at` indexet. A backtest és az SM
kizárólag ezt a mezőt használja belépési időpontként. Ez biztosítja, hogy az offline
parser és az online SM azonos viselkedésű legyen.

### 6. TF-független ATR-szorzók

A `min_reversal_atr` és az `eps_atr` ATR-szorzóként van megadva (nem fix pip értékként).
Az ATR értéke timeframe-arányosan változik (kb. `sqrt(TF_szorzó)` skálán), így ugyanaz a
szorzó 1m-en és 15m-en is ésszerű abszolút értékeket ad.

### 7. 1212 scanner visszafelé kompatibilis marad

A `scanners/setup_1212.py` a meglévő `detect_1212` logikát az új Pivot dataclass-okra
írja át, de azonos kimeneti formátumot tart (DF, ugyanazok az oszlopok).
A meglévő `scripts/scan_1212.py` és `scripts/plot_1212_example.py` minimális
módosítással importálhat az új helyről.

---

## Döntött kérdések

| # | Kérdés | Döntés |
|---|--------|--------|
| 1 | Substructure rekurzió mélysége | **2 fokozat** (degree_0 belső + degree_1 külső) |
| 2 | Online SM Wave3 trigger megerősítés | **2 egymást követő close** > P1 + buffer szükséges |
| 3 | Backtest label granularitás | **Külön modell** Wave3 / Wave5 / ABC setup-onként |
| 4 | Viz réteg | **Csak matplotlib/seaborn** script szintű plotok; Streamlit integráció nem kell |
