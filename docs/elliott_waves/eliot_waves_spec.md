# Elliott Wave algoritmikus specifikáció v1

**Cél:** 1m OHLCV adatokból olyan moduláris Python-rendszer építése, amely nemcsak az **1-2-1-2** setupot, hanem a **3-4-5**, majd az **A-B-C / komplex korrekciós** szakaszokat és a **teljes 1-2-3-4-5-A-B-C ciklust** is detektálni tudja.

Az Elliott-rendszerben a legfontosabb különbség: vannak **kemény szabályok**, amelyeket egy algoritmusnak kötelezően validálnia kell, és vannak **guideline-ok**, például Fibonacci-arányok, alternation, channeling, volume/momentum, amelyeket pontozásra kell használni. A motive hullámok öt hullámúak, a corrective hullámok három hullámúak vagy három hullámú minták kombinációi; egy teljes ciklus 5 hullámos motive szakaszból és 3 hullámos korrekcióból áll. ([Elliott Wave International][1])

---

## 0. A teljes rendszer alapelve

A rendszer ne egyetlen „helyes Elliott-countot” próbáljon kikényszeríteni, hanem **jelölteket** generáljon és pontozzon.

```text
1. 1m OHLCV → megerősített pivotok
2. pivotok → mono-wave szegmensek
3. szegmensek → kis minták: 1-2, ABC, zigzag, flat, triangle
4. kis minták → motive / corrective jelöltek
5. motive + corrective → teljes ciklusjelöltek
6. minden jelölt kap:
   - hard_rule_pass: True/False
   - pattern_type
   - confidence_score: 0–100
   - invalidation_level
   - target_zones
   - detected_at / confirmed_at
```

A fő algoritmikus döntés: **nem binary “ez Elliott / nem Elliott”**, hanem:

```text
valid_candidate = hard_rules_passed
ranking = soft_guidelines_score
```

Így a rendszer képes több lehetséges számozást megtartani, majd backtesttel kiválasztani, melyik működik az adott piacon, sessionben, volatilitási rezsimben.

---

# 1. Pivot- és mono-wave réteg

## 1.1. Pivot definíció

Elliott-algoritmushoz nem szabad nyers gyertyákon közvetlenül hullámot keresni. Először objektív swing-pontokat kell képezni.

**Ajánlott hibrid pivot:**

```text
pivot_high[i] akkor valid, ha:
    high[i] lokális maximum L bal és R jobb gyertyán belül
    ÉS az i utáni ellenirányú elmozdulás >= min_reversal

pivot_low[i] akkor valid, ha:
    low[i] lokális minimum L bal és R jobb gyertyán belül
    ÉS az i utáni ellenirányú elmozdulás >= min_reversal
```

A Williams Fractal klasszikus formája 5 gyertyás mintát használ, ahol a középső gyertya a legmagasabb high vagy legalacsonyabb low; ez jól használható objektív lokális pivot-definícióként, de késleltetett, mert a pivot csak a jobb oldali gyertyák után ismert. ([TradingView][2])

**1m-re induló paraméter:**

```python
L = 2 vagy 3
R = 2 vagy 3
min_reversal = max(
    0.5 * ATR_14,
    0.0005 * price,      # instrumentenként állítható
    2 * tick_size
)
```

A ZigZag típusú pivot motor hasznosabb magasabb fokozathoz, mert százalékos vagy amplitúdó-küszöb alapján kiszűri a kisebb zajt, de az utolsó szakasza nem determinisztikus, ezért backtestben csak megerősített pivotot szabad használni. ([Stock Indicators for Python][3])

## 1.2. Több fokozatú pivot

A teljes Elliott-elmélet fraktális, ezért legalább két pivot-fokozat kell.

```text
degree_0 = micro pivot
    1m gyertyákon
    L/R = 2/2 vagy 3/3
    min_reversal = 0.5–1.0 ATR

degree_1 = trade pivot
    1m-en, de erősebb szűrővel
    L/R = 5/5 vagy 8/8
    min_reversal = 1.5–3.0 ATR

degree_2 = macro intraday pivot
    5m/10m aggregált gyertyákon vagy 1m-en 3–5 ATR küszöbbel
```

A magasabb fokozatú 1-2-3-4-5 szerkezetet degree_1 vagy degree_2 pivotokon érdemes keresni, az alhullám-validációt pedig degree_0 pivotokon.

---

# 2. Koordináta-rendszer és segédfüggvények

Az összes szabályt érdemes egy irányfüggetlen koordinátában megírni.

Bullish trendnél:

```python
y = price
direction = +1
```

Bearish trendnél:

```python
y = -price
direction = -1
```

Így az algoritmus mindig „felfelé menő” mintát validál:

```text
bullish impulse: low-high-low-high-low-high
bearish impulse: high-low-high-low-high-low
```

A transzformáció után mindkettő ugyanaz:

```text
P0 < P1 > P2 < P3 > P4 < P5
```

## 2.1. Hullámhosszak

```python
W1 = P1.y - P0.y
W2 = P1.y - P2.y
W3 = P3.y - P2.y
W4 = P3.y - P4.y
W5 = P5.y - P4.y
```

Minden `W` pozitív szám legyen a validált irány szerint.

## 2.2. Fibonacci segédek

Elliott Wave International szerint a Fibonacci-arányok a Wave Principle matematikai alapját adják; a korrekciók gyakran 38%, 50% vagy 62% körüli retracementet mutatnak, az azonos fokozatú impulzív hullámok pedig szintén Fibonacci-kapcsolatban állhatnak. ([Elliott Wave International][4])

Algoritmikus tolerance:

```python
FIB_LEVELS = [0.236, 0.382, 0.500, 0.618, 0.786, 0.854, 1.000, 1.236, 1.382, 1.618, 2.618, 4.236]

fib_tol_abs = 0.05      # szigorú
fib_tol_abs = 0.10      # praktikus
fib_tol_abs = 0.15      # laza, több jelölt
```

Scoring helper:

```python
def fib_score(value, ideal_levels, tolerance=0.10):
    d = min(abs(value - level) for level in ideal_levels)
    return max(0.0, 1.0 - d / tolerance)
```

## 2.3. Ár- és overlap-tolerancia

```python
eps = max(
    2 * tick_size,
    0.05 * ATR_14_at_pivot
)

overlap_eps = max(
    2 * tick_size,
    0.10 * ATR_14_at_pivot
)
```

Ezt minden „>`/`<` hard rule-nál használni kell, különösen 1m adaton.

---

# 3. Motive hullámok: 1-2-3-4-5

Az Elliott-elméletben a motive hullám két fő típusa: **impulse** és **diagonal**. Motive hullámon belül Wave 2 nem retrace-elheti Wave 1-et 100%-nál mélyebben, Wave 4 nem retrace-elheti Wave 3-at 100%-nál mélyebben, Wave 3 túl kell menjen Wave 1 végén, és Wave 3 nem lehet a legrövidebb az 1, 3, 5 közül. ([Elliott Wave International][1])

---

## 3.1. Normál impulse modul

### Bemenet

```text
P0, P1, P2, P3, P4, P5
```

Bullish transzformált alak:

```text
P0 low
P1 high = Wave 1 vége
P2 low  = Wave 2 vége
P3 high = Wave 3 vége
P4 low  = Wave 4 vége
P5 high = Wave 5 vége
```

### Hard rule-ok

| Szabály                                               | Feltétel                                    |
| ----------------------------------------------------- | ------------------------------------------- |
| Sorrend                                               | `P0 < P1 > P2 < P3 > P4 < P5`               |
| Wave 2 nem törölheti Wave 1-et                        | `P2.y > P0.y + eps`                         |
| Wave 3 túlmegy Wave 1 végén                           | `P3.y > P1.y + eps`                         |
| Wave 4 nem retrace-elheti teljes Wave 3-at            | `P4.y > P2.y + eps`                         |
| Normál impulse-ban Wave 4 nem overlapelheti Wave 1-et | `P4.y > P1.y - overlap_eps`                 |
| Wave 3 nem lehet a legrövidebb                        | `not (W3 < W1*(1-tol) and W3 < W5*(1-tol))` |
| Normál Wave 5 új csúcsot/aljat üt                     | `P5.y > P3.y + eps`                         |

Az impulse alapdefiníció szerint öt hullámú 5-3-5-3-5 szerkezet, a Wave 4 normál esetben nem lép be Wave 1 árterületére, az 1/3/5 actionary subwave-ek motive jellegűek, és Wave 3 mindig impulse. ([Elliott Wave International][5])

### Soft guideline-ok

| Elem            |                                         Ajánlott zóna | Pontozás         |
| --------------- | ----------------------------------------------------: | ---------------- |
| Wave 2 retrace  |                   `0.382–0.786`, ideális `0.50–0.618` | magasabb score   |
| Wave 3 / Wave 1 |                    `1.0–2.618`, ideális `1.382–1.618` | trenderejű score |
| Wave 4 retrace  |                   `0.146–0.50`, ideális `0.236–0.382` | Wave 4 score     |
| Wave 5 / Wave 1 | `0.618–1.618`, ha Wave 3 extended, gyakran egyszerűbb | Wave 5 score     |
| Alternation     |         ha Wave 2 sharp, Wave 4 sideways; és fordítva | structural score |
| Channeling      |                       Wave 4/5 channel target zónában | channel score    |

Az alternation guideline szerint, ha Wave 2 sharp korrekció, Wave 4 gyakran sideways, és fordítva; a channeling guideline pedig kifejezetten Wave 4 és Wave 5 célzónák becslésére hasznos. ([Elliott Wave International][6])

### Impulse validator pszeudó

```python
def validate_impulse(P, cfg):
    # P = [P0, P1, P2, P3, P4, P5] már direction szerint transzformálva
    P0, P1, P2, P3, P4, P5 = P
    eps = cfg.eps(P)

    if not (P0.y < P1.y and P2.y < P1.y and P2.y < P3.y and P4.y < P3.y and P4.y < P5.y):
        return fail("wrong alternating structure")

    W1 = P1.y - P0.y
    W2 = P1.y - P2.y
    W3 = P3.y - P2.y
    W4 = P3.y - P4.y
    W5 = P5.y - P4.y

    if P2.y <= P0.y + eps:
        return fail("wave2 retraced more than 100% of wave1")

    if P3.y <= P1.y + eps:
        return fail("wave3 did not exceed wave1 end")

    if P4.y <= P2.y + eps:
        return fail("wave4 retraced more than 100% of wave3")

    if P4.y <= P1.y - cfg.overlap_eps(P4):
        return fail("wave4 overlaps wave1 territory in normal impulse")

    if W3 < W1 * (1 - cfg.shortest_tol) and W3 < W5 * (1 - cfg.shortest_tol):
        return fail("wave3 is shortest actionary wave")

    if P5.y <= P3.y + eps:
        if not cfg.allow_truncation:
            return fail("wave5 did not exceed wave3")
        truncation_penalty = 0.25
    else:
        truncation_penalty = 0.0

    R2 = W2 / W1
    R4 = W4 / W3
    E3 = W3 / W1
    E5 = W5 / W1

    score = weighted_score({
        "r2": band_score(R2, hard=(0.236, 0.854), ideal=[0.5, 0.618]),
        "r4": band_score(R4, hard=(0.146, 0.5), ideal=[0.236, 0.382]),
        "e3": band_score(E3, hard=(0.618, 4.236), ideal=[1.382, 1.618, 2.618]),
        "e5": band_score(E5, hard=(0.382, 2.618), ideal=[0.618, 1.0, 1.618]),
        "alternation": alternation_score(P),
        "channel": channel_score(P),
    })

    score *= (1.0 - truncation_penalty)

    return candidate("IMPULSE", P, score)
```

---

# 4. Wave 3 detektálása

A Wave 3 modul célja: az 1-2 után megtalálni, hogy valóban elindult-e a 3-as hullám, majd később eldönteni, hogy befejeződött-e.

## 4.1. Wave 3 indulási feltétel

Bemenet:

```text
P0, P1, P2 = valid 1-2
```

Bullish esetben:

```text
P1 > P0
P2 > P0
R2 = (P1 - P2) / (P1 - P0)
R2 < 1.0
```

Wave 3 **provisionally active**, ha:

```text
close > P1 + buffer
```

ahol:

```python
buffer = max(0.10 * ATR_14, 2 * tick_size)
```

Erősebb trigger:

```text
close > P1 + buffer
AND range_expansion > median_range_20
AND EMA20_slope > 0
AND close > EMA20
```

## 4.2. Wave 3 teljesülési feltétel

Wave 3 jelölt vége:

```text
P3 = megerősített pivot high P2 után
```

Hard feltételek:

```text
P3 > P1
W3 = P3 - P2
```

Fontos: **Wave 3-nak nem kell mindig hosszabbnak lennie Wave 1-nél**, de később nem lehet a legrövidebb az 1/3/5 közül. Ezért ha `W3 < W1`, akkor a későbbi Wave 5 maximális hossza korlátozott:

```python
if W3 < W1:
    max_allowed_W5 = W3 * (1 + shortest_tol)
```

Praktikus Wave 3 score:

```text
W3/W1 >= 1.000     elfogadható
W3/W1 ≈ 1.382      jó
W3/W1 ≈ 1.618      ideális
W3/W1 >= 2.618     extended wave 3
```

Az extension guideline szerint az impulse-ok többsége tartalmaz extensiont, gyakran egyetlen actionary hullámban; részvénypiacokon különösen gyakori, hogy a Wave 3 extended, és Wave 3 nem lehet a legrövidebb. ([Elliott Wave International][7])

## 4.3. Wave 3 belső validáció

Wave 3 legyen legalább az alábbiak egyike:

```text
A) degree_0 pivotokon valid 5 hullámos impulse
B) degree_0 pivotokon 1-2-1-2 után momentum breakout
C) magas momentum + kevés és sekély pullback
```

Erős Wave 3 score:

```python
wave3_score = (
    0.35 * extension_score(W3 / W1)
  + 0.25 * internal_impulse_score(P2, P3)
  + 0.20 * momentum_score(P2, P3)
  + 0.10 * volume_score(P2, P3)
  + 0.10 * shallow_pullback_score(P2, P3)
)
```

---

# 5. Wave 4 detektálása

Wave 4 a Wave 3 után jövő korrekció. Ez algoritmikusan nem egyetlen low pivot, hanem egy **korrekciós pattern**, amelynek végpontja P4.

## 5.1. Wave 4 hard rule-ok

Bemenet:

```text
P0, P1, P2, P3 = valid 1-2-3
```

Wave 4 jelölt:

```text
P3 → P4 korrekció
```

Hard feltételek normál impulse esetén:

```text
P4 > P2                  # nem retrace-elheti a teljes Wave 3-at
P4 > P1 - overlap_eps    # nem overlapelheti Wave 1 árterületét
```

Ha `P4 <= P1`, akkor három lehetőség van:

```text
1. Nem normál impulse, hanem diagonal.
2. A számozás hibás; az ár még mindig extended Wave 3-ban van.
3. Leveraged / intraday wick miatt toleranciával még elfogadható, de score-penalty kell.
```

Az Elliott Wave International kifejezetten megjegyzi, hogy ha egy látszólagos Wave 4 overlapel Wave 1-gyel, vagy Wave 3 túl rövid, a számozást gyakran át kell értelmezni extended Wave 3-ként. ([Elliott Wave International][7])

## 5.2. Wave 4 korrekciós típusok

Wave 4 lehet:

```text
zigzag
flat
triangle
combination
```

A korrekciók fő kategóriái: zigzag, flat, triangle, illetve ezek kombinációi. Fontos Elliott-szabály: egy korrekció önmagában nem lehet teljes „five”; ha az első ellenirányú mozgás öt hullámos, az csak a korrekció része, tipikusan egy zigzag A hulláma. ([Elliott Wave International][8])

## 5.3. Wave 4 retrace

```python
R4 = (P3.y - P4.y) / (P3.y - P2.y)
```

Ajánlott:

```text
hard: 0.0 < R4 < 1.0
praktikus: 0.146–0.500
ideális: 0.236–0.382
```

A Methodology for Elliott Waves Pattern Recognition című tanulmány összefoglalója Wave 4-re tipikusan 38,2% alatti Wave 3 retracementet említ, de ezt algoritmusban guideline-ként, nem abszolút szabályként kell kezelni. ([StudyRes][9])

---

# 6. Wave 5 detektálása

Wave 5 a végső trendirányú hullám az impulse-on belül.

## 6.1. Normál Wave 5

Bemenet:

```text
P0, P1, P2, P3, P4 = valid 1-2-3-4
```

Wave 5 jelölt:

```text
P4 → P5
```

Hard feltételek:

```text
P5 > P3 + eps       # normál impulse új csúcsot üt
W3 nem lehet a legrövidebb
```

Ha `P5 <= P3`, akkor ez **truncated fifth** lehet, de csak kivételként.

A truncation definíció szerint a Wave 5 nem haladja meg Wave 3 extrémumát; ez gyakran erős Wave 3 után fordul elő, és közelgő trendfordulóra figyelmeztet. ([Elliott Wave International][10])

Truncation hard feltételek:

```text
P5 > P4                     # azért felfelé hullám
P5 <= P3 + eps              # nem üt új csúcsot
Wave 5 belül 5 alhullámú vagy ending diagonal
Wave 3 erős: W3/W1 >= 1.618 ajánlott
momentum divergence opcionális, de hasznos
```

## 6.2. Wave 5 típusok

Wave 5 lehet:

```text
A) normál impulse
B) ending diagonal
C) truncated fifth
D) extended fifth
```

Ending diagonal főleg Wave 5-ben és C hullámban fordul elő, 3-3-3-3-3 belső szerkezetű, gyakran overlapel, ék alakú, és kimerülési patternként gyors fordulat követheti. ([Elliott Wave International][11])

## 6.3. Wave 5 célárak

```python
target_5_eq_1 = P4 + 1.000 * W1
target_5_0618_1 = P4 + 0.618 * W1
target_5_1618_1 = P4 + 1.618 * W1

target_5_channel = upper_channel_line_at(P5_time)
```

Channeling:

```text
1. Wave 3 után: kösd össze Wave 1 és Wave 3 végét, húzz párhuzamost Wave 2-ből → Wave 4 zóna.
2. Wave 4 után: kösd össze Wave 2 és Wave 4 végét, húzz párhuzamost Wave 3-ból → Wave 5 zóna.
```

Ezt Elliott Wave International is klasszikus impulse-channel technikaként írja le. ([Elliott Wave International][12])

---

# 7. Diagonal modul

A diagonal motive pattern, de nem impulse. Algoritmikusan külön validator kell rá.

## 7.1. Diagonal fő szabályok

Bemenet:

```text
P0, P1, P2, P3, P4, P5
```

Hard rule-ok:

```text
P2 > P0                  # Wave 2 nem törölheti Wave 1-et
P3 > P1                  # Wave 3 túlmegy Wave 1-en
P4 > P2                  # Wave 4 nem törölheti Wave 3-at
Wave 3 nem legrövidebb
P4 <= P1 + overlap_eps   # overlap elvárt / gyakori
```

A diagonal az egyetlen öt hullámos trendirányú szerkezet, amelyben Wave 4 szinte mindig belép Wave 1 árterületére, és a klasszikus ending diagonal belső szerkezete 3-3-3-3-3. ([Elliott Wave International][11])

## 7.2. Ending diagonal

Pozíció:

```text
Wave 5
C hullám
komplex korrekció utolsó C hulláma
```

Feltételek:

```text
internal_structure = 3-3-3-3-3
wedge_shape = True
overlap = True
volume/range gyakran csökken, throw-over lehetséges
```

Geometry score:

```python
upper_line = line(P1, P3)
lower_line = line(P2, P4)

contracting = distance_between_lines_at_P5 < distance_between_lines_at_P1
expanding = distance_between_lines_at_P5 > distance_between_lines_at_P1

diagonal_score = (
    0.30 * hard_structure_score
  + 0.25 * overlap_score
  + 0.20 * wedge_geometry_score
  + 0.15 * internal_33333_score
  + 0.10 * position_score
)
```

## 7.3. Leading diagonal

Pozíció:

```text
Wave 1
A hullám zigzagban
```

Belső szerkezet:

```text
3-3-3-3-3 vagy ritkábban 5-3-5-3-5
```

Elliott Wave International szerint leading diagonal előfordulhat Wave 1-ben és zigzag A hullámában; a strict belső definíció kevésbé egyértelmű, ezért ezt algoritmusban alacsonyabb priorral kell kezelni. ([Elliott Wave International][11])

---

# 8. ABC és korrekciós pattern modulok

A teljes 1-2-3-4-5 után jön az ABC korrekció. Egy bullish impulse után az ABC lefelé korrigál.

Transzformált bullish előzmény után:

```text
P5 = impulse top
A  = lefelé
B  = felfelé korrekció
C  = lefelé
```

Pivot alak:

```text
P5 high → A low → B high → C low
```

A corrective hullámok ellenirányúak a magasabb fokozat trendjéhez képest, és lehetnek zigzag, flat, triangle vagy kombinációk. ([Elliott Wave International][8])

---

## 8.1. Zigzag modul

### Szerkezet

```text
A-B-C = 5-3-5
```

A zigzag sharp correction, ahol A öt hullámos, B három hullámos, C öt hullámos. ([Elliott Wave Forecast][13])

### Bemenet

```text
Q0 = korrekció kezdete, impulse top
Q1 = A vége
Q2 = B vége
Q3 = C vége
```

Bullish impulse utáni korrekcióban:

```text
Q0 high
Q1 low
Q2 high
Q3 low
```

### Hard rule-ok

```text
Q1 < Q0
Q2 > Q1
Q2 < Q0 + eps        # B nem mehet A kezdete fölé zigzagban
Q3 < Q1 - eps        # C normál esetben túlmegy A végén
A internal = motive 5
B internal = corrective 3
C internal = motive 5
```

### Ratio guideline-ok

```python
A_len = Q0.y - Q1.y
B_len = Q2.y - Q1.y
C_len = Q2.y - Q3.y

B_retrace = B_len / A_len
C_vs_A = C_len / A_len
```

Ajánlott:

```text
B_retrace: 0.236–0.786, ideális 0.382–0.618
C_vs_A:    0.618–1.618, ideális 1.000 vagy 1.618
```

### Zigzag validator

```python
def validate_zigzag(Q, cfg):
    Q0, Q1, Q2, Q3 = Q
    eps = cfg.eps(Q)

    if not (Q1.y < Q0.y and Q2.y > Q1.y and Q3.y < Q2.y):
        return fail("wrong ABC alternation")

    if Q2.y >= Q0.y + eps:
        return fail("zigzag B exceeded start of A")

    if Q3.y >= Q1.y - eps:
        return fail("zigzag C did not exceed A end")

    A_len = Q0.y - Q1.y
    B_ret = (Q2.y - Q1.y) / A_len
    C_ext = (Q2.y - Q3.y) / A_len

    score = weighted_score({
        "b_retrace": band_score(B_ret, hard=(0.236, 0.786), ideal=[0.382, 0.500, 0.618]),
        "c_extension": band_score(C_ext, hard=(0.618, 2.618), ideal=[1.000, 1.618]),
        "internal_A": motive_substructure_score(Q0, Q1),
        "internal_B": corrective_substructure_score(Q1, Q2),
        "internal_C": motive_substructure_score(Q2, Q3),
    })

    return candidate("ZIGZAG", Q, score)
```

---

## 8.2. Flat modul

### Szerkezet

```text
A-B-C = 3-3-5
```

A flat sideways korrekció, 3-3-5 belső szerkezettel; B gyakran visszajön A kezdete közelébe, C pedig általában csak kicsivel megy túl A végén. ([Elliott Wave International][14])

### Flat típusok

```text
regular flat
expanded flat
running flat
```

## 8.2.1. Regular flat

Bullish impulse utáni korrekció:

```text
Q0 high
Q1 low = A
Q2 high = B, közel Q0-hoz
Q3 low = C, kicsivel Q1 alatt
```

Feltételek:

```python
A_len = Q0.y - Q1.y
B_retrace = (Q2.y - Q1.y) / A_len
C_vs_A = (Q2.y - Q3.y) / A_len
```

```text
A internal = corrective 3
B internal = corrective 3
C internal = motive 5

B_retrace ≈ 0.90–1.05
Q3 <= Q1 - eps
C_vs_A ≈ 0.618–1.382
```

## 8.2.2. Expanded flat

Expanded flat esetén B túlmegy A kezdetén, C pedig erősebben túlmegy A végén. ([Elliott Wave International][14])

```text
Q2 > Q0 + eps
Q3 < Q1 - eps
B_retrace ≈ 1.05–1.382, laza max 1.618
C_vs_A ≈ 1.000–2.000, ideális 1.236–1.618
```

## 8.2.3. Running flat

Running flat esetén B messze túlmegy A kezdetén, de C nem jut el A végéig; ez ritka, és csak erős trendkörnyezetben érdemes elfogadni. ([Elliott Wave International][14])

```text
Q2 > Q0 + eps
Q3 > Q1 - eps      # C nem éri el A végét
strong_trend_context = True
score_penalty = nagy
```

### Flat validator

```python
def validate_flat(Q, cfg):
    Q0, Q1, Q2, Q3 = Q
    eps = cfg.eps(Q)

    if not (Q1.y < Q0.y and Q2.y > Q1.y and Q3.y < Q2.y):
        return fail("wrong flat alternation")

    A_len = Q0.y - Q1.y
    B_ret = (Q2.y - Q1.y) / A_len
    C_ext = (Q2.y - Q3.y) / A_len

    if not is_corrective(Q0, Q1):
        return fail("flat A must be corrective 3")
    if not is_corrective(Q1, Q2):
        return fail("flat B must be corrective 3")
    if not is_motive(Q2, Q3):
        return fail("flat C must be motive 5")

    if 0.90 <= B_ret <= 1.05 and Q3.y <= Q1.y - eps:
        typ = "REGULAR_FLAT"
        score = score_regular_flat(B_ret, C_ext)

    elif B_ret > 1.05 and Q2.y > Q0.y + eps and Q3.y < Q1.y - eps:
        typ = "EXPANDED_FLAT"
        score = score_expanded_flat(B_ret, C_ext)

    elif B_ret > 1.05 and Q2.y > Q0.y + eps and Q3.y > Q1.y - eps:
        if not strong_trend_context(Q0, Q3):
            return fail("running flat requires strong trend context")
        typ = "RUNNING_FLAT"
        score = score_running_flat(B_ret, C_ext) * 0.70

    else:
        return fail("no flat subtype matched")

    return candidate(typ, Q, score)
```

---

## 8.3. Triangle modul

### Szerkezet

```text
A-B-C-D-E = 3-3-3-3-3
```

A triangle öt átfedő, 3-3-3-3-3 szerkezetű hullámból áll, A-B-C-D-E jelöléssel; típusai contracting, barrier és expanding, valamint running variation is lehet. ([Elliott Wave International][15])

### Pozíció

Triangle tipikusan itt fordul elő:

```text
Wave 4
B hullám
utolsó X hullám
korrekciós kombináció utolsó eleme
```

Triangle ritkán valódi Wave 2; ha ott látszik, gyakran komplex korrekció része. ([Elliott Wave International][15])

### Bemenet

Bullish impulse Wave 4 triangle esetén:

```text
Q0 = Wave 3 top
Q1 = A low
Q2 = B high
Q3 = C low
Q4 = D high
Q5 = E low
```

### Általános hard rule-ok

```text
Q1 < Q0
Q2 > Q1
Q3 < Q2
Q4 > Q3
Q5 < Q4

A/B/C/D/E mind corrective 3
ármozgás oldalazó / átfedő
range nem trendelhet erősen egy irányba
```

### Contracting triangle

```text
lower_lows emelkednek:
    Q3 > Q1
    Q5 >= Q3 - eps

upper_highs csökkennek:
    Q4 < Q2
```

Lineáris score:

```python
lower_line = line_through(Q1, Q3)
upper_line = line_through(Q2, Q4)

contracting = slope(lower_line) > 0 and slope(upper_line) < 0
```

### Barrier triangle

Bullish Wave 4-ben a felső oldal közel horizontális lehet, mert a következő thrust ezt az oldalt fogja áttörni.

```text
abs(Q4 - Q2) <= barrier_eps
Q3 > Q1
Q5 >= Q3 - eps
```

### Expanding triangle

Ritkább, zajosabb, alacsonyabb prior:

```text
Q4 > Q2
Q3 < Q1
range_E > range_A lehetséges
```

### Running triangle

```text
Q2 > Q0 + eps      # B túlmegy a triangle kezdete fölé
majd a pattern mégis net korrekciót ad E végén
```

### Triangle validator

```python
def validate_triangle(Q, cfg):
    Q0, Q1, Q2, Q3, Q4, Q5 = Q
    eps = cfg.eps(Q)

    if not alternating(Q, expected="down-up-down-up-down"):
        return fail("wrong triangle alternation")

    for a, b in pairs(Q[0:5], Q[1:6]):
        if not is_corrective(a, b):
            return fail("triangle subwaves must be corrective")

    contracting = (Q3.y > Q1.y + eps and Q4.y < Q2.y - eps and Q5.y >= Q3.y - cfg.triangle_e_overshoot_eps)
    barrier = (abs(Q4.y - Q2.y) <= cfg.barrier_eps and Q3.y > Q1.y + eps)
    expanding = (Q3.y < Q1.y - eps and Q4.y > Q2.y + eps)

    if contracting:
        typ = "CONTRACTING_TRIANGLE"
    elif barrier:
        typ = "BARRIER_TRIANGLE"
    elif expanding and cfg.allow_expanding_triangle:
        typ = "EXPANDING_TRIANGLE"
    else:
        return fail("no triangle subtype matched")

    score = (
        0.30 * internal_33333_score(Q)
      + 0.25 * geometry_score(Q, typ)
      + 0.20 * overlap_score(Q)
      + 0.15 * volatility_contraction_score(Q)
      + 0.10 * position_score(Q)
    )

    return candidate(typ, Q, score)
```

---

## 8.4. Double zigzag és triple zigzag

### Szerkezet

```text
Double zigzag: W-X-Y
Triple zigzag: W-X-Y-X-Z
```

Itt W, Y, Z általában zigzag, X pedig ellenirányú corrective hullám.

Elliott Wave International szerint a double/triple correction jelölésben W, Y és Z az egymást követő actionary corrective komponensek, az X hullámok pedig reactionary, corrective hullámok. ([Elliott Wave International][16])

### Double zigzag validator

```text
W = zigzag
X = corrective
Y = zigzag
Y vége a korrekció irányában továbbmegy, mint W vége
```

Bullish impulse utáni downward correction esetén:

```text
W_end < W_start
X_end > W_end
Y_end < W_end
```

```python
def validate_double_zigzag(pivots, cfg):
    # split pontokat keresünk:
    # Q0..Qa = W
    # Qa..Qb = X
    # Qb..Qc = Y
    candidates = []

    for a, b in possible_splits(pivots):
        W = validate_zigzag(pivots[0:a], cfg)
        X = validate_any_corrective(pivots[a:b], cfg)
        Y = validate_zigzag(pivots[b:], cfg)

        if W and X and Y:
            if Y.end.y < W.end.y - cfg.eps(Y.end):
                score = 0.4 * W.score + 0.2 * X.score + 0.4 * Y.score
                candidates.append(candidate("DOUBLE_ZIGZAG", pivots, score))

    return best(candidates)
```

---

## 8.5. Combination modul

### Szerkezet

```text
Double three: W-X-Y
Triple three: W-X-Y-X-Z
```

W/Y/Z lehet:

```text
zigzag
flat
triangle csak jellemzően végső komponensként
```

A combination egyszerű korrekciós mintákból épül fel, például zigzag, flat és triangle; Elliott szerint a double three oldalazó jellegű kombináció. ([Elliott Wave International][17])

### Combination hard logic

```text
W = zigzag vagy flat
X = corrective
Y = zigzag vagy flat vagy triangle
ha Z van: Z lehet zigzag/flat/triangle
triangle csak utolsó komponensként kapjon magas score-t
overall shape = inkább oldalazó, nem erősen trendelő
```

### Combination validator

```python
def validate_combination(pivots, cfg):
    results = []

    for split1, split2 in possible_two_splits(pivots):
        W_p = pivots[:split1]
        X_p = pivots[split1:split2]
        Y_p = pivots[split2:]

        W = validate_zigzag(W_p, cfg) or validate_flat(W_p, cfg)
        X = validate_any_corrective(X_p, cfg)
        Y = (
            validate_zigzag(Y_p, cfg)
            or validate_flat(Y_p, cfg)
            or validate_triangle(Y_p, cfg)
        )

        if W and X and Y:
            sideways = sideways_score(pivots)
            complexity_penalty = 0.90
            score = complexity_penalty * (
                0.35 * W.score + 0.20 * X.score + 0.35 * Y.score + 0.10 * sideways
            )
            results.append(candidate("COMBINATION_WXY", pivots, score))

    return best(results)
```

---

# 9. Teljes 1-2-3-4-5-A-B-C ciklus

## 9.1. Egyszerű teljes ciklus

Bullish teljes ciklus major pivotokon:

```text
P0 low
P1 high = 1
P2 low  = 2
P3 high = 3
P4 low  = 4
P5 high = 5
P6 low  = A
P7 high = B
P8 low  = C
```

Validáció:

```python
motive = validate_impulse(P0..P5) or validate_diagonal(P0..P5)
correction = (
    validate_zigzag(P5..P8)
    or validate_flat(P5..P8)
    or validate_triangle(P5..?)
    or validate_double_zigzag(...)
    or validate_combination(...)
)

full_cycle = motive.valid and correction.valid
```

Elliott Wave International szerint az egész ciklus nyolc hullámból áll: öt hullámos motive phase számozott 1–5-tel, majd három hullámos corrective phase A-B-C betűkkel. ([Elliott Wave International][8])

## 9.2. Teljes ciklus hard rule-ok

```text
1. Motive szakasz hard rule pass.
2. Correction szakasz valamelyik corrective validatorral pass.
3. ABC nem lehet önmagában csak egy 5 hullámos ellenirányú mozgás.
4. C / teljes korrekció vége nem törheti automatikusan az egész impulse P0-ját, de ha töri, akkor magasabb fokozaton valószínűleg trendforduló / nagyobb korrekció.
5. Ha correction túl kicsi: lehet csak Wave A vagy Wave 4 alacsonyabb fokozaton.
6. Ha correction túl komplex: WXY / WXYXZ parserre kell átadni.
```

## 9.3. Teljes ciklus célzónák

Bullish impulse után:

```python
impulse_len = P5.y - P0.y
correction_depth = (P5.y - C.y) / impulse_len
```

Guideline zónák:

```text
shallow correction: 0.236–0.382
normal correction:  0.382–0.618
deep correction:    0.618–0.786
invalid / nagyobb trendveszély: C <= P0
```

Gyakorlati guideline: a korrekció gyakran visszatér az előző, kisebb fokozatú Wave 4 tartományába; ezt Elliott Wave Forecast is gyakori support/resistance zónaként említi. ([Elliott Wave Forecast][13])

---

# 10. Pattern grammar: teljes Elliott-parser

A teljes elmélethez érdemes grammar-alapú parserben gondolkodni.

```text
CYCLE
    = MOTIVE + CORRECTIVE

MOTIVE
    = IMPULSE
    | DIAGONAL

IMPULSE
    = MOTIVE(1) + CORRECTIVE(2) + IMPULSE(3) + CORRECTIVE(4) + MOTIVE(5)

DIAGONAL
    = CORRECTIVE(1) + CORRECTIVE(2) + CORRECTIVE(3) + CORRECTIVE(4) + CORRECTIVE(5)
    # ending diagonal 3-3-3-3-3
    # leading diagonal opcionálisan 5-3-5-3-5 is lehet

CORRECTIVE
    = ZIGZAG
    | FLAT
    | TRIANGLE
    | DOUBLE_ZIGZAG
    | COMBINATION

ZIGZAG
    = MOTIVE(A) + CORRECTIVE(B) + MOTIVE(C)

FLAT
    = CORRECTIVE(A) + CORRECTIVE(B) + MOTIVE(C)

TRIANGLE
    = CORRECTIVE(A) + CORRECTIVE(B) + CORRECTIVE(C) + CORRECTIVE(D) + CORRECTIVE(E)

DOUBLE_ZIGZAG
    = ZIGZAG(W) + CORRECTIVE(X) + ZIGZAG(Y)

COMBINATION
    = SIMPLE_CORRECTION(W) + CORRECTIVE(X) + SIMPLE_CORRECTION(Y)
```

Ez azért fontos, mert az Elliott-fraktalitás miatt egy Wave 3 például maga is egy teljes kisebb fokozatú impulse. Az Investopedia is kiemeli, hogy a hullámok kisebb, önhasonló mintákat tartalmaznak, és különböző időskálákon jelenhetnek meg. ([Investopedia][18])

---

# 11. Scoring rendszer

Minden validator ezt adja vissza:

```python
@dataclass
class PatternCandidate:
    pattern_type: str
    start_idx: int
    end_idx: int
    pivots: list[Pivot]
    direction: int
    hard_pass: bool
    score: float
    subpatterns: list["PatternCandidate"]
    invalidation_level: float | None
    target_zones: dict[str, float]
    diagnostics: dict[str, float | str]
```

## 11.1. Általános pontozás

```text
score = 
    35% hard-structure clarity
  + 20% Fibonacci relationships
  + 15% internal substructure
  + 10% alternation / pattern position
  + 10% channel / geometry
  + 10% momentum / volatility / volume context
```

## 11.2. Hard rule vs soft rule

Példa impulse:

```python
if not hard_rules:
    return None

score = 0
score += 35 * structure_score
score += 20 * fib_score
score += 15 * substructure_score
score += 10 * alternation_score
score += 10 * channel_score
score += 10 * momentum_score
```

Jelzés csak:

```text
score >= 70 = erős candidate
score >= 55 = figyelőlistás candidate
score < 55  = ne trade setup, csak count lehetőség
```

A pattern-recognition irodalom is jellemzően nem abszolút wave-countot kezel, hanem felismerési/egyezési pontosságot; például Kotyrba és szerzőtársai neural-network multi-classifierrel százalékos hasonlósági küszöbökkel dolgoztak, Vantuch és társai pedig EW-detektort és gépi tanulási modelleket kombináltak trend-előrejelzéshez. ([StudyRes][9])

---

# 12. Python moduláris kódterv

## 12.1. Mappastruktúra

```text
elliott/
    __init__.py

    config.py
    data.py
    pivots.py
    monowaves.py
    ratios.py
    geometry.py
    scoring.py

    validators/
        __init__.py
        base.py
        impulse.py
        diagonal.py
        zigzag.py
        flat.py
        triangle.py
        combination.py
        full_cycle.py

    parser/
        __init__.py
        candidate_store.py
        wave_grammar.py
        dynamic_parser.py
        online_state_machine.py

    scanners/
        wave3_scanner.py
        wave4_scanner.py
        wave5_scanner.py
        abc_scanner.py

    backtest/
        labels.py
        evaluator.py
        walkforward.py

    viz/
        plot_candidates.py
```

---

## 12.2. `data.py`

```python
from dataclasses import dataclass
from enum import Enum
from typing import Optional
import pandas as pd


class PivotKind(str, Enum):
    HIGH = "HIGH"
    LOW = "LOW"


@dataclass(frozen=True)
class Candle:
    ts: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None


@dataclass(frozen=True)
class Pivot:
    idx: int
    ts: pd.Timestamp
    price: float
    kind: PivotKind
    degree: int
    confirmed_idx: int
    atr: float | None = None

    def y(self, direction: int) -> float:
        return direction * self.price


@dataclass
class WaveSegment:
    start: Pivot
    end: Pivot
    direction: int

    @property
    def length(self) -> float:
        return abs(self.end.price - self.start.price)

    @property
    def bars(self) -> int:
        return self.end.idx - self.start.idx
```

---

## 12.3. `config.py`

```python
from dataclasses import dataclass


@dataclass
class ElliottConfig:
    tick_size: float = 0.01

    # pivot
    fractal_left: int = 3
    fractal_right: int = 3
    min_reversal_atr: float = 0.75
    min_reversal_pct: float = 0.0005
    min_reversal_ticks: int = 2

    # tolerances
    fib_tol: float = 0.10
    shortest_tol: float = 0.03
    overlap_atr: float = 0.10
    eps_atr: float = 0.05

    # pattern settings
    allow_truncation: bool = True
    allow_expanding_triangle: bool = False
    allow_running_flat: bool = True

    # parser
    min_score: float = 55.0
    emit_score: float = 70.0
    top_k_per_interval: int = 5
```

---

## 12.4. `pivots.py`

```python
def detect_fractal_pivots(df, cfg, degree=0):
    """
    Confirmed pivotok:
    - high pivot: high[i] a legmagasabb L bal és R jobb gyertyán belül
    - low pivot: low[i] a legalacsonyabb L bal és R jobb gyertyán belül
    - confirmation: i + R
    - min_reversal: ATR/pct/tick alapú szűrés
    """
    pivots = []

    L = cfg.fractal_left
    R = cfg.fractal_right

    for i in range(L, len(df) - R):
        window = df.iloc[i - L : i + R + 1]

        is_high = df.high.iloc[i] >= window.high.max()
        is_low = df.low.iloc[i] <= window.low.min()

        if is_high:
            if confirmed_reversal_from_high(df, i, cfg):
                pivots.append(make_pivot(df, i, "HIGH", degree, confirmed_idx=i + R))

        if is_low:
            if confirmed_reversal_from_low(df, i, cfg):
                pivots.append(make_pivot(df, i, "LOW", degree, confirmed_idx=i + R))

    return compress_alternating_pivots(pivots)
```

---

## 12.5. `ratios.py`

```python
def retracement(numerator_move: float, base_move: float) -> float:
    if base_move <= 0:
        return float("nan")
    return numerator_move / base_move


def extension(extension_move: float, base_move: float) -> float:
    if base_move <= 0:
        return float("nan")
    return extension_move / base_move


def band_score(value: float, hard: tuple[float, float], ideal: list[float], tol: float = 0.10) -> float:
    lo, hi = hard
    if value < lo or value > hi:
        return 0.0

    d = min(abs(value - x) for x in ideal)
    return max(0.0, min(1.0, 1.0 - d / tol))
```

---

## 12.6. `validators/base.py`

```python
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ValidationResult:
    valid: bool
    pattern_type: str | None = None
    score: float = 0.0
    reason: str | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)
    subpatterns: list[Any] = field(default_factory=list)


class PatternValidator:
    pattern_type: str

    def validate(self, pivots, direction: int, cfg) -> ValidationResult:
        raise NotImplementedError
```

---

## 12.7. `validators/impulse.py`

```python
class ImpulseValidator(PatternValidator):
    pattern_type = "IMPULSE"

    def validate(self, pivots, direction, cfg):
        if len(pivots) != 6:
            return ValidationResult(False, reason="Impulse needs 6 pivots")

        y = [p.y(direction) for p in pivots]
        P0, P1, P2, P3, P4, P5 = y

        eps = eps_price(pivots, cfg)
        overlap_eps = overlap_price(pivots, cfg)

        # alternating high/low structure in transformed coordinates
        if not (P0 < P1 and P2 < P1 and P2 < P3 and P4 < P3 and P4 < P5):
            return ValidationResult(False, reason="Bad impulse alternation")

        W1 = P1 - P0
        W2 = P1 - P2
        W3 = P3 - P2
        W4 = P3 - P4
        W5 = P5 - P4

        if P2 <= P0 + eps:
            return ValidationResult(False, reason="Wave 2 invalidation")

        if P3 <= P1 + eps:
            return ValidationResult(False, reason="Wave 3 did not break Wave 1")

        if P4 <= P2 + eps:
            return ValidationResult(False, reason="Wave 4 fully retraced Wave 3")

        if P4 <= P1 - overlap_eps:
            return ValidationResult(False, reason="Wave 4 overlaps Wave 1 territory")

        if W3 < W1 * (1 - cfg.shortest_tol) and W3 < W5 * (1 - cfg.shortest_tol):
            return ValidationResult(False, reason="Wave 3 shortest")

        truncation = False
        if P5 <= P3 + eps:
            if not cfg.allow_truncation:
                return ValidationResult(False, reason="Wave 5 truncation not allowed")
            truncation = True

        R2 = W2 / W1
        R4 = W4 / W3
        E3 = W3 / W1
        E5 = W5 / W1

        score = 100 * (
            0.30 * 1.0
            + 0.20 * band_score(R2, (0.236, 0.854), [0.5, 0.618], cfg.fib_tol)
            + 0.15 * band_score(R4, (0.146, 0.5), [0.236, 0.382], cfg.fib_tol)
            + 0.15 * band_score(E3, (0.618, 4.236), [1.382, 1.618, 2.618], cfg.fib_tol)
            + 0.10 * band_score(E5, (0.382, 2.618), [0.618, 1.0, 1.618], cfg.fib_tol)
            + 0.10 * channel_score(pivots, direction, cfg)
        )

        if truncation:
            score *= 0.75

        return ValidationResult(
            valid=True,
            pattern_type="IMPULSE_TRUNCATED" if truncation else "IMPULSE",
            score=score,
            diagnostics={
                "R2": R2,
                "R4": R4,
                "E3": E3,
                "E5": E5,
                "W1": W1,
                "W3": W3,
                "W5": W5,
            },
        )
```

---

## 12.8. `validators/zigzag.py`

```python
class ZigZagValidator(PatternValidator):
    pattern_type = "ZIGZAG"

    def validate(self, pivots, direction, cfg):
        """
        direction itt a nagyobb trend iránya.
        Bullish nagyobb trend után a korrekció lefelé megy,
        ezért y=direction*price koordinátában az ABC lefelé fut.
        """
        if len(pivots) != 4:
            return ValidationResult(False, reason="Zigzag needs 4 pivots")

        y = [p.y(direction) for p in pivots]
        Q0, Q1, Q2, Q3 = y
        eps = eps_price(pivots, cfg)

        if not (Q1 < Q0 and Q2 > Q1 and Q3 < Q2):
            return ValidationResult(False, reason="Bad zigzag alternation")

        if Q2 >= Q0 + eps:
            return ValidationResult(False, reason="B exceeded start of A")

        if Q3 >= Q1 - eps:
            return ValidationResult(False, reason="C did not exceed A end")

        A = Q0 - Q1
        B_ret = (Q2 - Q1) / A
        C_ext = (Q2 - Q3) / A

        score = 100 * (
            0.35 * 1.0
            + 0.25 * band_score(B_ret, (0.236, 0.786), [0.382, 0.5, 0.618], cfg.fib_tol)
            + 0.25 * band_score(C_ext, (0.618, 2.618), [1.0, 1.618], cfg.fib_tol)
            + 0.15 * internal_535_score(pivots, direction, cfg)
        )

        return ValidationResult(
            True,
            pattern_type="ZIGZAG",
            score=score,
            diagnostics={"B_ret": B_ret, "C_ext": C_ext},
        )
```

---

## 12.9. `validators/full_cycle.py`

```python
class FullCycleValidator(PatternValidator):
    pattern_type = "FULL_CYCLE"

    def __init__(self):
        self.impulse = ImpulseValidator()
        self.diagonal = DiagonalValidator()
        self.corrections = [
            ZigZagValidator(),
            FlatValidator(),
            TriangleValidator(),
            CombinationValidator(),
        ]

    def validate(self, pivots, direction, cfg):
        results = []

        # Egyszerű 5 + ABC eset: 9 pivot
        if len(pivots) >= 9:
            motive_p = pivots[:6]
            corr_p = pivots[5:]

            motive = (
                self.impulse.validate(motive_p, direction, cfg)
                if self.impulse.validate(motive_p, direction, cfg).valid
                else self.diagonal.validate(motive_p, direction, cfg)
            )

            if not motive.valid:
                return ValidationResult(False, reason="No valid motive phase")

            corr_candidates = []
            for validator in self.corrections:
                r = validator.validate(corr_p, direction, cfg)
                if r.valid:
                    corr_candidates.append(r)

            if not corr_candidates:
                return ValidationResult(False, reason="No valid correction phase")

            corr = max(corr_candidates, key=lambda x: x.score)

            score = 0.60 * motive.score + 0.40 * corr.score

            return ValidationResult(
                True,
                pattern_type="FULL_CYCLE",
                score=score,
                subpatterns=[motive, corr],
                diagnostics={
                    "motive_type": motive.pattern_type,
                    "correction_type": corr.pattern_type,
                },
            )

        return ValidationResult(False, reason="Not enough pivots")
```

---

# 13. Parser stratégia

## 13.1. Miért kell parser?

Egy 1m adatfolyamon rengeteg pivot lesz. Ha minden lehetséges pivot-ablakra lefuttatod az impulse, zigzag, flat, triangle validátorokat, kombinatorikus robbanás jön. Ezért kell:

```text
1. degree szerint pivotokat külön kezelni
2. max pivot window limit
3. top-K candidate cache
4. dynamic programming / beam search
```

A 2023-as MDPI DL-EWP tanulmány is azt hangsúlyozza, hogy az Elliott-hullámok időtartama nem determinisztikus, ezért a modelleknek jelentős pontokat kell megtartaniuk és az idősor lépéshosszát normalizálniuk; ez nagyon közel áll a pivot-alapú reprezentációhoz. ([MDPI][19])

## 13.2. Candidate store

```python
class CandidateStore:
    def __init__(self, top_k=5):
        self.store = {}  # key = (start_idx, end_idx, direction, degree)

    def add(self, candidate):
        key = (candidate.start_idx, candidate.end_idx, candidate.direction, candidate.degree)
        self.store.setdefault(key, [])
        self.store[key].append(candidate)
        self.store[key] = sorted(self.store[key], key=lambda c: c.score, reverse=True)[:self.top_k]

    def get(self, start, end, direction, degree):
        return self.store.get((start, end, direction, degree), [])
```

## 13.3. Dynamic parser váz

```python
def parse_patterns(pivots, cfg):
    store = CandidateStore(top_k=cfg.top_k_per_interval)

    validators = [
        ImpulseValidator(),
        DiagonalValidator(),
        ZigZagValidator(),
        FlatValidator(),
        TriangleValidator(),
        CombinationValidator(),
    ]

    n = len(pivots)

    for width in range(4, min(n, cfg.max_window_pivots) + 1):
        for i in range(0, n - width + 1):
            window = pivots[i : i + width]

            for direction in (+1, -1):
                for validator in validators:
                    result = validator.validate(window, direction, cfg)
                    if result.valid and result.score >= cfg.min_score:
                        store.add(to_candidate(result, window, direction))

    # magasabb szintű full cycle kompozíció
    full_candidates = compose_full_cycles(store, pivots, cfg)

    return sorted(full_candidates, key=lambda c: c.score, reverse=True)
```

---

# 14. Online state machine: élő detektáláshoz

A teljes parser jó backtesthez és offline elemzéshez. Élő adatra kell egy state machine.

```text
STATE_IDLE
    keres valid P0-P1 1-es hullámot

STATE_WAVE2
    keres P2-t, amely nem törli P0-t és fib-zónában van

STATE_WAVE3_ACTIVE
    trigger: close > P1 + buffer
    invalid: close < P2 - buffer

STATE_WAVE3_COMPLETE
    P3 confirmed high
    keres Wave 4 corrective patternt

STATE_WAVE4
    validál zigzag/flat/triangle/combination
    invalid: P4 overlap P1 normál impulse esetén

STATE_WAVE5_ACTIVE
    trigger: close > Wave4 correction breakout
    cél: P5 > P3 vagy ending diagonal/truncation

STATE_ABC
    impulse kész, keres corrective szakaszt

STATE_FULL_CYCLE_COMPLETE
    teljes 1-5 + ABC lezárva
```

---

# 15. Modulok implementációs sorrendje

## Fázis 1 — Stabil pivot motor

```text
1. OHLCV import
2. ATR számítás
3. fractal pivot
4. ZigZag / ATR pivot
5. alternating pivot compression
6. confirmed_at mező kötelező
```

Kimenet:

```python
list[Pivot]
```

## Fázis 2 — Alap motive és 1-2-3-4-5

```text
1. ImpulseValidator
2. Wave3Scanner
3. Wave4Scanner, de először csak egyszerű P3→P4 retrace
4. Wave5Scanner
5. Full 1-5 candidate
```

## Fázis 3 — Simple corrections

```text
1. ZigZagValidator
2. FlatValidator
3. ABC correction after impulse
4. Full 1-5-ABC cycle
```

## Fázis 4 — Complex corrections

```text
1. TriangleValidator
2. DoubleZigZagValidator
3. CombinationValidator
4. WXY / WXYXZ parser
```

## Fázis 5 — Recursive grammar

```text
1. Substructure validation degree_0 pivotokon
2. degree_1 pattern → degree_0 internal validation
3. top-K candidate parser
4. full-cycle ranking
```

## Fázis 6 — Backtest és kalibráció

```text
1. pivot parameter sweep
2. fib tolerance sweep
3. score threshold sweep
4. out-of-sample validation
5. instrument/session bontás
6. latency/repaint ellenőrzés
```

---

# 16. Backtest label definíciók

## 16.1. Wave 3 setup siker

```text
entry = close > P1 + buffer, miután P2 validált
stop = P2 - buffer
target_1 = P2 + 1.000 * W1
target_2 = P2 + 1.618 * W1
target_3 = P2 + 2.618 * W1
```

Siker:

```text
target_2 elérése stop előtt
```

## 16.2. Wave 4 utáni Wave 5 siker

```text
entry = Wave4 correction breakout
stop = P4 - buffer
target = P4 + 0.618–1.000 * W1 vagy channel target
```

Siker:

```text
P5 > P3 vagy target elérése stop előtt
```

## 16.3. ABC siker

Impulse után:

```text
A valid = legalább 0.236 impulse retrace
ABC valid = zigzag/flat/triangle/combination hard pass
```

Kereskedési label például:

```text
short_after_5_success =
    price eléri 0.382 impulse retrace-et
    mielőtt új high P5 felett invalidál
```

---

# 17. Végső „definite” szabálylista

## 17.1. Impulse

```text
P0 < P1 > P2 < P3 > P4 < P5
P2 > P0
P3 > P1
P4 > P2
P4 > P1 normál impulse-ban
Wave3 nem legrövidebb W1/W3/W5 között
P5 > P3, kivéve truncation
```

## 17.2. Wave 3

```text
Előfeltétel: valid 1-2
Trigger: close > P1 + buffer
P3 valid: confirmed high, P3 > P1
Ideális: W3/W1 ≈ 1.382–1.618
Extended: W3/W1 >= 2.618
Ha W3 < W1, akkor később W5 nem lehet hosszabb W3-nál
```

## 17.3. Wave 4

```text
Előfeltétel: valid 1-2-3
P4 > P2
P4 > P1 normál impulse-ban
R4 = W4/W3 < 1.0
Ideális R4 = 0.236–0.382
Pattern: zigzag / flat / triangle / combination
Ha kezdő ellenirányú mozgás 5 hullámú, az csak A lehet, nem teljes korrekció
```

## 17.4. Wave 5

```text
Előfeltétel: valid 1-2-3-4
P5 > P3 normál esetben
Wave3 továbbra sem lehet legrövidebb
Wave5 lehet impulse vagy ending diagonal
Truncation csak erős Wave3 után, penalty score-ral
```

## 17.5. Zigzag ABC

```text
A-B-C = 5-3-5
B nem mehet A kezdete fölé/alá
C normál esetben túlmegy A végén
B retrace ideális: 0.382–0.618
C/A ideális: 1.000 vagy 1.618
```

## 17.6. Flat ABC

```text
A-B-C = 3-3-5

Regular:
    B ≈ A kezdete
    C kicsivel A vége után

Expanded:
    B túlmegy A kezdetén
    C erősen túlmegy A végén

Running:
    B túlmegy A kezdetén
    C nem éri el A végét
    ritka, csak erős trendben
```

## 17.7. Triangle

```text
A-B-C-D-E = 3-3-3-3-3
Minden alhullám corrective
Tipikus pozíció: Wave 4, B, utolsó X
Contracting: felső trendvonal le, alsó fel
Barrier: egyik oldal horizontális
Expanding: felső fel, alsó le, ritkább
E után thrust várható a nagyobb trend irányába
```

## 17.8. Combination

```text
W-X-Y vagy W-X-Y-X-Z
W/Y/Z = zigzag vagy flat; triangle jellemzően utolsó komponens
X = corrective reactionary wave
Double zigzag inkább halad a korrekció irányába
Double three inkább oldalazó
```

---

# 18. A legfontosabb gyakorlati döntés

A teljes Elliott-elmélet algoritmizálásához ne egyetlen „super validator” legyen, hanem:

```text
PivotEngine
    → MonoWaveBuilder
        → MotiveValidators
            → ImpulseValidator
            → DiagonalValidator
        → CorrectiveValidators
            → ZigZagValidator
            → FlatValidator
            → TriangleValidator
            → CombinationValidator
        → RecursiveParser
            → FullCycleValidator
        → OnlineStateMachine
        → BacktestEvaluator
```

A szabályalapú rész adja a **magyarázható Elliott-countot**, a scoring adja a **rangsorolást**, a backtest pedig eldönti, hogy 1m-en melyik paraméterhalmaz működik. A későbbi ML-réteg opcionális: a friss kutatásokban a szabály/pivot/Fibonacci reprezentációt gyakran gépi tanulási vagy deep-learning modellekkel kombinálják, például Vantuchék Random Forest/SVM megközelítése, illetve a 2023-as DL-EWP modell PLR_VIP + DBN architektúrája. ([UTP KnowledgeHub][20])

[1]: https://www.elliottwave.com/waveopedia/motive-waves/ "Motive Waves - Elliott Wave International"
[2]: https://www.tradingview.com/support/solutions/43000591663-williams-fractal/?utm_source=chatgpt.com "Williams Fractal — TradingView"
[3]: https://python.stockindicators.dev/indicators/ZigZag/?utm_source=chatgpt.com "Zig Zag - Stock Indicators for Python"
[4]: https://www.elliottwave.com/waveopedia/fibonacci-relationships "Fibonacci Relationships - Elliott Wave International"
[5]: https://www.elliottwave.com/waveopedia/impulse/ "Impulse - Elliott Wave International"
[6]: https://www.elliottwave.com/waveopedia/alternation "Alternation - Elliott Wave International"
[7]: https://www.elliottwave.com/waveopedia/extension "Extension - Elliott Wave International"
[8]: https://www.elliottwave.com/waveopedia/corrective-waves "Corrective Waves - Elliott Wave International"
[9]: https://studyres.com/doc/7934696/methodology-for-elliott-waves-pattern-recognition "methodology for elliott waves pattern recognition"
[10]: https://www.elliottwave.com/waveopedia/truncation "Truncation - Elliott Wave International"
[11]: https://www.elliottwave.com/waveopedia/diagonals "Elliott Wave Pattern: Diagonals | Elliott Wave International"
[12]: https://www.elliottwave.com/waveopedia/channeling "Channeling - Elliott Wave International"
[13]: https://elliottwave-forecast.com/trading/elliott-wave-corrective-waves/ "Elliott Wave Corrective Waves: Zigzag, Flat, Triangle Patterns"
[14]: https://www.elliottwave.com/waveopedia/flats "Flats - Elliott Wave International"
[15]: https://www.elliottwave.com/waveopedia/triangles/ "Triangles - Elliott Wave International"
[16]: https://www.elliottwave.com/waveopedia/zigzags/ "Zigzags - Elliott Wave International"
[17]: https://www.elliottwave.com/waveopedia/combinations/ "Combinations - Elliott Wave International"
[18]: https://www.investopedia.com/articles/technical/111401.asp "Elliott Wave Theory: What You Need to Know"
[19]: https://www.mdpi.com/2227-7390/11/6/1466 "An Improved Deep-Learning-Based Financial Market Forecasting Model in the Digital Economy"
[20]: https://khub.utp.edu.my/scholars/10880/ " An algorithm for Elliott Waves pattern detection  - UTP Scholars"
