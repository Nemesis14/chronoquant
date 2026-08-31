# Terv: Klaszter-alapú setup-felismerés (unsupervised regime/pattern stratégia)

Státusz: **draft — kulcsdöntések lezárva, végrehajtás előtt** (nem kanonikus, `_doc_/_plans_/` sandbox zóna)
Kapcsolódó jira: nincs még (előbb Fázis 1 kutatási prototípus, utána epic)

**Eldöntött kérdések (2026-08-31):**
1. Mintavételi sűrűség: **ritkítva** (15-30 percenként) a kutatási/klaszterezési fázisban — az élő szignál később maradhat perces.
2. Normalizálás: **kizárólag alak-felismerés** — ablakon belüli z-score/rang, ár-szint és volatilitás-rezsim független.
3. Hatókör: **önálló kutatási irány** `research/` sandboxban, LGBM-től függetlenül validálva; a bevezetésről (kiegészítés vagy önálló stratégia) csak Fázis 3 után döntünk.

---

## 1. Az ötlet (user megfogalmazásában, tömörítve)

Az eddigi LGBM-regressziós megközelítés helyett/mellett egy **unsupervised klaszterezésen
alapuló setup-felismerő** rendszer:

1. Minden 1 perces megfigyeléshez tartozik egy **6 órás (360 perces) visszatekintő ablak**
   SOLUSDT-re.
2. Az ablakban az árfolyam mellett **~10 db lehetőleg független, alacsony redundanciájú
   indikátor** (derivált feature-ök, likviditási/aktivitási mutatók stb.) — cél: minél
   több különböző *dimenziót* (trend, momentum, volatilitás, volumen/likviditás,
   mikrostruktúra) lefedni, minimális átfedéssel.
3. Így minden megfigyelés egy **360×10-es mátrix**.
4. Ezeket a mátrixokat **klaszterezzük** → hasonló piaci "setup"-ok csoportjai jönnek létre.
5. Minden klaszterre megnézzük az **előretekintő 6 órás átlagos MFE-t** — long és short
   irányba is (felfelé és lefelé kilengés).
6. Kiválasztjuk azokat a klasztereket, amik **szignifikáns long vagy short irányú
   torzítást** mutatnak **és** elég sok megfigyelést tartalmaznak.
7. **Cél:** ezeket a klasztereket kereskedési szignálként használni.

---

## 2. Hogyan viszonyul a jelenlegi LGBM megközelítéshez

| | LGBM (jelenlegi, `src/modeling/`) | Klaszter-alapú (javasolt) |
|---|---|---|
| Tanulás típusa | Felügyelt regresszió (pontbecslés minden bar-ra) | Felügyelet nélküli csoportosítás + utólagos kiértékelés |
| Input | Egyetlen időpontban vett feature-vektor (`feat_*`, t-1 lag) | 360 perces **idősor-szelet** minden feature-ből (alak/pálya, nem pillanatkép) |
| Kimenet | Folytonos MFE-becslés minden bar-ra | Diszkrét klaszter-tagság → klaszterenkénti átlagos MFE-statisztika |
| Interpretálhatóság | Alacsony (fekete doboz, SHAP kell) | Magas (klaszter = megnézhető, jellemezhető "piaci helyzet") |
| Kockázat | Overfit a sok feature-ön, split-eken | Overfit a klaszter-számban / adatszűrésben ("data dredging" sok klaszterrel) |
| Már megvan | Teljes pipeline, `feat_ohlcv_quant`, `fw60` target | Semmi — az `fw360` target és a windowed reprezentáció is új |

**Nem feltétlenül "vagy-vagy"**: a két megközelítés kiegészítheti egymást (pl. a klaszter-ID
mint kategorikus feature az LGBM-ben, vagy a klaszter-szignál mint független második
stratégia-réteg). Első körben viszont célszerű **önálló, LGBM-től független kutatásként**
kezelni, hogy tiszta képet kapjunk az edge-ről.

---

## 3. Módszertani lépések és döntési pontok

### 3.1 Megfigyelés és időablak definíciója

- Lookback: 360 perc (6 óra), forward horizon: 360 perc (6 óra) — szimmetrikus ablak.
  Ez eltér a jelenlegi `fw60` target-től → **új target kell: `fw360`** (long/short MFE
  360 bar előre). Kód-mintája megvan (`long_mfe_fw60`/`short_mfe_fw60` logika
  újrahasználható más horizonttal).
- **Eldöntve — mintavételi sűrűség:** a klaszterezési fázisban **ritkított mintavétel**
  (15-30 percenként) az autokorreláció csökkentésére és a számításigény kezelésére.
  Az élő szignál generálása (ha eljutunk odáig) maradhat perces — az élesben minden
  bar-hoz újraszámolt ablakot a rögzített klaszter-modellhez rendeljük (predict, nem
  refit), csak a *modell illesztése* történik ritkított mintán.

### 3.2 Feature-választás (max ~10, minimális redundancia)

A `config/features.json`-ban már megvan egy gazdag, kategorizált feature-készlet:
momentum (rsi, roc, stochastic, cci, williams_r, adx), trend (macd, sma/ema/wma/kama),
volatility (bollinger, atr, historical_vol), volume/aktivitás (volume_ratio, obv, mfi,
cmf, quote_volume_ratio, taker_buy_ratio), price_action (returns, skew, kurt),
market_structure (swing points), regime_rank (natr_rank, bb_width_rank, volume_rank).

Javasolt módszer a 10 kiválasztására **ne kézi/intuitív legyen**, hanem kvantitatív:

1. Számítsd ki a teljes `feat_*` halmaz páronkénti korrelációs mátrixát (a meglévő
   `src/modeling/` feature-quality eszközök erre valók, ha van redundancia-analízis).
2. Hierarchikus klaszterezés a feature-korrelációkon → válassz **1 reprezentánst
   kategóriánként** (trend / momentum / volatilitás / volumen-likviditás /
   mikrostruktúra-alak / idő-szezonalitás).
3. Végeredmény: ~8-10 feature, dimenziónként egy, korrelációjuk páronként alacsony.

Javasolt kategóriák (dimenziók), egyenként 1 feature-rel:
- **Ár/hozam** (kötelező): log-return vagy close ára normalizálva
- **Trend-irány**: pl. `feat_ema_slope` vagy `macd`
- **Momentum**: `rsi` vagy `roc`
- **Volatilitás**: `atr` vagy `historical_vol` / `natr_rank`
- **Volumen/likviditás**: `quote_volume_ratio` vagy `taker_buy_quote_ratio`
- **Aktivitás/mikrostruktúra**: `trade_count_ratio` vagy `avg_trade_quote`
- **Gyertya-alak**: `body_ratio_sma` / `wick_imbalance_sma`
- **Idő-szezonalitás** (opcionális, nem idősor-alakú): session/hour sin-cos

### 3.3 Normalizálás — kritikus döntés

A nyers ár és sok indikátor **nem stacionárius** (SOL ára $8-tól $300-ig mozgott
2021-2025 között) → klaszterezés előtt **kötelező normalizálni**, különben a
klaszterek csak "ársávot" fognak elkülöníteni, nem "alakzatot"/"viselkedést".

Javaslat:
- Az ár-dimenzió: **log-return relatív az ablak elejéhez** (`log(close_t / close_{t-360})`
  path), nem nyers ár.
- Minden más indikátor: **ablakon belüli z-score vagy percentil-rang**, hogy a
  360×10 mátrix minden oszlopa összemérhető legyen, és a klaszterezés az idősor
  *alakjára/dinamikájára* reagáljon, ne az abszolút szintre.
- **Eldöntve:** kizárólag alak-felismerés a cél — a klaszterek ár-szint és
  volatilitás-rezsim függetlenek legyenek. Rezsim-szintű megkülönböztetés (pl.
  "ez egy magas volatilitású időszak") explicit **nem** cél ebben a körben; ha
  utólag mégis hasznosnak bizonyul, külön dimenzióként utólag hozzáadható.

### 3.4 Mátrix → klaszterezhető reprezentáció

Nyers 360×10 = 3600 dimenziós vektor k-means-be téve rossz ötlet (dimenzió-átok,
skálázási problémák, nem szeparálja jól az alakzatokat). Három reális irány:

| Opció | Leírás | Előny | Hátrány |
|---|---|---|---|
| A) Összegző statisztikák | Minden 10 sorozatból: mean, slope, std, min/max az ablakon | Alacsony dim (~30-40), gyors | Elveszti a finom időbeli alakot |
| B) PCA/autoencoder embedding | 3600 dim → normalizálás után PCA (pl. 95% variancia) vagy kis autoencoder | Megtartja az alakot, kezelhető dim | Extra tanítási lépés, kevésbé interpretálható |
| C) Idősor-natív klaszterezés (DTW/k-shape) | Idősor-specifikus távolságmérték | Alak-érzékeny, nem torzítja a lineáris feltevés | Számításigényes nagy N-re (több millió ablak 2021-2025-re) |

**Javaslat:** kezdésnek A) opció (összegző statisztikák) egyszerűbb, gyorsabb, jól
validálható — utána, ha van edge, finomítható B) vagy C) irányba.

### 3.5 Klaszterezési algoritmus

- **k-means**: egyszerű, de kényszeríti, hogy minden pont valamelyik klaszterbe
  kerüljön (nincs "zaj" kategória), és feltételezi a klaszterek gömb-alakját.
- **GMM (Gaussian Mixture)**: soft-membership, BIC/AIC-vel válaszható a komponensszám.
- **HDBSCAN**: sűrűség-alapú, **nem kényszerít minden pontot klaszterbe** (van "noise"
  osztály) — ez valószínűleg jobban illik ide, mert a cél éppen az, hogy **csak
  néhány kifejezett, jól elkülönülő setup**-ot azonosítsunk, a piac nagy része pedig
  marad "nincs edge" zaj.
- **Javaslat:** HDBSCAN elsődleges jelölt, k-means/GMM mint baseline összehasonlítás.

### 3.6 Forward MFE célváltozó

- `fw360` target szükséges (jelenlegi `fw60` mintájára): `long_mfe_fw360`,
  `short_mfe_fw360` — 360 bar előretekintő log-MFE mindkét irányba.
- Ez egy **research célváltozó**, nem kell a live `target` táblába kerülnie —
  elég egy snapshot-alapú számítás a `research/` sandboxban.

### 3.7 Klaszter-értékelés

Minden klaszterre:
- `n_obs`, átlag/medián `long_mfe_fw360`, átlag/medián `short_mfe_fw360`
- Szórás / percentilis sáv (nem csak átlag — az átlagot 1-2 extrém eset torzíthatja)
- Effektív (nem-átfedő) mintaszám becslése (lásd 4.2)
- Statisztikai teszt: klaszter átlaga szignifikánsan eltér-e a globális bázis-átlagtól
  (pl. bootstrap CI, mert a minta autokorrelált — sima t-teszt itt félrevezető)

### 3.8 Out-of-sample validáció — kötelező, nem opcionális

A projekt már bevezetett konvenciója (walk-forward / train-valid split, purge/embargo)
itt is kell:
1. Klasztereket (centroidokat/HDBSCAN modellt) **csak a train perióduson** illeszd.
2. A valid/OOS periódus megfigyeléseit **a rögzített klaszter-modellhez rendeld hozzá**
   (predict, nem refit).
3. Ellenőrizd, hogy a train-en talált irány-bias **megmarad-e** az OOS mintán —
   enélkül a "jelentős long/short klaszter" megállapítás data-dredging.

---

## 4. Kritika / kockázatok

1. **Autokorreláció és "effektív N" csapda.** Percenkénti, 360 perces átfedő ablakokkal
   dolgozva egy 4 éves adatsor (~2.1M perc) "N=2.1M megfigyelést" ad, de ezek túlnyomó
   többsége majdnem ugyanaz a piaci pillanat 1 perccel eltolva. Egy klaszterben "10 000
   megfigyelés" lehet valójában csak ~50-100 valódi, egymástól független esemény. A
   "megfelelő mennyiségű megfigyelés" kritériumot **effektív mintaszámra** kell
   definiálni, nem nyers sorszámra — különben hamis magabiztosságot ad.
2. **Data dredging / multiple comparison.** Ha sok klasztert generálunk (mondjuk 20-50),
   és utólag kiválasztjuk "amelyik jól néz ki", az szinte garantáltan talál véletlenszerű
   mintázatot pusztán a klaszterszám miatt. Az OOS validáció (3.8) ez ellen véd, de
   emellett érdemes explicit multiple-testing korrekciót (pl. Bonferroni vagy false
   discovery rate) alkalmazni a klaszter-szignifikancia teszteknél.
3. **Rezsimváltás a 4+ éves adatsoron.** 2021 bull, 2022 bear, 2023-25 vegyes SOL
   piac — egy globálisan illesztett klaszter-modell összemoshatja a rezsimeket, vagy
   fordítva: egy klaszter csak egy adott évet reprezentálhat, ami nem general. Érdemes
   végiggondolni: globális klaszterezés (mint most tervezve) vs. évenkénti/walk-forward
   újra-illesztés (mint a jelenlegi éves LGBM modelleknél).
4. **Normalizálási döntés nagyban befolyásolja az eredményt** (lásd 3.3) — ár-szint
   független alak-felismerés vagy rezsim-szint is számítson bele. Ezt előre el kell
   dönteni, mert utólag más eredményt ad.
5. **Kereskedési végrehajtás átfedő szignálokkal.** Mivel a megfigyelések percenként
   csúsznak, ha egy megfigyelés egy "jó" klaszterbe esik, a következő percben szinte
   biztosan ugyanabba (vagy szomszédos) klaszterbe esik → sok egymást követő belépő
   jelzés ugyanarra a piaci mozgásra. Kell egy állapotgép/cooldown logika (hasonlóan a
   meglévő `src/trading/live/` FLAT→LONG/SHORT máshoz), különben az azonos setup-ra
   sokszor nyitna pozíciót.
6. **Számításigény.** 2.1M átfedő 360×10 ablak akár csak összegző statisztikákkal is
   jelentős előfeldolgozást igényel — érdemes a ritkított mintavétellel (3.1) kezdeni.
7. **Nincs jelenleg semmi kód-infrastruktúra ehhez** — teljesen új research irány,
   `research/` alá tartozik (a `_doc_/0000_project_overview.md` szerint a `research/`
   sandbox, "nem táplálja az éles pipeline-t"), amíg nincs bizonyított edge.

---

## 5. Nyitott kérdések (a 3 fő döntés lezárva — lásd fejléc; ez maradt)

1. **Globális vs. évenkénti klaszterezés**: egy klaszter-modell a teljes 2021-2025
   adatsoron, vagy walk-forward újra-illesztés évente/negyedévente (mint a jelenlegi
   éves LGBM modelleknél)? Javaslat: Fázis 1-ben kezdjük globálissal (egyszerűbb),
   és Fázis 2 OOS validációnál derül ki, hogy a rezsimváltás (4.3 pont) valóban
   problémát okoz-e — ha igen, áttérünk walk-forward újra-illesztésre.

---

## 6. Javasolt végrehajtási fázisok

**Fázis 0 — Feature-szelekció (kvantitatív, nem kézi)**
- Korrelációs/redundancia-analízis a meglévő `feat_*` halmazon → ~8-10 alacsony
  redundanciájú feature kiválasztása, dimenziónként egy (3.2).

**Fázis 1 — Kutatási prototípus (`research/cluster_regime/`)**
- `fw360` long/short MFE célváltozó számítása egy snapshot-on.
- Ablak-reprezentáció (3.3-3.4) + klaszterezés (3.5) egy **kisebb, egyetlen éves**
  mintán, gyors iterációhoz.
- Klaszter-profilozás: méret, irány-bias, effektív N, bootstrap CI.

**Fázis 2 — Multi-year validáció**
- Train/valid split OOS ellenőrzés (3.8) a teljes 2021-2025 adatsoron.
- Multiple-testing korrekció a szignifikáns klaszterek listáján.

**Fázis 3 — Döntési pont**
- Ha van robusztus, OOS-stabil, elég effektív N-nel rendelkező klaszter(ek):
  eldönteni, hogy önálló stratégia legyen, vagy LGBM-kiegészítő feature.
- Ha nincs: dokumentálni a negatív eredményt (`_doc_/methodology_doc/`) és lezárni.

Csak a Fázis 3 döntés után kerülne a projekt agent-delegálási táblájába
(`modeling_agent` — feature engineering + klaszterezés, `methodology_agent` —
rationale dokumentáció, `validator_agent` — OOS validáció).

---

## 7. Összefoglaló vélemény

Az ötlet módszertanilag értelmes és jó kiegészítése lehet a jelenlegi
pontbecslés-alapú LGBM megközelítésnek — különösen azért, mert **interpretálható**
("ez a klaszter azt jelenti, hogy alacsony volatilitás + felfelé lejtő trend +
erősödő volumen"), szemben az LGBM fekete doboz jellegével. A fő kockázat nem az
ötlet maga, hanem **három klasszikus buktató**, amit explicit kezelni kell, különben
hamis pozitív eredményt ad:

1. autokorrelált, átfedő ablakokból számolt "megfigyelésszám" félrevezető effektív N nélkül,
2. sok klaszter közül utólagos szelekció (data dredging) OOS validáció nélkül,
3. nem-stacionárius bemenet (nyers ár/szint) normalizálás nélkül.

Ha ezt a három pontot a tervbe beépítjük (megtörtént a fenti szakaszokban), az ötlet
jó eséllyel adhat egy valódi, kereskedésre alkalmas edge-et — vagy legalább egy
tiszta, megbízható negatív eredményt ("nincs itt semmi") ami is hasznos infó.
