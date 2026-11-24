# Multi-skálás nagy-mozgás katalógus (epic_077)

**Időszak:** 2026-11-24 · **Assetek:** BTCUSDT (fő), SOLUSDT, ETHUSDT · **Státusz:** lezárt, nemleges (az 1m ár geometriájából a folytatás nem becsülhető a véletlennél jobban) · **Riportok:** `_doc_/reports/move_census`, `move_census_placebo`, `move_census_anatomy`, `move_census_candidates`

## Kérdés

A Scout (epic_072/073) fix 4 órás skálája helyett skálafüggetlen, felülről lefelé indított megközelítés: hány nagy emelkedés (long) van 1m adaton, milyen hosszú az első lökésük, hány impulzus–korrekció ciklusból állnak, és mennyi ebből a struktúra egy azonos volatilitású véletlen bolyongáshoz képest. Modell nem készült.

## Módszer

- Nagy emelkedés = σ-normált zigzag felfelé lába: nettó ≥ k·σ_T, a fordulási küszöb 0,5·k·σ_T; σ_T = T perces hozamok 30 napos mozgó szórása (csak múltból). T = 15 perc … 3 nap (9 skála), k ∈ {3, 4, 6}; előre rögzítve. Ablak: 2021-02-04 → 2026-01-31 (a quant modell train-ablaka); OOS és holdout-2 nem töltődött be.
- Placebo: véletlen előjel (a 1m abszolút hozamok helyükön, az előjel véletlen) és 7 napos blokk-bootstrap. BTC: 200 + 200 realizáció; SOL/ETH: 40 (véletlen előjel); anatómia: 30 + 30.
- Anatómia: minden mozgás leg-felbontása a mozgás nettójának `c` törtrészével (előre rögzítve 0,10 elsődleges, 0,05, 0,20; a 0,33 **utólag** került be, jelölve). Ciklusszám, nincs Elliott-számozás.
- Nevező: megerősített mélypontok (kauzális jelöltek), ciklusszám = egymást követő emelkedő mélypontok; címkék: nagy mozgás lesz-e; R-szorzós első érintés (stop = mélypont, +1R/+2R/+3R, 13 bps költség). Nap-klaszteres CI.

## Eredmény (BTC)

- 40 592 nagy mozgás 27 cellában (k = 4: 5 903 db 15 percnél, 35 db 3 napnál). Az első lökés medián hossza ≈ 3,0 / 5,3 / 12,3 × T (k = 3 / 4 / 6): nincs szűk keret, ez a véletlen bolyongás skálázása.
- A véletlen előjelű placebóhoz képest a darabszám, hossz, hatékonyság, nagy-arány megkülönböztethetetlen (darabszám: 1 / 27 cella p < 0,05). Ciklus-szerkezet (c = 0,33): 86,5% tiszta lökés / 11,4% egy / 1,9% két ciklus; placebo 87,5 / 10,4 / 1,8%. Darabszám-skálázás meredeksége −0,91 (placebo −0,92 … −0,89).
- Valós eltérések (kicsik): variancia-arány 0,954 (15 perc) → 0,856 (3 nap), p = 0,005 minden skálán (az ár visszaforduló); a későbbi lökések lassabbak (2.: 0,89×, 3.+: 0,76× az első sebességéhez; placebo 0,96 / 0,94; a blokk-placebo reprodukálja).
- Jelöltek: P(nagy | jelölt) 0,39–0,43 minden skálán és ciklusszámon (placebo-egyező; zárt alak e⁻¹ = 0,368 + diszkrét overshoot). Találati arány +1R / +2R / +3R: 51 / 35 / 27% ≈ martingál (50 / 33 / 25%). Bruttó E[R] ≈ 0 … +0,09 (nagyrészt a BTC drift: drift nélkül ≈ 0); 13 bps költség után T ≤ 2 óra: negatív, T ≥ 4 óra ≈ 0. Jelölt-statisztikák p < 0,05: 7 / 270.
- Feltáró jel: volumen-kiszáradás (< 0,5) a korrekcióban, E[R₂] +0,11 [0,01; 0,22] BTC-n; **SOL-on (+0,015) és ETH-n (+0,050) nem replikálódik**.

## Eredmény (SOL, ETH)

- ETH ugyanazt mutatja, mint BTC (1 / 27 darabszám-cella p < 0,05 drift nélkül).
- **SOL eltér:** a valós darabszám 1 óránál nagyobb skálákon 5–21%-kal a placebo fölött (10 / 27 cella p < 0,05, a drift eltávolítása után is), hossz és hatékonyság is több cellában eltér. Az ok nem azonosított (hipotézisek: pozitívan ferde „pump" klaszterek, 2021–22-es vol-rezsimváltás). A jelöltek bázisrátája (42%) és találati aránya (51 / 35 / 27%) SOL-on is martingál.

## Scout elhelyezése

A Scout-esemény ≈ (T = 240 perc, k = 3) jelölt; az „impulzus" címke = +1R első érintés 8 órás időlimittel. A 2143 train Scout-esemény 67,5%-a (1446) egyezik egy jelölttel (mélypont ±15 perc). A Scout alacsony impulzus-aránya (~13%) főleg a 8 órás időkorlát miatt van; ugyanaz a +1R időlimit nélkül ~50% (martingál). Az egyező eseményeknél a jelölt-oldali +1R 68%, mert a Scout 4 órát vár, és a túlélő mélypontokat szűri (utólagos szűrés).

## Következtetés

- Az 1m ár alakjából (lökés, ciklus, korrekció-mélység, volumen-arány) a nagy mozgás korai szakaszára vonatkozó folytatás nem becsülhető a véletlennél jobban; a „fraktál" skálafüggetlenség a véletlen bolyongás tulajdonsága, nem információ. Ez a Scout eredményével összhangban van.
- Nem vizsgált: flow (taker arány, CVD), funding, nyitott pozíció, cross-asset vezetés, napszak, short irány (tükrözés, 2. kör).
- Javaslat: az „ár-geometria önmagában" ág lezárása; folytatás csak külső információval, a jelölt-keret (baseline + placebo) újrahasznosításával.

## Korlátok

Csak long, close-alapú zigzag; a 40 / 30 realizációs placebók minimális p-értéke 0,024 / 0,032; a három asset erősen korrelált, nem három független bizonyíték; a 3 napos skálán k = 6 mellett csak 13 mozgás van (BTC); a blokk-placebo nem tiszta null a hossz- és hatékonysági mutatókra (a vol-rezsimet szétzúzza).

## Újragenerálás

`uv run python -m reports.builders.move_census_report --asset-id btcusdt` (t950), `…move_census_placebo --n 200` + `…move_census_placebo_report` (t951), `…move_census_anatomy --stage real|placebo` + `…move_census_anatomy_report` (t952), `…move_census_candidates --stage real|placebo` + `…move_census_candidates_report` (t953), `…move_census_cross_asset --stage run --asset-id solusdt|ethusdt` és `--stage scout` (t954). Táblák: `research."move_census_v1_*"` az asset lab-adatbázisaiban. Kód: `src/reports/builders/move_census*.py` (a repóban marad).
