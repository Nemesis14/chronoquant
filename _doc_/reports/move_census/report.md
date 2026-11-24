## Cél

Hány **nagy emelkedés** (long) van BTCUSDT 1 perces adatán, és milyen hosszú szokott lenni? A definíció skálafüggetlen:
egy σ-normált zigzag felfelé tartó lába, ahol a „nagy" = nettó emelkedés ≥ k × σ_T (σ_T: az adott skálán, T-perces hozamokon mért,
30 napos trailing szórás — empirikus, nem √T-skálázás). A zigzag akkor fordul, ha a ár a csúcsról ρ·k·σ_T-t visszaesik (ρ = 0,5), így
egy nagy mozgáson belül a visszaesés mindig kisebb a minimális méret felénél. Rács: T ∈ 15 perc … 3 nap (9 skála, duplázó lépés),
k ∈ {3, 4, 6} (előre rögzítve). Ablak: **2021-02-04 → 2026-01-31** (4.99 év), OOS és holdout-2 nem töltődött be.

**Fontos:** a katalógus utólagos (hindsight) — egy lábat csak a vége után ismerünk fel. Leíró keret, nem előrejelzés. A `T` az
„egység" skálája (mennyi σ-t mérünk), a mozgás tényleges hossza (percekben) ebből **adódik**, nem rögzített.

## Darabszámok skála × k

A cellák egymástól függetlenek, ugyanaz a mozgás több cellában is szerepelhet (alább: beágyazottság).

{{chart:count_heatmap}}

{{table:cells}}

## Mennyi ideig tart?

A medián hossz / T arány k-nként nagyjából állandó a skálák között (az arányok skálafüggetlensége látszik); k-val négyzetesen nő, ami a **véletlen bolyongás** várható értéke is (mérete kσ_T-hez ~k²·T idő kell). Ezért kell a placebo (t951).

{{chart:dur_over_scale}}

{{chart:dur_box_k4}}

{{table:dur_quantiles}}

## Mennyire trendes?

A hatékonysági arány (net / út, T/8 mintavétellel) mutatja, mennyire egyenes a mozgás; 1 = tökéletesen egyenes.

{{chart:er_box_k4}}

## Időbeli stabilitás

A nagy mozgások éves rátája (db / év, az ablakban lévő részév arányában).

{{chart:year_rates_k4}}

{{table:year_rates}}

## Átfedés és beágyazottság

Egy cellán belül a mozgások nem fedik egymást (a zigzag láb-váltakozó). Cellák között igen: ugyanaz a nagy emelkedés kisebb skálán több kisebb mozgásra is eshet. A `scale_min = 0` sor az összes skála uniójáról szól.

{{table:coverage}}

## Értelmezés

Lásd az epic zárójelentésében; ez a riport a t950 leíró képe. A következő lépés a placebo (t951).
