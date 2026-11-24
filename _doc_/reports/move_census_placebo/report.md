## Cél

A t950 katalógus (27 cella, ~40 ezer nagy emelkedés) önmagában nem mond semmit, amíg nem tudjuk, **mennyi jön ki ugyanebből egy véletlen
bolyongásból, amelynek a volatilitása ugyanolyan**. Ugyanazt a detektort és rácsot lefuttattuk 200-200 placebo-útvonalon:

* **véletlen előjel:** a valós 1m hozamok abszolút értéke marad a helyén (a vol-klaszterezés, a napszaki mintázat és a tüskék megmaradnak),
  az előjel véletlen. Ez minden irány-függést (trend, visszafordulás) elvesz.
* **7 napos blokk-bootstrap:** a valós hozamok 7 napos, nap-igazított blokkjai visszatevéssel; a napon és héten belüli szerkezet (irány is)
  megmarad, ami hosszabb, az összekeveredik.

Minden realizáción ugyanaz az ablak (2021-02-04 → 2026-01-31) és ugyanaz a σ-normálás. A p-érték kétoldali, empirikus:
`(1 + #{|placebo − átlag| ≥ |valós − átlag|}) / (N + 1)`. A valós és a placebo azonos mintájú, ezért a kis p-ket nem szabad egyenként
értelmezni: 27 cella × 6 mutató = több száz teszt fut egyszerre.

## Darabszám: valós / placebo

Az 1,0 körüli érték azt jelenti, hogy a valós adatban pont annyi nagy emelkedés van, mint egy ugyanilyen volatilitású véletlen bolyongásban.

{{chart:count_ratio_k3}}

{{chart:count_ratio_k4}}

{{chart:count_ratio_k6}}

## Mennyire trendes?

A hatékonysági arány (net / út) és a hossz azt mutatja, mennyire egyenes egy nagy mozgás a véletlenhez képest.

{{chart:er_k4}}

{{chart:dur_k4}}

{{chart:share_big_k4}}

## Skálázás: variancia-arány

A σ_T / (σ_1m·√T) arány 1 a véletlen bolyongásnál; fölötte a mozgás trendesebb (momentum), alatta visszaforduló.

{{chart:vr}}

## Összesítő tábla

Minden cella, mutató és placebo-fajta egy sor; `ratio` = valós / placebo-átlag.

{{table:summary}}

## Értelmezés

Lásd a zárójelentést (epic_077). Ez a riport a t951 placebo-mérése.
