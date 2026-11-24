## Cél

Egy nagy emelkedésen (t950) belül mi a szerkezet? Hány impulzus–korrekció ciklusból áll, mekkora és milyen hosszú az **első lökés**
(a „keret"), mennyire hasonlít egymásra a különböző skálákon, és mennyi ebből a véletlen bolyongás sajátja? Minden nagy mozgást a saját méretéhez
viszonyított zigzaggal bontottunk legekre: egy leg akkor fordul, ha a ár a mozgás nettójának `c` törtrészét visszaadja.
Rögzített szintek: **c = 0,10 (elsődleges), 0,05 és 0,20 (érzékenység)**. A **0,33-as szint utólag került be** (a 0,10-es bontásban a medián mozgás ~4 impulzusra esett
szét, ami nagyrészt finom szerkezet); ez áll legközelebb az „1-2-1-2" képhez. A fogalmak: *ciklusszám* = a legbelső korrekciók száma, nincs Elliott-számozás.
A placebo: 30-30 útvonal (véletlen előjel, 7 napos blokk-bootstrap), k = 4 mozgásokon.

## Hány ciklusból áll egy nagy mozgás?

{{chart:cycles_c20}}

{{chart:cycles_c33}}

## Az első lökés időkerete (1m-ben)

A `dur_*` oszlopok az első felfelé szakasz hosszát adják percben, skálánként (k = 4).

{{chart:first_leg_c20}}

{{table:first_leg_table_c20}}

{{chart:first_leg_c33}}

{{table:first_leg_table_c33}}

## Mit hoz a korrekció utáni lökés? (elsődleges bontás, c = 0,10)

{{chart:order_stats}}

{{chart:retr_stats}}

A volumen-arány (korrekció / előtte lévő lökés átlagos volumene, medián) csak a valós adatra értelmezhető (a placebo útvonalon a volumen a valós idősor marad, így a hozam–volumen kapcsolat megszakad):

{{table:volume_ratio}}

## Skála-invariancia

A fraktál-feltevés: az arányok skálánként hasonlók. Elsődleges bontás (c = 0,10) mozgás-szintű arányai skálánként:

{{chart:inv_net}}

{{chart:inv_dur}}

{{chart:inv_retr}}

{{table:ks_to_pooled}}

## Darabszám-skálázás

A nagy mozgások száma a skálával hatványfüggvény szerint csökken; a log-log meredekség (N ~ T^b) valós és placebo útvonalon:

{{table:slopes}}

## Értelmezés

Lásd az epic zárójelentését. Ez a riport a t952 leíró mérése.
