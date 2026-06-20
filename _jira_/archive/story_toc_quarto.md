# quarto-ba a toc beállítás mert túl szűk és töri a sorokat

Megnéztem. A releváns fájl nem analyst/quart.yml, hanem src/analyst/_quarto.yml. Ebben most ez van:

grid:
  sidebar-width: 320px
  body-width: 900px
  margin-width: 200px
  gutter-width: 2rem

A body-width most fixen 900px, tehát ezt nem kell bántani. A TOC bal oldalon van (toc-location: left), ezért a TOC rendelkezésére álló helyet főleg a sidebar-width szabályozza.

A legjobb megoldás nálad: növeld a sidebar-width-et, és ugyanennyivel csökkentsd a margin-width-et. Így a body 900px marad, a teljes layout pedig nem lesz sokkal szélesebb.

Javasolt módosítás:

grid:
  sidebar-width: 400px
  body-width: 900px
  margin-width: 120px
  gutter-width: 2rem

Vagy óvatosabb verzió:

grid:
  sidebar-width: 380px
  body-width: 900px
  margin-width: 140px
  gutter-width: 2rem

Én a 380px / 140px verzióval kezdeném. Ez +60px-et ad a TOC-nak, miközben a body változatlan marad.

A CSS-ben már jól célzod a bal oldali TOC-ot:

#quarto-sidebar-toc-left {
  padding-right: 1.5rem !important;
  padding-left: 0.25rem !important;
}

nav#TOC {
  width: 100% !important;
}

Ez viszont csak a rendelkezésre álló sidebaron belül dolgozik; ha maga a sidebar 320px, akkor a width: 100% is csak 320px-ig tud menni. Ezért kell a _quarto.yml-ben a sidebar-width-et növelni.

Plusz finomításként a CSS-ben érdemes a TOC betűméretét picit csökkenteni, mert ez sokszor elég ahhoz, hogy ne törjön két sorba:

nav#TOC {
  width: 100% !important;
  font-size: 0.92rem;
}

Tehát a konkrét ajánlásom:

grid:
  sidebar-width: 380px
  body-width: 900px
  margin-width: 140px
  gutter-width: 2rem

és opcionálisan:

nav#TOC {
  width: 100% !important;
  font-size: 0.92rem;
}

Így a TOC szélesebb lesz, a body marad 900px, és nem a fő tartalom rovására történik a változtatás.