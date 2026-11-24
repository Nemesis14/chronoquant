# Epic 055: Kereskedési költség-modell a strategy rácskeresésbe

> **Kiváltó ok:** az `epic_054` kimutatta, hogy a `src/strategy/` **sehol nem
> számol díjjal, csúszással vagy fundinggal**. Minden riportált hozam bruttó.

## Goal

A rácskeresés célfüggvénye legyen **nettó** hozam, ne bruttó.

Ez nem finomhangolás. A jelenlegi kereső szisztematikusan a túlkereskedő
beállításokat választja, mert a forgalomnak nincs ára a modelljében.

## A bizonyíték

Az `epic_054` kezelt modelljének **bruttó optimuma** (2026-02 → 2026-07,
1000 USD notional, 100 USD tőke, 10x):

```
1984 kötés
bruttó nyereség                    +649 USD
díj (1984 × 0,80 USD @ 8 bps)     −1587 USD
──────────────────────────────────────────
nettó                              −938 USD   =  a tőke kilencszeres elvesztése
```

Ugyanezen modell **költség-tudatos optimuma** 401 kötéssel **+267 USD**.
Ugyanaz a modell, ugyanaz az adat — a különbség kizárólag az, hogy a kereső
tud-e a díjról.

## Scope

- `src/strategy/strategy/search.py` — nettó célfüggvény
- `src/strategy/strategy/costs.py` — új modul (díj + csúszás + funding)
- `config/strategies.json` — díjparaméterek
- `src/strategy/tests/` — költség-számítás tesztek

Nem érinti: `src/modeling/`, a modellek nem változnak.

## Tasks

- t401: Költség-modell modul + konfiguráció → modeling_agent
- t402: Funding-költség a tényleges kötésekből → modeling_agent
- t403: Nettó célfüggvény a rácskeresésbe → modeling_agent
- t404: Meglévő strategy sessionök újrafuttatása és a különbség kimutatása → analyst_agent

Sorrend: (t401 ∥ t402) → t403 → t404

## Key Decisions

- **A díj a notionalra megy, nem a tőkeáttételes részre.** 1000 USD pozíción a
  taker díj 1000 × 0,05% = 0,50 USD oldalanként, függetlenül attól, hogy 100
  vagy 1000 dollár saját tőkét tettél mögé.
- **A tőkeáttétel nem változtat a díj/hozam arányon.** Mindkettőt ugyanúgy
  felszorozza. Ami 1x-en veszteséges díjak után, az 10x-en is az.
- **Kilépés típusa szerint eltérő díj.** A take profit limit orderként
  teljesülhet (maker), a stop loss és az időtúllépés viszont market (taker).
  Egységes taker-taker feltevés felülbecsülné a költséget.
- **A funding NEM átalány.** Csak a ténylegesen átnyúlt elszámolásokra jár.
  Az `epic_054` mérése szerint a kötések 3–13%-a nyúlik át egy 8 órás
  elszámoláson (15–50 perces átlagos tartás mellett).
- **A funding jelenleg bevétel, nem költség.** 2026-ban a SOLUSDT funding
  negatív (−1,93%/év), tehát a long pozíció **kapja**. Mért érték a valós
  kötéseken: +11 USD / 1000 USD notional / 6 hónap. Elhanyagolható a díjhoz
  képest, de az előjelet nem szabad elrontani.
- **A díjszint a user fiókjától függ.** A VIP0 alapdíj 0,02% maker / 0,05%
  taker; BNB-kedvezménnyel ~10%-kal kevesebb. A tényleges szintet a user
  ellenőrzi — addig a VIP0 a default, és a paraméter konfigurálható marad.

## Notes

Az `epic_054` riportja ezt nevezte meg a vizsgálat legfontosabb
melléktermékeként: `_doc_/research/altdata_gate.md` 6. és 7. szakasz.
