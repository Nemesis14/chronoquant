# Epic 034: Sample-scoped modeling architecture refactor

## Goal
A modellezési architektúra tényleges összhangba hozása a kívánt reprodukálható
lánccal:

`live data -> quant_train -> snapshot range -> model.__sample -> FE -> search -> fit -> model.__pred -> strategy -> live signals`

A fő kiváltó ok, hogy a jelenlegi `feature_engineering` lépés nem az adott modell
valódi fejlesztési mintáján fut, hanem csak a `model.__sample` időhatárára szűkített
teljes `quant_train`-on. Ez megsérti a modell-scope szerződést.

## Scope
- `src/modeling/01_feature_engineering.ipynb`
- `src/modeling/feature_engineering/`
- `src/modeling/sampling/`
- `src/modeling/search/`
- `src/modeling/training/`
- `src/modeling/predict.py`
- `src/strategy/`
- kapcsolódó config- és provenance-szerződések
- kapcsolódó `_doc_/methodology_doc/` és `_doc_/database_and_code_doc/` frissítések

## Tasks
- t41: végleges sample-scoped architektúra és invariánsok rögzítése (modeling_agent)
- t42: feature_engineering input refaktor az adott modell mintájára / snapshot-projekciójára (modeling_agent)
- t43: downstream modeling contract igazítása search/train/predict felé (modeling_agent)
- t44: strategy contract audit az új model-scope láncra (modeling_agent)
- t45: dokumentáció és provenance frissítése az új architektúrára (code_doc_agent)
- t46: teljes újrafuttatás az új architektúrán (modeling_agent)
- t47: végső validáció és konzisztencia-ellenőrzés (validator_agent)

## Key Decisions
- A sampling adja meg az adott modell fejlesztési mintáját; minden modellhez kötött
  műveletnek ezen a scope-on kell futnia.
- A `feature_engineering` nem elégedhet meg puszta időablak-szűkítéssel; vagy a
  tényleges `model.__sample` sorokra, vagy egy abból épített explicit train-input
  nézetre/táblára kell támaszkodnia.
- A végső elfogadási kritérium nem csak a kódmódosítás, hanem a teljes lánc újrafuttatása
  és az új artifactok/provenance létrejötte.

## Risks
- A meglévő notebook és FE library implicit `quant_train`-feltételezései több helyen
  is szét lehetnek szórva.
- A search/train/predict jelenleg részben már snapshot-native, ezért az átállásnál
  könnyű fél-legacy állapotban maradni.
- A teljes újrafuttatás időigényes, és csak akkor értelmes, ha a pipeline contractok
  már egységesek.
