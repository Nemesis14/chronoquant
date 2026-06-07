# SOLUSDT Elliott Event Study V1

## Scope

- Window: `2025-06-07 17:00:00` to `2026-06-07 17:00:00` UTC
- Engine: `in_house_confirmed_pivot` `v1`
- Profile: `long_1212_v1`
- Source: raw `solusdt_1m` OHLCV only
- Excluded: feature table, prediction table, wave-5, and C-wave analysis
- Direction: long only
- Setup types: bullish own-detector and package-detector 1-2 / nested 1-2-1-2 wave-3 setups
- Package detector: `taew` `0.0.3` on `1h` and `2h` bars
- Raw upside hit threshold: `0.0100` over the next 60 minutes

## Summary

- Detected Elliott setup events: `1087`
- Detected nested 1-2-1-2 events: `259`
- Close was higher after 60m: `724` / `1087` (66.61%)
- Forward high reached +1.00% within 60m: `436` / `1087` (40.11%)
- Median 60m close return: `0.00457`
- Median forward max-up 60m: `0.00792`
- Median confirmation lag: `0.0` bars

## Timeframe Breakdown

| Timeframe | Events | Nested 1-2-1-2 | 60m Close Up | +Threshold Hit | Median Close Ret | Median Max Up | Median Lag Bars |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1h | 614 | 16 | 63.84% | 40.72% | 0.00513 | 0.00802 | 0.0 |
| 2h | 270 | 8 | 56.30% | 25.56% | 0.00082 | 0.00491 | 0.0 |
| 30m | 203 | 41 | 88.67% | 57.64% | 0.00675 | 0.01196 | 7.0 |

## Engine Breakdown

| Engine | Events | Nested 1-2-1-2 | 60m Close Up | +Threshold Hit | Median Close Ret | Median Max Up |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| in_house_confirmed_pivot | 331 | 65 | 89.12% | 55.29% | 0.00739 | 0.01137 |
| taew | 756 | 194 | 56.75% | 33.47% | 0.00216 | 0.00601 |

## Event Type Breakdown

| Engine | Timeframe | Event Type | Events | 60m Close Up | +Threshold Hit | Median Close Ret | Median Max Up |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| in_house_confirmed_pivot | 1h | wave3_setup_12 | 71 | 98.59% | 61.97% | 0.00961 | 0.01271 |
| in_house_confirmed_pivot | 1h | wave3_setup_nested_1212 | 16 | 100.00% | 75.00% | 0.01233 | 0.01678 |
| in_house_confirmed_pivot | 2h | wave3_setup_12 | 33 | 72.73% | 24.24% | 0.00195 | 0.00654 |
| in_house_confirmed_pivot | 2h | wave3_setup_nested_1212 | 8 | 62.50% | 25.00% | 0.00092 | 0.00438 |
| in_house_confirmed_pivot | 30m | wave3_setup_12 | 162 | 89.51% | 58.02% | 0.00692 | 0.01195 |
| in_house_confirmed_pivot | 30m | wave3_setup_nested_1212 | 41 | 85.37% | 56.10% | 0.00582 | 0.01196 |
| taew | 1h | taew_wave3_setup_12 | 387 | 58.66% | 37.47% | 0.00437 | 0.00676 |
| taew | 1h | taew_wave3_setup_nested_1212 | 140 | 56.43% | 35.00% | 0.00308 | 0.00608 |
| taew | 2h | taew_wave3_setup_12 | 175 | 52.00% | 24.57% | 0.00045 | 0.00479 |
| taew | 2h | taew_wave3_setup_nested_1212 | 54 | 59.26% | 29.63% | 0.00106 | 0.00501 |

## Nested 1-2-1-2 Outcome

- Nested events: `259`
- Close was higher after 60m: `167` / `259` (64.48%)
- Close was not higher after 60m: `92` / `259`
- Forward high reached +1.00% within 60m: `102` / `259` (39.38%)
- Median 60m close return: `0.00389`
- Median forward max-up 60m: `0.00727`

## Event Examples

| Open Time | Timeframe | Type | Score | Close Ret 60m | Max Up 60m |
| --- | --- | --- | ---: | ---: | ---: |
| 2025-10-12 14:30:00 | 30m | wave3_setup_12 | 0.510 | 0.05262 | 0.06299 |
| 2025-12-09 15:00:00 | 1h | taew_wave3_setup_nested_1212 | 0.950 | 0.04802 | 0.04848 |
| 2025-12-09 15:00:00 | 1h | taew_wave3_setup_12 | 0.993 | 0.04802 | 0.04848 |
| 2026-02-06 05:00:00 | 1h | wave3_setup_12 | 0.786 | 0.04596 | 0.05111 |
| 2025-08-22 14:00:00 | 1h | taew_wave3_setup_nested_1212 | 0.998 | 0.04544 | 0.05181 |

## Failure Examples

| Open Time | Timeframe | Type | Score | Close Ret 60m | Max Up 60m |
| --- | --- | --- | ---: | ---: | ---: |
| 2025-12-17 15:00:00 | 1h | taew_wave3_setup_12 | 0.965 | -0.04417 | 0.00813 |
| 2025-10-10 15:00:00 | 1h | taew_wave3_setup_12 | 0.983 | -0.04131 | 0.00073 |
| 2025-10-16 15:00:00 | 1h | taew_wave3_setup_nested_1212 | 0.939 | -0.03787 | 0.00113 |
| 2025-10-16 15:00:00 | 1h | taew_wave3_setup_12 | 0.963 | -0.03787 | 0.00113 |
| 2025-09-05 14:00:00 | 2h | taew_wave3_setup_12 | 0.948 | -0.03424 | 0.00000 |

## Recommendation

- Treat this as a first deterministic event-study baseline, not a production Elliott feature module.
- For 1h prediction research, the strongest practical timeframe is `30m` with the in-house detector: `203` events, `88.67%` 60m close-up rate, and `57.64%` +threshold hit rate.
- The in-house `1h` nested setup is cleaner but sparse: `16` events, `100.00%` 60m close-up rate.
- The package `taew` `1h` nested setup is broader but weaker: `140` events, `56.43%` 60m close-up rate.
- Do not build Elliott LightGBM features until this signal is retested for threshold stability and leakage with synthetic perturbation tests.
