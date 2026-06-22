"""Smoke tests for strategy.strategy.session_naming."""

from strategy.strategy.session_naming import (
    derive_session_id,
    derive_strategy_session_id,
)


def test_derive_session_id_plan6_format() -> None:
    session_id = derive_session_id(
        "lgbm_solusdt_l_fw60_2101_2605",
        "lgbm_solusdt_s_fw60_2101_2605",
    )
    assert session_id == "strat_solusdt_fw60_combo_2101_2605"


def test_derive_session_id_falls_back_for_mismatched_pair() -> None:
    session_id = derive_session_id(
        "lgbm_solusdt_l_fw60_2101_2605",
        "lgbm_btcusdt_s_fw60_2101_2605",
    )
    # Falls back to the compact pairing id.
    assert session_id.startswith("strategy_")


def test_derive_strategy_session_id_compacts_matching_pair() -> None:
    session_id = derive_strategy_session_id(
        "lgbm_solusdt_l_fw60_2101_2605",
        "lgbm_solusdt_s_fw60_2101_2605",
    )
    assert session_id == "strategy_lgbm_solusdt_fw60_2101_2605"


def test_derive_strategy_session_id_falls_back_for_mismatched_pair() -> None:
    session_id = derive_strategy_session_id(
        "lgbm_solusdt_l_fw60_2101_2605",
        "lgbm_btcusdt_s_fw60_2101_2605",
    )
    assert session_id == (
        "strategy_lgbm_solusdt_l_fw60_2101_2605__"
        "lgbm_btcusdt_s_fw60_2101_2605"
    )
