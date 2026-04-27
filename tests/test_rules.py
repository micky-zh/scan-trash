from pathlib import Path

from hk_value_screener.rules import apply_rule_set, load_rule_file
from hk_value_screener.sample_data import sample_universe


def test_load_baseline_rule_file() -> None:
    rule_file = load_rule_file(Path("rules/screening/baseline.yaml"))
    assert rule_file.rule_set.name == "baseline_value_first_pass"
    assert len(rule_file.rule_set.conditions) >= 1


def test_apply_baseline_rule_file_filters_sample_universe() -> None:
    rule_file = load_rule_file(Path("rules/screening/baseline.yaml"))
    screened = apply_rule_set(sample_universe(), rule_file.rule_set)
    assert not screened.empty
    assert all(screened["price_hkd"] >= 0.5)
    assert all(screened["market_cap_b_hkd"] >= 10)
