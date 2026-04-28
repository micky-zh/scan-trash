from pathlib import Path

import pandas as pd

from hk_value_screener.rules import apply_rule_set, load_rule_file


def test_load_hk_spot_baseline_rule_file() -> None:
    rule_file = load_rule_file(Path("rules/screening/hk_spot_baseline.yaml"))
    assert rule_file.rule_set.name == "hk_spot_baseline_first_pass"
    assert len(rule_file.rule_set.conditions) >= 1


def test_apply_hk_spot_baseline_rule_file_filters_realistic_columns() -> None:
    rule_file = load_rule_file(Path("rules/screening/hk_spot_baseline.yaml"))
    frame = pd.DataFrame(
        [
            {"代码": "00001", "名称": "A", "最新价": 0.4, "成交额": 1200000, "总市值": 6000000000, "市盈率-动态": 10},
            {"代码": "00002", "名称": "B", "最新价": 2.0, "成交额": 200000, "总市值": 6000000000, "市盈率-动态": 10},
            {"代码": "00003", "名称": "C", "最新价": 2.0, "成交额": 1200000, "总市值": 800000000, "市盈率-动态": 10},
            {"代码": "00004", "名称": "D", "最新价": 2.0, "成交额": 1200000, "总市值": 6000000000, "市盈率-动态": -5},
            {"代码": "00005", "名称": "E", "最新价": 2.0, "成交额": 1200000, "总市值": 6000000000, "市盈率-动态": 10},
        ]
    )
    screened = apply_rule_set(frame, rule_file.rule_set)
    assert list(screened["代码"]) == ["00005"]
