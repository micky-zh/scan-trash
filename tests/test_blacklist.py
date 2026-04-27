from pathlib import Path

import pandas as pd

from hk_value_screener.rules import apply_blacklist, load_blacklist_file


def test_load_blacklist_file() -> None:
    blacklist = load_blacklist_file(Path("rules/blacklists/default.yaml"))
    assert blacklist.version == "0.1"
    assert len(blacklist.entries) >= 1


def test_apply_blacklist_filters_codes() -> None:
    blacklist = load_blacklist_file(Path("rules/blacklists/default.yaml"))
    frame = pd.DataFrame(
        [
            {"代码": "089988", "名称": "阿里巴巴-WR"},
            {"代码": "00700", "名称": "腾讯控股"},
        ]
    )
    filtered = apply_blacklist(frame, blacklist)
    assert "089988" not in set(filtered["代码"])
    assert "00700" in set(filtered["代码"])
