import pandas as pd

from hk_value_screener.data_sources import HK_SPOT_FULL_COLUMN_ORDER, normalize_hk_spot_full


def test_normalize_hk_spot_full_keeps_value_fields() -> None:
    raw = pd.DataFrame(
        [
            [
                1,
                None,
                "3.5",
                "1.2",
                "0.04",
                "1000",
                "250000",
                "2.1",
                "0.8",
                "7.9",
                "1.1",
                None,
                "00005",
                None,
                "汇丰控股",
                "3.6",
                "3.4",
                "3.45",
                "3.46",
                "1234567890",
                "1000000000",
                "0.95",
                "4.5",
                "6.8",
                "0.2",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ]
        ]
    )

    normalized = normalize_hk_spot_full(raw)

    assert list(normalized.columns) == HK_SPOT_FULL_COLUMN_ORDER
    assert normalized.loc[0, "代码"] == "00005"
    assert normalized.loc[0, "市盈率-动态"] == 7.9
    assert normalized.loc[0, "市净率"] == 0.95
    assert normalized.loc[0, "总市值"] == 1234567890
    assert normalized.loc[0, "换手率"] == 0.8
