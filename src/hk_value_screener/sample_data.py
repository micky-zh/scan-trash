from __future__ import annotations

import pandas as pd


def sample_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "00005",
                "name": "汇丰控股",
                "price_hkd": 67.3,
                "market_cap_b_hkd": 1210.0,
                "pe_ttm": 8.9,
                "pb": 0.95,
                "roe_pct": 11.8,
                "debt_ratio_pct": 78.0,
                "avg_turnover_m_hkd": 820.0,
                "dividend_yield_pct": 6.2,
            },
            {
                "code": "00019",
                "name": "太古股份公司A",
                "price_hkd": 62.1,
                "market_cap_b_hkd": 560.0,
                "pe_ttm": 7.5,
                "pb": 0.36,
                "roe_pct": 5.9,
                "debt_ratio_pct": 31.0,
                "avg_turnover_m_hkd": 96.0,
                "dividend_yield_pct": 5.1,
            },
            {
                "code": "00341",
                "name": "大家乐集团",
                "price_hkd": 8.6,
                "market_cap_b_hkd": 5.0,
                "pe_ttm": 13.0,
                "pb": 1.2,
                "roe_pct": 9.5,
                "debt_ratio_pct": 24.0,
                "avg_turnover_m_hkd": 8.0,
                "dividend_yield_pct": 7.0,
            },
            {
                "code": "00883",
                "name": "中国海洋石油",
                "price_hkd": 19.1,
                "market_cap_b_hkd": 910.0,
                "pe_ttm": 5.7,
                "pb": 1.0,
                "roe_pct": 18.4,
                "debt_ratio_pct": 22.0,
                "avg_turnover_m_hkd": 1550.0,
                "dividend_yield_pct": 8.3,
            },
            {
                "code": "01398",
                "name": "工商银行",
                "price_hkd": 4.2,
                "market_cap_b_hkd": 1500.0,
                "pe_ttm": 4.8,
                "pb": 0.42,
                "roe_pct": 9.7,
                "debt_ratio_pct": 91.0,
                "avg_turnover_m_hkd": 430.0,
                "dividend_yield_pct": 7.5,
            },
        ]
    )
