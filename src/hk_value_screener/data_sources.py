from __future__ import annotations

from pathlib import Path

import pandas as pd

from hk_value_screener.config import RAW_DATA_DIR

HK_SPOT_FULL_FIELDS = (
    "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,"
    "f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152"
)

HK_SPOT_FULL_COLUMN_ORDER = [
    "序号",
    "代码",
    "名称",
    "最新价",
    "涨跌幅",
    "涨跌额",
    "成交量",
    "成交额",
    "振幅",
    "换手率",
    "市盈率-动态",
    "量比",
    "最高",
    "最低",
    "今开",
    "昨收",
    "总市值",
    "流通市值",
    "涨速",
    "5分钟涨跌",
    "60日涨跌幅",
    "年初至今涨跌幅",
    "市净率",
]

_HK_SPOT_RAW_COLUMNS = [
    "index",
    "f1",
    "f2",
    "f3",
    "f4",
    "f5",
    "f6",
    "f7",
    "f8",
    "f9",
    "f10",
    "f11",
    "f12",
    "f13",
    "f14",
    "f15",
    "f16",
    "f17",
    "f18",
    "f20",
    "f21",
    "f23",
    "f24",
    "f25",
    "f22",
    "f62",
    "f128",
    "f140",
    "f141",
    "f136",
    "f115",
    "f152",
]

_HK_SPOT_COLUMN_MAP = {
    "index": "序号",
    "f2": "最新价",
    "f3": "涨跌幅",
    "f4": "涨跌额",
    "f5": "成交量",
    "f6": "成交额",
    "f7": "振幅",
    "f8": "换手率",
    "f9": "市盈率-动态",
    "f10": "量比",
    "f11": "5分钟涨跌",
    "f12": "代码",
    "f14": "名称",
    "f15": "最高",
    "f16": "最低",
    "f17": "今开",
    "f18": "昨收",
    "f20": "总市值",
    "f21": "流通市值",
    "f22": "涨速",
    "f23": "市净率",
    "f24": "60日涨跌幅",
    "f25": "年初至今涨跌幅",
}

_NUMERIC_COLUMNS = [
    "序号",
    "最新价",
    "涨跌幅",
    "涨跌额",
    "成交量",
    "成交额",
    "振幅",
    "换手率",
    "市盈率-动态",
    "量比",
    "最高",
    "最低",
    "今开",
    "昨收",
    "总市值",
    "流通市值",
    "涨速",
    "5分钟涨跌",
    "60日涨跌幅",
    "年初至今涨跌幅",
    "市净率",
]


def get_hk_spot_full() -> pd.DataFrame:
    """
    Fetch full Hong Kong spot data from Eastmoney and preserve value-screening fields.
    """
    from akshare.utils.func import fetch_paginated_data

    url = "https://72.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "100",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:128 t:3,m:128 t:4,m:128 t:1,m:128 t:2",
        "fields": HK_SPOT_FULL_FIELDS,
    }
    frame = fetch_paginated_data(url, params)
    return normalize_hk_spot_full(frame)


def normalize_hk_spot_full(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the full column mapping used by Eastmoney Hong Kong spot quotes.
    """
    normalized = frame.copy()
    normalized.columns = _HK_SPOT_RAW_COLUMNS[: len(normalized.columns)]
    normalized = normalized.rename(columns=_HK_SPOT_COLUMN_MAP)
    normalized = normalized.reindex(columns=HK_SPOT_FULL_COLUMN_ORDER).copy()

    for column in _NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    return normalized


def save_hk_spot_full_csv(frame: pd.DataFrame, path: Path | None = None) -> Path:
    """
    Save the fetched Hong Kong spot data to CSV.
    """
    output_path = path or (RAW_DATA_DIR / "hk_spot_full.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return output_path
