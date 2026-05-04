from __future__ import annotations

import io
import json
import os
import re
import signal
import shutil
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

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

US_SPOT_FULL_COLUMN_ORDER = [
    "序号",
    "代码",
    "名称",
    "最新价",
    "涨跌额",
    "涨跌幅",
    "开盘价",
    "最高价",
    "最低价",
    "昨收价",
    "总市值",
    "市盈率",
    "成交量",
    "成交额",
    "振幅",
    "换手率",
]

CN_SPOT_FULL_COLUMN_ORDER = [
    "序号",
    "代码",
    "名称",
    "最新价",
    "涨跌幅",
    "涨跌额",
    "成交量",
    "成交额",
    "振幅",
    "最高",
    "最低",
    "今开",
    "昨收",
    "量比",
    "换手率",
    "市盈率-动态",
    "市净率",
    "总市值",
    "流通市值",
    "涨速",
    "5分钟涨跌",
    "60日涨跌幅",
    "年初至今涨跌幅",
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
    "f22",
    "f23",
    "f24",
    "f25",
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
    "f23": "市净率",
    "f24": "60日涨跌幅",
    "f25": "年初至今涨跌幅",
    "f22": "涨速",
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

HK_RESEARCH_VIEW_COLUMNS = [
    "代码",
    "名称",
    "所属行业",
    "最新价",
    "总市值",
    "成交额",
    "换手率",
    "市盈率-动态",
    "市净率",
    "股息率TTM(%)",
    "每股净资产(元)",
    "基本每股收益(元)",
    "每股经营现金流(元)",
    "股东权益回报率(%)",
    "净资产收益率(平均)(%)",
    "总资产收益率(%)",
    "投入资本回报率(%)",
    "毛利率(%)",
    "销售净利率(%)",
    "营业总收入",
    "净利润",
    "营业收入同比增长率(%)",
    "净利润同比增长率(%)",
    "EPS同比增长率(%)",
    "过去3年营业总收入CAGR(%)",
    "过去5年营业总收入CAGR(%)",
    "过去3年净利润CAGR(%)",
    "过去5年净利润CAGR(%)",
    "经营现金流净额",
    "资本开支",
    "自由现金流",
    "FCF/净利润",
    "每股自由现金流",
    "市销率",
    "市现率",
    "PEG",
    "流动比率",
    "速动比率",
    "净现比",
    "净负债/EBITDA",
    "净债务/EBITDA",
    "有息负债率(%)",
    "利息保障倍数",
    "应收账款周转率",
    "存货周转率",
    "总资产周转率",
    "过去5年净利润为正年数",
    "过去5年经营现金流为正年数",
    "过去5年经营现金流/净利润",
    "过去5年自由现金流为正年数",
    "资产负债率(%)",
    "现金比率",
    "最新公告日期",
    "最新财政年度",
    "最新分红方案",
    "补充数据状态",
    "黑名单",
    "黑名单原因",
]

US_RESEARCH_VIEW_COLUMNS = [
    "代码",
    "名称",
    "最新价",
    "总市值",
    "成交额",
    "换手率",
    "市盈率",
    "营业收入",
    "毛利",
    "归母净利润",
    "基本每股收益",
    "稀释每股收益",
    "毛利率(%)",
    "销售净利率(%)",
    "净资产收益率(平均)(%)",
    "总资产收益率(%)",
    "营业收入同比增长率(%)",
    "净利润同比增长率(%)",
    "EPS同比增长率(%)",
    "过去3年营业收入CAGR(%)",
    "过去5年营业收入CAGR(%)",
    "过去3年归母净利润CAGR(%)",
    "过去5年归母净利润CAGR(%)",
    "经营现金流净额",
    "资本开支",
    "自由现金流",
    "FCF/净利润",
    "每股自由现金流",
    "市销率",
    "市现率",
    "PEG",
    "流动比率",
    "速动比率",
    "经营现金流/流动负债",
    "资产负债率(%)",
    "权益比率(%)",
    "有息负债率(%)",
    "净债务/EBITDA",
    "过去5年归母净利润为正年数",
    "过去5年经营现金流为正年数",
    "过去5年经营现金流/净利润",
    "过去5年自由现金流为正年数",
    "应收账款周转率",
    "存货周转率",
    "总资产周转率",
    "报告期",
    "报告日期",
    "币种",
    "补充数据状态",
    "黑名单",
    "黑名单原因",
]

CN_RESEARCH_VIEW_COLUMNS = [
    "代码",
    "名称",
    "最新价",
    "总市值",
    "流通市值",
    "成交额",
    "换手率",
    "市盈率-动态",
    "市净率",
    "营业总收入",
    "营业总收入同比增长率",
    "净利润",
    "净利润同比增长率",
    "扣非净利润",
    "扣非净利润同比增长率",
    "基本每股收益",
    "每股净资产",
    "每股经营现金流",
    "销售毛利率(%)",
    "销售净利率(%)",
    "净资产收益率(%)",
    "加权净资产收益率(%)",
    "总资产收益率(%)",
    "营业收入同比增长率(%)",
    "净利润同比增长率(%)",
    "EPS同比增长率(%)",
    "经营现金流净额",
    "资本开支",
    "自由现金流",
    "FCF/净利润",
    "每股自由现金流",
    "市销率",
    "市现率",
    "PEG",
    "流动比率",
    "速动比率",
    "保守速动比率",
    "现金比率(%)",
    "利息支付倍数",
    "经营现金净流量与净利润的比率(%)",
    "经营现金净流量对负债比率(%)",
    "现金流量比率(%)",
    "资产负债率(%)",
    "产权比率",
    "有息负债率(%)",
    "应收账款周转率",
    "应收账款周转天数",
    "存货周转率",
    "存货周转天数",
    "总资产周转率",
    "报告期",
    "财务指标日期",
    "补充数据状态",
    "黑名单",
    "黑名单原因",
]

MARKET_CODE_PAD_WIDTH = {
    "hk": 5,
    "us": 0,
    "cn": 6,
}

CN_FILING_CATEGORIES = ["一季报", "半年报", "三季报", "年报"]
HK_FILING_CATEGORIES = ["一季报", "半年报", "三季报", "年报"]
US_FILING_CATEGORIES = ["10-K", "10-Q", "20-F", "6-K"]
US_FILING_CATEGORY_ALIASES = {
    "年报": "10-K",
}
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "hk-value-screener/1.0")
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_SUBMISSIONS_FILE_URL = "https://data.sec.gov/submissions/{name}"
SEC_ARCHIVE_INDEX_URL = (
    "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/index.json"
)
SEC_ARCHIVE_FILE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{name}"
HKEX_ACTIVE_STOCK_URL = "https://www1.hkexnews.hk/ncms/script/eds/activestock_sehk_e.json"
HKEX_TITLE_SEARCH_URL = "https://www1.hkexnews.hk/search/titlesearch.xhtml"


@dataclass(frozen=True)
class FinancialHistoryCacheResult:
    code: str
    added_rows_by_statement: dict[str, int]
    paths_by_statement: dict[str, Path]
    status: str

    @property
    def added_rows(self) -> int:
        return sum(self.added_rows_by_statement.values())


@dataclass
class FilingCacheResult:
    code: str
    added_rows: int
    downloaded_files: int
    index_path: Path
    status: str


def get_hk_spot_full() -> pd.DataFrame:
    """
    Fetch full Hong Kong spot data from Eastmoney and preserve research fields.
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


def get_us_spot_full() -> pd.DataFrame:
    """
    Fetch full United States spot data from Eastmoney through AKShare.
    """
    import akshare as ak

    frame = ak.stock_us_spot_em()
    return normalize_us_spot_full(frame)


def get_cn_spot_full() -> pd.DataFrame:
    """
    Fetch full China A-share spot data from Eastmoney through AKShare.
    """
    import akshare as ak

    frame = ak.stock_zh_a_spot_em()
    return normalize_cn_spot_full(frame)


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


def normalize_us_spot_full(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize AKShare's Eastmoney US spot quotes to the project's column style.
    """
    normalized = frame.copy()

    if "代码" in normalized.columns:
        normalized["代码"] = normalize_security_codes(normalized["代码"], market="us")

    missing_columns = [
        column for column in US_SPOT_FULL_COLUMN_ORDER if column not in normalized.columns
    ]
    for column in missing_columns:
        normalized[column] = pd.NA

    numeric_columns = [
        column for column in US_SPOT_FULL_COLUMN_ORDER if column not in {"名称", "代码"}
    ]
    for column in numeric_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    return normalized.reindex(columns=US_SPOT_FULL_COLUMN_ORDER).copy()


def normalize_cn_spot_full(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize AKShare's Eastmoney A-share spot quotes to the project's column style.
    """
    normalized = frame.copy()

    if "代码" in normalized.columns:
        normalized["代码"] = normalize_security_codes(normalized["代码"], market="cn")

    missing_columns = [
        column for column in CN_SPOT_FULL_COLUMN_ORDER if column not in normalized.columns
    ]
    for column in missing_columns:
        normalized[column] = pd.NA

    numeric_columns = [
        column for column in CN_SPOT_FULL_COLUMN_ORDER if column not in {"名称", "代码"}
    ]
    for column in numeric_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    return normalized.reindex(columns=CN_SPOT_FULL_COLUMN_ORDER).copy()


def save_spot_full_csv(frame: pd.DataFrame, path: Path | None = None, market: str = "hk") -> Path:
    """
    Save the fetched Hong Kong spot data to CSV.
    """
    default_name = f"{market}_spot_full.csv"
    output_path = path or (RAW_DATA_DIR / default_name)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return output_path


def normalize_security_code(value: object, market: str = "hk") -> str:
    text = str(value).strip()
    if market in {"hk", "cn"}:
        digits = "".join(character for character in text if character.isdigit())
        return digits.zfill(MARKET_CODE_PAD_WIDTH[market]) if digits else text
    if "." in text:
        text = text.split(".", 1)[1]
    return text.upper()


def normalize_security_codes(series: pd.Series, market: str = "hk") -> pd.Series:
    return series.astype(str).map(lambda value: normalize_security_code(value, market=market))


def _to_number(value: object) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)

    text = str(value).strip().replace(",", "")
    if not text or text in {"False", "None", "nan", "--", "-"}:
        return None

    multiplier = 1.0
    if text.endswith("%"):
        text = text[:-1]
    if text.endswith("万亿"):
        multiplier = 1_000_000_000_000
        text = text[:-2]
    elif text.endswith("亿"):
        multiplier = 100_000_000
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 10_000
        text = text[:-1]

    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _safe_divide(numerator: object, denominator: object) -> float | None:
    numerator_number = _to_number(numerator)
    denominator_number = _to_number(denominator)
    if numerator_number is None or denominator_number in (None, 0):
        return None
    return numerator_number / denominator_number


def _percentage_growth(current: object, previous: object) -> float | None:
    ratio = _safe_divide(current, previous)
    if ratio is None:
        return None
    return (ratio - 1) * 100


def _safe_subtract(left: object, right: object) -> float | None:
    left_number = _to_number(left)
    right_number = _to_number(right)
    if left_number is None or right_number is None:
        return None
    return left_number - right_number


def _sum_if_any(values: list[object]) -> float | None:
    numbers = [_to_number(value) for value in values]
    valid_numbers = [number for number in numbers if number is not None]
    if not valid_numbers:
        return None
    return sum(valid_numbers)


def _absolute_number(value: object) -> float | None:
    number = _to_number(value)
    return abs(number) if number is not None else None


def _latest_and_previous_report_rows(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty or "REPORT_DATE" not in frame.columns:
        return pd.DataFrame(), pd.DataFrame()

    dates = sorted(frame["REPORT_DATE"].dropna().astype(str).unique(), reverse=True)
    latest = frame[frame["REPORT_DATE"].astype(str) == dates[0]].copy() if dates else pd.DataFrame()
    previous = (
        frame[frame["REPORT_DATE"].astype(str) == dates[1]].copy()
        if len(dates) > 1
        else pd.DataFrame()
    )
    return latest, previous


def _sorted_report_dates(frame: pd.DataFrame, report_column: str) -> list[str]:
    if frame.empty or report_column not in frame.columns:
        return []

    raw_dates = frame[report_column].dropna().astype(str).drop_duplicates().tolist()
    if not raw_dates:
        return []

    parsed_dates = {date: pd.to_datetime(date, errors="coerce") for date in raw_dates}
    if any(pd.notna(value) for value in parsed_dates.values()):
        return sorted(
            raw_dates,
            key=lambda date: (
                pd.isna(parsed_dates[date]),
                parsed_dates[date] if pd.notna(parsed_dates[date]) else date,
                date,
            ),
        )
    return sorted(raw_dates)


def _annual_metric_map(
    frame: pd.DataFrame,
    report_column: str,
    patterns: list[str],
) -> dict[str, float]:
    if frame.empty or report_column not in frame.columns:
        return {}

    metric_map: dict[str, float] = {}
    report_dates = frame[report_column].astype(str)
    for report_date in _sorted_report_dates(frame, report_column):
        subset = frame[report_dates == report_date]
        value = _to_number(_find_amount(subset, patterns))
        if value is not None:
            metric_map[report_date] = value
    return metric_map


def _annual_free_cash_flow_map(
    frame: pd.DataFrame,
    report_column: str,
    operating_patterns: list[str],
    capex_pattern_groups: list[list[str]],
) -> dict[str, float]:
    if frame.empty or report_column not in frame.columns:
        return {}

    metric_map: dict[str, float] = {}
    report_dates = frame[report_column].astype(str)
    for report_date in _sorted_report_dates(frame, report_column):
        subset = frame[report_dates == report_date]
        operating_cash_flow = _to_number(_find_amount(subset, operating_patterns))
        capex_items = [
            _absolute_number(_find_amount(subset, patterns))
            for patterns in capex_pattern_groups
        ]
        capital_expenditure = _sum_if_any(capex_items)
        if operating_cash_flow is None or capital_expenditure is None:
            continue
        metric_map[report_date] = operating_cash_flow - capital_expenditure
    return metric_map


def _annual_cagr(metric_map: dict[str, float], years: int) -> float | None:
    values = list(metric_map.values())
    if len(values) < years + 1:
        return None
    first = values[-(years + 1)]
    last = values[-1]
    if first <= 0 or last <= 0:
        return None
    return ((last / first) ** (1 / years) - 1) * 100


def _positive_year_count(metric_map: dict[str, float], years: int = 5) -> int | None:
    values = list(metric_map.values())
    if len(values) < years:
        return None
    return sum(value > 0 for value in values[-years:])


def _ratio_from_annual_maps(
    numerator_map: dict[str, float],
    denominator_map: dict[str, float],
    years: int = 5,
) -> float | None:
    common_dates = [
        report_date
        for report_date in numerator_map
        if report_date in denominator_map
    ]
    if len(common_dates) < years:
        return None

    selected_dates = common_dates[-years:]
    numerator = sum(numerator_map[report_date] for report_date in selected_dates)
    denominator = sum(denominator_map[report_date] for report_date in selected_dates)
    return _safe_divide(numerator, denominator)


def build_hk_research_view(
    base_frame: pd.DataFrame,
    enriched_metrics_frame: pd.DataFrame,
    blacklist_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build an Excel-friendly Hong Kong value research view from a quote universe.
    """
    base = base_frame.copy()
    base["代码"] = normalize_security_codes(base["代码"], market="hk")

    if enriched_metrics_frame.empty or "代码" not in enriched_metrics_frame.columns:
        merged = base
    else:
        metrics = enriched_metrics_frame.copy()
        metrics["代码"] = normalize_security_codes(metrics["代码"], market="hk")
        metric_columns = [
            column
            for column in HK_RESEARCH_VIEW_COLUMNS
            if column == "代码" or (column in metrics.columns and column not in base.columns)
        ]
        merged = base.merge(metrics[metric_columns], on="代码", how="left")

    _add_common_valuation_metrics(
        merged,
        market_cap_column="总市值",
        pe_column="市盈率-动态",
        revenue_column="营业总收入",
        net_profit_growth_column="净利润同比增长率(%)",
        operating_cash_flow_column="经营现金流净额",
    )
    merged = _apply_blacklist_annotations(merged, market="hk", blacklist_frame=blacklist_frame)
    selected_columns = [column for column in HK_RESEARCH_VIEW_COLUMNS if column in merged.columns]
    return merged[selected_columns].copy()


def build_us_research_view(
    base_frame: pd.DataFrame,
    enriched_metrics_frame: pd.DataFrame,
    blacklist_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build an Excel-friendly US value research view from a quote universe.
    """
    base = base_frame.copy()
    base["代码"] = normalize_security_codes(base["代码"], market="us")

    if enriched_metrics_frame.empty or "代码" not in enriched_metrics_frame.columns:
        merged = base
    else:
        metrics = enriched_metrics_frame.copy()
        metrics["代码"] = normalize_security_codes(metrics["代码"], market="us")
        metric_columns = [
            column
            for column in US_RESEARCH_VIEW_COLUMNS
            if column == "代码" or (column in metrics.columns and column not in base.columns)
        ]
        merged = base.merge(metrics[metric_columns], on="代码", how="left")

    _add_common_valuation_metrics(
        merged,
        market_cap_column="总市值",
        pe_column="市盈率",
        revenue_column="营业收入",
        net_profit_growth_column="净利润同比增长率(%)",
        operating_cash_flow_column="经营现金流净额",
    )
    merged = _apply_blacklist_annotations(merged, market="us", blacklist_frame=blacklist_frame)
    selected_columns = [column for column in US_RESEARCH_VIEW_COLUMNS if column in merged.columns]
    return merged[selected_columns].copy()


def build_cn_research_view(
    base_frame: pd.DataFrame,
    enriched_metrics_frame: pd.DataFrame,
    blacklist_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build an Excel-friendly China A-share value research view from a quote universe.
    """
    base = base_frame.copy()
    base["代码"] = normalize_security_codes(base["代码"], market="cn")

    if enriched_metrics_frame.empty or "代码" not in enriched_metrics_frame.columns:
        merged = base
    else:
        metrics = enriched_metrics_frame.copy()
        metrics["代码"] = normalize_security_codes(metrics["代码"], market="cn")
        metric_columns = [
            column
            for column in CN_RESEARCH_VIEW_COLUMNS
            if column == "代码" or (column in metrics.columns and column not in base.columns)
        ]
        merged = base.merge(metrics[metric_columns], on="代码", how="left")

    _add_common_valuation_metrics(
        merged,
        market_cap_column="总市值",
        pe_column="市盈率-动态",
        revenue_column="营业总收入",
        net_profit_growth_column="净利润同比增长率(%)",
        operating_cash_flow_column="经营现金流净额",
    )
    merged = _apply_blacklist_annotations(merged, market="cn", blacklist_frame=blacklist_frame)
    selected_columns = [column for column in CN_RESEARCH_VIEW_COLUMNS if column in merged.columns]
    return merged[selected_columns].copy()


def _add_common_valuation_metrics(
    frame: pd.DataFrame,
    market_cap_column: str,
    pe_column: str,
    revenue_column: str,
    net_profit_growth_column: str,
    operating_cash_flow_column: str,
) -> None:
    if "市销率" not in frame.columns and {market_cap_column, revenue_column} <= set(frame.columns):
        frame["市销率"] = frame.apply(
            lambda row: _safe_divide(row.get(market_cap_column), row.get(revenue_column)),
            axis=1,
        )
    if "市现率" not in frame.columns and {market_cap_column, operating_cash_flow_column} <= set(
        frame.columns
    ):
        frame["市现率"] = frame.apply(
            lambda row: _safe_divide(
                row.get(market_cap_column),
                row.get(operating_cash_flow_column),
            ),
            axis=1,
        )
    if "PEG" not in frame.columns and {pe_column, net_profit_growth_column} <= set(frame.columns):
        frame["PEG"] = frame.apply(
            lambda row: _safe_divide(row.get(pe_column), row.get(net_profit_growth_column)),
            axis=1,
        )


def _apply_blacklist_annotations(
    frame: pd.DataFrame,
    market: str,
    blacklist_frame: pd.DataFrame | None,
) -> pd.DataFrame:
    output = frame.copy()
    output["黑名单"] = ""
    output["黑名单原因"] = ""

    if blacklist_frame is None or blacklist_frame.empty or "代码" not in output.columns:
        return output

    blacklist = blacklist_frame.copy()
    if "market" in blacklist.columns:
        blacklist = blacklist[blacklist["market"].astype(str).str.lower() == market]
    if blacklist.empty or "code" not in blacklist.columns:
        return output

    if "enabled" in blacklist.columns:
        blacklist = blacklist[blacklist["enabled"].fillna(False).astype(bool)]
    if blacklist.empty:
        return output

    blacklist["code"] = normalize_security_codes(blacklist["code"], market=market)
    if "reason" not in blacklist.columns:
        blacklist["reason"] = ""
    reason_map = (
        blacklist.dropna(subset=["code"])
        .drop_duplicates(subset=["code"], keep="last")
        .set_index("code")["reason"]
        .fillna("")
    )
    if reason_map.empty:
        return output

    masked = output["代码"].isin(reason_map.index)
    output.loc[masked, "黑名单"] = "是"
    output.loc[masked, "黑名单原因"] = output.loc[masked, "代码"].map(reason_map).fillna("")
    return output


def _fetch_hk_financial_indicator_snapshot(symbol: str) -> dict[str, object]:
    import akshare as ak

    frame = ak.stock_hk_financial_indicator_em(symbol=symbol)
    if frame.empty:
        return {}

    row = frame.iloc[0]
    previous_row = frame.iloc[1] if len(frame) > 1 else None
    selected_fields = [
        "基本每股收益(元)",
        "每股净资产(元)",
        "每股股息TTM(港元)",
        "股息率TTM(%)",
        "派息比率(%)",
        "每股经营现金流(元)",
        "股东权益回报率(%)",
        "总资产回报率(%)",
        "销售净利率(%)",
        "营业总收入",
        "净利润",
    ]
    result = {field: row.get(field) for field in selected_fields}
    result["EPS同比增长率(%)"] = (
        _percentage_growth(row.get("基本每股收益(元)"), previous_row.get("基本每股收益(元)"))
        if previous_row is not None
        else None
    )
    return result


def _fetch_hk_analysis_indicator_snapshot(symbol: str) -> dict[str, object]:
    import akshare as ak

    frame = ak.stock_financial_hk_analysis_indicator_em(symbol=symbol, indicator="年度")
    if frame.empty:
        return {}

    row = frame.iloc[0]
    return {
        "净资产收益率(平均)(%)": row.get("ROE_AVG"),
        "投入资本回报率(%)": row.get("ROIC_YEARLY"),
        "总资产收益率(%)": row.get("ROA"),
        "资产负债率(%)": row.get("DEBT_ASSET_RATIO"),
        "流动比率": row.get("CURRENT_RATIO"),
        "毛利率(%)": row.get("GROSS_PROFIT_RATIO"),
        "营业收入同比增长率(%)": row.get("OPERATE_INCOME_YOY"),
        "营业总收入环比(%)": row.get("OPERATE_INCOME_QOQ"),
        "净利润同比增长率(%)": row.get("HOLDER_PROFIT_YOY"),
        "净利润环比(%)": row.get("HOLDER_PROFIT_QOQ"),
    }


def _fetch_hk_dividend_snapshot(symbol: str) -> dict[str, object]:
    import akshare as ak

    frame = ak.stock_hk_dividend_payout_em(symbol=symbol)
    if frame.empty:
        return {}

    row = frame.iloc[0]
    return {
        "最新公告日期": row.get("最新公告日期"),
        "最新财政年度": row.get("财政年度"),
        "最新分红方案": row.get("分红方案"),
        "最新分配类型": row.get("分配类型"),
        "最新除净日": row.get("除净日"),
        "最新发放日": row.get("发放日"),
    }


def _fetch_hk_company_profile_snapshot(symbol: str) -> dict[str, object]:
    import akshare as ak

    frame = ak.stock_hk_company_profile_em(symbol=symbol)
    if frame.empty:
        return {}

    row = frame.iloc[0]
    return {
        "公司名称": row.get("公司名称"),
        "英文名称": row.get("英文名称"),
        "所属行业": row.get("所属行业"),
        "员工人数": row.get("员工人数"),
    }


def _fetch_hk_security_profile_snapshot(symbol: str) -> dict[str, object]:
    import akshare as ak

    frame = ak.stock_hk_security_profile_em(symbol=symbol)
    if frame.empty:
        return {}

    row = frame.iloc[0]
    return {
        "上市日期": row.get("上市日期"),
        "板块": row.get("板块"),
        "每手股数": row.get("每手股数"),
        "是否沪港通标的": row.get("是否沪港通标的"),
        "是否深港通标的": row.get("是否深港通标的"),
    }


def _latest_report_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    latest_report_date = frame["REPORT_DATE"].dropna().astype(str).max()
    return frame[frame["REPORT_DATE"].astype(str) == latest_report_date].copy()


def _find_amount(frame: pd.DataFrame, patterns: list[str]) -> float | None:
    name_column = "STD_ITEM_NAME" if "STD_ITEM_NAME" in frame.columns else "ITEM_NAME"
    if frame.empty or name_column not in frame.columns or "AMOUNT" not in frame.columns:
        return None

    names = frame[name_column].astype(str)

    for pattern in patterns:
        exact = frame[names == pattern]
        if not exact.empty:
            return pd.to_numeric(exact.iloc[0]["AMOUNT"], errors="coerce")

    for pattern in patterns:
        fuzzy = frame[names.str.contains(pattern, na=False)]
        if not fuzzy.empty:
            return pd.to_numeric(fuzzy.iloc[0]["AMOUNT"], errors="coerce")

    return None


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _clean_missing_metric_value(value: object) -> object:
    if isinstance(value, bool):
        return pd.NA
    if value is None:
        return pd.NA
    try:
        if pd.isna(value):
            return pd.NA
    except TypeError:
        return value
    return value


def _clean_missing_metric_values(record: dict[str, object]) -> dict[str, object]:
    return {key: _clean_missing_metric_value(value) for key, value in record.items()}


def _first_present(*values: object) -> object:
    for value in values:
        if _to_number(value) is not None:
            return value
    return pd.NA


class RequestTimeoutError(TimeoutError):
    """Raised when a single-stock enrichment request exceeds the timeout."""


@contextmanager
def _time_limit(seconds: int):
    if seconds <= 0:
        yield
        return

    def handler(signum, frame):  # pragma: no cover - signal path
        raise RequestTimeoutError(f"single-stock enrichment timed out after {seconds}s")

    previous = signal.signal(signal.SIGALRM, handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _fetch_hk_derived_report_metrics(symbol: str) -> dict[str, object]:
    balance_sheet = _read_financial_history_cache("hk", symbol, "balance")
    income_statement = _read_financial_history_cache("hk", symbol, "income")
    cash_flow = _read_financial_history_cache("hk", symbol, "cashflow")

    if balance_sheet.empty or income_statement.empty or cash_flow.empty:
        import akshare as ak

        if balance_sheet.empty:
            balance_sheet = ak.stock_financial_hk_report_em(
                stock=symbol,
                symbol="资产负债表",
                indicator="年度",
            )
        if income_statement.empty:
            income_statement = ak.stock_financial_hk_report_em(
                stock=symbol,
                symbol="利润表",
                indicator="年度",
            )
        if cash_flow.empty:
            cash_flow = ak.stock_financial_hk_report_em(
                stock=symbol,
                symbol="现金流量表",
                indicator="年度",
            )

    bs = _latest_report_rows(balance_sheet)
    latest_income, previous_income = _latest_and_previous_report_rows(income_statement)
    cf = _latest_report_rows(cash_flow)

    revenue = _find_amount(latest_income, ["营业额", "营运收入"])
    previous_revenue = _find_amount(previous_income, ["营业额", "营运收入"])
    shareholder_profit = _find_amount(latest_income, ["股东应占溢利"])
    previous_shareholder_profit = _find_amount(previous_income, ["股东应占溢利"])
    total_assets = _find_amount(bs, ["总资产"])
    current_assets = _find_amount(bs, ["流动资产合计"])
    current_liabilities = _find_amount(bs, ["流动负债合计"])
    inventory = _find_amount(bs, ["存货"])
    cash_and_equivalents = _find_amount(bs, ["现金及等价物", "现金及现金等价物", "现金及等价物"])
    receivables = _find_amount(bs, ["应收帐款", "应收账款", "贸易应收款", "贸易应收账款"])
    short_term_debt = _find_amount(bs, ["短期贷款"])
    long_term_debt = _find_amount(bs, ["长期贷款"])
    operating_cash_flow = _find_amount(cf, ["经营业务现金净额", "经营产生现金", "经营活动现金净额"])
    capex_fixed_assets = _find_amount(cf, ["购建固定资产"])
    capex_intangible_assets = _find_amount(cf, ["购建无形资产及其他资产"])
    depreciation_amortization = _find_amount(cf, ["折旧及摊销"])

    quick_ratio = _safe_ratio(
        None if current_assets is None or inventory is None else current_assets - inventory,
        current_liabilities,
    )
    cash_ratio = _safe_ratio(cash_and_equivalents, current_liabilities)
    capital_expenditure = _sum_if_any(
        [_absolute_number(capex_fixed_assets), _absolute_number(capex_intangible_assets)]
    )
    free_cash_flow = _safe_subtract(operating_cash_flow, capital_expenditure)
    interest_bearing_debt = _sum_if_any([short_term_debt, long_term_debt])
    shares = _safe_divide(
        shareholder_profit,
        _find_amount(latest_income, ["每股盈利", "基本每股盈利"]),
    )
    ebitda = (
        None
        if shareholder_profit is None and depreciation_amortization is None
        else (shareholder_profit or 0) + (depreciation_amortization or 0)
    )

    revenue_map = _annual_metric_map(income_statement, "REPORT_DATE", ["营业额", "营运收入"])
    profit_map = _annual_metric_map(income_statement, "REPORT_DATE", ["股东应占溢利"])
    operating_cash_flow_map = _annual_metric_map(
        cash_flow,
        "REPORT_DATE",
        ["经营业务现金净额", "经营产生现金", "经营活动现金净额"],
    )
    free_cash_flow_map = _annual_free_cash_flow_map(
        cash_flow,
        "REPORT_DATE",
        ["经营业务现金净额", "经营产生现金", "经营活动现金净额"],
        [["购建固定资产"], ["购建无形资产及其他资产"]],
    )

    return {
        "营业总收入": revenue,
        "流动资产合计": current_assets,
        "流动负债合计": current_liabilities,
        "存货": inventory,
        "现金及等价物": cash_and_equivalents,
        "应收账款": receivables,
        "经营业务现金净额": operating_cash_flow,
        "经营现金流净额": operating_cash_flow,
        "资本开支": capital_expenditure,
        "自由现金流": free_cash_flow,
        "FCF/净利润": _safe_divide(free_cash_flow, shareholder_profit),
        "每股自由现金流": _safe_divide(free_cash_flow, shares),
        "过去3年营业总收入CAGR(%)": _annual_cagr(revenue_map, 3),
        "过去5年营业总收入CAGR(%)": _annual_cagr(revenue_map, 5),
        "过去3年净利润CAGR(%)": _annual_cagr(profit_map, 3),
        "过去5年净利润CAGR(%)": _annual_cagr(profit_map, 5),
        "营业收入同比增长率(%)": _percentage_growth(revenue, previous_revenue),
        "净利润同比增长率(%)": _percentage_growth(
            shareholder_profit,
            previous_shareholder_profit,
        ),
        "有息负债率(%)": (
            _safe_divide(interest_bearing_debt, total_assets) * 100
            if _safe_divide(interest_bearing_debt, total_assets) is not None
            else None
        ),
        "EBITDA": ebitda,
        "净债务/EBITDA": _safe_divide(
            None
            if interest_bearing_debt is None and cash_and_equivalents is None
            else (interest_bearing_debt or 0) - (cash_and_equivalents or 0),
            ebitda,
        ),
        "应收账款周转率": _safe_divide(revenue, receivables),
        "存货周转率": _safe_divide(revenue, inventory),
        "总资产周转率": _safe_divide(revenue, total_assets),
        "速动比率": quick_ratio,
        "现金比率": cash_ratio,
        "过去5年净利润为正年数": _positive_year_count(profit_map, 5),
        "过去5年经营现金流为正年数": _positive_year_count(operating_cash_flow_map, 5),
        "过去5年经营现金流/净利润": _ratio_from_annual_maps(
            operating_cash_flow_map,
            profit_map,
            5,
        ),
        "过去5年自由现金流为正年数": _positive_year_count(free_cash_flow_map, 5),
    }


def fetch_hk_enriched_metrics(symbol: str, timeout_seconds: int = 20) -> dict[str, object]:
    """
    Fetch a richer Hong Kong stock profile using AKShare's Hong Kong-specific endpoints.
    """
    normalized_symbol = normalize_security_code(symbol, market="hk")
    result: dict[str, object] = {"代码": normalized_symbol, "补充数据状态": "成功"}

    try:
        with _time_limit(timeout_seconds):
            result.update(_fetch_hk_financial_indicator_snapshot(normalized_symbol))
            result.update(_fetch_hk_analysis_indicator_snapshot(normalized_symbol))
            result.update(_fetch_hk_dividend_snapshot(normalized_symbol))
            result.update(_fetch_hk_company_profile_snapshot(normalized_symbol))
            result.update(_fetch_hk_security_profile_snapshot(normalized_symbol))
            result.update(_fetch_hk_derived_report_metrics(normalized_symbol))

            operating_cash_flow = pd.to_numeric(result.get("经营业务现金净额"), errors="coerce")
            net_profit = pd.to_numeric(result.get("净利润"), errors="coerce")
            result["净现比"] = _safe_ratio(
                None if pd.isna(operating_cash_flow) else float(operating_cash_flow),
                None if pd.isna(net_profit) else float(net_profit),
            )
            shares = _safe_divide(result.get("净利润"), result.get("基本每股收益(元)"))
            result["每股自由现金流"] = _safe_divide(result.get("自由现金流"), shares)
            if result.get("净债务/EBITDA") is not None and result.get("净负债/EBITDA") is None:
                result["净负债/EBITDA"] = result.get("净债务/EBITDA")
            if result.get("利息支付倍数") is not None and result.get("利息保障倍数") is None:
                result["利息保障倍数"] = result.get("利息支付倍数")
    except Exception as exc:  # pragma: no cover - network failure path
        result["补充数据状态"] = f"失败: {str(exc)[:120]}"

    return _clean_missing_metric_values(result)


def fetch_us_enriched_metrics(symbol: str, timeout_seconds: int = 20) -> dict[str, object]:
    """
    Fetch a richer US stock profile using AKShare's Eastmoney US F10 endpoint.
    """
    import akshare as ak

    normalized_symbol = normalize_security_code(symbol, market="us")
    result: dict[str, object] = {"代码": normalized_symbol, "补充数据状态": "成功"}

    try:
        with _time_limit(timeout_seconds):
            frame = ak.stock_financial_us_analysis_indicator_em(
                symbol=normalized_symbol,
                indicator="年报",
            )
            if frame.empty:
                result["补充数据状态"] = "失败: empty financial indicator response"
                return _clean_missing_metric_values(result)

            row = frame.iloc[0]
            previous_row = frame.iloc[1] if len(frame) > 1 else None
            result.update(
                {
                    "营业收入": row.get("OPERATE_INCOME"),
                    "毛利": row.get("GROSS_PROFIT"),
                    "归母净利润": row.get("PARENT_HOLDER_NETPROFIT"),
                    "基本每股收益": row.get("BASIC_EPS"),
                    "稀释每股收益": row.get("DILUTED_EPS"),
                    "毛利率(%)": row.get("GROSS_PROFIT_RATIO"),
                    "销售净利率(%)": row.get("NET_PROFIT_RATIO"),
                    "净资产收益率(平均)(%)": row.get("ROE_AVG"),
                    "总资产收益率(%)": row.get("ROA"),
                    "流动比率": row.get("CURRENT_RATIO"),
                    "速动比率": row.get("SPEED_RATIO"),
                    "经营现金流/流动负债": row.get("OCF_LIQDEBT"),
                    "资产负债率(%)": row.get("DEBT_ASSET_RATIO"),
                    "权益比率(%)": row.get("EQUITY_RATIO"),
                    "EPS同比增长率(%)": (
                        _percentage_growth(row.get("BASIC_EPS"), previous_row.get("BASIC_EPS"))
                        if previous_row is not None
                        else None
                    ),
                    "报告期": row.get("REPORT_TYPE"),
                    "报告日期": row.get("REPORT_DATE"),
                    "币种": row.get("CURRENCY_ABBR"),
                }
            )
            result.update(_fetch_us_derived_report_metrics(normalized_symbol, row.get("BASIC_EPS")))
    except Exception as exc:  # pragma: no cover - network failure path
        result["补充数据状态"] = f"失败: {str(exc)[:120]}"

    return _clean_missing_metric_values(result)


def _fetch_us_derived_report_metrics(symbol: str, basic_eps: object) -> dict[str, object]:
    balance_sheet = _read_financial_history_cache("us", symbol, "balance")
    income_statement = _read_financial_history_cache("us", symbol, "income")
    cash_flow = _read_financial_history_cache("us", symbol, "cashflow")

    if balance_sheet.empty or income_statement.empty or cash_flow.empty:
        import akshare as ak

        def fetch_statement(symbol_name: str) -> pd.DataFrame:
            try:
                frame = ak.stock_financial_us_report_em(
                    stock=symbol,
                    symbol=symbol_name,
                    indicator="年报",
                )
            except TypeError as exc:
                if "'NoneType' object is not subscriptable" in str(exc):
                    return pd.DataFrame()
                raise
            return frame if frame is not None else pd.DataFrame()

        if balance_sheet.empty:
            balance_sheet = fetch_statement("资产负债表")
        if income_statement.empty:
            income_statement = fetch_statement("综合损益表")
        if cash_flow.empty:
            cash_flow = fetch_statement("现金流量表")

    latest_balance, _ = _latest_and_previous_report_rows(balance_sheet)
    latest_income, previous_income = _latest_and_previous_report_rows(income_statement)
    latest_cash_flow, _ = _latest_and_previous_report_rows(cash_flow)

    revenue = _find_amount(latest_income, ["营业收入", "主营收入"])
    previous_revenue = _find_amount(previous_income, ["营业收入", "主营收入"])
    net_profit = _find_amount(
        latest_income,
        ["归属于普通股股东净利润", "归属于母公司股东净利润", "净利润"],
    )
    previous_net_profit = _find_amount(
        previous_income,
        ["归属于普通股股东净利润", "归属于母公司股东净利润", "净利润"],
    )
    operating_cash_flow = _find_amount(latest_cash_flow, ["经营活动产生的现金流量净额"])
    capital_expenditure = _absolute_number(_find_amount(latest_cash_flow, ["购买固定资产"]))
    depreciation_amortization = _find_amount(latest_cash_flow, ["折旧及摊销"])
    free_cash_flow = _safe_subtract(operating_cash_flow, capital_expenditure)
    total_assets = _find_amount(latest_balance, ["总资产"])
    cash_and_equivalents = _find_amount(latest_balance, ["现金及现金等价物"])
    short_term_debt = _find_amount(latest_balance, ["短期债务"])
    long_term_debt = _find_amount(latest_balance, ["长期负债"])
    receivables = _find_amount(latest_balance, ["应收账款"])
    inventory = _find_amount(latest_balance, ["存货"])
    interest_bearing_debt = _sum_if_any([short_term_debt, long_term_debt])
    shares = _safe_divide(net_profit, basic_eps)
    ebitda = (
        None
        if net_profit is None and depreciation_amortization is None
        else (net_profit or 0) + (depreciation_amortization or 0)
    )

    revenue_map = _annual_metric_map(income_statement, "REPORT_DATE", ["营业收入", "主营收入"])
    profit_map = _annual_metric_map(
        income_statement,
        "REPORT_DATE",
        ["归属于普通股股东净利润", "归属于母公司股东净利润", "净利润"],
    )
    operating_cash_flow_map = _annual_metric_map(
        cash_flow,
        "REPORT_DATE",
        ["经营活动产生的现金流量净额"],
    )
    free_cash_flow_map = _annual_free_cash_flow_map(
        cash_flow,
        "REPORT_DATE",
        ["经营活动产生的现金流量净额"],
        [["购买固定资产"]],
    )

    return {
        "经营现金流净额": operating_cash_flow,
        "资本开支": capital_expenditure,
        "自由现金流": free_cash_flow,
        "FCF/净利润": _safe_divide(free_cash_flow, net_profit),
        "每股自由现金流": _safe_divide(free_cash_flow, shares),
        "过去3年营业收入CAGR(%)": _annual_cagr(revenue_map, 3),
        "过去5年营业收入CAGR(%)": _annual_cagr(revenue_map, 5),
        "过去3年归母净利润CAGR(%)": _annual_cagr(profit_map, 3),
        "过去5年归母净利润CAGR(%)": _annual_cagr(profit_map, 5),
        "营业收入同比增长率(%)": _percentage_growth(revenue, previous_revenue),
        "净利润同比增长率(%)": _percentage_growth(net_profit, previous_net_profit),
        "有息负债率(%)": (
            _safe_divide(interest_bearing_debt, total_assets) * 100
            if _safe_divide(interest_bearing_debt, total_assets) is not None
            else None
        ),
        "净债务/EBITDA": _safe_divide(
            None
            if interest_bearing_debt is None and cash_and_equivalents is None
            else (interest_bearing_debt or 0) - (cash_and_equivalents or 0),
            ebitda,
        ),
        "应收账款周转率": _safe_divide(revenue, receivables),
        "存货周转率": _safe_divide(revenue, inventory),
        "总资产周转率": _safe_divide(revenue, total_assets),
        "过去5年归母净利润为正年数": _positive_year_count(profit_map, 5),
        "过去5年经营现金流为正年数": _positive_year_count(operating_cash_flow_map, 5),
        "过去5年经营现金流/净利润": _ratio_from_annual_maps(
            operating_cash_flow_map,
            profit_map,
            5,
        ),
        "过去5年自由现金流为正年数": _positive_year_count(free_cash_flow_map, 5),
    }


def _latest_row_by_column(frame: pd.DataFrame, column: str) -> pd.Series | None:
    if frame.empty or column not in frame.columns:
        return None

    working = frame.copy()
    working["_latest_sort_key"] = pd.to_numeric(working[column], errors="coerce")
    if working["_latest_sort_key"].isna().all():
        working["_latest_sort_key"] = pd.to_datetime(
            working[column],
            errors="coerce",
        ).map(lambda value: value.timestamp() if pd.notna(value) else pd.NA)
    working = working.dropna(subset=["_latest_sort_key"])
    if working.empty:
        return None
    return working.sort_values("_latest_sort_key").iloc[-1]


def _latest_and_previous_rows_by_column(
    frame: pd.DataFrame,
    column: str,
) -> tuple[pd.Series | None, pd.Series | None]:
    if frame.empty or column not in frame.columns:
        return None, None

    working = frame.copy()
    working["_latest_sort_key"] = pd.to_numeric(working[column], errors="coerce")
    if working["_latest_sort_key"].isna().all():
        working["_latest_sort_key"] = pd.to_datetime(
            working[column],
            errors="coerce",
        ).map(lambda value: value.timestamp() if pd.notna(value) else pd.NA)
    working = working.dropna(subset=["_latest_sort_key"]).sort_values("_latest_sort_key")
    if working.empty:
        return None, None
    latest = working.iloc[-1]
    previous = working.iloc[-2] if len(working) > 1 else None
    return latest, previous


def fetch_cn_enriched_metrics(symbol: str, timeout_seconds: int = 20) -> dict[str, object]:
    """
    Fetch a richer China A-share profile using AKShare financial endpoints.
    """
    import akshare as ak

    normalized_symbol = normalize_security_code(symbol, market="cn")
    result: dict[str, object] = {"代码": normalized_symbol, "补充数据状态": "成功"}

    try:
        with _time_limit(timeout_seconds):
            abstract = _read_financial_history_cache("cn", normalized_symbol, "abstracts")
            if abstract.empty:
                abstract = ak.stock_financial_abstract_ths(
                    symbol=normalized_symbol,
                    indicator="按年度",
                )
            abstract_row, previous_abstract_row = _latest_and_previous_rows_by_column(
                abstract,
                "报告期",
            )
            if abstract_row is not None:
                net_profit = abstract_row.get("净利润")
                eps = abstract_row.get("基本每股收益")
                operating_cash_flow_per_share = abstract_row.get("每股经营现金流")
                shares = _safe_divide(net_profit, eps)
                operating_cash_flow = (
                    None
                    if shares is None
                    else _safe_divide(operating_cash_flow_per_share, 1) * shares
                    if _safe_divide(operating_cash_flow_per_share, 1) is not None
                    else None
                )
                result.update(
                    {
                        "报告期": abstract_row.get("报告期"),
                        "净利润": net_profit,
                        "净利润同比增长率": abstract_row.get("净利润同比增长率"),
                        "净利润同比增长率(%)": abstract_row.get("净利润同比增长率"),
                        "扣非净利润": abstract_row.get("扣非净利润"),
                        "扣非净利润同比增长率": abstract_row.get("扣非净利润同比增长率"),
                        "营业总收入": abstract_row.get("营业总收入"),
                        "营业总收入同比增长率": abstract_row.get("营业总收入同比增长率"),
                        "营业收入同比增长率(%)": abstract_row.get("营业总收入同比增长率"),
                        "基本每股收益": abstract_row.get("基本每股收益"),
                        "每股净资产": abstract_row.get("每股净资产"),
                        "每股经营现金流": abstract_row.get("每股经营现金流"),
                        "经营现金流净额": operating_cash_flow,
                        "FCF/净利润": None,
                        "每股自由现金流": None,
                        "EPS同比增长率(%)": (
                            _percentage_growth(
                                abstract_row.get("基本每股收益"),
                                previous_abstract_row.get("基本每股收益"),
                            )
                            if previous_abstract_row is not None
                            else None
                        ),
                        "销售净利率(%)": abstract_row.get("销售净利率"),
                        "净资产收益率(%)": abstract_row.get("净资产收益率"),
                        "流动比率": abstract_row.get("流动比率"),
                        "速动比率": abstract_row.get("速动比率"),
                        "保守速动比率": abstract_row.get("保守速动比率"),
                        "产权比率": abstract_row.get("产权比率"),
                        "资产负债率(%)": abstract_row.get("资产负债率"),
                    }
                )

            indicators = _read_financial_history_cache("cn", normalized_symbol, "indicators")
            if indicators.empty:
                indicators = ak.stock_financial_analysis_indicator(
                    symbol=normalized_symbol,
                    start_year="2020",
                )
            indicator_row = _latest_row_by_column(indicators, "日期")
            if indicator_row is not None:
                result.update(
                    {
                        "财务指标日期": indicator_row.get("日期"),
                        "销售毛利率(%)": indicator_row.get("销售毛利率(%)"),
                        "销售净利率(%)": indicator_row.get("销售净利率(%)"),
                        "净资产收益率(%)": indicator_row.get("净资产收益率(%)"),
                        "加权净资产收益率(%)": indicator_row.get("加权净资产收益率(%)"),
                        "总资产收益率(%)": indicator_row.get("总资产净利润率(%)"),
                        "营业收入同比增长率(%)": _first_present(
                            indicator_row.get("主营业务收入增长率(%)"),
                            result.get("营业收入同比增长率(%)"),
                        ),
                        "净利润同比增长率(%)": _first_present(
                            indicator_row.get("净利润增长率(%)"),
                            result.get("净利润同比增长率(%)"),
                        ),
                        "流动比率": indicator_row.get("流动比率"),
                        "速动比率": indicator_row.get("速动比率"),
                        "现金比率(%)": indicator_row.get("现金比率(%)"),
                        "利息支付倍数": indicator_row.get("利息支付倍数"),
                        "经营现金净流量与净利润的比率(%)": indicator_row.get(
                            "经营现金净流量与净利润的比率(%)"
                        ),
                        "经营现金净流量对负债比率(%)": indicator_row.get(
                            "经营现金净流量对负债比率(%)"
                        ),
                        "现金流量比率(%)": indicator_row.get("现金流量比率(%)"),
                        "资产负债率(%)": indicator_row.get("资产负债率(%)"),
                        "应收账款周转率": indicator_row.get("应收账款周转率(次)"),
                        "应收账款周转天数": indicator_row.get("应收账款周转天数(天)"),
                        "存货周转率": indicator_row.get("存货周转率(次)"),
                        "存货周转天数": indicator_row.get("存货周转天数(天)"),
                        "总资产周转率": indicator_row.get("总资产周转率(次)"),
                    }
                )

            if abstract_row is None and indicator_row is None:
                result["补充数据状态"] = "失败: empty financial responses"
    except Exception as exc:  # pragma: no cover - network failure path
        result["补充数据状态"] = f"失败: {str(exc)[:120]}"

    return _clean_missing_metric_values(result)


def load_enriched_metrics_cache(path: Path, market: str = "hk") -> pd.DataFrame:
    """
    Load local enrichment cache if present.
    """
    if not path.exists():
        return pd.DataFrame()

    cache = pd.read_csv(path, dtype={"代码": str})
    if "代码" in cache.columns:
        cache["代码"] = normalize_security_codes(cache["代码"], market=market)
    return cache


def save_enriched_metrics_cache(frame: pd.DataFrame, path: Path, market: str = "hk") -> Path:
    """
    Save local enrichment cache.
    """
    output = frame.copy()
    if "代码" in output.columns:
        output["代码"] = normalize_security_codes(output["代码"], market=market)
    path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(path, index=False)
    return path


def merge_enriched_cache(
    existing: pd.DataFrame,
    new_records: pd.DataFrame,
    market: str = "hk",
) -> pd.DataFrame:
    """
    Merge newly fetched enrichment records into cache, preferring the latest rows by code.
    """
    if existing.empty:
        merged = new_records.copy()
    elif new_records.empty:
        merged = existing.copy()
    else:
        merged = pd.concat([existing, new_records], ignore_index=True, sort=False)

    if "代码" not in merged.columns:
        return merged

    merged["代码"] = normalize_security_codes(merged["代码"], market=market)
    merged = merged.drop_duplicates(subset=["代码"], keep="last").reset_index(drop=True)
    return merged


def financial_history_cache_path(
    market: str,
    code: str,
    statement: str,
    root_dir: Path | None = None,
) -> Path:
    normalized_code = normalize_security_code(code, market=market)
    root = root_dir or RAW_DATA_DIR / "financials" / market
    return root / statement / f"{normalized_code}.csv"


def financial_history_statements(market: str) -> list[str]:
    return {
        "cn": ["indicators", "abstracts"],
        "hk": ["balance", "income", "cashflow"],
        "us": ["balance", "income", "cashflow"],
    }[market]


def _read_financial_history_cache(market: str, code: str, statement: str) -> pd.DataFrame:
    return _read_csv_if_present(financial_history_cache_path(market, code, statement))


def filing_index_cache_path(
    market: str,
    code: str,
    root_dir: Path | None = None,
) -> Path:
    normalized_code = normalize_security_code(code, market=market)
    root = root_dir or RAW_DATA_DIR / "filings" / market
    return root / normalized_code / "index.csv"


def parse_cninfo_disclosure_link(link: str) -> dict[str, str]:
    parsed = urlparse(str(link))
    params = parse_qs(parsed.query)
    announcement_id = params.get("announcementId", [""])[0]
    announcement_time = params.get("announcementTime", [""])[0]
    if not announcement_id:
        match = re.search(r"announcementId=(\d+)", str(link))
        announcement_id = match.group(1) if match else ""
    return {
        "announcement_id": announcement_id,
        "announcement_time": announcement_time,
    }


def cninfo_pdf_url(disclosure_link: str, announcement_time: str | None = None) -> str:
    parsed = parse_cninfo_disclosure_link(disclosure_link)
    announcement_id = parsed["announcement_id"]
    report_date = str(announcement_time or parsed["announcement_time"])[:10]
    if not announcement_id or not report_date:
        return ""
    return f"http://static.cninfo.com.cn/finalpage/{report_date}/{announcement_id}.PDF"


def _safe_filename(value: str, max_length: int = 80) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|\s]+', "_", str(value)).strip("_")
    return cleaned[:max_length] or "filing"


def filing_pdf_cache_path(
    market: str,
    code: str,
    announcement_time: str,
    announcement_id: str,
    title: str,
    root_dir: Path | None = None,
) -> Path:
    normalized_code = normalize_security_code(code, market=market)
    root = root_dir or RAW_DATA_DIR / "filings" / market
    filename = "_".join(
        [
            _safe_filename(announcement_time, max_length=20),
            _safe_filename(announcement_id, max_length=30),
            _safe_filename(title),
        ]
    )
    return root / normalized_code / "pdfs" / f"{filename}.pdf"


def _build_request(url: str, user_agent: str | None = None) -> Request:
    headers = {"User-Agent": user_agent or "Mozilla/5.0"}
    return Request(url, headers=headers)


def _request_json(url: str, timeout_seconds: int = 30, user_agent: str | None = None) -> object:
    request = _build_request(url, user_agent=user_agent)
    with urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8-sig", "ignore"))


def _safe_us_ticker(value: object) -> str:
    text = str(value).strip().upper()
    if not text:
        return text
    if re.fullmatch(r"\d+\..+", text):
        return text.split(".", 1)[1]
    if "." in text:
        return text.replace(".", "-")
    return text


def normalize_us_filing_ticker(value: object) -> str:
    return _safe_us_ticker(value)


def resolve_us_cik(ticker: str) -> str:
    normalized = normalize_us_filing_ticker(ticker)
    if normalized.isdigit():
        return normalized.zfill(10)
    mapping = load_sec_company_ticker_map()
    cik = mapping.get(normalized)
    if cik is None:
        raise ValueError(f"CIK not found for US ticker: {ticker}")
    return cik


@lru_cache(maxsize=1)
def load_sec_company_ticker_map() -> dict[str, str]:
    data = _request_json(SEC_COMPANY_TICKERS_URL, user_agent=SEC_USER_AGENT)
    if not isinstance(data, dict):
        return {}

    mapping: dict[str, str] = {}
    for item in data.values():
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker", "")).strip().upper()
        cik = str(item.get("cik_str", "")).strip()
        if not ticker or not cik:
            continue
        mapping.setdefault(ticker, cik.zfill(10))
    return mapping


def sec_submissions_url(cik: str) -> str:
    return SEC_SUBMISSIONS_URL.format(cik=str(cik).zfill(10))


def sec_archive_index_url(cik: str, accession_number: str) -> str:
    return SEC_ARCHIVE_INDEX_URL.format(
        cik=str(cik).lstrip("0") or "0",
        accession=accession_number.replace("-", ""),
    )


def sec_archive_file_url(cik: str, accession_number: str, name: str) -> str:
    return SEC_ARCHIVE_FILE_URL.format(
        cik=str(cik).lstrip("0") or "0",
        accession=accession_number.replace("-", ""),
        name=quote(str(name)),
    )


def download_file(
    url: str,
    path: Path,
    refresh: bool = False,
    timeout_seconds: int = 30,
    user_agent: str | None = None,
) -> bool:
    if not url:
        return False
    if path.exists() and path.stat().st_size > 0 and not refresh:
        return False

    request = _build_request(url, user_agent=user_agent)
    with urlopen(request, timeout=timeout_seconds) as response:
        content = response.read()
    if not content:
        return False

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return True


def merge_filing_index_cache(
    existing: pd.DataFrame,
    fetched: pd.DataFrame,
    refresh: bool = False,
) -> tuple[pd.DataFrame, int]:
    if fetched.empty:
        return existing.copy(), 0
    if existing.empty or "公告链接" not in existing.columns:
        return fetched.copy(), len(fetched)
    if refresh:
        merged = pd.concat([existing, fetched], ignore_index=True, sort=False)
        return merged, len(fetched)

    existing_links = set(existing["公告链接"].dropna().astype(str).tolist())
    new_rows = fetched[~fetched["公告链接"].astype(str).isin(existing_links)].copy()
    if new_rows.empty:
        return existing.copy(), 0

    merged = pd.concat([existing, new_rows], ignore_index=True, sort=False)
    return merged, len(new_rows)


def _retention_cutoff(years: int = 5, as_of: datetime | pd.Timestamp | None = None) -> pd.Timestamp:
    anchor = pd.Timestamp(as_of or datetime.now())
    return anchor.normalize() - pd.DateOffset(years=years)


def _prune_recent_rows(
    frame: pd.DataFrame,
    date_columns: list[str],
    years: int = 5,
    as_of: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    parsed_dates = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
    for column in date_columns:
        if column not in frame.columns:
            continue
        parsed_dates = parsed_dates.combine_first(pd.to_datetime(frame[column], errors="coerce"))

    if parsed_dates.notna().sum() == 0:
        return frame.copy()

    cutoff = _retention_cutoff(years=years, as_of=as_of)
    return frame.loc[parsed_dates >= cutoff].copy()


def prune_recent_financial_history(
    frame: pd.DataFrame,
    report_column: str,
    years: int = 5,
    as_of: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    return _prune_recent_rows(frame, [report_column], years=years, as_of=as_of)


def prune_recent_filing_index(
    frame: pd.DataFrame,
    years: int = 5,
    as_of: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    return _prune_recent_rows(frame, ["公告日期", "公告时间"], years=years, as_of=as_of)


def prune_recent_us_filing_index(
    frame: pd.DataFrame,
    years: int = 5,
    as_of: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    return _prune_recent_rows(frame, ["filing_date", "report_date"], years=years, as_of=as_of)


def cleanup_filing_pdf_cache(
    index_frame: pd.DataFrame,
    pdf_dir: Path,
    years: int = 5,
    as_of: datetime | pd.Timestamp | None = None,
) -> int:
    if not pdf_dir.exists():
        return 0

    if index_frame.empty:
        keep_paths: set[Path] | None = set()
    else:
        pruned_index = prune_recent_filing_index(index_frame, years=years, as_of=as_of)
        if "本地文件路径" not in pruned_index.columns:
            return 0
        keep_paths = {Path(value).resolve() for value in pruned_index["本地文件路径"].dropna().astype(str)}

    deleted_files = 0
    for pdf_path in pdf_dir.glob("*.pdf"):
        if keep_paths is not None and pdf_path.resolve() in keep_paths:
            continue
        pdf_path.unlink(missing_ok=True)
        deleted_files += 1
    return deleted_files


def cleanup_us_filing_raw_cache(
    index_frame: pd.DataFrame,
    raw_dir: Path,
    years: int = 5,
    as_of: datetime | pd.Timestamp | None = None,
) -> int:
    if not raw_dir.exists():
        return 0

    if index_frame.empty:
        keep_dirs: set[Path] = set()
    else:
        pruned_index = prune_recent_us_filing_index(index_frame, years=years, as_of=as_of)
        if "本地目录" not in pruned_index.columns:
            return 0
        keep_dirs = {Path(value).resolve() for value in pruned_index["本地目录"].dropna().astype(str)}

    deleted_dirs = 0
    for child in raw_dir.iterdir():
        if not child.is_dir():
            continue
        if keep_dirs and child.resolve() in keep_dirs:
            continue
        if not keep_dirs or child.resolve() not in keep_dirs:
            shutil.rmtree(child, ignore_errors=True)
            deleted_dirs += 1
    return deleted_dirs


def _normalize_us_filing_form(form: object) -> str:
    text = str(form).strip().upper()
    if not text:
        return text
    return US_FILING_CATEGORY_ALIASES.get(text, text)


def _us_submission_pages(submissions: object) -> list[object]:
    if not isinstance(submissions, dict):
        return []

    pages: list[object] = [submissions]
    filings = submissions.get("filings", {})
    if not isinstance(filings, dict):
        return pages

    for file_item in filings.get("files", []) or []:
        if not isinstance(file_item, dict):
            continue
        name = str(file_item.get("name", "")).strip()
        if not name:
            continue
        try:
            pages.append(_request_json(SEC_SUBMISSIONS_FILE_URL.format(name=name), user_agent=SEC_USER_AGENT))
        except Exception:  # pragma: no cover - network failure path
            continue

    return pages


def _us_submission_records(
    submissions: object,
    ticker: str,
    cik: str,
    root_dir: Path | None = None,
    years: int = 5,
    as_of: datetime | pd.Timestamp | None = None,
) -> list[dict[str, object]]:
    pages = _us_submission_pages(submissions)
    if not pages:
        return []

    cutoff = _retention_cutoff(years=years, as_of=as_of)
    records: list[dict[str, object]] = []
    normalized_ticker = normalize_us_filing_ticker(ticker)
    cik10 = str(cik).zfill(10)
    cik_int = str(int(cik10))

    for payload in pages:
        if not isinstance(payload, dict):
            continue
        filings = payload.get("filings", {})
        if not isinstance(filings, dict):
            continue
        recent = filings.get("recent", {})
        if not isinstance(recent, dict):
            continue

        forms = recent.get("form", []) or []
        accession_numbers = recent.get("accessionNumber", []) or []
        filing_dates = recent.get("filingDate", []) or []
        report_dates = recent.get("reportDate", []) or []
        primary_documents = recent.get("primaryDocument", []) or []
        descriptions = recent.get("primaryDocDescription", []) or []
        xbrl_flags = recent.get("isXBRL", []) or []
        inline_flags = recent.get("isInlineXBRL", []) or []
        periods = recent.get("period", []) or []

        max_len = max(
            len(forms),
            len(accession_numbers),
            len(filing_dates),
            len(report_dates),
            len(primary_documents),
            len(descriptions),
            len(xbrl_flags),
            len(inline_flags),
            len(periods),
            0,
        )

        for index in range(max_len):
            form = _normalize_us_filing_form(forms[index] if index < len(forms) else "")
            if form not in US_FILING_CATEGORIES:
                continue

            filing_date = str(filing_dates[index] if index < len(filing_dates) else "").strip()
            filing_ts = pd.to_datetime(filing_date, errors="coerce")
            if pd.notna(filing_ts) and filing_ts < cutoff:
                continue

            accession_number = str(
                accession_numbers[index] if index < len(accession_numbers) else ""
            ).strip()
            if not accession_number:
                continue

            primary_document = str(
                primary_documents[index] if index < len(primary_documents) else ""
            ).strip()
            report_date = str(report_dates[index] if index < len(report_dates) else "").strip()
            description = str(descriptions[index] if index < len(descriptions) else "").strip()
            local_dir = us_filing_local_dir(normalized_ticker, accession_number, root_dir=root_dir)

            records.append(
                {
                    "ticker": normalized_ticker,
                    "cik": cik10,
                    "company_name": str(payload.get("name", "")).strip(),
                    "form": form,
                    "filing_date": filing_date,
                    "report_date": report_date,
                    "accession_number": accession_number,
                    "primary_document": primary_document,
                    "primary_doc_description": description,
                    "is_xbrl": bool(xbrl_flags[index]) if index < len(xbrl_flags) else False,
                    "is_inline_xbrl": bool(inline_flags[index]) if index < len(inline_flags) else False,
                    "period": str(periods[index] if index < len(periods) else "").strip(),
                    "filing_url": f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_number.replace('-', '')}/{primary_document}"
                    if primary_document
                    else "",
                    "archive_index_url": sec_archive_index_url(cik10, accession_number),
                    "submission_url": sec_submissions_url(cik10),
                    "本地目录": str(local_dir),
                    "本地文件路径": str(local_dir / primary_document) if primary_document else "",
                    "下载状态": "未下载",
                }
            )

    return records


def us_filing_local_dir(
    ticker: str,
    accession_number: str,
    root_dir: Path | None = None,
) -> Path:
    normalized_ticker = normalize_us_filing_ticker(ticker)
    root = root_dir or RAW_DATA_DIR / "filings" / "us"
    accession_nodash = accession_number.replace("-", "")
    return root / normalized_ticker / "raw" / accession_nodash


def us_filing_index_path(
    ticker: str,
    root_dir: Path | None = None,
) -> Path:
    normalized_ticker = normalize_us_filing_ticker(ticker)
    root = root_dir or RAW_DATA_DIR / "filings" / "us"
    return root / normalized_ticker / "index.csv"


def merge_us_filing_index_cache(
    existing: pd.DataFrame,
    fetched: pd.DataFrame,
    refresh: bool = False,
) -> tuple[pd.DataFrame, int]:
    if fetched.empty:
        return existing.copy(), 0
    if existing.empty or "accession_number" not in existing.columns:
        return fetched.copy(), len(fetched)
    if refresh:
        merged = pd.concat([existing, fetched], ignore_index=True, sort=False)
        merged = merged.drop_duplicates(subset=["accession_number"], keep="last").reset_index(
            drop=True
        )
        return merged, len(fetched)

    existing_accessions = set(existing["accession_number"].dropna().astype(str).tolist())
    new_rows = fetched[~fetched["accession_number"].astype(str).isin(existing_accessions)].copy()
    if new_rows.empty:
        return existing.copy(), 0

    merged = pd.concat([existing, new_rows], ignore_index=True, sort=False)
    merged = merged.drop_duplicates(subset=["accession_number"], keep="last").reset_index(drop=True)
    return merged, len(new_rows)


def _download_us_filing_raw_files(
    row: pd.Series,
    refresh: bool = False,
    timeout_seconds: int = 30,
) -> tuple[int, str]:
    local_dir_text = str(row.get("本地目录", "")).strip()
    if not local_dir_text:
        return 0, "失败: 缺少本地目录"
    local_dir = Path(local_dir_text)

    accession_number = str(row.get("accession_number", "")).strip()
    cik = str(row.get("cik", "")).strip()
    if not accession_number or not cik:
        return 0, "失败: 缺少 accession_number 或 cik"

    archive_index_url = str(row.get("archive_index_url", "")).strip()
    if not archive_index_url:
        archive_index_url = sec_archive_index_url(cik, accession_number)

    local_dir.mkdir(parents=True, exist_ok=True)
    downloaded_files = 0

    try:
        archive_index = _request_json(
            archive_index_url,
            timeout_seconds=timeout_seconds,
            user_agent=SEC_USER_AGENT,
        )
    except Exception as exc:  # pragma: no cover - network failure path
        return 0, f"失败: {str(exc)[:120]}"

    index_path = local_dir / "index.json"
    index_path.write_text(json.dumps(archive_index, ensure_ascii=False, indent=2), encoding="utf-8")
    downloaded_files += 1

    primary_document = str(row.get("primary_document", "")).strip()
    if primary_document:
        file_url = sec_archive_file_url(cik, accession_number, primary_document)
        file_path = local_dir / Path(primary_document)
        if download_file(
            file_url,
            file_path,
            refresh=refresh,
            timeout_seconds=timeout_seconds,
            user_agent=SEC_USER_AGENT,
        ):
            downloaded_files += 1

    return downloaded_files, "已下载"


def _fill_missing_filing_categories(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "公告标题" not in frame.columns:
        return frame.copy()

    output = frame.copy()
    if "公告分类" not in output.columns:
        output["公告分类"] = pd.NA

    categories = output["公告分类"].fillna("").astype(str)
    titles = output["公告标题"].fillna("").astype(str)
    missing = categories == ""

    output.loc[missing & titles.str.contains("年度", na=False), "公告分类"] = "年报"
    output.loc[missing & titles.str.contains("半年", na=False), "公告分类"] = "半年报"
    output.loc[missing & titles.str.contains("一季|第一季度", na=False), "公告分类"] = "一季报"
    output.loc[missing & titles.str.contains("三季|第三季度", na=False), "公告分类"] = "三季报"
    return output


def _request_text(
    url: str,
    timeout_seconds: int = 30,
    user_agent: str | None = None,
) -> str:
    request = _build_request(url, user_agent=user_agent)
    with urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8-sig", "ignore")


def _hk_filing_category_from_text(text: str) -> str | None:
    lowered = text.lower()
    if "third quarterly" in lowered or "三季" in text or "第三季度" in text:
        return "三季报"
    if "first quarterly" in lowered or "一季" in text or "第一季度" in text:
        return "一季报"
    if "interim report" in lowered or "中期報告" in text or "中期报告" in text:
        return "半年报"
    if "annual report" in lowered or "年度報告" in text or "年度报告" in text:
        return "年报"
    return None


def _strip_html(value: str) -> str:
    text = re.sub(r"<br\\s*/?>", " ", value, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _hkex_stock_lookup() -> dict[str, dict[str, object]]:
    rows = json.loads(_request_text(HKEX_ACTIVE_STOCK_URL))
    return {str(row["c"]): row for row in rows}


def _hkex_title_search_url(stock_id: object) -> str:
    return f"{HKEX_TITLE_SEARCH_URL}?category=0&lang=EN&market=SEHK&stockId={stock_id}"


def _parse_hkex_title_search(html: str, code: str, root_dir: Path | None = None) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    rows = re.findall(r"<tr>(.*?)</tr>", html, flags=re.S | re.I)
    for row in rows:
        if "/listedco/listconews/" not in row:
            continue

        release_match = re.search(r"Release Time:\s*</span>(.*?)</td>", row, flags=re.S | re.I)
        name_match = re.search(r"Stock Short Name:\s*</span>(.*?)</td>", row, flags=re.S | re.I)
        headline_match = re.search(r'<div class="headline">(.*?)</div>', row, flags=re.S | re.I)
        link_match = re.search(r'<a href="([^"]+\.pdf)"[^>]*>(.*?)</a>', row, flags=re.S | re.I)
        if not link_match:
            continue

        title = _strip_html(link_match.group(2))
        headline = _strip_html(headline_match.group(1)) if headline_match else ""
        category = _hk_filing_category_from_text(f"{headline} {title}")
        if category is None:
            continue

        pdf_path = link_match.group(1)
        pdf_url = pdf_path if pdf_path.startswith("http") else f"https://www1.hkexnews.hk{pdf_path}"
        announcement_id = Path(urlparse(pdf_url).path).stem

        records.append(
            {
                "代码": normalize_security_code(code, market="hk"),
                "简称": _strip_html(name_match.group(1)) if name_match else "",
                "公告分类": category,
                "公告标题": title,
                "公告时间": _strip_html(release_match.group(1)) if release_match else "",
                "公告链接": pdf_url,
                "公告日期": "",
                "announcement_id": announcement_id,
                "pdf_url": pdf_url,
                "本地文件路径": str(
                    filing_pdf_cache_path(
                        "hk",
                        code,
                        announcement_id[:8],
                        announcement_id,
                        title,
                        root_dir,
                    )
                ),
                "下载状态": "未下载",
            }
        )

    return pd.DataFrame(records)


def merge_financial_history_cache(
    existing: pd.DataFrame,
    fetched: pd.DataFrame,
    report_column: str,
    refresh: bool = False,
) -> tuple[pd.DataFrame, int]:
    if fetched.empty:
        return existing.copy(), 0
    if report_column not in fetched.columns:
        return existing.copy(), 0
    if existing.empty or report_column not in existing.columns:
        return fetched.copy(), len(fetched)

    if refresh:
        merged = pd.concat([existing, fetched], ignore_index=True, sort=False)
        return merged, len(fetched)

    existing_reports = set(existing[report_column].dropna().astype(str).tolist())
    new_rows = fetched[~fetched[report_column].astype(str).isin(existing_reports)].copy()
    if new_rows.empty:
        return existing.copy(), 0

    merged = pd.concat([existing, new_rows], ignore_index=True, sort=False)
    return merged, len(new_rows)


def _add_financial_cache_metadata(
    frame: pd.DataFrame,
    code: str,
    market: str,
    fetched_at: str,
) -> pd.DataFrame:
    output = frame.copy()
    output.insert(0, "代码", normalize_security_code(code, market=market))
    output.insert(1, "抓取时间", fetched_at)
    return output


def _read_csv_if_present(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype={"代码": str})
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _save_financial_history_statement(
    existing_path: Path,
    fetched_frame: pd.DataFrame,
    code: str,
    market: str,
    fetched_at: str,
    report_column: str,
    refresh: bool,
) -> int:
    if fetched_frame is None:
        return 0
    existing = _read_csv_if_present(existing_path)
    fetched = _add_financial_cache_metadata(fetched_frame, code, market, fetched_at)
    merged, added_rows = merge_financial_history_cache(
        existing,
        fetched,
        report_column=report_column,
        refresh=refresh,
    )
    merged = merged.drop_duplicates(subset=[report_column], keep="last").reset_index(drop=True)
    merged = prune_recent_financial_history(merged, report_column=report_column)
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(existing_path, index=False)
    return added_rows


def _selected_financial_history_statements(
    market: str,
    statements: list[str] | None,
) -> list[str]:
    all_statements = financial_history_statements(market)
    if statements is None:
        return all_statements

    requested = set(statements)
    return [statement for statement in all_statements if statement in requested]


def _financial_history_failure_result(
    code: str,
    market: str,
    root_dir: Path | None,
    statements: list[str],
    status: str,
) -> FinancialHistoryCacheResult:
    normalized_code = normalize_security_code(code, market=market)
    return FinancialHistoryCacheResult(
        code=normalized_code,
        added_rows_by_statement={statement: 0 for statement in statements},
        paths_by_statement={
            statement: financial_history_cache_path(market, normalized_code, statement, root_dir)
            for statement in statements
        },
        status=status,
    )


def cache_cn_filings(
    code: str,
    root_dir: Path | None = None,
    refresh: bool = False,
    download: bool = False,
    category: str = "年报",
) -> FilingCacheResult:
    import akshare as ak

    if category not in CN_FILING_CATEGORIES:
        raise ValueError(f"Unsupported China A-share filing category: {category}")

    normalized_code = normalize_security_code(code, market="cn")
    fetched_at = datetime.now().isoformat(timespec="seconds")
    index_path = filing_index_cache_path("cn", normalized_code, root_dir)

    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            frame = ak.stock_zh_a_disclosure_report_cninfo(
                symbol=normalized_code,
                market="沪深京",
                category=category,
                start_date="20000101",
                end_date=datetime.now().strftime("%Y%m%d"),
            )
        if frame is None:
            frame = pd.DataFrame()
        if frame.empty:
            index_path.parent.mkdir(parents=True, exist_ok=True)
            if not index_path.exists():
                pd.DataFrame().to_csv(index_path, index=False)
            return FilingCacheResult(
                code=normalized_code,
                added_rows=0,
                downloaded_files=0,
                index_path=index_path,
                status="成功",
            )

        fetched = frame.copy()
        fetched["代码"] = normalize_security_codes(fetched["代码"], market="cn")
        fetched.insert(1, "抓取时间", fetched_at)
        fetched.insert(2, "公告分类", category)

        parsed_links = fetched["公告链接"].map(parse_cninfo_disclosure_link)
        fetched["announcement_id"] = parsed_links.map(lambda item: item["announcement_id"])
        fetched["公告日期"] = fetched["公告时间"].astype(str).str[:10]
        fetched["pdf_url"] = fetched.apply(
            lambda row: cninfo_pdf_url(row["公告链接"], str(row["公告日期"])),
            axis=1,
        )
        fetched["本地文件路径"] = fetched.apply(
            lambda row: str(
                filing_pdf_cache_path(
                    "cn",
                    normalized_code,
                    str(row["公告日期"]),
                    str(row["announcement_id"]),
                    str(row["公告标题"]),
                    root_dir,
                )
            ),
            axis=1,
        )
        fetched["下载状态"] = "未下载"

        existing = _fill_missing_filing_categories(_read_csv_if_present(index_path))
        merged, added_rows = merge_filing_index_cache(existing, fetched, refresh=refresh)

        downloaded_files = 0
        if download:
            for row_index, row in fetched.iterrows():
                pdf_path = Path(row["本地文件路径"])
                try:
                    downloaded = download_file(
                        str(row["pdf_url"]),
                        pdf_path,
                        refresh=refresh,
                    )
                    status = "已下载" if downloaded or pdf_path.exists() else "下载失败"
                    downloaded_files += int(downloaded)
                except Exception as exc:  # pragma: no cover - network failure path
                    status = f"下载失败: {str(exc)[:80]}"
                fetched.loc[row_index, "下载状态"] = status

            if refresh or existing.empty or "公告链接" not in existing.columns:
                merged, added_rows = merge_filing_index_cache(existing, fetched, refresh=refresh)
            else:
                merged = pd.concat([existing, fetched], ignore_index=True, sort=False)
                merged = merged.drop_duplicates(subset=["公告链接"], keep="last").reset_index(
                    drop=True
                )

        merged = merged.drop_duplicates(subset=["公告链接"], keep="last").reset_index(drop=True)
        merged = prune_recent_filing_index(merged)
        cleanup_filing_pdf_cache(merged, index_path.parent / "pdfs")
        index_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(index_path, index=False)
        return FilingCacheResult(
            code=normalized_code,
            added_rows=added_rows,
            downloaded_files=downloaded_files,
            index_path=index_path,
            status="成功",
        )
    except Exception as exc:  # pragma: no cover - network failure path
        return FilingCacheResult(
            code=normalized_code,
            added_rows=0,
            downloaded_files=0,
            index_path=index_path,
            status=f"失败: {str(exc)[:120]}",
        )


def cache_cn_annual_filings(
    code: str,
    root_dir: Path | None = None,
    refresh: bool = False,
    download: bool = False,
) -> FilingCacheResult:
    return cache_cn_filings(
        code,
        root_dir=root_dir,
        refresh=refresh,
        download=download,
        category="年报",
    )


def cache_hk_filings(
    code: str,
    root_dir: Path | None = None,
    refresh: bool = False,
    download: bool = False,
    category: str = "年报",
) -> FilingCacheResult:
    if category not in HK_FILING_CATEGORIES:
        raise ValueError(f"Unsupported Hong Kong filing category: {category}")

    normalized_code = normalize_security_code(code, market="hk")
    fetched_at = datetime.now().isoformat(timespec="seconds")
    index_path = filing_index_cache_path("hk", normalized_code, root_dir)

    try:
        stock = _hkex_stock_lookup().get(normalized_code)
        if stock is None:
            raise ValueError(f"HKEX stock id not found for {normalized_code}")

        frame = _parse_hkex_title_search(
            _request_text(_hkex_title_search_url(stock["i"])),
            normalized_code,
            root_dir,
        )
        if frame.empty:
            index_path.parent.mkdir(parents=True, exist_ok=True)
            if not index_path.exists():
                pd.DataFrame().to_csv(index_path, index=False)
            return FilingCacheResult(
                code=normalized_code,
                added_rows=0,
                downloaded_files=0,
                index_path=index_path,
                status="成功",
            )

        fetched = frame[frame["公告分类"] == category].copy()
        if fetched.empty:
            return FilingCacheResult(
                code=normalized_code,
                added_rows=0,
                downloaded_files=0,
                index_path=index_path,
                status="成功",
            )
        fetched.insert(1, "抓取时间", fetched_at)

        existing = _read_csv_if_present(index_path)
        merged, added_rows = merge_filing_index_cache(existing, fetched, refresh=refresh)

        downloaded_files = 0
        if download:
            for row_index, row in fetched.iterrows():
                pdf_path = Path(row["本地文件路径"])
                try:
                    downloaded = download_file(
                        str(row["pdf_url"]),
                        pdf_path,
                        refresh=refresh,
                    )
                    status = "已下载" if downloaded or pdf_path.exists() else "下载失败"
                    downloaded_files += int(downloaded)
                except Exception as exc:  # pragma: no cover - network failure path
                    status = f"下载失败: {str(exc)[:80]}"
                fetched.loc[row_index, "下载状态"] = status

            if refresh or existing.empty or "公告链接" not in existing.columns:
                merged, added_rows = merge_filing_index_cache(existing, fetched, refresh=refresh)
            else:
                merged = pd.concat([existing, fetched], ignore_index=True, sort=False)
                merged = merged.drop_duplicates(subset=["公告链接"], keep="last").reset_index(
                    drop=True
                )

        merged = merged.drop_duplicates(subset=["公告链接"], keep="last").reset_index(drop=True)
        merged = prune_recent_filing_index(merged)
        cleanup_filing_pdf_cache(merged, index_path.parent / "pdfs")
        index_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(index_path, index=False)
        return FilingCacheResult(
            code=normalized_code,
            added_rows=added_rows,
            downloaded_files=downloaded_files,
            index_path=index_path,
            status="成功",
        )
    except Exception as exc:  # pragma: no cover - network failure path
        return FilingCacheResult(
            code=normalized_code,
            added_rows=0,
            downloaded_files=0,
            index_path=index_path,
            status=f"失败: {str(exc)[:120]}",
        )


def cache_us_filings(
    code: str,
    root_dir: Path | None = None,
    refresh: bool = False,
    download: bool = False,
    category: str = "10-K",
) -> FilingCacheResult:
    normalized_ticker = normalize_us_filing_ticker(code)
    normalized_category = _normalize_us_filing_form(category)
    if normalized_category == "年报":
        normalized_category = "10-K"
    if normalized_category != "all" and normalized_category not in US_FILING_CATEGORIES:
        raise ValueError(f"Unsupported US filing category: {category}")

    try:
        cik = resolve_us_cik(normalized_ticker)
    except Exception as exc:
        return FilingCacheResult(
            code=normalized_ticker,
            added_rows=0,
            downloaded_files=0,
            index_path=us_filing_index_path(normalized_ticker, root_dir),
            status=f"失败: {str(exc)[:120]}",
        )

    index_path = us_filing_index_path(normalized_ticker, root_dir)
    fetched_at = datetime.now().isoformat(timespec="seconds")
    existing = _read_csv_if_present(index_path)

    try:
        submissions = _request_json(sec_submissions_url(cik), user_agent=SEC_USER_AGENT)
        records = _us_submission_records(
            submissions,
            normalized_ticker,
            cik,
            root_dir=root_dir,
            as_of=pd.Timestamp.now(),
        )
        if not records:
            merged = prune_recent_us_filing_index(existing)
            cleanup_us_filing_raw_cache(merged, index_path.parent / "raw")
            index_path.parent.mkdir(parents=True, exist_ok=True)
            merged.to_csv(index_path, index=False)
            return FilingCacheResult(
                code=normalized_ticker,
                added_rows=0,
                downloaded_files=0,
                index_path=index_path,
                status="成功",
            )

        fetched = pd.DataFrame(records)
        if normalized_category != "all":
            fetched = fetched[fetched["form"] == normalized_category].copy()

        if fetched.empty:
            merged = prune_recent_us_filing_index(existing)
            cleanup_us_filing_raw_cache(merged, index_path.parent / "raw")
            index_path.parent.mkdir(parents=True, exist_ok=True)
            merged.to_csv(index_path, index=False)
            return FilingCacheResult(
                code=normalized_ticker,
                added_rows=0,
                downloaded_files=0,
                index_path=index_path,
                status="成功",
            )

        fetched.insert(1, "抓取时间", fetched_at)
        fetched["下载状态"] = "未下载"

        merged, added_rows = merge_us_filing_index_cache(existing, fetched, refresh=refresh)

        downloaded_files = 0
        if download:
            for row_index, row in fetched.iterrows():
                downloaded_count, status = _download_us_filing_raw_files(
                    row,
                    refresh=refresh,
                )
                downloaded_files += downloaded_count
                fetched.loc[row_index, "下载状态"] = status

            if refresh or existing.empty or "accession_number" not in existing.columns:
                merged, added_rows = merge_us_filing_index_cache(existing, fetched, refresh=refresh)
            else:
                merged = pd.concat([existing, fetched], ignore_index=True, sort=False)
                merged = merged.drop_duplicates(subset=["accession_number"], keep="last").reset_index(
                    drop=True
                )

        merged = merged.drop_duplicates(subset=["accession_number"], keep="last").reset_index(
            drop=True
        )
        merged = prune_recent_us_filing_index(merged)
        cleanup_us_filing_raw_cache(merged, index_path.parent / "raw")
        index_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(index_path, index=False)
        return FilingCacheResult(
            code=normalized_ticker,
            added_rows=added_rows,
            downloaded_files=downloaded_files,
            index_path=index_path,
            status="成功",
        )
    except Exception as exc:  # pragma: no cover - network failure path
        return FilingCacheResult(
            code=normalized_ticker,
            added_rows=0,
            downloaded_files=0,
            index_path=index_path,
            status=f"失败: {str(exc)[:120]}",
        )


def cache_cn_financial_history(
    code: str,
    root_dir: Path | None = None,
    refresh: bool = False,
    statements: list[str] | None = None,
) -> FinancialHistoryCacheResult:
    import akshare as ak

    normalized_code = normalize_security_code(code, market="cn")
    fetched_at = datetime.now().isoformat(timespec="seconds")
    all_statements = financial_history_statements("cn")
    selected_statements = _selected_financial_history_statements("cn", statements)
    paths = {
        statement: financial_history_cache_path("cn", normalized_code, statement, root_dir)
        for statement in all_statements
    }

    try:
        indicator_added = 0
        abstract_added = 0
        if "indicators" in selected_statements:
            indicator_frame = ak.stock_financial_analysis_indicator(
                symbol=normalized_code,
                start_year="2000",
            )
            indicator_added = _save_financial_history_statement(
                paths["indicators"],
                indicator_frame,
                normalized_code,
                "cn",
                fetched_at,
                report_column="日期",
                refresh=refresh,
            )
        if "abstracts" in selected_statements:
            abstract_frame = ak.stock_financial_abstract_ths(
                symbol=normalized_code,
                indicator="按年度",
            )
            abstract_added = _save_financial_history_statement(
                paths["abstracts"],
                abstract_frame,
                normalized_code,
                "cn",
                fetched_at,
                report_column="报告期",
                refresh=refresh,
            )

        return FinancialHistoryCacheResult(
            code=normalized_code,
            added_rows_by_statement={
                "indicators": indicator_added,
                "abstracts": abstract_added,
            },
            paths_by_statement=paths,
            status="成功",
        )
    except Exception as exc:  # pragma: no cover - network failure path
        return _financial_history_failure_result(
            code=normalized_code,
            market="cn",
            root_dir=root_dir,
            statements=statements,
            status=f"失败: {str(exc)[:120]}",
        )


def cache_hk_financial_history(
    code: str,
    root_dir: Path | None = None,
    refresh: bool = False,
    statements: list[str] | None = None,
) -> FinancialHistoryCacheResult:
    import akshare as ak

    normalized_code = normalize_security_code(code, market="hk")
    fetched_at = datetime.now().isoformat(timespec="seconds")
    all_statements = financial_history_statements("hk")
    selected_statements = _selected_financial_history_statements("hk", statements)
    paths = {
        statement: financial_history_cache_path("hk", normalized_code, statement, root_dir)
        for statement in all_statements
    }

    try:
        added_rows = {"balance": 0, "income": 0, "cashflow": 0}
        if "balance" in selected_statements:
            balance_frame = ak.stock_financial_hk_report_em(
                stock=normalized_code,
                symbol="资产负债表",
                indicator="年度",
            )
            added_rows["balance"] = _save_financial_history_statement(
                paths["balance"],
                balance_frame,
                normalized_code,
                "hk",
                fetched_at,
                "REPORT_DATE",
                refresh,
            )
        if "income" in selected_statements:
            income_frame = ak.stock_financial_hk_report_em(
                stock=normalized_code,
                symbol="利润表",
                indicator="年度",
            )
            added_rows["income"] = _save_financial_history_statement(
                paths["income"],
                income_frame,
                normalized_code,
                "hk",
                fetched_at,
                "REPORT_DATE",
                refresh,
            )
        if "cashflow" in selected_statements:
            cashflow_frame = ak.stock_financial_hk_report_em(
                stock=normalized_code,
                symbol="现金流量表",
                indicator="年度",
            )
            added_rows["cashflow"] = _save_financial_history_statement(
                paths["cashflow"],
                cashflow_frame,
                normalized_code,
                "hk",
                fetched_at,
                "REPORT_DATE",
                refresh,
            )
        return FinancialHistoryCacheResult(
            code=normalized_code,
            added_rows_by_statement=added_rows,
            paths_by_statement=paths,
            status="成功",
        )
    except Exception as exc:  # pragma: no cover - network failure path
        return _financial_history_failure_result(
            code=normalized_code,
            market="hk",
            root_dir=root_dir,
            statements=statements,
            status=f"失败: {str(exc)[:120]}",
        )


def cache_us_financial_history(
    code: str,
    root_dir: Path | None = None,
    refresh: bool = False,
    statements: list[str] | None = None,
) -> FinancialHistoryCacheResult:
    import akshare as ak

    normalized_code = normalize_security_code(code, market="us")
    fetched_at = datetime.now().isoformat(timespec="seconds")
    all_statements = financial_history_statements("us")
    selected_statements = _selected_financial_history_statements("us", statements)
    paths = {
        statement: financial_history_cache_path("us", normalized_code, statement, root_dir)
        for statement in all_statements
    }

    def fetch_statement(symbol: str) -> pd.DataFrame:
        try:
            frame = ak.stock_financial_us_report_em(
                stock=normalized_code,
                symbol=symbol,
                indicator="年报",
            )
        except TypeError as exc:
            if "'NoneType' object is not subscriptable" in str(exc):
                return pd.DataFrame()
            raise
        return frame if frame is not None else pd.DataFrame()

    try:
        added_rows = {"balance": 0, "income": 0, "cashflow": 0}
        if "balance" in selected_statements:
            balance_frame = fetch_statement("资产负债表")
            added_rows["balance"] = _save_financial_history_statement(
                paths["balance"],
                balance_frame,
                normalized_code,
                "us",
                fetched_at,
                "REPORT_DATE",
                refresh,
            )
        if "income" in selected_statements:
            income_frame = fetch_statement("综合损益表")
            added_rows["income"] = _save_financial_history_statement(
                paths["income"],
                income_frame,
                normalized_code,
                "us",
                fetched_at,
                "REPORT_DATE",
                refresh,
            )
        if "cashflow" in selected_statements:
            cashflow_frame = fetch_statement("现金流量表")
            added_rows["cashflow"] = _save_financial_history_statement(
                paths["cashflow"],
                cashflow_frame,
                normalized_code,
                "us",
                fetched_at,
                "REPORT_DATE",
                refresh,
            )
        return FinancialHistoryCacheResult(
            code=normalized_code,
            added_rows_by_statement=added_rows,
            paths_by_statement=paths,
            status="成功",
        )
    except Exception as exc:  # pragma: no cover - network failure path
        return _financial_history_failure_result(
            code=normalized_code,
            market="us",
            root_dir=root_dir,
            statements=statements,
            status=f"失败: {str(exc)[:120]}",
        )
