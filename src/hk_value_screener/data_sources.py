from __future__ import annotations

import signal
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
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
    "应收账款周转率",
    "存货周转率",
    "总资产周转率",
    "报告期",
    "报告日期",
    "币种",
    "补充数据状态",
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
]

MARKET_CODE_PAD_WIDTH = {
    "hk": 5,
    "us": 0,
    "cn": 6,
}


@dataclass(frozen=True)
class FinancialHistoryCacheResult:
    code: str
    added_rows_by_statement: dict[str, int]
    paths_by_statement: dict[str, Path]
    status: str

    @property
    def added_rows(self) -> int:
        return sum(self.added_rows_by_statement.values())


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


def build_hk_research_view(
    base_frame: pd.DataFrame,
    enriched_metrics_frame: pd.DataFrame,
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
    selected_columns = [column for column in HK_RESEARCH_VIEW_COLUMNS if column in merged.columns]
    return merged[selected_columns].copy()


def build_us_research_view(
    base_frame: pd.DataFrame,
    enriched_metrics_frame: pd.DataFrame,
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
    selected_columns = [column for column in US_RESEARCH_VIEW_COLUMNS if column in merged.columns]
    return merged[selected_columns].copy()


def build_cn_research_view(
    base_frame: pd.DataFrame,
    enriched_metrics_frame: pd.DataFrame,
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
    import akshare as ak

    balance_sheet = ak.stock_financial_hk_report_em(
        stock=symbol,
        symbol="资产负债表",
        indicator="年度",
    )
    income_statement = ak.stock_financial_hk_report_em(
        stock=symbol,
        symbol="利润表",
        indicator="年度",
    )
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
    import akshare as ak

    balance_sheet = ak.stock_financial_us_report_em(
        stock=symbol,
        symbol="资产负债表",
        indicator="年报",
    )
    income_statement = ak.stock_financial_us_report_em(
        stock=symbol,
        symbol="综合损益表",
        indicator="年报",
    )
    cash_flow = ak.stock_financial_us_report_em(
        stock=symbol,
        symbol="现金流量表",
        indicator="年报",
    )

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

    return {
        "经营现金流净额": operating_cash_flow,
        "资本开支": capital_expenditure,
        "自由现金流": free_cash_flow,
        "FCF/净利润": _safe_divide(free_cash_flow, net_profit),
        "每股自由现金流": _safe_divide(free_cash_flow, shares),
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
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(existing_path, index=False)
    return added_rows


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


def cache_cn_financial_history(
    code: str,
    root_dir: Path | None = None,
    refresh: bool = False,
) -> FinancialHistoryCacheResult:
    import akshare as ak

    normalized_code = normalize_security_code(code, market="cn")
    fetched_at = datetime.now().isoformat(timespec="seconds")
    statements = ["indicators", "abstracts"]
    paths = {
        statement: financial_history_cache_path("cn", normalized_code, statement, root_dir)
        for statement in statements
    }

    try:
        indicator_frame = ak.stock_financial_analysis_indicator(
            symbol=normalized_code,
            start_year="2000",
        )
        abstract_frame = ak.stock_financial_abstract_ths(
            symbol=normalized_code,
            indicator="按年度",
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
) -> FinancialHistoryCacheResult:
    import akshare as ak

    normalized_code = normalize_security_code(code, market="hk")
    fetched_at = datetime.now().isoformat(timespec="seconds")
    statements = ["balance", "income", "cashflow"]
    paths = {
        statement: financial_history_cache_path("hk", normalized_code, statement, root_dir)
        for statement in statements
    }

    try:
        balance_frame = ak.stock_financial_hk_report_em(
            stock=normalized_code,
            symbol="资产负债表",
            indicator="年度",
        )
        income_frame = ak.stock_financial_hk_report_em(
            stock=normalized_code,
            symbol="利润表",
            indicator="年度",
        )
        cashflow_frame = ak.stock_financial_hk_report_em(
            stock=normalized_code,
            symbol="现金流量表",
            indicator="年度",
        )

        added_rows = {
            "balance": _save_financial_history_statement(
                paths["balance"],
                balance_frame,
                normalized_code,
                "hk",
                fetched_at,
                "REPORT_DATE",
                refresh,
            ),
            "income": _save_financial_history_statement(
                paths["income"],
                income_frame,
                normalized_code,
                "hk",
                fetched_at,
                "REPORT_DATE",
                refresh,
            ),
            "cashflow": _save_financial_history_statement(
                paths["cashflow"],
                cashflow_frame,
                normalized_code,
                "hk",
                fetched_at,
                "REPORT_DATE",
                refresh,
            ),
        }
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
) -> FinancialHistoryCacheResult:
    import akshare as ak

    normalized_code = normalize_security_code(code, market="us")
    fetched_at = datetime.now().isoformat(timespec="seconds")
    statements = ["balance", "income", "cashflow"]
    paths = {
        statement: financial_history_cache_path("us", normalized_code, statement, root_dir)
        for statement in statements
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
        balance_frame = fetch_statement("资产负债表")
        income_frame = fetch_statement("综合损益表")
        cashflow_frame = fetch_statement("现金流量表")

        added_rows = {
            "balance": _save_financial_history_statement(
                paths["balance"],
                balance_frame,
                normalized_code,
                "us",
                fetched_at,
                "REPORT_DATE",
                refresh,
            ),
            "income": _save_financial_history_statement(
                paths["income"],
                income_frame,
                normalized_code,
                "us",
                fetched_at,
                "REPORT_DATE",
                refresh,
            ),
            "cashflow": _save_financial_history_statement(
                paths["cashflow"],
                cashflow_frame,
                normalized_code,
                "us",
                fetched_at,
                "REPORT_DATE",
                refresh,
            ),
        }
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
