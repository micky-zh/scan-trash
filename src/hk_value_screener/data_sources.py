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

FINANCIAL_REQUIRED_FIELDS = [
    "code",
    "roe_pct",
    "net_income_positive_years_5y",
    "operating_cashflow_positive_years_5y",
    "ocf_to_net_income_avg_5y",
    "fcf_positive_years_5y",
    "net_debt_to_ebitda",
    "interest_coverage",
]

_FINANCIAL_FIELD_ALIASES = {
    "code": ["code", "代码", "ticker", "symbol"],
    "roe_pct": ["roe_pct", "roe", "ROE", "净资产收益率", "净资产收益率_5y_avg"],
    "net_income_positive_years_5y": [
        "net_income_positive_years_5y",
        "净利润为正年数_5y",
        "过去5年净利润为正年数",
    ],
    "operating_cashflow_positive_years_5y": [
        "operating_cashflow_positive_years_5y",
        "经营现金流为正年数_5y",
        "过去5年经营现金流为正年数",
    ],
    "ocf_to_net_income_avg_5y": [
        "ocf_to_net_income_avg_5y",
        "经营现金流净利润比_5y_avg",
        "ocf_net_income_ratio_5y",
    ],
    "fcf_positive_years_5y": [
        "fcf_positive_years_5y",
        "自由现金流为正年数_5y",
        "过去5年自由现金流为正年数",
    ],
    "net_debt_to_ebitda": [
        "net_debt_to_ebitda",
        "净负债倍数",
        "净负债EBITDA比",
    ],
    "interest_coverage": [
        "interest_coverage",
        "利息保障倍数",
        "ebit_interest_coverage",
    ],
}

ENRICHED_DISPLAY_COLUMNS = [
    "序号",
    "代码",
    "名称",
    "公司名称",
    "英文名称",
    "所属行业",
    "上市日期",
    "板块",
    "是否沪港通标的",
    "是否深港通标的",
    "每手股数",
    "员工人数",
    "最新价",
    "总市值",
    "流通市值",
    "涨跌幅",
    "涨跌额",
    "60日涨跌幅",
    "年初至今涨跌幅",
    "成交量",
    "成交额",
    "换手率",
    "市盈率-动态",
    "市净率",
    "股息率TTM(%)",
    "每股股息TTM(港元)",
    "派息比率(%)",
    "基本每股收益(元)",
    "每股净资产(元)",
    "每股经营现金流(元)",
    "营业总收入",
    "营业总收入同比(%)",
    "营业总收入环比(%)",
    "净利润",
    "净利润同比(%)",
    "净利润环比(%)",
    "股东权益回报率(%)",
    "总资产回报率(%)",
    "净资产收益率(平均)(%)",
    "毛利率(%)",
    "销售净利率(%)",
    "投入资本回报率(%)",
    "资产负债率(%)",
    "流动比率",
    "速动比率",
    "现金比率",
    "净现比",
    "流动资产合计",
    "流动负债合计",
    "存货",
    "现金及等价物",
    "应收账款",
    "最新公告日期",
    "最新财政年度",
    "最新分红方案",
    "最新分配类型",
    "最新除净日",
    "最新发放日",
    "补充数据状态",
]

FINANCIAL_SCREENED_DISPLAY_COLUMNS = [
    "序号",
    "代码",
    "名称",
    "最新价",
    "总市值",
    "流通市值",
    "成交额",
    "换手率",
    "60日涨跌幅",
    "年初至今涨跌幅",
    "市盈率-动态",
    "市净率",
    "股东权益回报率(%)",
    "过去5年净利润为正年数",
    "过去5年经营现金流为正年数",
    "过去5年经营现金流/净利润",
    "过去5年自由现金流为正年数",
    "净负债/EBITDA",
    "利息保障倍数",
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


def load_financial_indicators_csv(path: Path, code_column: str = "代码") -> pd.DataFrame:
    """
    Load and normalize a local financial indicators CSV to the canonical baseline fields.
    """
    frame = pd.read_csv(path, dtype={code_column: str, "code": str})
    normalized = frame.copy()

    rename_map: dict[str, str] = {}
    for canonical_field, aliases in _FINANCIAL_FIELD_ALIASES.items():
        for alias in aliases:
            if alias in normalized.columns:
                rename_map[alias] = canonical_field
                break

    normalized = normalized.rename(columns=rename_map)

    missing_fields = [field for field in FINANCIAL_REQUIRED_FIELDS if field not in normalized.columns]
    if missing_fields:
        missing_text = ", ".join(missing_fields)
        raise ValueError(f"Missing required financial fields: {missing_text}")

    normalized["code"] = normalized["code"].astype(str).str.zfill(5)

    numeric_fields = [field for field in FINANCIAL_REQUIRED_FIELDS if field != "code"]
    for field in numeric_fields:
        normalized[field] = pd.to_numeric(normalized[field], errors="coerce")

    return normalized[FINANCIAL_REQUIRED_FIELDS].copy()


def merge_spot_with_financials(spot_frame: pd.DataFrame, financial_frame: pd.DataFrame) -> pd.DataFrame:
    """
    Merge live spot data with local financial indicators on stock code.
    """
    merged_spot = spot_frame.copy()
    merged_spot["代码"] = merged_spot["代码"].astype(str).str.zfill(5)
    financials = financial_frame.copy()
    financials["code"] = financials["code"].astype(str).str.zfill(5)
    merged = merged_spot.merge(financials, left_on="代码", right_on="code", how="inner")

    # Provide the canonical rule-engine field names expected by baseline.yaml.
    merged["price_hkd"] = pd.to_numeric(merged["最新价"], errors="coerce")
    merged["market_cap_b_hkd"] = pd.to_numeric(merged["总市值"], errors="coerce") / 1_000_000_000
    merged["pe_ttm"] = pd.to_numeric(merged["市盈率-动态"], errors="coerce")
    merged["avg_turnover_m_hkd"] = pd.to_numeric(merged["成交额"], errors="coerce") / 1_000_000
    merged["name"] = merged["名称"].astype(str)

    return merged


def build_financial_screened_export(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the internal financial-screened frame into a Chinese-only display table.
    """
    export = frame.copy()
    export["股东权益回报率(%)"] = export["roe_pct"]
    export["过去5年净利润为正年数"] = export["net_income_positive_years_5y"]
    export["过去5年经营现金流为正年数"] = export["operating_cashflow_positive_years_5y"]
    export["过去5年经营现金流/净利润"] = export["ocf_to_net_income_avg_5y"]
    export["过去5年自由现金流为正年数"] = export["fcf_positive_years_5y"]
    export["净负债/EBITDA"] = export["net_debt_to_ebitda"]
    export["利息保障倍数"] = export["interest_coverage"]

    selected_columns = [column for column in FINANCIAL_SCREENED_DISPLAY_COLUMNS if column in export.columns]
    cleaned = export[selected_columns].copy()
    cleaned["代码"] = cleaned["代码"].astype(str).str.zfill(5)
    return cleaned


def _fetch_hk_financial_indicator_snapshot(symbol: str) -> dict[str, object]:
    import akshare as ak

    frame = ak.stock_hk_financial_indicator_em(symbol=symbol)
    if frame.empty:
        return {}

    row = frame.iloc[0]
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
    return {field: row.get(field) for field in selected_fields}


def _fetch_hk_analysis_indicator_snapshot(symbol: str) -> dict[str, object]:
    import akshare as ak

    frame = ak.stock_financial_hk_analysis_indicator_em(symbol=symbol, indicator="年度")
    if frame.empty:
        return {}

    row = frame.iloc[0]
    return {
        "净资产收益率(平均)(%)": row.get("ROE_AVG"),
        "投入资本回报率(%)": row.get("ROIC_YEARLY"),
        "资产负债率(%)": row.get("DEBT_ASSET_RATIO"),
        "流动比率": row.get("CURRENT_RATIO"),
        "毛利率(%)": row.get("GROSS_PROFIT_RATIO"),
        "营业总收入同比(%)": row.get("OPERATE_INCOME_YOY"),
        "营业总收入环比(%)": row.get("OPERATE_INCOME_QOQ"),
        "净利润同比(%)": row.get("HOLDER_PROFIT_YOY"),
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
    names = frame["STD_ITEM_NAME"].astype(str)

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


def _fetch_hk_derived_report_metrics(symbol: str) -> dict[str, object]:
    import akshare as ak

    balance_sheet = ak.stock_financial_hk_report_em(stock=symbol, symbol="资产负债表", indicator="年度")
    cash_flow = ak.stock_financial_hk_report_em(stock=symbol, symbol="现金流量表", indicator="年度")

    bs = _latest_report_rows(balance_sheet)
    cf = _latest_report_rows(cash_flow)

    current_assets = _find_amount(bs, ["流动资产合计"])
    current_liabilities = _find_amount(bs, ["流动负债合计"])
    inventory = _find_amount(bs, ["存货"])
    cash_and_equivalents = _find_amount(bs, ["现金及等价物", "现金及现金等价物", "现金及等价物"])
    receivables = _find_amount(bs, ["应收帐款", "应收账款", "贸易应收款", "贸易应收账款"])
    operating_cash_flow = _find_amount(cf, ["经营业务现金净额", "经营产生现金", "经营活动现金净额"])

    quick_ratio = _safe_ratio(
        None if current_assets is None or inventory is None else current_assets - inventory,
        current_liabilities,
    )
    cash_ratio = _safe_ratio(cash_and_equivalents, current_liabilities)

    return {
        "流动资产合计": current_assets,
        "流动负债合计": current_liabilities,
        "存货": inventory,
        "现金及等价物": cash_and_equivalents,
        "应收账款": receivables,
        "经营业务现金净额": operating_cash_flow,
        "速动比率": quick_ratio,
        "现金比率": cash_ratio,
    }


def fetch_hk_enriched_metrics(symbol: str) -> dict[str, object]:
    """
    Fetch a richer Hong Kong stock profile using AKShare's Hong Kong-specific endpoints.
    """
    normalized_symbol = str(symbol).zfill(5)
    result: dict[str, object] = {"代码": normalized_symbol, "补充数据状态": "成功"}

    try:
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
    except Exception as exc:  # pragma: no cover - network failure path
        result["补充数据状态"] = f"失败: {str(exc)[:120]}"

    return result


def enrich_hk_screened_frame(frame: pd.DataFrame, limit: int | None = None) -> pd.DataFrame:
    """
    Enrich a screened Hong Kong stock list with broader financial and dividend fields.
    """
    working = frame.copy()
    working["代码"] = working["代码"].astype(str).str.zfill(5)
    if limit is not None:
        working = working.head(limit).copy()

    records = [fetch_hk_enriched_metrics(code) for code in working["代码"].tolist()]
    enriched_metrics = pd.DataFrame(records)
    merged = working.merge(enriched_metrics, on="代码", how="left")

    selected_columns = [column for column in ENRICHED_DISPLAY_COLUMNS if column in merged.columns]
    cleaned = merged[selected_columns].copy()
    cleaned["代码"] = cleaned["代码"].astype(str).str.zfill(5)
    return cleaned
