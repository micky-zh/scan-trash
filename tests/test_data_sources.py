import pandas as pd

from hk_value_screener.data_sources import (
    CN_SPOT_FULL_COLUMN_ORDER,
    HK_SPOT_FULL_COLUMN_ORDER,
    US_SPOT_FULL_COLUMN_ORDER,
    build_cn_research_view,
    build_hk_research_view,
    build_us_research_view,
    cninfo_pdf_url,
    filing_index_cache_path,
    filing_pdf_cache_path,
    financial_history_cache_path,
    merge_filing_index_cache,
    merge_financial_history_cache,
    normalize_cn_spot_full,
    normalize_hk_spot_full,
    normalize_us_spot_full,
    parse_cninfo_disclosure_link,
)


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
                "0.2",
                "0.95",
                "4.5",
                "6.8",
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
    assert normalized.loc[0, "涨速"] == 0.2
    assert normalized.loc[0, "60日涨跌幅"] == 4.5
    assert normalized.loc[0, "年初至今涨跌幅"] == 6.8
    assert normalized.loc[0, "总市值"] == 1234567890
    assert normalized.loc[0, "换手率"] == 0.8


def test_normalize_us_spot_full_keeps_value_fields() -> None:
    raw = pd.DataFrame(
        [
            {
                "序号": 1,
                "名称": "Apple Inc.",
                "最新价": "189.98",
                "涨跌额": "1.25",
                "涨跌幅": "0.66",
                "开盘价": "188.20",
                "最高价": "190.10",
                "最低价": "187.50",
                "昨收价": "188.73",
                "总市值": "2900000000000",
                "市盈率": "29.5",
                "成交量": "50000000",
                "成交额": "9500000000",
                "振幅": "1.4",
                "换手率": "0.88",
                "代码": "105.AAPL",
            }
        ]
    )

    normalized = normalize_us_spot_full(raw)

    assert list(normalized.columns) == US_SPOT_FULL_COLUMN_ORDER
    assert normalized.loc[0, "代码"] == "AAPL"
    assert normalized.loc[0, "市盈率"] == 29.5
    assert normalized.loc[0, "总市值"] == 2900000000000
    assert normalized.loc[0, "成交额"] == 9500000000
    assert normalized.loc[0, "开盘价"] == 188.2


def test_normalize_cn_spot_full_keeps_value_fields() -> None:
    raw = pd.DataFrame(
        [
            {
                "序号": 1,
                "代码": "1",
                "名称": "平安银行",
                "最新价": "10.25",
                "涨跌幅": "1.2",
                "成交额": "1200000000",
                "换手率": "0.85",
                "市盈率-动态": "5.9",
                "市净率": "0.62",
                "总市值": "240000000000",
                "流通市值": "230000000000",
            }
        ]
    )

    normalized = normalize_cn_spot_full(raw)

    assert list(normalized.columns) == CN_SPOT_FULL_COLUMN_ORDER
    assert normalized.loc[0, "代码"] == "000001"
    assert normalized.loc[0, "市盈率-动态"] == 5.9
    assert normalized.loc[0, "市净率"] == 0.62
    assert normalized.loc[0, "总市值"] == 240000000000
    assert normalized.loc[0, "成交额"] == 1200000000


def test_build_hk_research_view_merges_enriched_fields() -> None:
    spot = pd.DataFrame(
        [
            {
                "代码": "700",
                "名称": "腾讯控股",
                "最新价": 380.0,
                "总市值": 3600000000000,
                "成交额": 1200000000,
                "换手率": 0.3,
                "市盈率-动态": 18.0,
                "市净率": 3.2,
                "股东权益回报率(%)": 22.0,
                "过去5年经营现金流/净利润": 1.1,
                "净负债/EBITDA": 0.1,
                "利息保障倍数": 40.0,
            }
        ]
    )
    enriched = pd.DataFrame(
        [
            {
                "代码": "00700",
                "所属行业": "互联网服务",
                "净资产收益率(平均)(%)": 20.0,
                "毛利率(%)": 48.0,
                "销售净利率(%)": 24.0,
                "流动比率": 1.4,
                "速动比率": 1.2,
                "净现比": 1.05,
                "营业总收入": 600000000000,
                "经营现金流净额": 150000000000,
                "净利润同比增长率(%)": 20.0,
                "补充数据状态": "成功",
            }
        ]
    )

    research_view = build_hk_research_view(spot, enriched)

    assert research_view.loc[0, "代码"] == "00700"
    assert research_view.loc[0, "所属行业"] == "互联网服务"
    assert research_view.loc[0, "毛利率(%)"] == 48.0
    assert research_view.loc[0, "市销率"] == 6.0
    assert research_view.loc[0, "市现率"] == 24.0
    assert research_view.loc[0, "PEG"] == 0.9
    assert list(research_view.columns[:10]) == [
        "代码",
        "名称",
        "所属行业",
        "最新价",
        "总市值",
        "成交额",
        "换手率",
        "市盈率-动态",
        "市净率",
        "股东权益回报率(%)",
    ]


def test_build_us_research_view_merges_financial_ratio_fields() -> None:
    spot = pd.DataFrame(
        [
            {
                "代码": "105.AAPL",
                "名称": "Apple Inc.",
                "最新价": 190.0,
                "总市值": 2900000000000,
                "成交额": 9000000000,
                "换手率": 0.8,
                "市盈率": 29.5,
            }
        ]
    )
    enriched = pd.DataFrame(
        [
            {
                "代码": "AAPL",
                "毛利率(%)": 46.9,
                "销售净利率(%)": 26.9,
                "净资产收益率(平均)(%)": 171.4,
                "流动比率": 0.89,
                "速动比率": 0.86,
                "营业收入": 400000000000,
                "经营现金流净额": 100000000000,
                "净利润同比增长率(%)": 10.0,
                "报告期": "2025/FY",
                "补充数据状态": "成功",
            }
        ]
    )

    research_view = build_us_research_view(spot, enriched)

    assert research_view.loc[0, "代码"] == "AAPL"
    assert research_view.loc[0, "毛利率(%)"] == 46.9
    assert research_view.loc[0, "报告期"] == "2025/FY"
    assert research_view.loc[0, "市销率"] == 7.25
    assert research_view.loc[0, "市现率"] == 29.0
    assert research_view.loc[0, "PEG"] == 2.95
    assert list(research_view.columns[:7]) == [
        "代码",
        "名称",
        "最新价",
        "总市值",
        "成交额",
        "换手率",
        "市盈率",
    ]


def test_build_cn_research_view_merges_financial_ratio_fields() -> None:
    spot = pd.DataFrame(
        [
            {
                "代码": "1",
                "名称": "平安银行",
                "最新价": 10.25,
                "总市值": 240000000000,
                "流通市值": 230000000000,
                "成交额": 1200000000,
                "换手率": 0.85,
                "市盈率-动态": 5.9,
                "市净率": 0.62,
            }
        ]
    )
    enriched = pd.DataFrame(
        [
            {
                "代码": "000001",
                "销售毛利率(%)": 50.0,
                "销售净利率(%)": 35.0,
                "净资产收益率(%)": 10.5,
                "流动比率": 1.1,
                "速动比率": 0.95,
                "营业总收入": "1200亿",
                "经营现金流净额": "300亿",
                "净利润同比增长率(%)": "-5%",
                "报告期": "2025",
                "补充数据状态": "成功",
            }
        ]
    )

    research_view = build_cn_research_view(spot, enriched)

    assert research_view.loc[0, "代码"] == "000001"
    assert research_view.loc[0, "销售毛利率(%)"] == 50.0
    assert research_view.loc[0, "报告期"] == "2025"
    assert research_view.loc[0, "市销率"] == 2.0
    assert research_view.loc[0, "市现率"] == 8.0
    assert round(research_view.loc[0, "PEG"], 2) == -1.18
    assert list(research_view.columns[:9]) == [
        "代码",
        "名称",
        "最新价",
        "总市值",
        "流通市值",
        "成交额",
        "换手率",
        "市盈率-动态",
        "市净率",
    ]


def test_merge_financial_history_cache_appends_only_new_reports() -> None:
    existing = pd.DataFrame(
        [
            {"代码": "000001", "抓取时间": "2026-01-01T00:00:00", "日期": "2025-12-31", "ROA": 1.0}
        ]
    )
    fetched = pd.DataFrame(
        [
            {"代码": "000001", "抓取时间": "2026-04-01T00:00:00", "日期": "2025-12-31", "ROA": 1.1},
            {"代码": "000001", "抓取时间": "2026-04-01T00:00:00", "日期": "2026-03-31", "ROA": 1.2},
        ]
    )

    merged, added_count = merge_financial_history_cache(existing, fetched, report_column="日期")

    assert added_count == 1
    assert list(merged["日期"]) == ["2025-12-31", "2026-03-31"]


def test_merge_financial_history_cache_refresh_keeps_old_reports() -> None:
    existing = pd.DataFrame(
        [
            {"代码": "000001", "抓取时间": "2026-01-01T00:00:00", "报告期": "2025", "净利润": "1亿"}
        ]
    )
    fetched = pd.DataFrame(
        [
            {"代码": "000001", "抓取时间": "2026-04-01T00:00:00", "报告期": "2025", "净利润": "2亿"}
        ]
    )

    merged, added_count = merge_financial_history_cache(
        existing,
        fetched,
        report_column="报告期",
        refresh=True,
    )

    assert added_count == 1
    assert len(merged) == 2
    assert list(merged["净利润"]) == ["1亿", "2亿"]


def test_financial_history_cache_path_normalizes_codes_by_market() -> None:
    assert str(financial_history_cache_path("cn", "1", "indicators")).endswith(
        "data/raw/financials/cn/indicators/000001.csv"
    )
    assert str(financial_history_cache_path("hk", "700", "balance")).endswith(
        "data/raw/financials/hk/balance/00700.csv"
    )
    assert str(financial_history_cache_path("us", "105.AAPL", "cashflow")).endswith(
        "data/raw/financials/us/cashflow/AAPL.csv"
    )


def test_parse_cninfo_disclosure_link_and_builds_pdf_url() -> None:
    link = (
        "http://www.cninfo.com.cn/new/disclosure/detail?"
        "stockCode=000001&announcementId=1225022887&"
        "orgId=gssz0000001&announcementTime=2026-03-21"
    )

    parsed = parse_cninfo_disclosure_link(link)

    assert parsed["announcement_id"] == "1225022887"
    assert parsed["announcement_time"] == "2026-03-21"
    assert cninfo_pdf_url(link) == (
        "http://static.cninfo.com.cn/finalpage/2026-03-21/1225022887.PDF"
    )


def test_filing_paths_normalize_code_and_safe_filename() -> None:
    assert str(filing_index_cache_path("cn", "1")).endswith(
        "data/raw/filings/cn/000001/index.csv"
    )
    assert str(
        filing_pdf_cache_path("cn", "1", "2026-03-21", "1225022887", "2025年年度报告/摘要")
    ).endswith("data/raw/filings/cn/000001/pdfs/2026-03-21_1225022887_2025年年度报告_摘要.pdf")


def test_merge_filing_index_cache_appends_only_new_links() -> None:
    existing = pd.DataFrame(
        [
            {
                "代码": "000001",
                "公告标题": "2024年年度报告",
                "公告链接": "https://example.com/old",
            }
        ]
    )
    fetched = pd.DataFrame(
        [
            {
                "代码": "000001",
                "公告标题": "2024年年度报告更新",
                "公告链接": "https://example.com/old",
            },
            {
                "代码": "000001",
                "公告标题": "2025年年度报告",
                "公告链接": "https://example.com/new",
            },
        ]
    )

    merged, added_count = merge_filing_index_cache(existing, fetched)

    assert added_count == 1
    assert list(merged["公告链接"]) == ["https://example.com/old", "https://example.com/new"]
