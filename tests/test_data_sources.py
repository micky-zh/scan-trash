import pandas as pd
import pytest

from hk_value_screener.data_sources import (
    CN_FILING_CATEGORIES,
    CN_SPOT_FULL_COLUMN_ORDER,
    cleanup_filing_pdf_cache,
    cache_us_financial_history,
    cache_us_filings,
    HK_FILING_CATEGORIES,
    HK_SPOT_FULL_COLUMN_ORDER,
    US_SPOT_FULL_COLUMN_ORDER,
    _fetch_hk_derived_report_metrics,
    _fetch_cn_derived_history_metrics,
    _fetch_us_derived_report_metrics,
    _fill_missing_filing_categories,
    _parse_hkex_title_search,
    build_cn_research_view,
    build_hk_research_view,
    fetch_hk_enriched_metrics,
    build_us_research_view,
    cninfo_pdf_url,
    filing_index_cache_path,
    filing_pdf_cache_path,
    financial_history_cache_path,
    merge_filing_index_cache,
    merge_financial_history_cache,
    prune_recent_filing_index,
    prune_recent_financial_history,
    normalize_cn_spot_full,
    normalize_hk_spot_full,
    normalize_us_filing_ticker,
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
    blacklist = pd.DataFrame(
        [
            {
                "market": "hk",
                "code": "00700",
                "reason": "业务太复杂",
                "enabled": True,
            }
        ]
    )

    research_view = build_hk_research_view(spot, enriched, blacklist_frame=blacklist)

    assert research_view.loc[0, "代码"] == "00700"
    assert research_view.loc[0, "所属行业"] == "互联网服务"
    assert research_view.loc[0, "毛利率(%)"] == 48.0
    assert research_view.loc[0, "市销率"] == 6.0
    assert research_view.loc[0, "市现率"] == 24.0
    assert research_view.loc[0, "PEG"] == 0.9
    assert research_view.loc[0, "黑名单"] == "是"
    assert research_view.loc[0, "黑名单原因"] == "业务太复杂"
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


def test_fetch_hk_enriched_metrics_fills_alias_columns(monkeypatch) -> None:
    monkeypatch.setattr(
        "hk_value_screener.data_sources._fetch_hk_financial_indicator_snapshot",
        lambda symbol: {},
    )
    monkeypatch.setattr(
        "hk_value_screener.data_sources._fetch_hk_analysis_indicator_snapshot",
        lambda symbol: {},
    )
    monkeypatch.setattr(
        "hk_value_screener.data_sources._fetch_hk_dividend_snapshot",
        lambda symbol: {},
    )
    monkeypatch.setattr(
        "hk_value_screener.data_sources._fetch_hk_company_profile_snapshot",
        lambda symbol: {},
    )
    monkeypatch.setattr(
        "hk_value_screener.data_sources._fetch_hk_security_profile_snapshot",
        lambda symbol: {},
    )
    monkeypatch.setattr(
        "hk_value_screener.data_sources._fetch_hk_derived_report_metrics",
        lambda symbol: {
            "净债务/EBITDA": 1.2,
            "利息支付倍数": 8.8,
        },
    )

    metrics = fetch_hk_enriched_metrics("00700", timeout_seconds=0)

    assert metrics["净债务/EBITDA"] == 1.2
    assert metrics["净负债/EBITDA"] == 1.2
    assert metrics["利息支付倍数"] == 8.8
    assert metrics["利息保障倍数"] == 8.8


def test_fetch_hk_derived_report_metrics_computes_long_term_fields(monkeypatch) -> None:
    def make_frame(kind: str) -> pd.DataFrame:
        rows = [{"REPORT_DATE": "2024-12-31"}]
        if kind == "income":
            for year, revenue, profit in [
                (2019, 100.0, 20.0),
                (2020, 125.0, 25.0),
                (2021, 156.25, 31.25),
                (2022, 195.3125, 39.0625),
                (2023, 244.140625, 48.828125),
                (2024, 305.17578125, 61.03515625),
            ]:
                report_date = f"{year}-12-31"
                rows.extend(
                    [
                        {"REPORT_DATE": report_date, "ITEM_NAME": "营业额", "AMOUNT": revenue},
                        {"REPORT_DATE": report_date, "ITEM_NAME": "股东应占溢利", "AMOUNT": profit},
                    ]
                )
        elif kind == "cashflow":
            for year, ocf in [
                (2019, 120.0),
                (2020, 150.0),
                (2021, 187.5),
                (2022, 234.375),
                (2023, 292.96875),
                (2024, 366.2109375),
            ]:
                report_date = f"{year}-12-31"
                rows.extend(
                    [
                        {
                            "REPORT_DATE": report_date,
                            "ITEM_NAME": "经营业务现金净额",
                            "AMOUNT": ocf,
                        },
                        {
                            "REPORT_DATE": report_date,
                            "ITEM_NAME": "购建固定资产",
                            "AMOUNT": 20.0,
                        },
                    ]
                )
        return pd.DataFrame(rows)

    def fake_read_cache(market: str, code: str, statement: str) -> pd.DataFrame:
        if market != "hk":
            return pd.DataFrame()
        if statement == "balance":
            return pd.DataFrame([{"REPORT_DATE": "2024-12-31"}])
        if statement == "income":
            return make_frame("income")
        if statement == "cashflow":
            return make_frame("cashflow")
        return pd.DataFrame()

    monkeypatch.setattr("hk_value_screener.data_sources._read_financial_history_cache", fake_read_cache)

    metrics = _fetch_hk_derived_report_metrics("00700")

    assert metrics["过去3年营业总收入CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去5年营业总收入CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去3年净利润CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去5年净利润CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去5年净利润为正年数"] == 5
    assert metrics["过去5年经营现金流为正年数"] == 5
    assert metrics["过去5年经营现金流/净利润"] == pytest.approx(6.0)
    assert metrics["过去5年自由现金流为正年数"] == 5


def test_fetch_us_derived_report_metrics_computes_long_term_fields(monkeypatch) -> None:
    def make_frame(kind: str) -> pd.DataFrame:
        rows = [{"REPORT_DATE": "2024-12-31"}]
        if kind == "income":
            for year, revenue, profit in [
                (2019, 100.0, 20.0),
                (2020, 125.0, 25.0),
                (2021, 156.25, 31.25),
                (2022, 195.3125, 39.0625),
                (2023, 244.140625, 48.828125),
                (2024, 305.17578125, 61.03515625),
            ]:
                report_date = f"{year}-12-31"
                rows.extend(
                    [
                        {"REPORT_DATE": report_date, "ITEM_NAME": "营业收入", "AMOUNT": revenue},
                        {
                            "REPORT_DATE": report_date,
                            "ITEM_NAME": "归属于普通股股东净利润",
                            "AMOUNT": profit,
                        },
                    ]
                )
        elif kind == "cashflow":
            for year, ocf in [
                (2019, 120.0),
                (2020, 150.0),
                (2021, 187.5),
                (2022, 234.375),
                (2023, 292.96875),
                (2024, 366.2109375),
            ]:
                report_date = f"{year}-12-31"
                rows.extend(
                    [
                        {
                            "REPORT_DATE": report_date,
                            "ITEM_NAME": "经营活动产生的现金流量净额",
                            "AMOUNT": ocf,
                        },
                        {
                            "REPORT_DATE": report_date,
                            "ITEM_NAME": "购买固定资产",
                            "AMOUNT": 20.0,
                        },
                    ]
                )
        return pd.DataFrame(rows)

    def fake_read_cache(market: str, code: str, statement: str) -> pd.DataFrame:
        if market != "us":
            return pd.DataFrame()
        if statement == "balance":
            return pd.DataFrame([{"REPORT_DATE": "2024-12-31"}])
        if statement == "income":
            return make_frame("income")
        if statement == "cashflow":
            return make_frame("cashflow")
        return pd.DataFrame()

    monkeypatch.setattr("hk_value_screener.data_sources._read_financial_history_cache", fake_read_cache)

    metrics = _fetch_us_derived_report_metrics("AAPL", 1.0)

    assert metrics["过去3年营业收入CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去5年营业收入CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去3年归母净利润CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去5年归母净利润CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去5年归母净利润为正年数"] == 5
    assert metrics["过去5年经营现金流为正年数"] == 5
    assert metrics["过去5年经营现金流/净利润"] == pytest.approx(6.0)
    assert metrics["过去5年自由现金流为正年数"] == 5


def test_fetch_cn_derived_history_metrics_computes_long_term_fields() -> None:
    abstract = pd.DataFrame(
        [
            {
                "报告期": str(year),
                "营业总收入": revenue,
                "净利润": profit,
                "基本每股收益": 2.0,
                "每股经营现金流": 12.0,
            }
            for year, revenue, profit in [
                (2019, 100.0, 20.0),
                (2020, 125.0, 25.0),
                (2021, 156.25, 31.25),
                (2022, 195.3125, 39.0625),
                (2023, 244.140625, 48.828125),
                (2024, 305.17578125, 61.03515625),
            ]
        ]
    )

    metrics = _fetch_cn_derived_history_metrics(abstract)

    assert metrics["过去3年营业总收入CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去5年营业总收入CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去3年净利润CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去5年净利润CAGR(%)"] == pytest.approx(25.0)
    assert metrics["过去5年净利润为正年数"] == 5
    assert metrics["过去5年经营现金流为正年数"] == 5
    assert metrics["过去5年经营现金流/净利润"] == pytest.approx(6.0)


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
    blacklist = pd.DataFrame(
        [
            {
                "market": "us",
                "code": "AAPL",
                "reason": "不懂业务",
                "enabled": True,
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

    research_view = build_us_research_view(spot, enriched, blacklist_frame=blacklist)

    assert research_view.loc[0, "代码"] == "AAPL"
    assert research_view.loc[0, "毛利率(%)"] == 46.9
    assert research_view.loc[0, "报告期"] == "2025/FY"
    assert research_view.loc[0, "市销率"] == 7.25
    assert research_view.loc[0, "市现率"] == 29.0
    assert research_view.loc[0, "PEG"] == 2.95
    assert research_view.loc[0, "黑名单"] == "是"
    assert research_view.loc[0, "黑名单原因"] == "不懂业务"
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
    blacklist = pd.DataFrame(
        [
            {
                "market": "cn",
                "code": "000001",
                "reason": "数据口径不清楚",
                "enabled": True,
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

    research_view = build_cn_research_view(spot, enriched, blacklist_frame=blacklist)

    assert research_view.loc[0, "代码"] == "000001"
    assert research_view.loc[0, "销售毛利率(%)"] == 50.0
    assert research_view.loc[0, "报告期"] == "2025"
    assert research_view.loc[0, "市销率"] == 2.0
    assert research_view.loc[0, "市现率"] == 8.0
    assert round(research_view.loc[0, "PEG"], 2) == -1.18
    assert research_view.loc[0, "黑名单"] == "是"
    assert research_view.loc[0, "黑名单原因"] == "数据口径不清楚"
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


def test_prune_recent_financial_history_keeps_recent_rows() -> None:
    frame = pd.DataFrame(
        [
            {"日期": "2020-12-31", "值": 1},
            {"日期": "2021-06-30", "值": 2},
            {"日期": "2024-12-31", "值": 3},
        ]
    )

    pruned = prune_recent_financial_history(
        frame,
        report_column="日期",
        as_of=pd.Timestamp("2026-05-05"),
    )

    assert pruned["值"].tolist() == [2, 3]


def test_prune_recent_filing_index_keeps_recent_rows() -> None:
    frame = pd.DataFrame(
        [
            {"公告时间": "2020-12-31 10:00:00", "公告链接": "old"},
            {"公告时间": "2021-06-30 10:00:00", "公告链接": "keep"},
            {"公告时间": "2024-12-31 10:00:00", "公告链接": "keep2"},
        ]
    )

    pruned = prune_recent_filing_index(frame, as_of=pd.Timestamp("2026-05-05"))

    assert pruned["公告链接"].tolist() == ["keep", "keep2"]


def test_cleanup_filing_pdf_cache_removes_orphans(tmp_path) -> None:
    pdf_dir = tmp_path / "pdfs"
    pdf_dir.mkdir()
    kept_pdf = pdf_dir / "2024-12-31_keep.pdf"
    old_pdf = pdf_dir / "2020-12-31_old.pdf"
    extra_pdf = pdf_dir / "2024-12-31_extra.pdf"
    kept_pdf.write_text("kept")
    old_pdf.write_text("old")
    extra_pdf.write_text("extra")

    index_frame = pd.DataFrame(
        [
            {
                "公告时间": "2024-12-31 10:00:00",
                "本地文件路径": str(kept_pdf),
            }
        ]
    )

    deleted = cleanup_filing_pdf_cache(
        index_frame,
        pdf_dir,
        as_of=pd.Timestamp("2026-05-05"),
    )

    assert deleted == 2
    assert kept_pdf.exists()
    assert not old_pdf.exists()
    assert not extra_pdf.exists()


def test_normalize_us_filing_ticker_keeps_class_shares() -> None:
    assert normalize_us_filing_ticker("105.AAPL") == "AAPL"
    assert normalize_us_filing_ticker("BRK.B") == "BRK-B"


def test_cache_us_filings_writes_recent_index_and_prunes_raw(
    tmp_path,
    monkeypatch,
) -> None:
    root_dir = tmp_path / "data" / "raw" / "filings" / "us"
    old_dir = root_dir / "AAPL" / "raw" / "000032019326000000"
    old_dir.mkdir(parents=True)
    (old_dir / "old.txt").write_text("old")

    monkeypatch.setattr(
        "hk_value_screener.data_sources.load_sec_company_ticker_map",
        lambda: {"AAPL": "0000320193"},
    )
    monkeypatch.setattr(
        "hk_value_screener.data_sources._retention_cutoff",
        lambda years=5, as_of=None: pd.Timestamp("2021-01-01"),
    )

    submissions_payload = {
        "name": "Apple Inc.",
        "filings": {
            "recent": {
                "form": ["10-K", "10-K"],
                "accessionNumber": ["0000320193-26-000001", "0000320193-20-000001"],
                "filingDate": ["2026-01-01", "2020-01-01"],
                "reportDate": ["2025-12-31", "2019-12-31"],
                "primaryDocument": ["aapl-20251231.htm", "aapl-20191231.htm"],
                "primaryDocDescription": ["Annual report", "Annual report"],
                "isXBRL": [True, True],
                "isInlineXBRL": [True, True],
                "period": ["2025-12-31", "2019-12-31"],
            },
            "files": [],
        },
    }

    def fake_request_json(url: str, timeout_seconds: int = 30, user_agent: str | None = None):
        if "submissions/CIK0000320193.json" in url:
            return submissions_payload
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr("hk_value_screener.data_sources._request_json", fake_request_json)

    result = cache_us_filings("AAPL", root_dir=root_dir, download=False)

    assert result.status == "成功"
    index_path = root_dir / "AAPL" / "index.csv"
    assert index_path.exists()

    frame = pd.read_csv(index_path)
    assert frame["accession_number"].tolist() == ["0000320193-26-000001"]
    assert not old_dir.exists()


def test_cache_us_filings_downloads_only_primary_document_and_index(
    tmp_path,
    monkeypatch,
) -> None:
    root_dir = tmp_path / "data" / "raw" / "filings" / "us"
    root_dir.mkdir(parents=True)

    monkeypatch.setattr(
        "hk_value_screener.data_sources.load_sec_company_ticker_map",
        lambda: {"AAPL": "0000320193"},
    )
    monkeypatch.setattr(
        "hk_value_screener.data_sources._retention_cutoff",
        lambda years=5, as_of=None: pd.Timestamp("2021-01-01"),
    )

    submissions_payload = {
        "name": "Apple Inc.",
        "filings": {
            "recent": {
                "form": ["10-K"],
                "accessionNumber": ["0000320193-26-000001"],
                "filingDate": ["2026-01-01"],
                "reportDate": ["2025-12-31"],
                "primaryDocument": ["aapl-20251231.htm"],
                "primaryDocDescription": ["Annual report"],
                "isXBRL": [True],
                "isInlineXBRL": [True],
                "period": ["2025-12-31"],
            },
            "files": [],
        },
    }
    archive_payload = {"directory": {"item": [{"name": "index.html"}, {"name": "extra.xml"}]}}
    requested_urls: list[str] = []

    def fake_request_json(url: str, timeout_seconds: int = 30, user_agent: str | None = None):
        requested_urls.append(url)
        if "submissions/CIK0000320193.json" in url:
            return submissions_payload
        if url.endswith("/index.json"):
            return archive_payload
        raise AssertionError(f"Unexpected URL: {url}")

    def fake_download_file(
        url: str,
        path,
        refresh: bool = False,
        timeout_seconds: int = 30,
        user_agent: str | None = None,
    ):
        requested_urls.append(url)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("downloaded")
        return True

    monkeypatch.setattr("hk_value_screener.data_sources._request_json", fake_request_json)
    monkeypatch.setattr("hk_value_screener.data_sources.download_file", fake_download_file)

    result = cache_us_filings("AAPL", root_dir=root_dir, download=True)

    assert result.status == "成功"
    assert any(url.endswith("/index.json") for url in requested_urls)
    assert any(url.endswith("aapl-20251231.htm") for url in requested_urls)
    assert not any(url.endswith("extra.xml") for url in requested_urls)

    local_dir = root_dir / "AAPL" / "raw" / "000032019326000001"
    assert (local_dir / "index.json").exists()
    assert (local_dir / "aapl-20251231.htm").exists()
    assert not (local_dir / "extra.xml").exists()


def test_cache_us_financial_history_can_fetch_only_requested_statements(
    tmp_path,
    monkeypatch,
) -> None:
    import sys
    import types

    fake_akshare = types.ModuleType("akshare")
    calls: list[tuple[str, str, str]] = []

    def fake_stock_financial_us_report_em(stock: str, symbol: str, indicator: str):
        calls.append((stock, symbol, indicator))
        return pd.DataFrame([{"REPORT_DATE": "2025-12-31", "value": 1}])

    fake_akshare.stock_financial_us_report_em = fake_stock_financial_us_report_em
    monkeypatch.setitem(sys.modules, "akshare", fake_akshare)

    root_dir = tmp_path / "data" / "raw" / "financials" / "us"
    result = cache_us_financial_history(
        "AAPL",
        root_dir=root_dir,
        statements=["income"],
    )

    assert result.status == "成功"
    assert calls == [("AAPL", "综合损益表", "年报")]
    assert result.added_rows_by_statement == {"balance": 0, "income": 1, "cashflow": 0}
    assert not (root_dir / "balance" / "AAPL.csv").exists()
    assert (root_dir / "income" / "AAPL.csv").exists()
    assert not (root_dir / "cashflow" / "AAPL.csv").exists()


def test_cn_filing_categories_keep_stable_order() -> None:
    assert CN_FILING_CATEGORIES == ["一季报", "半年报", "三季报", "年报"]


def test_hk_filing_categories_keep_stable_order() -> None:
    assert HK_FILING_CATEGORIES == ["一季报", "半年报", "三季报", "年报"]


def test_fill_missing_filing_categories_from_titles() -> None:
    frame = pd.DataFrame(
        [
            {"公告标题": "2025年年度报告"},
            {"公告标题": "2025年半年度报告"},
            {"公告标题": "2025年第一季度报告"},
            {"公告标题": "2025年第三季度报告"},
        ]
    )

    filled = _fill_missing_filing_categories(frame)

    assert filled["公告分类"].tolist() == ["年报", "半年报", "一季报", "三季报"]


def test_parse_hkex_title_search_extracts_financial_pdf() -> None:
    html = """
    <tr>
      <td class="release-time"><span>Release Time: </span>09/04/2026 18:14</td>
      <td><span>Stock Code: </span>00700</td>
      <td><span>Stock Short Name: </span>TENCENT</td>
      <td>
        <div class="headline">Financial Statements/ESG Information - [Annual Report]<br/></div>
        <div class="doc-link">
          <a href="/listedco/listconews/sehk/2026/0409/2026040901231.pdf">2025 Annual Report</a>
        </div>
      </td>
    </tr>
    """

    frame = _parse_hkex_title_search(html, "700")

    assert frame.loc[0, "代码"] == "00700"
    assert frame.loc[0, "公告分类"] == "年报"
    assert frame.loc[0, "公告标题"] == "2025 Annual Report"
    assert frame.loc[0, "pdf_url"] == (
        "https://www1.hkexnews.hk/listedco/listconews/sehk/2026/0409/2026040901231.pdf"
    )
