import pandas as pd
import pytest
import typer

from hk_value_screener.cli import (
    _financial_history_cache_complete,
    _financial_history_candidates,
    _financial_history_missing_statements,
    filings,
)


def test_us_financial_history_candidates_skip_non_company_symbols() -> None:
    frame = pd.DataFrame(
        [
            {
                "代码": "AAA",
                "名称": "Alternative Access First Priority ETF",
                "最新价": 24.9,
                "总市值": None,
            },
            {
                "代码": "AAAU",
                "名称": "Goldman Sachs Physical Gold ETF",
                "最新价": 45.4,
                "总市值": None,
            },
            {
                "代码": "AACBR",
                "名称": "Artius II Acquisition Inc Rt",
                "最新价": 0.33,
                "总市值": None,
            },
            {
                "代码": "ABR_D",
                "名称": "Arbor Realty Trust Inc Series D",
                "最新价": 16.9,
                "总市值": 155480000,
            },
            {
                "代码": "ACGLN",
                "名称": "Arch Capital Group Ltd Series G",
                "最新价": 17.15,
                "总市值": 343002000,
            },
            {
                "代码": "ADAMZ",
                "名称": "Adamas Trust Inc Series G Pfd",
                "最新价": 18.7,
                "总市值": 55667226,
            },
            {"代码": "AAPL", "名称": "Apple Inc.", "最新价": 190.0, "总市值": 2900000000000},
        ]
    )

    candidates = _financial_history_candidates(frame, market="us")

    assert candidates["代码"].tolist() == ["AAPL"]


def test_financial_history_cache_complete_requires_all_market_files(tmp_path) -> None:
    (tmp_path / "indicators").mkdir()
    (tmp_path / "abstracts").mkdir()
    (tmp_path / "indicators" / "000001.csv").write_text("代码\n000001\n")

    assert not _financial_history_cache_complete("cn", "000001", tmp_path)

    (tmp_path / "abstracts" / "000001.csv").write_text("代码\n000001\n")

    assert _financial_history_cache_complete("cn", "000001", tmp_path)


def test_financial_history_missing_statements_detects_partial_cache(tmp_path) -> None:
    (tmp_path / "balance").mkdir()
    (tmp_path / "income").mkdir()
    (tmp_path / "balance" / "AAPL.csv").write_text("代码\nAAPL\n")

    assert _financial_history_missing_statements("us", "AAPL", tmp_path) == [
        "income",
        "cashflow",
    ]


def test_filings_rejects_invalid_workers() -> None:
    with pytest.raises(typer.Exit) as exc_info:
        filings(market="cn", workers=0)

    assert exc_info.value.exit_code == 1
