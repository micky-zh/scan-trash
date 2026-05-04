from __future__ import annotations

import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

import pandas as pd
import typer
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from hk_value_screener.app_config import load_app_config, resolve_project_path
from hk_value_screener.blacklist import (
    disable_blacklist_entry,
    list_blacklist_entries,
    load_active_blacklist,
    upsert_blacklist_entry,
)
from hk_value_screener.config import (
    CONFIGS_DIR,
    ensure_directories,
)
from hk_value_screener.data_sources import (
    CN_FILING_CATEGORIES,
    HK_FILING_CATEGORIES,
    US_FILING_CATEGORIES,
    build_cn_research_view,
    build_hk_research_view,
    build_us_research_view,
    cache_cn_filings,
    cache_cn_financial_history,
    cache_hk_filings,
    cache_hk_financial_history,
    cache_us_filings,
    cache_us_financial_history,
    fetch_cn_enriched_metrics,
    fetch_hk_enriched_metrics,
    fetch_us_enriched_metrics,
    financial_history_cache_path,
    financial_history_statements,
    get_cn_spot_full,
    get_hk_spot_full,
    get_us_spot_full,
    load_enriched_metrics_cache,
    merge_enriched_cache,
    normalize_security_codes,
    normalize_security_code,
    normalize_us_filing_ticker,
    save_enriched_metrics_cache,
    save_spot_full_csv,
)

app = typer.Typer(help="Value research export toolkit.")
blacklist_app = typer.Typer(help="Manage local research blacklists.")
app.add_typer(blacklist_app, name="blacklist")
console = Console()

MARKET_LABELS = {"hk": "Hong Kong", "us": "United States", "cn": "China A-share"}
MARKET_PE_COLUMNS = {"hk": "市盈率-动态", "us": "市盈率", "cn": "市盈率-动态"}


def _market_label(market: str) -> str:
    return MARKET_LABELS.get(market, market.upper())


def _preview_columns_for_market(market: str) -> list[str]:
    return ["代码", "名称", "最新价", "成交额", MARKET_PE_COLUMNS[market], "市净率", "总市值"]


def _normalize_symbol(symbol: str, market: str) -> str:
    return normalize_security_codes(pd.Series([symbol]), market=market).iloc[0]


def _filter_frame_by_symbol(frame: pd.DataFrame, market: str, symbol: str) -> pd.DataFrame:
    normalized_symbol = _normalize_symbol(symbol, market)
    filtered = frame[frame["代码"] == normalized_symbol].copy()
    if filtered.empty:
        console.print(f"Missing {_market_label(market)} symbol in local spot CSV: {normalized_symbol}")
        raise typer.Exit(code=1)
    return filtered


def _normalize_us_filing_category(category: str) -> str:
    normalized = category.strip().upper()
    if normalized == "年报":
        return "10-K"
    return normalized


def _financial_history_candidates(frame: pd.DataFrame, market: str) -> pd.DataFrame:
    output = frame.copy()
    if market != "us":
        return output

    for column in ["最新价", "总市值"]:
        if column in output.columns:
            output[column] = pd.to_numeric(output[column], errors="coerce")
            output = output[output[column].notna()]

    if "名称" in output.columns:
        names = output["名称"].fillna("").astype(str)
        non_company_pattern = (
            "ETF|ETN|Fund|Index|Treasury|Bond|Physical Gold|"
            " Wt$| Warrant| Rt$| Right| Unit| Acquisition|"
            " Series | Pfd| Preferred"
        )
        output = output[~names.str.contains(non_company_pattern, case=False, regex=True)]

    if "代码" in output.columns:
        codes = output["代码"].fillna("").astype(str)
        output = output[
            ~codes.str.contains(r"(?:_|-|/|\^|\.P|(?:W|WS|WT|R|U)$)", case=False, regex=True)
        ]

    return output


def _financial_history_missing_statements(market: str, code: str, root_dir: Path) -> list[str]:
    return [
        statement
        for statement in financial_history_statements(market)
        if not financial_history_cache_path(market, code, statement, root_dir).exists()
    ]


def _load_market_blacklist(market: str) -> pd.DataFrame:
    return load_active_blacklist(market=market)


def _filings_us_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in ["最新价", "总市值"]:
        if column in output.columns:
            output[column] = pd.to_numeric(output[column], errors="coerce")
            output = output[output[column].notna()]

    if "名称" in output.columns:
        names = output["名称"].fillna("").astype(str)
        non_company_pattern = (
            "ETF|ETN|Fund|Index|Treasury|Bond|Physical Gold|"
            " Wt$| Warrant| Rt$| Right| Unit| Acquisition|"
            " Series | Pfd| Preferred"
        )
        output = output[~names.str.contains(non_company_pattern, case=False, regex=True)]

    return output


@app.command("bootstrap")
def bootstrap() -> None:
    """Create local project directories."""
    directories = ensure_directories()
    console.print(f"Initialized {len(directories)} directories.")


@app.command("show-config")
def show_config(config_file: Path = CONFIGS_DIR / "default.yaml") -> None:
    """Display the active application config."""
    ensure_directories()
    config = load_app_config(config_file)

    table = Table(title=f"{config.name} ({config.version})")
    table.add_column("Section")
    table.add_column("Value")
    table.add_row("description", config.description)
    table.add_row("market", config.market)
    table.add_row("fetch.enabled", str(config.fetch.enabled))
    table.add_row("output.save_csv", str(config.output.save_csv))
    table.add_row("output.raw_csv_path", config.output.raw_csv_path)
    table.add_row("output.enriched_cache_csv_path", config.output.enriched_cache_csv_path)
    table.add_row("output.research_csv_path", config.output.research_csv_path)
    table.add_row("sector_profiles", ", ".join(config.sector_profiles) or "-")
    console.print(table)


def _fetch_spot_full_by_market(market: str) -> pd.DataFrame:
    if market == "hk":
        return get_hk_spot_full()
    if market == "us":
        return get_us_spot_full()
    if market == "cn":
        return get_cn_spot_full()
    raise ValueError(f"Unsupported market: {market}")


def _fetch_spot_full(config_file: Path) -> None:
    ensure_directories()
    config = load_app_config(config_file)
    if not config.fetch.enabled:
        console.print("Fetch is disabled in config.")
        raise typer.Exit(code=1)

    frame = _fetch_spot_full_by_market(config.market)

    preview_columns = _preview_columns_for_market(config.market)
    table = Table(title=f"{_market_label(config.market)} spot full fields: {len(frame)} companies")
    for column in preview_columns:
        if column in frame.columns:
            table.add_column(column)

    for _, row in frame.head(10).iterrows():
        values = [str(row[column]) for column in preview_columns if column in frame.columns]
        table.add_row(*values)
    console.print(table)

    if config.output.save_csv:
        output_path = save_spot_full_csv(
            frame,
            path=resolve_project_path(config.output.raw_csv_path),
            market=config.market,
        )
        console.print(f"Saved CSV to {output_path}")


def _load_hk_research_base(config_file: Path, symbol: str | None = None) -> pd.DataFrame:
    config = load_app_config(config_file)
    source_csv = resolve_project_path(config.output.raw_csv_path)
    if not source_csv.exists():
        console.print(f"Missing research source CSV: {source_csv}")
        console.print("Run `uv run vr hk` first.")
        raise typer.Exit(code=1)

    frame = pd.read_csv(source_csv, dtype={"代码": str})
    frame["代码"] = normalize_security_codes(frame["代码"], market="hk")
    return _filter_frame_by_symbol(frame, market="hk", symbol=symbol) if symbol else frame


def _fetch_hk_enriched_metrics_for_codes(
    codes: list[str],
    cache: pd.DataFrame,
    cache_path: Path,
    refresh_enrich: bool = False,
) -> tuple[pd.DataFrame, int, int]:
    return _fetch_enriched_metrics_for_codes(
        codes=codes,
        cache=cache,
        cache_path=cache_path,
        market="hk",
        task_description="补充港股研究字段",
        fetcher=fetch_hk_enriched_metrics,
        refresh_enrich=refresh_enrich,
    )


def _fetch_us_enriched_metrics_for_codes(
    codes: list[str],
    cache: pd.DataFrame,
    cache_path: Path,
    refresh_enrich: bool = False,
) -> tuple[pd.DataFrame, int, int]:
    return _fetch_enriched_metrics_for_codes(
        codes=codes,
        cache=cache,
        cache_path=cache_path,
        market="us",
        task_description="补充美股研究字段",
        fetcher=fetch_us_enriched_metrics,
        refresh_enrich=refresh_enrich,
    )


def _fetch_cn_enriched_metrics_for_codes(
    codes: list[str],
    cache: pd.DataFrame,
    cache_path: Path,
    refresh_enrich: bool = False,
) -> tuple[pd.DataFrame, int, int]:
    return _fetch_enriched_metrics_for_codes(
        codes=codes,
        cache=cache,
        cache_path=cache_path,
        market="cn",
        task_description="补充A股研究字段",
        fetcher=fetch_cn_enriched_metrics,
        refresh_enrich=refresh_enrich,
    )


def _fetch_enriched_metrics_for_codes(
    codes: list[str],
    cache: pd.DataFrame,
    cache_path: Path,
    market: str,
    task_description: str,
    fetcher,
    refresh_enrich: bool = False,
) -> tuple[pd.DataFrame, int, int]:
    cached_success_codes: set[str] = set()
    if not cache.empty and "代码" in cache.columns and "补充数据状态" in cache.columns:
        cached_success_codes = set(
            cache.loc[cache["补充数据状态"] == "成功", "代码"].astype(str).tolist()
        )

    missing_codes = (
        codes
        if refresh_enrich
        else [code for code in codes if code not in cached_success_codes]
    )
    fetched_records: list[dict[str, object]] = []

    if missing_codes:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task(task_description, total=len(missing_codes))
            for code in missing_codes:
                record = fetcher(code, timeout_seconds=20)
                fetched_records.append(record)
                cache = merge_enriched_cache(cache, pd.DataFrame([record]), market=market)
                save_enriched_metrics_cache(cache, cache_path, market=market)
                progress.advance(task_id)

    fetched_frame = pd.DataFrame(fetched_records)
    updated_cache = merge_enriched_cache(cache, fetched_frame, market=market)
    save_enriched_metrics_cache(updated_cache, cache_path, market=market)

    return updated_cache, len(cached_success_codes & set(codes)), len(missing_codes)


def _build_hk_research_view(
    config_file: Path,
    refresh_enrich: bool,
    symbol: str | None = None,
) -> Path:
    config = load_app_config(config_file)
    base_frame = _load_hk_research_base(config_file, symbol=symbol)
    blacklist_frame = _load_market_blacklist("hk")

    cache_path = resolve_project_path(config.output.enriched_cache_csv_path)
    cache = load_enriched_metrics_cache(cache_path, market="hk")
    codes = base_frame["代码"].tolist()

    output_path = resolve_project_path(config.output.research_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cached_metrics_frame = (
        cache[cache["代码"].isin(codes)].copy()
        if not cache.empty and "代码" in cache.columns
        else pd.DataFrame()
    )
    initial_research_view = build_hk_research_view(
        base_frame,
        cached_metrics_frame,
        blacklist_frame=blacklist_frame,
    )
    initial_research_view.to_csv(output_path, index=False)
    console.print(f"Saved initial full research view CSV to {output_path}")

    updated_cache, cached_count, fetched_count = _fetch_hk_enriched_metrics_for_codes(
        codes,
        cache,
        cache_path,
        refresh_enrich=refresh_enrich,
    )

    metrics_frame = (
        updated_cache[updated_cache["代码"].isin(codes)].copy()
        if not updated_cache.empty and "代码" in updated_cache.columns
        else pd.DataFrame()
    )
    research_view = build_hk_research_view(
        base_frame,
        metrics_frame,
        blacklist_frame=blacklist_frame,
    )

    research_view.to_csv(output_path, index=False)

    table = Table(title=f"Hong Kong research view: {len(research_view)} companies")
    preview_columns = [
        "代码",
        "名称",
        "所属行业",
        "市盈率-动态",
        "市净率",
        "净现比",
        "流动比率",
        "速动比率",
        "毛利率(%)",
        "销售净利率(%)",
    ]
    for column in preview_columns:
        if column in research_view.columns:
            table.add_column(column)
    for _, row in research_view.head(15).iterrows():
        values = [str(row[column]) for column in preview_columns if column in research_view.columns]
        table.add_row(*values)
    console.print(table)
    console.print(f"Used cache for {cached_count} companies.")
    console.print(f"Fetched {fetched_count} companies from network.")
    console.print(f"Saved research view CSV to {output_path}")
    return output_path


def _load_us_research_base(config_file: Path, symbol: str | None = None) -> pd.DataFrame:
    config = load_app_config(config_file)
    source_csv = resolve_project_path(config.output.raw_csv_path)
    if not source_csv.exists():
        console.print(f"Missing research source CSV: {source_csv}")
        console.print("Run `uv run vr us` first.")
        raise typer.Exit(code=1)

    frame = pd.read_csv(source_csv, dtype={"代码": str})
    frame["代码"] = normalize_security_codes(frame["代码"], market="us")
    return _filter_frame_by_symbol(frame, market="us", symbol=symbol) if symbol else frame


def _build_us_research_view(
    config_file: Path,
    refresh_enrich: bool,
    symbol: str | None = None,
) -> Path:
    config = load_app_config(config_file)
    base_frame = _load_us_research_base(config_file, symbol=symbol)
    blacklist_frame = _load_market_blacklist("us")

    cache_path = resolve_project_path(config.output.enriched_cache_csv_path)
    cache = load_enriched_metrics_cache(cache_path, market="us")
    codes = base_frame["代码"].tolist()

    output_path = resolve_project_path(config.output.research_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cached_metrics_frame = (
        cache[cache["代码"].isin(codes)].copy()
        if not cache.empty and "代码" in cache.columns
        else pd.DataFrame()
    )
    initial_research_view = build_us_research_view(
        base_frame,
        cached_metrics_frame,
        blacklist_frame=blacklist_frame,
    )
    initial_research_view.to_csv(output_path, index=False)
    console.print(f"Saved initial full research view CSV to {output_path}")

    updated_cache, cached_count, fetched_count = _fetch_us_enriched_metrics_for_codes(
        codes,
        cache,
        cache_path,
        refresh_enrich=refresh_enrich,
    )

    metrics_frame = (
        updated_cache[updated_cache["代码"].isin(codes)].copy()
        if not updated_cache.empty and "代码" in updated_cache.columns
        else pd.DataFrame()
    )
    research_view = build_us_research_view(
        base_frame,
        metrics_frame,
        blacklist_frame=blacklist_frame,
    )

    research_view.to_csv(output_path, index=False)

    table = Table(title=f"United States research view: {len(research_view)} companies")
    preview_columns = [
        "代码",
        "名称",
        "市盈率",
        "毛利率(%)",
        "销售净利率(%)",
        "净资产收益率(平均)(%)",
        "流动比率",
        "速动比率",
        "资产负债率(%)",
        "补充数据状态",
    ]
    for column in preview_columns:
        if column in research_view.columns:
            table.add_column(column)
    for _, row in research_view.head(15).iterrows():
        values = [str(row[column]) for column in preview_columns if column in research_view.columns]
        table.add_row(*values)
    console.print(table)
    console.print(f"Used cache for {cached_count} companies.")
    console.print(f"Fetched {fetched_count} companies from network.")
    console.print(f"Saved research view CSV to {output_path}")
    return output_path


def _load_cn_research_base(config_file: Path, symbol: str | None = None) -> pd.DataFrame:
    config = load_app_config(config_file)
    source_csv = resolve_project_path(config.output.raw_csv_path)
    if not source_csv.exists():
        console.print(f"Missing research source CSV: {source_csv}")
        console.print("Run `uv run vr cn` first.")
        raise typer.Exit(code=1)

    frame = pd.read_csv(source_csv, dtype={"代码": str})
    frame["代码"] = normalize_security_codes(frame["代码"], market="cn")
    return _filter_frame_by_symbol(frame, market="cn", symbol=symbol) if symbol else frame


def _build_cn_research_view(
    config_file: Path,
    refresh_enrich: bool,
    symbol: str | None = None,
) -> Path:
    config = load_app_config(config_file)
    base_frame = _load_cn_research_base(config_file, symbol=symbol)
    blacklist_frame = _load_market_blacklist("cn")

    cache_path = resolve_project_path(config.output.enriched_cache_csv_path)
    cache = load_enriched_metrics_cache(cache_path, market="cn")
    codes = base_frame["代码"].tolist()

    output_path = resolve_project_path(config.output.research_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cached_metrics_frame = (
        cache[cache["代码"].isin(codes)].copy()
        if not cache.empty and "代码" in cache.columns
        else pd.DataFrame()
    )
    initial_research_view = build_cn_research_view(
        base_frame,
        cached_metrics_frame,
        blacklist_frame=blacklist_frame,
    )
    initial_research_view.to_csv(output_path, index=False)
    console.print(f"Saved initial full research view CSV to {output_path}")

    updated_cache, cached_count, fetched_count = _fetch_cn_enriched_metrics_for_codes(
        codes,
        cache,
        cache_path,
        refresh_enrich=refresh_enrich,
    )

    metrics_frame = (
        updated_cache[updated_cache["代码"].isin(codes)].copy()
        if not updated_cache.empty and "代码" in updated_cache.columns
        else pd.DataFrame()
    )
    research_view = build_cn_research_view(
        base_frame,
        metrics_frame,
        blacklist_frame=blacklist_frame,
    )

    research_view.to_csv(output_path, index=False)

    table = Table(title=f"China A-share research view: {len(research_view)} companies")
    preview_columns = [
        "代码",
        "名称",
        "市盈率-动态",
        "市净率",
        "销售毛利率(%)",
        "销售净利率(%)",
        "净资产收益率(%)",
        "流动比率",
        "速动比率",
        "补充数据状态",
    ]
    for column in preview_columns:
        if column in research_view.columns:
            table.add_column(column)
    for _, row in research_view.head(15).iterrows():
        values = [str(row[column]) for column in preview_columns if column in research_view.columns]
        table.add_row(*values)
    console.print(table)
    console.print(f"Used cache for {cached_count} companies.")
    console.print(f"Fetched {fetched_count} companies from network.")
    console.print(f"Saved research view CSV to {output_path}")
    return output_path


@app.command("financials")
def financials(
    market: str = "cn",
    config_file: Path | None = None,
    symbol: str | None = None,
    limit: int | None = None,
    refresh: bool = False,
    sleep_seconds: float = 1.5,
    batch_size: int = 10,
    batch_sleep_seconds: float = 5.0,
    workers: int = 1,
    missing_only: bool = False,
) -> None:
    """Cache raw financial histories locally."""
    if market not in {"hk", "us", "cn", "all"}:
        console.print("`financials` supports only `hk`, `us`, `cn`, or `all`.")
        raise typer.Exit(code=1)
    if workers < 1:
        console.print("`--workers` must be >= 1.")
        raise typer.Exit(code=1)
    if market == "all" and config_file is not None:
        console.print(
            "`financials --market all` uses default configs and does not accept --config-file."
        )
        raise typer.Exit(code=1)
    if market == "all" and symbol is not None:
        console.print("`financials --market all` does not support `--symbol`.")
        raise typer.Exit(code=1)

    markets = ["hk", "us", "cn"] if market == "all" else [market]
    for selected_market in markets:
        _run_financials_for_market(
            selected_market,
            config_file=config_file,
            symbol=symbol,
            limit=limit,
            refresh=refresh,
            sleep_seconds=sleep_seconds,
            batch_size=batch_size,
            batch_sleep_seconds=batch_sleep_seconds,
            workers=workers,
            missing_only=missing_only,
        )


def _run_financials_for_market(
    market: str,
    config_file: Path | None,
    symbol: str | None,
    limit: int | None,
    refresh: bool,
    sleep_seconds: float,
    batch_size: int,
    batch_sleep_seconds: float,
    workers: int,
    missing_only: bool,
) -> None:
    ensure_directories()
    if config_file is None:
        config_file = {
            "hk": CONFIGS_DIR / "default.yaml",
            "us": CONFIGS_DIR / "us.yaml",
            "cn": CONFIGS_DIR / "cn.yaml",
        }[market]
    config = load_app_config(config_file)
    if config.market != market:
        console.print(f"`financials --market {market}` requires a {market} config.")
        raise typer.Exit(code=1)

    if symbol is not None:
        codes = [_normalize_symbol(symbol, market)]
        raw_count = candidate_count = len(codes)
    else:
        source_csv = resolve_project_path(config.output.raw_csv_path)
        if not source_csv.exists():
            console.print(f"Missing {_market_label(market)} spot CSV: {source_csv}")
            console.print(f"Fetching {_market_label(market)} spot data first.")
            _fetch_spot_full(config_file)

        frame = pd.read_csv(source_csv, dtype={"代码": str})
        frame["代码"] = normalize_security_codes(frame["代码"], market=market)
        raw_count = len(frame)
        frame = _financial_history_candidates(frame, market)
        candidate_count = len(frame)
        codes = sorted(frame["代码"].dropna().drop_duplicates().tolist())
        if limit is not None:
            codes = codes[:limit]

    root_dir = resolve_project_path(f"data/raw/financials/{market}")
    checked_count = len(codes)
    missing_statements_by_code: dict[str, list[str]] = {}
    if missing_only:
        before_count = len(codes)
        for code in codes:
            missing_statements = _financial_history_missing_statements(market, code, root_dir)
            if missing_statements:
                missing_statements_by_code[code] = missing_statements
        codes = [code for code in codes if code in missing_statements_by_code]
        skipped_cached_count = before_count - len(codes)
    else:
        skipped_cached_count = 0

    cache_fetcher = {
        "hk": cache_hk_financial_history,
        "us": cache_us_financial_history,
        "cn": cache_cn_financial_history,
    }[market]
    added_rows_by_statement: dict[str, int] = {}
    failed_count = 0

    def record_result(result: object) -> None:
        nonlocal failed_count
        for statement, added_rows in result.added_rows_by_statement.items():
            added_rows_by_statement[statement] = (
                added_rows_by_statement.get(statement, 0) + added_rows
            )
        if result.status != "成功":
            failed_count += 1
            console.print(f"{result.code}: {result.status}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        progress_total = checked_count if missing_only else len(codes)
        task_id = progress.add_task(f"缓存{_market_label(market)}历史财报", total=progress_total)
        if skipped_cached_count:
            progress.advance(task_id, skipped_cached_count)
        if workers == 1:
            for index, code in enumerate(codes, start=1):
                result = cache_fetcher(
                    code,
                    root_dir=root_dir,
                    refresh=refresh,
                    statements=missing_statements_by_code.get(code),
                )
                record_result(result)
                progress.advance(task_id)
                if index < len(codes) and sleep_seconds > 0:
                    time.sleep(sleep_seconds)
                if batch_size > 0 and index % batch_size == 0 and index < len(codes):
                    time.sleep(batch_sleep_seconds)
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                pending = set()
                for index, code in enumerate(codes, start=1):
                    while len(pending) >= workers:
                        done, pending = wait(pending, return_when=FIRST_COMPLETED)
                        for future in done:
                            record_result(future.result())
                            progress.advance(task_id)

                    pending.add(
                        executor.submit(
                            cache_fetcher,
                            code,
                            root_dir=root_dir,
                            refresh=refresh,
                            statements=missing_statements_by_code.get(code),
                        )
                    )
                    if index < len(codes) and sleep_seconds > 0:
                        time.sleep(sleep_seconds)
                    if batch_size > 0 and index % batch_size == 0 and index < len(codes):
                        time.sleep(batch_sleep_seconds)

                while pending:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        record_result(future.result())
                        progress.advance(task_id)

    if missing_only:
        console.print(
            f"Checked financial histories for {checked_count} {_market_label(market)} companies."
        )
    console.print(f"Cached financial histories for {len(codes)} {_market_label(market)} companies.")
    if missing_only:
        console.print(f"Skipped cached companies: {skipped_cached_count}.")
    if symbol is None and candidate_count < raw_count:
        console.print(f"Skipped {raw_count - candidate_count} non-company or unsupported symbols.")
    for statement, added_rows in sorted(added_rows_by_statement.items()):
        console.print(f"Added {statement} rows: {added_rows}.")
    console.print(f"Failed companies: {failed_count}.")
    console.print(f"Saved under {root_dir}")


def _financial_history_cache_complete(market: str, code: str, root_dir: Path) -> bool:
    statements = financial_history_statements(market)
    return all(
        financial_history_cache_path(market, code, statement, root_dir).exists()
        for statement in statements
    )


@app.command("filings")
def filings(
    market: str = "cn",
    config_file: Path | None = None,
    symbol: str | None = None,
    limit: int | None = None,
    refresh: bool = False,
    download: bool = False,
    sleep_seconds: float = 1.5,
    batch_size: int = 10,
    batch_sleep_seconds: float = 5.0,
    workers: int = 1,
    category: str = "年报",
) -> None:
    """Cache filing indexes and optional raw filing files locally."""
    if market not in {"cn", "hk", "us"}:
        console.print("`filings` currently supports only `cn`, `hk`, and `us`.")
        raise typer.Exit(code=1)
    if workers < 1:
        console.print("`--workers` must be >= 1.")
        raise typer.Exit(code=1)
    supported_categories = {
        "cn": CN_FILING_CATEGORIES,
        "hk": HK_FILING_CATEGORIES,
        "us": US_FILING_CATEGORIES,
    }[market]
    normalized_category = _normalize_us_filing_category(category) if market == "us" else category
    if normalized_category != "all" and normalized_category not in supported_categories:
        if market == "us":
            console.print("`--category` supports only `10-K`, `10-Q`, `20-F`, `6-K`, or `all`.")
        else:
            console.print(
                "`--category` supports only `年报`, `半年报`, `一季报`, `三季报`, or `all`."
            )
        raise typer.Exit(code=1)

    ensure_directories()
    if config_file is None:
        config_file = {
            "cn": CONFIGS_DIR / "cn.yaml",
            "hk": CONFIGS_DIR / "default.yaml",
            "us": CONFIGS_DIR / "us.yaml",
        }[market]
    config = load_app_config(config_file)
    if config.market != market:
        console.print(f"`filings --market {market}` requires a {market} config.")
        raise typer.Exit(code=1)

    if symbol:
        if market == "us":
            codes = [normalize_us_filing_ticker(symbol)]
        else:
            codes = [normalize_security_codes(pd.Series([symbol]), market=market).iloc[0]]
    else:
        source_csv = resolve_project_path(config.output.raw_csv_path)
        if not source_csv.exists():
            console.print(f"Missing {_market_label(market)} spot CSV: {source_csv}")
            console.print(f"Fetching {_market_label(market)} spot data first.")
            _fetch_spot_full(config_file)

        frame = pd.read_csv(source_csv, dtype={"代码": str})
        frame["代码"] = normalize_security_codes(frame["代码"], market=market)
        if market == "us":
            frame = _filings_us_candidates(frame)
        codes = sorted(frame["代码"].dropna().drop_duplicates().tolist())
        if limit is not None:
            codes = codes[:limit]

    root_dir = resolve_project_path(f"data/raw/filings/{market}")
    categories = supported_categories if normalized_category == "all" else [normalized_category]
    cache_fetcher = {
        "cn": cache_cn_filings,
        "hk": cache_hk_filings,
        "us": cache_us_filings,
    }[market]
    added_rows = 0
    downloaded_files = 0
    failed_count = 0
    tasks = [(code, selected_category) for code in codes for selected_category in categories]
    total_tasks = len(tasks)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task(f"缓存{_market_label(market)}公告", total=total_tasks)
        if workers == 1:
            for index, (code, selected_category) in enumerate(tasks, start=1):
                result = cache_fetcher(
                    code,
                    root_dir=root_dir,
                    refresh=refresh,
                    download=download,
                    category=selected_category,
                )
                added_rows += result.added_rows
                downloaded_files += result.downloaded_files
                if result.status != "成功":
                    failed_count += 1
                    console.print(f"{result.code} {selected_category}: {result.status}")

                progress.advance(task_id)
                if index < total_tasks and sleep_seconds > 0:
                    time.sleep(sleep_seconds)
                if (
                    batch_size > 0
                    and index % batch_size == 0
                    and index < total_tasks
                ):
                    time.sleep(batch_sleep_seconds)
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                pending: dict[object, str] = {}
                for index, (code, selected_category) in enumerate(tasks, start=1):
                    while len(pending) >= workers:
                        done, _ = wait(set(pending), return_when=FIRST_COMPLETED)
                        for future in done:
                            result = future.result()
                            result_category = pending.pop(future, selected_category)
                            added_rows += result.added_rows
                            downloaded_files += result.downloaded_files
                            if result.status != "成功":
                                failed_count += 1
                                console.print(
                                    f"{result.code} {result_category}: {result.status}"
                                )
                            progress.advance(task_id)

                    future = executor.submit(
                        cache_fetcher,
                        code,
                        root_dir=root_dir,
                        refresh=refresh,
                        download=download,
                        category=selected_category,
                    )
                    pending[future] = selected_category
                    if index < total_tasks and sleep_seconds > 0:
                        time.sleep(sleep_seconds)
                    if (
                        batch_size > 0
                        and index % batch_size == 0
                        and index < total_tasks
                    ):
                        time.sleep(batch_sleep_seconds)

                while pending:
                    done, _ = wait(set(pending), return_when=FIRST_COMPLETED)
                    for future in done:
                        result = future.result()
                        result_category = pending.pop(future, "")
                        added_rows += result.added_rows
                        downloaded_files += result.downloaded_files
                        if result.status != "成功":
                            failed_count += 1
                            console.print(f"{result.code} {result_category}: {result.status}")
                        progress.advance(task_id)

    console.print(
        f"Cached filings for {len(codes)} {_market_label(market)} companies "
        f"and {len(categories)} categories."
    )
    console.print(f"Added filing rows: {added_rows}.")
    console.print(f"Downloaded files: {downloaded_files}.")
    console.print(f"Failed companies: {failed_count}.")
    console.print(f"Saved under {root_dir}")


@app.command("hk")
def hk(
    config_file: Path = CONFIGS_DIR / "default.yaml",
    symbol: str | None = None,
    refresh_enrich: bool = False,
    refresh_all: bool = False,
) -> None:
    """Export the Hong Kong value research CSV."""
    config = load_app_config(config_file)
    if config.market != "hk":
        console.print("`hk` only supports Hong Kong configs.")
        raise typer.Exit(code=1)

    console.print("Step 1/3: Fetching Hong Kong spot data.")
    _fetch_spot_full(config_file)
    console.print("Step 2/3: No built-in filtering.")
    console.print("Step 3/3: Building research view.")
    _build_hk_research_view(
        config_file,
        refresh_enrich=refresh_enrich or refresh_all,
        symbol=symbol,
    )


@app.command("us")
def us(
    config_file: Path = CONFIGS_DIR / "us.yaml",
    symbol: str | None = None,
    refresh_enrich: bool = False,
    refresh_all: bool = False,
) -> None:
    """Export the United States value research CSV."""
    config = load_app_config(config_file)
    if config.market != "us":
        console.print("`us` only supports United States configs.")
        raise typer.Exit(code=1)

    console.print("Step 1/3: Fetching United States spot data.")
    _fetch_spot_full(config_file)
    console.print("Step 2/3: No built-in filtering.")
    console.print("Step 3/3: Building research view.")
    _build_us_research_view(
        config_file,
        refresh_enrich=refresh_enrich or refresh_all,
        symbol=symbol,
    )


@app.command("cn")
def cn(
    config_file: Path = CONFIGS_DIR / "cn.yaml",
    symbol: str | None = None,
    refresh_enrich: bool = False,
    refresh_all: bool = False,
) -> None:
    """Export the China A-share value research CSV."""
    config = load_app_config(config_file)
    if config.market != "cn":
        console.print("`cn` only supports China A-share configs.")
        raise typer.Exit(code=1)

    console.print("Step 1/3: Fetching China A-share spot data.")
    _fetch_spot_full(config_file)
    console.print("Step 2/3: No built-in filtering.")
    console.print("Step 3/3: Building research view.")
    _build_cn_research_view(
        config_file,
        refresh_enrich=refresh_enrich or refresh_all,
        symbol=symbol,
    )


@blacklist_app.command("add")
def blacklist_add(
    market: str = typer.Option(..., "--market", help="Market code: hk, us, or cn."),
    symbol: str = typer.Option(..., "--symbol", help="Stock code to blacklist."),
    reason: str = typer.Option(..., "--reason", help="Why this company is blacklisted."),
    name: str = typer.Option("", "--name", help="Optional company name."),
) -> None:
    """Add or re-enable a local blacklist entry."""
    ensure_directories()
    try:
        entry = upsert_blacklist_entry(
            market=market,
            code=symbol,
            reason=reason,
            name=name,
        )
    except ValueError as exc:
        console.print(str(exc))
        raise typer.Exit(code=1) from exc

    normalized_market = market.strip().lower()
    normalized_code = normalize_security_code(symbol, market=normalized_market)
    row = entry[
        (entry["market"] == normalized_market) & (entry["code"] == normalized_code)
    ].iloc[0]
    console.print(
        f"Saved blacklist entry: {row['market'].upper()} {row['code']} "
        f"({row['name'] or '-'})"
    )


@blacklist_app.command("remove")
def blacklist_remove(
    market: str = typer.Option(..., "--market", help="Market code: hk, us, or cn."),
    symbol: str = typer.Option(..., "--symbol", help="Stock code to remove."),
) -> None:
    """Disable a local blacklist entry without deleting history."""
    ensure_directories()
    try:
        removed = disable_blacklist_entry(market=market, code=symbol)
    except ValueError as exc:
        console.print(str(exc))
        raise typer.Exit(code=1) from exc

    if not removed:
        console.print("Blacklist entry not found.")
        raise typer.Exit(code=1)

    normalized_market = market.strip().lower()
    normalized_code = normalize_security_code(symbol, market=normalized_market)
    console.print(f"Disabled blacklist entry: {normalized_market.upper()} {normalized_code}")


@blacklist_app.command("list")
def blacklist_list(
    market: str | None = typer.Option(None, "--market", help="Optional market filter."),
) -> None:
    """Show local blacklist entries."""
    ensure_directories()
    try:
        entries = list_blacklist_entries(market=market)
    except ValueError as exc:
        console.print(str(exc))
        raise typer.Exit(code=1) from exc

    if entries.empty:
        console.print("No blacklist entries.")
        return

    table = Table(title="Local blacklist")
    for column in ["market", "code", "name", "reason", "enabled", "created_at", "updated_at"]:
        table.add_column(column)

    for _, row in entries.iterrows():
        table.add_row(
            str(row.get("market", "")),
            str(row.get("code", "")),
            str(row.get("name", "")),
            str(row.get("reason", "")),
            "yes" if bool(row.get("enabled", False)) else "no",
            str(row.get("created_at", "")),
            str(row.get("updated_at", "")),
        )

    console.print(table)
