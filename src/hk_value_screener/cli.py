from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import typer
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from hk_value_screener.app_config import load_app_config, resolve_project_path
from hk_value_screener.config import (
    CONFIGS_DIR,
    ensure_directories,
)
from hk_value_screener.data_sources import (
    build_cn_research_view,
    build_hk_research_view,
    build_us_research_view,
    cache_cn_financial_history,
    cache_hk_financial_history,
    cache_us_financial_history,
    fetch_cn_enriched_metrics,
    fetch_hk_enriched_metrics,
    fetch_us_enriched_metrics,
    get_cn_spot_full,
    get_hk_spot_full,
    get_us_spot_full,
    load_enriched_metrics_cache,
    merge_enriched_cache,
    normalize_security_codes,
    save_enriched_metrics_cache,
    save_spot_full_csv,
)

app = typer.Typer(help="Value research export toolkit.")
console = Console()

MARKET_LABELS = {"hk": "Hong Kong", "us": "United States", "cn": "China A-share"}
MARKET_PE_COLUMNS = {"hk": "市盈率-动态", "us": "市盈率", "cn": "市盈率-动态"}


def _market_label(market: str) -> str:
    return MARKET_LABELS.get(market, market.upper())


def _preview_columns_for_market(market: str) -> list[str]:
    return ["代码", "名称", "最新价", "成交额", MARKET_PE_COLUMNS[market], "市净率", "总市值"]


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


def _load_hk_research_base(config_file: Path) -> pd.DataFrame:
    config = load_app_config(config_file)
    source_csv = resolve_project_path(config.output.raw_csv_path)
    if not source_csv.exists():
        console.print(f"Missing research source CSV: {source_csv}")
        console.print("Run `uv run vr hk` first.")
        raise typer.Exit(code=1)

    frame = pd.read_csv(source_csv, dtype={"代码": str})
    frame["代码"] = normalize_security_codes(frame["代码"], market="hk")
    return frame


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


def _build_hk_research_view(config_file: Path, refresh_enrich: bool) -> Path:
    config = load_app_config(config_file)
    base_frame = _load_hk_research_base(config_file)

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
    initial_research_view = build_hk_research_view(base_frame, cached_metrics_frame)
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
    research_view = build_hk_research_view(base_frame, metrics_frame)

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


def _load_us_research_base(config_file: Path) -> pd.DataFrame:
    config = load_app_config(config_file)
    source_csv = resolve_project_path(config.output.raw_csv_path)
    if not source_csv.exists():
        console.print(f"Missing research source CSV: {source_csv}")
        console.print("Run `uv run vr us` first.")
        raise typer.Exit(code=1)

    frame = pd.read_csv(source_csv, dtype={"代码": str})
    frame["代码"] = normalize_security_codes(frame["代码"], market="us")
    return frame


def _build_us_research_view(config_file: Path, refresh_enrich: bool) -> Path:
    config = load_app_config(config_file)
    base_frame = _load_us_research_base(config_file)

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
    initial_research_view = build_us_research_view(base_frame, cached_metrics_frame)
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
    research_view = build_us_research_view(base_frame, metrics_frame)

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


def _load_cn_research_base(config_file: Path) -> pd.DataFrame:
    config = load_app_config(config_file)
    source_csv = resolve_project_path(config.output.raw_csv_path)
    if not source_csv.exists():
        console.print(f"Missing research source CSV: {source_csv}")
        console.print("Run `uv run vr cn` first.")
        raise typer.Exit(code=1)

    frame = pd.read_csv(source_csv, dtype={"代码": str})
    frame["代码"] = normalize_security_codes(frame["代码"], market="cn")
    return frame


def _build_cn_research_view(config_file: Path, refresh_enrich: bool) -> Path:
    config = load_app_config(config_file)
    base_frame = _load_cn_research_base(config_file)

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
    initial_research_view = build_cn_research_view(base_frame, cached_metrics_frame)
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
    research_view = build_cn_research_view(base_frame, metrics_frame)

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
    limit: int | None = None,
    refresh: bool = False,
    sleep_seconds: float = 1.5,
    batch_size: int = 10,
    batch_sleep_seconds: float = 5.0,
) -> None:
    """Cache raw financial histories locally."""
    if market not in {"hk", "us", "cn"}:
        console.print("`financials` supports only `hk`, `us`, or `cn`.")
        raise typer.Exit(code=1)

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

    source_csv = resolve_project_path(config.output.raw_csv_path)
    if not source_csv.exists():
        console.print(f"Missing {_market_label(market)} spot CSV: {source_csv}")
        console.print(f"Fetching {_market_label(market)} spot data first.")
        _fetch_spot_full(config_file)

    frame = pd.read_csv(source_csv, dtype={"代码": str})
    frame["代码"] = normalize_security_codes(frame["代码"], market=market)
    codes = sorted(frame["代码"].dropna().drop_duplicates().tolist())
    if limit is not None:
        codes = codes[:limit]

    root_dir = resolve_project_path(f"data/raw/financials/{market}")
    cache_fetcher = {
        "hk": cache_hk_financial_history,
        "us": cache_us_financial_history,
        "cn": cache_cn_financial_history,
    }[market]
    added_rows_by_statement: dict[str, int] = {}
    failed_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task(f"缓存{_market_label(market)}历史财报", total=len(codes))
        for index, code in enumerate(codes, start=1):
            result = cache_fetcher(code, root_dir=root_dir, refresh=refresh)
            for statement, added_rows in result.added_rows_by_statement.items():
                added_rows_by_statement[statement] = (
                    added_rows_by_statement.get(statement, 0) + added_rows
                )
            if result.status != "成功":
                failed_count += 1
                console.print(f"{result.code}: {result.status}")

            progress.advance(task_id)
            if index < len(codes) and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            if batch_size > 0 and index % batch_size == 0 and index < len(codes):
                time.sleep(batch_sleep_seconds)

    console.print(f"Cached financial histories for {len(codes)} {_market_label(market)} companies.")
    for statement, added_rows in sorted(added_rows_by_statement.items()):
        console.print(f"Added {statement} rows: {added_rows}.")
    console.print(f"Failed companies: {failed_count}.")
    console.print(f"Saved under {root_dir}")


@app.command("hk")
def hk(
    config_file: Path = CONFIGS_DIR / "default.yaml",
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
    )


@app.command("us")
def us(
    config_file: Path = CONFIGS_DIR / "us.yaml",
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
    )


@app.command("cn")
def cn(
    config_file: Path = CONFIGS_DIR / "cn.yaml",
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
    )
