from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from hk_value_screener.app_config import load_app_config, resolve_project_path
from hk_value_screener.config import (
    BLACKLISTS_DIR,
    CONFIGS_DIR,
    SCREENING_RULES_DIR,
    ensure_directories,
)
from hk_value_screener.data_sources import get_hk_spot_full, save_hk_spot_full_csv
from hk_value_screener.models import RuleNoteTemplate
from hk_value_screener.rules import (
    apply_blacklist,
    apply_rule_set,
    load_blacklist_file,
    load_rule_file,
    render_rule_note_template,
)
from hk_value_screener.sample_data import sample_universe

app = typer.Typer(help="Hong Kong value screening research toolkit.")
console = Console()


@app.command("bootstrap")
def bootstrap() -> None:
    """Create local project directories."""
    directories = ensure_directories()
    console.print(f"Initialized {len(directories)} directories.")


@app.command("show-rules")
def show_rules(rule_file: Path = SCREENING_RULES_DIR / "baseline.yaml") -> None:
    """Display a screening rule set."""
    ensure_directories()
    rule = load_rule_file(rule_file)

    table = Table(title=f"{rule.rule_set.name} ({rule.rule_set.version})")
    table.add_column("Field")
    table.add_column("Operator")
    table.add_column("Value")
    table.add_column("Rationale")

    for condition in rule.rule_set.conditions:
        table.add_row(
            condition.field,
            condition.operator,
            str(condition.value),
            condition.rationale,
        )

    console.print(table)


@app.command("show-config")
def show_config(config_file: Path = CONFIGS_DIR / "default.yaml") -> None:
    """Display the active application config."""
    ensure_directories()
    config = load_app_config(config_file)

    table = Table(title=f"{config.name} ({config.version})")
    table.add_column("Section")
    table.add_column("Value")
    table.add_row("description", config.description)
    table.add_row("fetch.enabled", str(config.fetch.enabled))
    table.add_row("fetch.apply_blacklist", str(config.fetch.apply_blacklist))
    table.add_row("fetch.blacklist_file", config.fetch.blacklist_file)
    table.add_row("baseline.rule_file", config.baseline.rule_file)
    table.add_row("output.save_csv", str(config.output.save_csv))
    table.add_row("output.raw_csv_path", config.output.raw_csv_path)
    table.add_row("output.screened_csv_path", config.output.screened_csv_path)
    table.add_row("sector_profiles", ", ".join(config.sector_profiles) or "-")
    console.print(table)


@app.command("run-sample-screen")
def run_sample_screen(rule_file: Path = SCREENING_RULES_DIR / "baseline.yaml") -> None:
    """Apply rules to bundled sample data."""
    ensure_directories()
    rule = load_rule_file(rule_file)
    frame = sample_universe()
    filtered = apply_rule_set(frame, rule.rule_set)

    table = Table(title=f"Sample screen result: {len(filtered)} companies")
    for column in ["code", "name", "price_hkd", "pe_ttm", "pb", "roe_pct", "dividend_yield_pct"]:
        table.add_column(column)

    for _, row in filtered.iterrows():
        table.add_row(
            str(row["code"]),
            str(row["name"]),
            str(row["price_hkd"]),
            str(row["pe_ttm"]),
            str(row["pb"]),
            str(row["roe_pct"]),
            str(row["dividend_yield_pct"]),
        )
    console.print(table)


@app.command("scaffold-rules-note")
def scaffold_rules_note(
    name: str = "new-observation",
    output_dir: Path = Path("rules/notes"),
) -> None:
    """Create a note template for rule iteration."""
    ensure_directories()
    output_dir.mkdir(parents=True, exist_ok=True)
    note = RuleNoteTemplate(
        title="Rule iteration note",
        date=str(date.today()),
        tags=["screening", "postmortem"],
        hypothesis="What did I expect this rule to capture or eliminate?",
        lesson="What actually happened?",
        action="How should the rule or checklist change?",
        evidence=["Add tickers, dates, and concise evidence here."],
    )
    path = output_dir / f"{name}.md"
    path.write_text(render_rule_note_template(note), encoding="utf-8")
    console.print(f"Created {path}")


@app.command("fetch-hk-spot-full")
def fetch_hk_spot_full(
    config_file: Path = CONFIGS_DIR / "default.yaml",
) -> None:
    """Fetch full Hong Kong spot data with value-screening fields preserved."""
    ensure_directories()
    config = load_app_config(config_file)
    if not config.fetch.enabled:
        console.print("Fetch is disabled in config.")
        raise typer.Exit(code=1)

    frame = get_hk_spot_full()
    filtered = frame
    excluded_count = 0
    if config.fetch.apply_blacklist:
        blacklist_file = resolve_project_path(config.fetch.blacklist_file)
        blacklist = load_blacklist_file(blacklist_file)
        filtered = apply_blacklist(frame, blacklist)
        excluded_count = len(frame) - len(filtered)

    table = Table(
        title=f"Hong Kong spot full fields: {len(filtered)} companies "
        f"(excluded {excluded_count} blacklisted names)"
    )
    for column in ["代码", "名称", "最新价", "换手率", "市盈率-动态", "市净率", "总市值"]:
        table.add_column(column)

    preview = filtered.head(10)
    for _, row in preview.iterrows():
        table.add_row(
            str(row["代码"]),
            str(row["名称"]),
            str(row["最新价"]),
            str(row["换手率"]),
            str(row["市盈率-动态"]),
            str(row["市净率"]),
            str(row["总市值"]),
        )
    console.print(table)

    if config.output.save_csv:
        output_path = save_hk_spot_full_csv(
            filtered, path=resolve_project_path(config.output.raw_csv_path)
        )
        console.print(f"Saved CSV to {output_path}")


@app.command("screen-hk-spot")
def screen_hk_spot(
    config_file: Path = CONFIGS_DIR / "default.yaml",
) -> None:
    """Apply baseline rules to saved Hong Kong spot CSV and export a screened CSV."""
    ensure_directories()
    config = load_app_config(config_file)
    source_csv = resolve_project_path(config.output.raw_csv_path)
    if not source_csv.exists():
        console.print(f"Missing source CSV: {source_csv}")
        console.print("Run `uv run hkvs fetch-hk-spot-full` first.")
        raise typer.Exit(code=1)

    frame = pd.read_csv(source_csv)
    initial_count = len(frame)

    blacklist_excluded = 0
    if config.fetch.apply_blacklist:
        blacklist_file = resolve_project_path(config.fetch.blacklist_file)
        blacklist = load_blacklist_file(blacklist_file)
        blacklisted = apply_blacklist(frame, blacklist)
        blacklist_excluded = len(frame) - len(blacklisted)
        frame = blacklisted

    rule_file = resolve_project_path(config.baseline.rule_file)
    rule = load_rule_file(rule_file)
    screened = apply_rule_set(frame, rule.rule_set)

    output_path = resolve_project_path(config.output.screened_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    screened.to_csv(output_path, index=False)

    table = Table(
        title=f"Screened Hong Kong spot: {len(screened)} companies "
        f"(from {initial_count}, blacklist excluded {blacklist_excluded})"
    )
    for column in ["代码", "名称", "最新价", "成交额", "市盈率-动态", "总市值"]:
        table.add_column(column)

    preview = screened.head(15)
    for _, row in preview.iterrows():
        table.add_row(
            str(row["代码"]),
            str(row["名称"]),
            str(row["最新价"]),
            str(row["成交额"]),
            str(row["市盈率-动态"]),
            str(row["总市值"]),
        )
    console.print(table)
    console.print(f"Saved screened CSV to {output_path}")


@app.command("show-blacklist")
def show_blacklist(
    blacklist_file: Path = BLACKLISTS_DIR / "default.yaml",
) -> None:
    """Display current active blacklist entries."""
    ensure_directories()
    blacklist = load_blacklist_file(blacklist_file)
    table = Table(title=f"Blacklist: {blacklist_file.name}")
    table.add_column("Code")
    table.add_column("Name")
    table.add_column("Category")
    table.add_column("Added")
    table.add_column("Active")
    table.add_column("Reason")

    for entry in blacklist.entries:
        table.add_row(
            entry.code,
            entry.name,
            entry.category,
            entry.added_date,
            "yes" if entry.active else "no",
            entry.reason,
        )

    console.print(table)
