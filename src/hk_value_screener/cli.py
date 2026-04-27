from __future__ import annotations

from datetime import date
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from hk_value_screener.config import SCREENING_RULES_DIR, ensure_directories
from hk_value_screener.data_sources import get_hk_spot_full, save_hk_spot_full_csv
from hk_value_screener.models import RuleNoteTemplate
from hk_value_screener.rules import apply_rule_set, load_rule_file, render_rule_note_template
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
    save_csv: bool = True,
) -> None:
    """Fetch full Hong Kong spot data with value-screening fields preserved."""
    ensure_directories()
    frame = get_hk_spot_full()

    table = Table(title=f"Hong Kong spot full fields: {len(frame)} companies")
    for column in ["代码", "名称", "最新价", "换手率", "市盈率-动态", "市净率", "总市值"]:
        table.add_column(column)

    preview = frame.head(10)
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

    if save_csv:
        output_path = save_hk_spot_full_csv(frame)
        console.print(f"Saved CSV to {output_path}")
