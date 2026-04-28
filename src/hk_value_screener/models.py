from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field


ComparisonOperator = Literal[">", ">=", "<", "<=", "==", "!="]


class RuleCondition(BaseModel):
    field: str
    operator: ComparisonOperator
    value: float | int | str
    rationale: str = ""


class ScreeningRuleSet(BaseModel):
    name: str
    version: str
    objective: str
    universe: str = "hk_equities"
    exclude_tags: list[str] = Field(default_factory=list)
    sort_by: list[str] = Field(default_factory=list)
    conditions: list[RuleCondition] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class RuleNoteTemplate(BaseModel):
    title: str
    date: str
    tags: list[str] = Field(default_factory=list)
    hypothesis: str
    lesson: str
    action: str
    evidence: list[str] = Field(default_factory=list)


class RuleFile(BaseModel):
    path: Path
    rule_set: ScreeningRuleSet
    raw: dict[str, Any]


class BlacklistEntry(BaseModel):
    code: str
    name: str = ""
    category: str = "manual"
    active: bool = True
    added_date: str
    reason: str
    note: str = ""


class BlacklistFile(BaseModel):
    version: str
    updated_at: str
    entries: list[BlacklistEntry] = Field(default_factory=list)


class OutputConfig(BaseModel):
    save_csv: bool = True
    raw_csv_path: str = "data/raw/hk_spot_full.csv"
    screened_csv_path: str = "data/processed/hk_screened.csv"
    enriched_screened_csv_path: str = "data/processed/hk_screened_enriched.csv"
    financial_screened_csv_path: str = "data/processed/hk_financial_screened.csv"


class FetchConfig(BaseModel):
    enabled: bool = True
    apply_blacklist: bool = True
    blacklist_file: str = "rules/blacklists/default.yaml"


class BaselineRuleConfig(BaseModel):
    rule_file: str = "rules/screening/baseline.yaml"


class FinancialDataConfig(BaseModel):
    financial_csv_path: str = "data/raw/hk_financial_indicators.csv"
    code_column: str = "代码"
    rule_file: str = "rules/screening/baseline.yaml"


class AppConfig(BaseModel):
    name: str
    version: str
    description: str = ""
    fetch: FetchConfig = Field(default_factory=FetchConfig)
    baseline: BaselineRuleConfig = Field(default_factory=BaselineRuleConfig)
    financial: FinancialDataConfig = Field(default_factory=FinancialDataConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    sector_profiles: list[str] = Field(default_factory=list)
