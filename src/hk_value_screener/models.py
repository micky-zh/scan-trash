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
