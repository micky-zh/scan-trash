from __future__ import annotations

from pathlib import Path

import yaml
from pandas import DataFrame

from hk_value_screener.models import RuleFile, RuleNoteTemplate, ScreeningRuleSet


def load_rule_file(path: Path) -> RuleFile:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    rule_set = ScreeningRuleSet.model_validate(raw)
    return RuleFile(path=path, rule_set=rule_set, raw=raw)


def apply_rule_set(frame: DataFrame, rule_set: ScreeningRuleSet) -> DataFrame:
    filtered = frame.copy()
    for condition in rule_set.conditions:
        series = filtered[condition.field]
        value = condition.value
        operator = condition.operator

        if operator == ">":
            filtered = filtered[series > value]
        elif operator == ">=":
            filtered = filtered[series >= value]
        elif operator == "<":
            filtered = filtered[series < value]
        elif operator == "<=":
            filtered = filtered[series <= value]
        elif operator == "==":
            filtered = filtered[series == value]
        elif operator == "!=":
            filtered = filtered[series != value]
        else:
            raise ValueError(f"Unsupported operator: {operator}")
    return filtered


def render_rule_note_template(note: RuleNoteTemplate) -> str:
    evidence_lines = "\n".join(f"- {item}" for item in note.evidence) if note.evidence else "- "
    tags = ", ".join(note.tags)
    return f"""# {note.title}

date: {note.date}
tags: {tags}

## Hypothesis
{note.hypothesis}

## Lesson
{note.lesson}

## Action
{note.action}

## Evidence
{evidence_lines}
"""
