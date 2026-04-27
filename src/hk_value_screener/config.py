from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXPORTS_DIR = DATA_DIR / "exports"
RULES_DIR = PROJECT_ROOT / "rules"
SCREENING_RULES_DIR = RULES_DIR / "screening"
RULE_NOTES_DIR = RULES_DIR / "notes"
DOCS_DIR = PROJECT_ROOT / "docs"


def ensure_directories() -> list[Path]:
    directories = [
        DATA_DIR,
        RAW_DATA_DIR,
        PROCESSED_DATA_DIR,
        EXPORTS_DIR,
        RULES_DIR,
        SCREENING_RULES_DIR,
        RULE_NOTES_DIR,
        DOCS_DIR / "principles",
        DOCS_DIR / "playbooks",
        DOCS_DIR / "postmortems",
        DOCS_DIR / "decisions",
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    return directories
