from __future__ import annotations

from pathlib import Path

import yaml

from hk_value_screener.config import PROJECT_ROOT
from hk_value_screener.models import AppConfig


def resolve_project_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return (PROJECT_ROOT / candidate).resolve()


def load_app_config(path: Path) -> AppConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return AppConfig.model_validate(raw)
