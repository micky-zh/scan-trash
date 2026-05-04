from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class OutputConfig(BaseModel):
    save_csv: bool = True
    raw_csv_path: str = "data/raw/hk_spot_full.csv"
    enriched_cache_csv_path: str = "data/raw/hk_enriched_cache.csv"
    research_csv_path: str = "data/processed/hk_research_view.csv"


class FetchConfig(BaseModel):
    enabled: bool = True


class AppConfig(BaseModel):
    name: str
    version: str
    description: str = ""
    market: Literal["hk", "us", "cn"] = "hk"
    fetch: FetchConfig = Field(default_factory=FetchConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    sector_profiles: list[str] = Field(default_factory=list)
