from pathlib import Path

from hk_value_screener.app_config import load_app_config, resolve_project_path


def test_load_default_app_config() -> None:
    config = load_app_config(Path("configs/default.yaml"))
    assert config.name == "default"
    assert config.market == "hk"
    assert config.output.raw_csv_path.endswith("hk_spot_full.csv")
    assert config.output.enriched_cache_csv_path.endswith("hk_enriched_cache.csv")
    assert config.output.research_csv_path.endswith("hk_research_view.csv")


def test_load_us_app_config() -> None:
    config = load_app_config(Path("configs/us.yaml"))
    assert config.name == "us"
    assert config.market == "us"
    assert config.output.raw_csv_path.endswith("us_spot_full.csv")
    assert config.output.enriched_cache_csv_path.endswith("us_enriched_cache.csv")
    assert config.output.research_csv_path.endswith("us_research_view.csv")


def test_load_cn_app_config() -> None:
    config = load_app_config(Path("configs/cn.yaml"))
    assert config.name == "cn"
    assert config.market == "cn"
    assert config.output.raw_csv_path.endswith("cn_spot_full.csv")
    assert config.output.enriched_cache_csv_path.endswith("cn_enriched_cache.csv")
    assert config.output.research_csv_path.endswith("cn_research_view.csv")


def test_resolve_project_path_resolves_relative_path() -> None:
    resolved = resolve_project_path("configs/default.yaml")
    assert resolved.is_absolute()
    assert resolved.name == "default.yaml"
