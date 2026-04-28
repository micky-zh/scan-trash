from pathlib import Path

from hk_value_screener.app_config import load_app_config, resolve_project_path


def test_load_default_app_config() -> None:
    config = load_app_config(Path("configs/default.yaml"))
    assert config.name == "default"
    assert config.fetch.apply_blacklist is True
    assert config.output.raw_csv_path.endswith("hk_spot_full.csv")
    assert config.output.screened_csv_path.endswith("hk_screened.csv")
    assert config.output.enriched_cache_csv_path.endswith("hk_screened_enriched_cache.csv")


def test_resolve_project_path_resolves_relative_path() -> None:
    resolved = resolve_project_path("rules/blacklists/default.yaml")
    assert resolved.is_absolute()
    assert resolved.name == "default.yaml"
