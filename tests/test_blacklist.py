from __future__ import annotations

from typer.testing import CliRunner

from hk_value_screener.blacklist import (
    disable_blacklist_entry,
    load_active_blacklist,
    load_blacklist,
    upsert_blacklist_entry,
)
from hk_value_screener.cli import app


def test_blacklist_module_roundtrip(tmp_path) -> None:
    path = tmp_path / "blacklist.csv"

    upsert_blacklist_entry(
        market="hk",
        code="700",
        reason="业务太复杂",
        name="腾讯控股",
        path=path,
    )
    frame = load_blacklist(path)

    assert frame.loc[0, "market"] == "hk"
    assert frame.loc[0, "code"] == "00700"
    assert frame.loc[0, "name"] == "腾讯控股"
    assert frame.loc[0, "reason"] == "业务太复杂"
    assert bool(frame.loc[0, "enabled"]) is True
    assert load_active_blacklist(path).loc[0, "code"] == "00700"

    removed = disable_blacklist_entry("hk", "700", path=path)
    assert removed is True

    updated = load_blacklist(path)
    assert bool(updated.loc[0, "enabled"]) is False
    assert load_active_blacklist(path).empty


def test_blacklist_cli_commands_use_local_state(tmp_path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    state_dir = data_dir / "state"
    monkeypatch.setattr("hk_value_screener.config.DATA_DIR", data_dir)
    monkeypatch.setattr("hk_value_screener.config.RAW_DATA_DIR", data_dir / "raw")
    monkeypatch.setattr("hk_value_screener.config.STATE_DIR", state_dir)
    monkeypatch.setattr("hk_value_screener.config.PROCESSED_DATA_DIR", data_dir / "processed")
    monkeypatch.setattr("hk_value_screener.config.CONFIGS_DIR", tmp_path / "configs")
    monkeypatch.setattr("hk_value_screener.blacklist.STATE_DIR", state_dir)

    runner = CliRunner()
    add_result = runner.invoke(
        app,
        [
            "blacklist",
            "add",
            "--market",
            "hk",
            "--symbol",
            "700",
            "--reason",
            "业务太复杂",
            "--name",
            "腾讯控股",
        ],
    )
    assert add_result.exit_code == 0

    list_result = runner.invoke(app, ["blacklist", "list", "--market", "hk"])
    assert list_result.exit_code == 0
    assert "业务太复杂" in list_result.output

    remove_result = runner.invoke(
        app,
        ["blacklist", "remove", "--market", "hk", "--symbol", "700"],
    )
    assert remove_result.exit_code == 0

    frame = load_blacklist(state_dir / "blacklist.csv")
    assert bool(frame.loc[0, "enabled"]) is False
