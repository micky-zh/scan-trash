from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from hk_value_screener.config import STATE_DIR
from hk_value_screener.data_sources import normalize_security_code

BLACKLIST_FILENAME = "blacklist.csv"
BLACKLIST_COLUMNS = [
    "market",
    "code",
    "name",
    "reason",
    "enabled",
    "created_at",
    "updated_at",
]
SUPPORTED_MARKETS = {"hk", "us", "cn"}


def blacklist_path(path: Path | None = None) -> Path:
    return path if path is not None else STATE_DIR / BLACKLIST_FILENAME


def _normalize_market(market: str) -> str:
    normalized = market.strip().lower()
    if normalized not in SUPPORTED_MARKETS:
        raise ValueError(f"Unsupported market: {market}")
    return normalized


def _timestamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _normalize_enabled(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or pd.isna(value):
        return False
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "y", "是", "启用", "active", "enabled"}


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=BLACKLIST_COLUMNS)


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return _empty_frame()

    output = frame.copy()
    for column in BLACKLIST_COLUMNS:
        if column not in output.columns:
            output[column] = ""

    output = output[BLACKLIST_COLUMNS].copy()
    output["market"] = output["market"].astype(str).str.strip().str.lower()
    output = output[output["market"].isin(SUPPORTED_MARKETS)]
    if output.empty:
        return _empty_frame()

    output["code"] = output.apply(
        lambda row: normalize_security_code(row["code"], market=row["market"]),
        axis=1,
    )
    output = output[output["code"].astype(str).str.len() > 0].copy()
    if output.empty:
        return _empty_frame()

    output["name"] = output["name"].fillna("").astype(str).str.strip()
    output["reason"] = output["reason"].fillna("").astype(str).str.strip()
    output["enabled"] = output["enabled"].map(_normalize_enabled)
    output["created_at"] = output["created_at"].fillna("").astype(str).str.strip()
    output["updated_at"] = output["updated_at"].fillna("").astype(str).str.strip()
    output = output.drop_duplicates(subset=["market", "code"], keep="last").reset_index(drop=True)
    return output


def load_blacklist(path: Path | None = None, enabled_only: bool = False) -> pd.DataFrame:
    file_path = blacklist_path(path)
    if not file_path.exists():
        return _empty_frame()

    frame = pd.read_csv(file_path, dtype=str).fillna("")
    normalized = _normalize_frame(frame)
    if enabled_only and not normalized.empty:
        normalized = normalized[normalized["enabled"]].copy()
    return normalized.reset_index(drop=True)


def save_blacklist(frame: pd.DataFrame, path: Path | None = None) -> Path:
    file_path = blacklist_path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_frame(frame)
    if not normalized.empty:
        normalized = normalized.sort_values(
            by=["enabled", "market", "code", "updated_at"],
            ascending=[False, True, True, False],
        ).reset_index(drop=True)
    normalized.to_csv(file_path, index=False)
    return file_path


def upsert_blacklist_entry(
    market: str,
    code: str,
    reason: str,
    name: str = "",
    path: Path | None = None,
) -> pd.DataFrame:
    normalized_market = _normalize_market(market)
    normalized_code = normalize_security_code(code, market=normalized_market)
    timestamp = _timestamp()

    frame = load_blacklist(path)
    mask = (frame["market"] == normalized_market) & (frame["code"] == normalized_code)
    if mask.any():
        index = frame.index[mask][0]
        if name.strip():
            frame.at[index, "name"] = name.strip()
        if reason.strip():
            frame.at[index, "reason"] = reason.strip()
        frame.at[index, "enabled"] = True
        if not frame.at[index, "created_at"]:
            frame.at[index, "created_at"] = timestamp
        frame.at[index, "updated_at"] = timestamp
    else:
        frame = pd.concat(
            [
                frame,
                pd.DataFrame(
                    [
                        {
                            "market": normalized_market,
                            "code": normalized_code,
                            "name": name.strip(),
                            "reason": reason.strip(),
                            "enabled": True,
                            "created_at": timestamp,
                            "updated_at": timestamp,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    save_blacklist(frame, path)
    return load_blacklist(path)


def disable_blacklist_entry(market: str, code: str, path: Path | None = None) -> bool:
    normalized_market = _normalize_market(market)
    normalized_code = normalize_security_code(code, market=normalized_market)
    frame = load_blacklist(path)
    if frame.empty:
        return False

    mask = (frame["market"] == normalized_market) & (frame["code"] == normalized_code)
    if not mask.any():
        return False

    timestamp = _timestamp()
    index = frame.index[mask][0]
    frame.at[index, "enabled"] = False
    frame.at[index, "updated_at"] = timestamp
    save_blacklist(frame, path)
    return True


def list_blacklist_entries(
    path: Path | None = None,
    market: str | None = None,
    enabled_only: bool = False,
) -> pd.DataFrame:
    frame = load_blacklist(path, enabled_only=enabled_only)
    if market is None or frame.empty:
        return frame

    normalized_market = _normalize_market(market)
    return frame[frame["market"] == normalized_market].reset_index(drop=True)


def load_active_blacklist(path: Path | None = None, market: str | None = None) -> pd.DataFrame:
    frame = load_blacklist(path, enabled_only=True)
    if market is None or frame.empty:
        return frame

    normalized_market = _normalize_market(market)
    return frame[frame["market"] == normalized_market].reset_index(drop=True)
