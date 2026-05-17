"""Shared live-trading permission gate for execution entrypoints."""

from __future__ import annotations

from collections.abc import Iterable

CONFIRM_AUTO_TRADING = "CONFIRM_AUTO_TRADING"


def normalize_allowed_account_ids(value: object) -> list[str]:
    """Normalize CLI/API account whitelist payloads."""

    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Iterable):
        items: list[str] = []
        for item in value:
            items.extend(normalize_allowed_account_ids(item))
        return items
    return [str(value).strip()] if str(value).strip() else []


def validate_live_trading_permission(
    *,
    enable_trading: bool,
    disable_trading: bool = False,
    account_id: str | None,
    allowed_account_ids: object,
    confirmation: str | None,
    option_label: str = "--enable-trading",
    allowed_account_label: str = "--allowed-account-id",
) -> bool:
    """Return whether real order submission is allowed.

    Live trading is opt-in only. Every execution surface must call this helper
    before passing allow_trading=True into a broker or bridge adapter.
    """

    if disable_trading:
        return False
    if not enable_trading:
        return False
    normalized_account = str(account_id or "").strip()
    if not normalized_account:
        raise ValueError(f"{option_label} requires account id")
    allowed = normalize_allowed_account_ids(allowed_account_ids)
    if not allowed or normalized_account not in allowed:
        raise ValueError(f"{option_label} requires matching {allowed_account_label} whitelist entry")
    if str(confirmation or "") != CONFIRM_AUTO_TRADING:
        raise ValueError(f"{option_label} requires confirmation {CONFIRM_AUTO_TRADING}")
    return True
