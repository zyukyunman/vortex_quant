"""市场最小报价单位、交易板块与最小申报数量规则。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketPermissionConfig:
    """账户可交易板块配置。

    QMT bridge 目前没有稳定暴露账户板块权限读取接口时，自动执行链路用这份配置
    明确表达“允许哪些板块进入订单生成”。后续若能从柜台读取权限，可用真实权限覆盖。
    """

    allow_main: bool = True
    allow_chinext: bool = True
    allow_star: bool = True
    allow_bse: bool = False


def price_tick(symbol: str) -> float:
    """返回当前自动执行主链路使用的最小报价单位。

    当前 QMT 自动执行只覆盖 A 股股票，默认先按 0.01 元报价单位处理，
    至少保证不会向柜台发送非法的 4 位小数价格。
    """

    if symbol.endswith((".SH", ".SZ", ".BJ")):
        return 0.01
    return 0.01


def market_board(symbol: str) -> str:
    """返回 A 股代码所属交易板块。"""

    code = symbol.split(".", 1)[0]
    suffix = symbol.rsplit(".", 1)[-1] if "." in symbol else ""
    if suffix == "BJ":
        return "bse"
    if suffix == "SH" and code.startswith("688"):
        return "star"
    if suffix == "SZ" and code.startswith(("300", "301")):
        return "chinext"
    return "main"


def is_market_allowed(symbol: str, permissions: MarketPermissionConfig | None = None) -> bool:
    config = permissions or MarketPermissionConfig()
    board = market_board(symbol)
    if board == "main":
        return config.allow_main
    if board == "chinext":
        return config.allow_chinext
    if board == "star":
        return config.allow_star
    if board == "bse":
        return config.allow_bse
    return False


def min_order_shares(symbol: str, side: str) -> int:
    """返回给定市场与方向下的最小申报股数。

    当前已实测到的实盘约束是：科创板（688.*）买入最小申报为 200 股；
    其他 A 股股票仍按 100 股整手处理。
    """

    if side == "buy" and market_board(symbol) == "star":
        return 200
    return 100


def is_valid_order_shares(symbol: str, side: str, shares: int) -> bool:
    if shares <= 0 or shares % 100 != 0:
        return False
    return shares >= min_order_shares(symbol, side)
