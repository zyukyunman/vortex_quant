"""Xueqiu portfolio rebalance adapter.

The Xueqiu "portfolio" product is a percentage-based simulated portfolio.  It
does not expose a stable public API, so this adapter keeps the integration
isolated from the QMT execution path and records the exact payload that would be
submitted.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from vortex.trade.models import TargetPortfolio
from vortex.trade.serialization import write_json

Transport = Callable[[str, str, dict[str, Any] | None, dict[str, str]], dict[str, Any]]
XUEQIU_LOGIN_REQUIRED_ERROR_CODES = {"400016", "20842"}


class XueqiuAuthRequiredError(ConnectionError):
    """雪球登录态失效或需要人工验证。"""

    def __init__(
        self,
        *,
        error_code: str,
        error_description: str,
        error_uri: str | None = None,
    ) -> None:
        self.error_code = str(error_code)
        self.error_description = str(error_description)
        self.error_uri = error_uri
        suffix = f" {self.error_uri}" if self.error_uri else ""
        super().__init__(f"xueqiu auth required {self.error_code}: {self.error_description}{suffix}")


@dataclass(frozen=True)
class XueqiuConfig:
    cube_symbol: str
    market: str = "cn"
    cookie: str | None = None
    cookie_file: str | Path | None = None
    allow_submit: bool = False
    ignore_minor_weight_pct: float = 0.0
    request_timeout_seconds: float = 10.0


@dataclass(frozen=True)
class XueqiuRebalanceArtifacts:
    sync_id: str
    root_dir: Path
    payload_path: Path
    report_path: Path
    report_md_path: Path
    summary: dict[str, Any]


def classify_xueqiu_exception(exc: BaseException) -> dict[str, Any]:
    """把雪球接口异常归类为可执行状态，供通知和工作台展示使用。"""

    error_code = getattr(exc, "error_code", None)
    text = str(exc)
    if error_code is None:
        error_code = _extract_xueqiu_error_code(text)
    error_code_text = str(error_code) if error_code is not None else ""
    login_required = (
        isinstance(exc, XueqiuAuthRequiredError)
        or error_code_text in XUEQIU_LOGIN_REQUIRED_ERROR_CODES
        or "login expired" in text.lower()
        or "重新登录" in text
    )
    return {
        "status": "login_required" if login_required else "error",
        "login_required": login_required,
        "error_code": error_code_text or None,
        "error": text,
    }


def is_xueqiu_auth_error(exc: BaseException) -> bool:
    return bool(classify_xueqiu_exception(exc)["login_required"])


def check_xueqiu_auth(
    *,
    config: XueqiuConfig,
    transport: Transport | None = None,
) -> dict[str, Any]:
    """检查雪球组合接口登录态，不提交任何调仓请求。"""

    checked_at = datetime.now().isoformat(timespec="seconds")
    adapter = XueqiuAdapter(config, transport=transport)
    try:
        current = adapter.get_current()
    except Exception as exc:  # noqa: BLE001 - auth check must return a status payload.
        classified = classify_xueqiu_exception(exc)
        if classified["error_code"] == "10022":
            try:
                quote = adapter.get_quote()
            except Exception:
                pass
            else:
                quote_item = quote.get(config.cube_symbol)
                if isinstance(quote_item, dict):
                    return {
                        "status": "quote_ok_current_unavailable",
                        "authenticated": True,
                        "login_required": False,
                        "cube_symbol": config.cube_symbol,
                        "checked_at": checked_at,
                        "holding_count": 0,
                        "cube_name": quote_item.get("name", ""),
                        "net_value": quote_item.get("net_value", ""),
                        "warning": "组合可访问，但雪球 current/history 调仓接口返回 10022；可能是组合暂无可读调仓当前态。",
                    }
        return {
            "status": classified["status"],
            "authenticated": False,
            "login_required": bool(classified["login_required"]),
            "cube_symbol": config.cube_symbol,
            "checked_at": checked_at,
            "error_code": classified["error_code"],
            "error": classified["error"],
        }
    holdings = _current_holdings(current)
    quote_item: dict[str, Any] = {}
    try:
        quote = adapter.get_quote()
    except Exception:
        quote_item = {}
    else:
        maybe_quote_item = quote.get(config.cube_symbol)
        quote_item = maybe_quote_item if isinstance(maybe_quote_item, dict) else {}
    return {
        "status": "ok",
        "authenticated": True,
        "login_required": False,
        "cube_symbol": config.cube_symbol,
        "checked_at": checked_at,
        "holding_count": len(holdings),
        "current_holdings": _public_xueqiu_holdings(holdings),
        "current_cash_pct": _current_cash_pct(current),
        "cube_name": quote_item.get("name", ""),
        "net_value": quote_item.get("net_value", ""),
    }


class XueqiuAdapter:
    """Thin HTTP adapter for Xueqiu's percentage-rebalance endpoints."""

    BASE_URL = "https://xueqiu.com"
    SEARCH_URL = f"{BASE_URL}/stock/p/search.json"
    CURRENT_URL = f"{BASE_URL}/cubes/rebalancing/current.json"
    REBALANCE_URL = f"{BASE_URL}/cubes/rebalancing/create.json"

    def __init__(self, config: XueqiuConfig, transport: Transport | None = None) -> None:
        if not config.cube_symbol:
            raise ValueError("cube_symbol is required")
        if config.market not in {"cn", "us", "hk"}:
            raise ValueError("market must be one of: cn, us, hk")
        self.config = config
        self._transport = transport or self._default_transport

    def get_current(self) -> dict[str, Any]:
        return self._request("GET", self.CURRENT_URL, {"cube_symbol": self.config.cube_symbol})

    def get_quote(self) -> dict[str, Any]:
        return self._request("GET", f"{self.BASE_URL}/cubes/quote.json", {"code": self.config.cube_symbol})

    def search_stock(self, symbol: str) -> dict[str, Any]:
        query = _xueqiu_search_code(symbol)
        data = self._request(
            "GET",
            self.SEARCH_URL,
            {
                "code": query,
                "size": "300",
                "key": "47bce5c74f",
                "market": self.config.market,
            },
        )
        rows = data.get("stocks")
        if not isinstance(rows, list) or not rows:
            raise ValueError(f"xueqiu stock search returned no rows for {symbol}")
        stock = rows[0]
        if not isinstance(stock, dict):
            raise ValueError(f"xueqiu stock search returned invalid row for {symbol}")
        if int(stock.get("flag", 0)) != 1:
            raise ValueError(f"xueqiu stock is not tradable: {symbol} flag={stock.get('flag')}")
        return stock

    def submit_rebalance(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.config.allow_submit:
            return {"dry_run": True, "message": "xueqiu submit disabled"}
        return self._request("POST", self.REBALANCE_URL, payload)

    def _request(self, method: str, url: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Host": "xueqiu.com",
            "Pragma": "no-cache",
            "Referer": f"https://xueqiu.com/p/update?action=holdings&symbol={self.config.cube_symbol}",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
        }
        cookie = self._cookie()
        if cookie:
            headers["Cookie"] = cookie
        data = self._transport(method, url, payload, headers)
        _raise_for_xueqiu_error(data)
        return data

    def _cookie(self) -> str:
        if self.config.cookie:
            return self.config.cookie.strip()
        if self.config.cookie_file:
            path = Path(self.config.cookie_file).expanduser()
            return path.read_text(encoding="utf-8").strip()
        return ""

    def _default_transport(
        self,
        method: str,
        url: str,
        payload: dict[str, Any] | None,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        request_url = url
        body: bytes | None = None
        if method == "GET" and payload:
            request_url = f"{url}?{urlencode(payload)}"
        elif method == "POST":
            body = urlencode(payload or {}).encode("utf-8")
            headers = {**headers, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

        request = Request(request_url, data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.config.request_timeout_seconds) as response:  # noqa: S310 - fixed HTTPS host.
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            error_payload = _parse_xueqiu_error_payload(raw)
            error_code = str(error_payload.get("error_code", "")) if error_payload else ""
            if error_code in XUEQIU_LOGIN_REQUIRED_ERROR_CODES:
                raise XueqiuAuthRequiredError(
                    error_code=error_code,
                    error_description=str(error_payload.get("error_description", raw[:300])),
                    error_uri=str(error_payload.get("error_uri")) if error_payload.get("error_uri") else None,
                ) from exc
            raise ConnectionError(f"xueqiu request failed: HTTP {exc.code} {raw[:300]}") from exc
        except URLError as exc:
            raise ConnectionError(f"xueqiu request failed: {exc}") from exc

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"xueqiu returned non-json response: {raw[:300]}") from exc
        _raise_for_xueqiu_error(data)
        if not isinstance(data, dict):
            raise ValueError("xueqiu returned unexpected response shape")
        return data


def run_xueqiu_rebalance(
    portfolio: TargetPortfolio,
    *,
    config: XueqiuConfig,
    output_root: Path,
    comment: str | None = None,
    skip_existing: bool = True,
    transport: Transport | None = None,
) -> XueqiuRebalanceArtifacts:
    """Build and optionally submit a Xueqiu rebalance from a frozen TargetPortfolio."""

    adapter = XueqiuAdapter(config, transport=transport)
    sync_id = _sync_id(portfolio, config.cube_symbol)
    root_dir = output_root / "trade" / "xueqiu" / sync_id
    payload_path = root_dir / "rebalance_payload.json"
    report_path = root_dir / "xueqiu_report.json"
    report_md_path = root_dir / "xueqiu_report.md"

    if skip_existing and report_path.exists():
        existing = json.loads(report_path.read_text(encoding="utf-8"))
        existing_status = str(existing.get("status", ""))
        if existing_status == "submitted" or not config.allow_submit:
            summary = {
                **existing,
                "status": "skipped_existing",
                "submitted": False,
                "report_path": str(report_path),
                "payload_path": str(payload_path),
            }
            return XueqiuRebalanceArtifacts(
                sync_id=sync_id,
                root_dir=root_dir,
                payload_path=payload_path,
                report_path=report_path,
                report_md_path=report_md_path,
                summary=summary,
            )

    current = adapter.get_current()
    payload, diagnostics = build_xueqiu_rebalance_payload(
        portfolio,
        current=current,
        lookup_stock=adapter.search_stock,
        config=config,
        comment=comment,
    )
    response = adapter.submit_rebalance(payload)
    submitted = bool(config.allow_submit)
    status = "submitted" if submitted else "dry_run"
    report = {
        "sync_id": sync_id,
        "mode": "xueqiu",
        "status": status,
        "submitted": submitted,
        "cube_symbol": config.cube_symbol,
        "trade_date": portfolio.trade_date,
        "portfolio_id": portfolio.portfolio_id,
        "strategy_version": portfolio.strategy_version,
        "target_position_count": len(portfolio.positions),
        "xueqiu_holding_count": diagnostics["holding_count"],
        "cash_pct": diagnostics["cash_pct"],
        "weight_sum_pct": diagnostics["weight_sum_pct"],
        "changed_symbols": diagnostics["changed_symbols"],
        "response": response,
        "payload_path": str(payload_path),
        "report_path": str(report_path),
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    write_json(payload_path, payload)
    write_json(report_path, report)
    _write_xueqiu_markdown_report(report_md_path, report)
    return XueqiuRebalanceArtifacts(
        sync_id=sync_id,
        root_dir=root_dir,
        payload_path=payload_path,
        report_path=report_path,
        report_md_path=report_md_path,
        summary=report,
    )


def build_xueqiu_rebalance_payload(
    portfolio: TargetPortfolio,
    *,
    current: dict[str, Any],
    lookup_stock: Callable[[str], dict[str, Any]],
    config: XueqiuConfig,
    comment: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    current_holdings = _current_holdings(current)
    current_by_symbol = {_normalize_xueqiu_symbol(str(item.get("stock_symbol", item.get("code", "")))): item for item in current_holdings}
    current_by_stock_id = {
        int(item["stock_id"]): item
        for item in current_holdings
        if item.get("stock_id") is not None
    }

    holdings: list[dict[str, Any]] = []
    changed_symbols: list[str] = []
    for position in portfolio.positions:
        weight_pct = round(float(position.target_weight) * 100.0, 2)
        if weight_pct <= 0:
            continue
        stock = lookup_stock(position.symbol)
        stock_symbol = _normalize_xueqiu_symbol(str(stock.get("code", position.symbol)))
        previous = current_by_symbol.get(stock_symbol) or current_by_stock_id.get(int(stock["stock_id"]))
        holding = _holding_from_stock(stock) if previous is None else dict(previous)
        previous_weight = float(holding.get("weight", 0.0) or 0.0)
        holding.update(
            {
                "weight": weight_pct,
                "proactive": abs(weight_pct - previous_weight) > float(config.ignore_minor_weight_pct),
                "stock_symbol": stock_symbol,
                "stock_name": str(stock.get("name", holding.get("stock_name", ""))),
                "textname": str(stock.get("name", holding.get("textname", ""))),
                "url": f"/S/{stock_symbol}",
                "price": str(stock.get("current", holding.get("price", position.reference_price))),
                "flag": int(stock.get("flag", holding.get("flag", 1))),
            }
        )
        if holding["proactive"]:
            changed_symbols.append(stock_symbol)
        holdings.append(holding)

    weight_sum = round(sum(float(item.get("weight", 0.0) or 0.0) for item in holdings), 2)
    if weight_sum > 100.000001:
        raise ValueError(f"xueqiu weight sum cannot exceed 100: {weight_sum}")
    cash = round(100.0 - weight_sum, 2)
    payload_comment = comment
    if payload_comment is None:
        payload_comment = f"Vortex sync {portfolio.strategy_version} {portfolio.trade_date} {portfolio.portfolio_id}"
    payload = {
        "cash": cash,
        "holdings": json.dumps(holdings, ensure_ascii=False, separators=(",", ":")),
        "cube_symbol": config.cube_symbol,
        "segment": "true",
        "comment": payload_comment,
    }
    diagnostics = {
        "cash_pct": cash,
        "weight_sum_pct": weight_sum,
        "holding_count": len(holdings),
        "changed_symbols": changed_symbols,
    }
    return payload, diagnostics


def _current_holdings(current: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("last_rb", "view_rebalancing", "last_rebalancing"):
        value = current.get(key)
        if isinstance(value, dict) and isinstance(value.get("holdings"), list):
            return [dict(item) for item in value["holdings"] if isinstance(item, dict)]
    if isinstance(current.get("holdings"), list):
        return [dict(item) for item in current["holdings"] if isinstance(item, dict)]
    return []


def _current_cash_pct(current: dict[str, Any]) -> float | None:
    for key in ("last_rb", "view_rebalancing", "last_rebalancing"):
        value = current.get(key)
        if isinstance(value, dict):
            cash = _float_or_none(value.get("cash"))
            if cash is not None:
                return cash
    return _float_or_none(current.get("cash"))


def _public_xueqiu_holdings(holdings: list[dict[str, Any]], *, limit: int = 80) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in holdings[:limit]:
        rows.append(
            {
                "symbol": str(item.get("stock_symbol") or item.get("code") or ""),
                "name": str(item.get("stock_name") or item.get("textname") or item.get("name") or ""),
                "weight": _float_or_none(item.get("weight")),
                "price": _float_or_none(item.get("price") or item.get("current")),
                "cost_price": _xueqiu_cost_price(item),
                "proactive": bool(item.get("proactive")),
                "segment": str(item.get("segment_name") or item.get("ind_name") or ""),
            }
        )
    return rows


def _xueqiu_cost_price(item: dict[str, Any]) -> float | None:
    for key in (
        "cost_price",
        "costPrice",
        "avg_cost",
        "avgCost",
        "average_cost",
        "averageCost",
        "position_cost",
        "positionCost",
        "holding_cost",
        "holdingCost",
        "buy_price",
        "buyPrice",
    ):
        value = _float_or_none(item.get(key))
        if value is not None and value > 0:
            return value
    return None


def _float_or_none(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _holding_from_stock(stock: dict[str, Any]) -> dict[str, Any]:
    stock_symbol = _normalize_xueqiu_symbol(str(stock["code"]))
    stock_name = str(stock["name"])
    segment_id = stock.get("ind_id", stock.get("segment_id", 0))
    segment_name = stock.get("ind_name", stock.get("segment_name", ""))
    segment_color = stock.get("ind_color", stock.get("segment_color", ""))
    return {
        "code": stock_symbol,
        "name": stock_name,
        "stock_symbol": stock_symbol,
        "stock_name": stock_name,
        "flag": int(stock.get("flag", 1)),
        "current": stock.get("current", 0),
        "chg": stock.get("chg", 0),
        "percent": str(stock.get("percent", 0)),
        "stock_id": int(stock["stock_id"]),
        "ind_id": segment_id,
        "ind_name": segment_name,
        "ind_color": segment_color,
        "segment_id": segment_id,
        "segment_name": segment_name,
        "segment_color": segment_color,
        "textname": stock_name,
        "url": f"/S/{stock_symbol}",
        "proactive": True,
        "price": str(stock.get("current", 0)),
    }


def _xueqiu_search_code(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if symbol.endswith((".SH", ".SZ", ".BJ")):
        return symbol[:6]
    if symbol.startswith(("SH", "SZ", "BJ")) and len(symbol) >= 8:
        return symbol[2:]
    return symbol


def _normalize_xueqiu_symbol(symbol: str) -> str:
    text = symbol.strip().upper()
    if text.endswith(".SH"):
        return f"SH{text[:6]}"
    if text.endswith(".SZ"):
        return f"SZ{text[:6]}"
    if text.endswith(".BJ"):
        return f"BJ{text[:6]}"
    return text


def _parse_xueqiu_error_payload(raw: str) -> dict[str, Any]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _raise_for_xueqiu_error(data: dict[str, Any]) -> None:
    if not isinstance(data, dict) or data.get("error_code") is None:
        return
    error_code = str(data.get("error_code"))
    if error_code in XUEQIU_LOGIN_REQUIRED_ERROR_CODES:
        raise XueqiuAuthRequiredError(
            error_code=error_code,
            error_description=str(data.get("error_description", data.get("error_uri", "unknown error"))),
            error_uri=str(data.get("error_uri")) if data.get("error_uri") else None,
        )
    raise ValueError(
        f"xueqiu error {error_code}: "
        f"{data.get('error_description', data.get('error_uri', 'unknown error'))}"
    )


def _extract_xueqiu_error_code(text: str) -> str | None:
    for code in XUEQIU_LOGIN_REQUIRED_ERROR_CODES:
        if code in text:
            return code
    match = re.search(r"xueqiu (?:error|auth required) ([0-9]+)", text)
    if match:
        return match.group(1)
    match = re.search(r'"error_code"\s*:\s*"?([0-9]+)"?', text)
    if match:
        return match.group(1)
    return None


def _sync_id(portfolio: TargetPortfolio, cube_symbol: str) -> str:
    digest = hashlib.sha1(f"{portfolio.portfolio_id}|{portfolio.trade_date}|{cube_symbol}".encode("utf-8")).hexdigest()[:10]
    return f"xq_{portfolio.trade_date}_{digest}"


def _write_xueqiu_markdown_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Xueqiu Rebalance Report: {report['sync_id']}",
        "",
        "## Summary",
        "",
        f"- Status: {report['status']}",
        f"- Submitted: {'yes' if report['submitted'] else 'no'}",
        f"- Trade date: {report['trade_date']}",
        f"- Portfolio: {report['portfolio_id']}",
        f"- Cube: {report['cube_symbol']}",
        f"- Holdings: {report['xueqiu_holding_count']}",
        f"- Weight sum: {report['weight_sum_pct']:.2f}%",
        f"- Cash: {report['cash_pct']:.2f}%",
        f"- Changed symbols: {', '.join(report['changed_symbols']) if report['changed_symbols'] else '-'}",
        "",
        "## Artifacts",
        "",
        f"- Payload: {report['payload_path']}",
        f"- Report: {report['report_path']}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
