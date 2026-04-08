"""DataProvider Protocol — 所有数据源必须实现的接口（06 §3.2）。"""
from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any, Callable, Protocol

if TYPE_CHECKING:
    import pandas as pd

    DataFrame = pd.DataFrame
else:
    # 这里只是协议定义层；运行时不该因为读取类型签名就强制导入 pandas。
    DataFrame = Any


class DataProvider(Protocol):
    """数据源接口 — 所有 provider 必须实现。"""

    @property
    def name(self) -> str:
        """provider 标识，如 'tushare'。"""
        ...

    @property
    def supported_markets(self) -> list[str]:
        """支持的市场列表，如 ['cn_stock']。"""
        ...

    @property
    def dataset_registry(self) -> dict[str, dict[str, object]]:
        """Provider 自带的完整数据集注册表。

        返回格式：{dataset_name: {"api": "接口名", "description": "中文说明", ...}}
        选择了 provider 就等于选择了它全部的数据采集能力。
        """
        ...

    def resolve_dataset(self, dataset: str) -> str:
        """将用户输入的 dataset 名解析为 provider 的 canonical 名。"""
        ...

    def smoke_test(self) -> bool:
        """冒烟测试：验证 Token / 凭证可用，返回 True/False。"""
        ...

    def fetch_instruments(self, market: str) -> DataFrame:
        """获取标的列表。

        返回列：symbol, name, list_date, delist_date, industry, market_cap
        排序：symbol ASC
        异常：DataError(DATA_PROVIDER_FETCH_FAILED)
        """
        ...

    def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
        """获取交易日历。返回已排序的交易日列表。

        不变量：返回日期 ⊆ [start, end]
        """
        ...

    def fetch_bars(
        self,
        market: str,
        symbols: list[str],
        freq: str,
        start: date,
        end: date,
    ) -> DataFrame:
        """获取行情数据。

        返回列：symbol, date, open, high, low, close, volume, amount
        不变量：返回数据的日期 ⊆ [start, end]
        不变量：返回的 symbol ⊆ 请求的 symbols
        排序：date ASC, symbol ASC
        """
        ...

    def fetch_fundamental(
        self,
        market: str,
        symbols: list[str],
        fields: list[str],
        start: date,
        end: date,
    ) -> DataFrame:
        """获取基本面数据。

        返回列：symbol, ann_date, report_date, ...fields
        PIT 要求：必须包含 ann_date 字段
        排序：ann_date ASC, symbol ASC
        """
        ...

    def fetch_events(
        self,
        market: str,
        symbols: list[str],
        start: date,
        end: date,
    ) -> DataFrame:
        """获取事件数据（分红、配股、停牌等）。

        排序：date ASC, symbol ASC
        """
        ...

    def fetch_dataset(
        self,
        dataset: str,
        market: str,
        start: date,
        end: date,
        *,
        symbols: list[str] | None = None,
        trading_days: list[date] | None = None,
        partition_values: list[str] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> DataFrame:
        """按 dataset 名抓取并标准化为 DataFrame。

        这是 registry 驱动的通用入口；Provider 可以在内部把它分发到：
        - 基础专用 fetch（如 fetch_bars / fetch_fundamental）
        - 通用模式（如按交易日、按 symbol、按季度、按日期范围）
        """
        ...
