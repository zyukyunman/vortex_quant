"""
notifier.py
通知网关 — 消息分级、防骚扰、分发

消息分级:
  P0 紧急: 回撤预警、数据异常 → 立即推送
  P1 重要: 选股信号、策略告警 → 每日推送
  P2 常规: 数据更新、运行日志 → 汇总推送
  P3 周报: 估值报告、绩效     → 定时推送

防骚扰:
  - 同一标题当日不重复推送
  - 静默时段: 23:00-07:00 (P0 除外)
  - 每日推送上限 5 条 (Server酱免费额度)
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional

from app.models import SelectionResult
from app.notify.serverchan import send_serverchan
from app.notify.templates import (
    format_daily_summary,
    format_risk_alert,
    format_selection_result,
)

logger = logging.getLogger(__name__)


class Notifier:
    """通知网关"""

    def __init__(self, serverchan_key: str = ""):
        self.serverchan_key = serverchan_key
        self._sent_today: Dict[str, int] = defaultdict(int)  # date → count
        self._sent_titles: set = set()  # 今日已推送标题 (防重复)
        self._today: str = ""

    def _reset_daily(self):
        """每日重置计数"""
        today = datetime.now().strftime("%Y%m%d")
        if today != self._today:
            self._today = today
            self._sent_today.clear()
            self._sent_titles.clear()

    def _can_send(self, level: str, title: str) -> bool:
        """检查是否可以发送"""
        self._reset_daily()

        # 防重复
        if title in self._sent_titles:
            logger.debug("消息已推送过，跳过: %s", title)
            return False

        # 每日上限
        if self._sent_today[self._today] >= 5:
            if level != "P0":
                logger.warning("今日推送已达上限(5条)，跳过: %s", title)
                return False

        # 静默时段 (23:00-07:00), P0 除外
        hour = datetime.now().hour
        if (hour >= 23 or hour < 7) and level != "P0":
            logger.debug("静默时段，跳过: %s", title)
            return False

        return True

    def _do_send(self, title: str, desp: str):
        """实际发送"""
        self._reset_daily()
        success = send_serverchan(self.serverchan_key, title, desp)
        if success:
            self._sent_titles.add(title)
            self._sent_today[self._today] += 1

    def notify_selection(self, result: SelectionResult):
        """推送选股结果"""
        title, desp = format_selection_result(result)
        if self._can_send("P1", title):
            self._do_send(title, desp)

    def notify_risk_alert(self, level: str, message: str, details: str = ""):
        """推送风控告警"""
        title, desp = format_risk_alert(level, message, details)
        if self._can_send(level, title):
            self._do_send(title, desp)

    def notify_daily_summary(
        self, date: str, results: List[SelectionResult], errors: List[str]
    ):
        """推送每日摘要"""
        title, desp = format_daily_summary(date, results, errors)
        if self._can_send("P2", title):
            self._do_send(title, desp)

    def notify_custom(self, level: str, title: str, desp: str):
        """推送自定义消息"""
        if self._can_send(level, title):
            self._do_send(title, desp)
