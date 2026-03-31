"""test_notifier.py — 通知模块单元测试"""
from unittest.mock import MagicMock, patch

import pytest

from vortex.models import SelectionResult, Signal
from vortex.notify.notifier import Notifier
from vortex.notify.serverchan import send_serverchan
from vortex.notify.templates import (
    format_data_update,
    format_daily_summary,
    format_risk_alert,
    format_selection_result,
)


class TestServerchan:
    @patch("vortex.notify.serverchan.requests.post")
    def test_send_success(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"code": 0, "message": "success"}
        mock_post.return_value = mock_resp

        result = send_serverchan("test_key", "测试标题", "测试内容")
        assert result is True

    def test_empty_key(self):
        result = send_serverchan("", "title", "body")
        assert result is False


class TestTemplates:
    def test_selection_result(self, sample_signals):
        result = SelectionResult(
            date="20250630", strategy="test_strat", signals=sample_signals,
            universe_size=5000, after_filter_size=150, top_n=3,
        )
        title, desp = format_selection_result(result)
        assert "test_strat" in title
        assert "格力电器" in desp

    def test_risk_alert(self):
        title, desp = format_risk_alert("P0", "最大回撤超限", "回撤 15.3%")
        assert "P0" in title
        assert "回撤" in desp

    def test_daily_summary(self, sample_signals):
        sr = SelectionResult(
            date="20250630", strategy="test", signals=sample_signals,
            universe_size=100, after_filter_size=30, top_n=3,
        )
        title, desp = format_daily_summary(
            date="20250630",
            results=[sr],
            errors=[],
        )
        assert "20250630" in title
        assert "test" in desp

    def test_data_update(self):
        title, desp = format_data_update("20250630", {"daily": 5000, "valuation": 5000})
        assert "daily" in desp


class TestNotifier:
    def test_rate_limit(self):
        notifier = Notifier(serverchan_key="test")
        # Manually patch send_serverchan to prevent actual calls
        with patch("vortex.notify.notifier.send_serverchan", return_value=True):
            notifier.notify_custom("P2", "test1", "body")
            notifier.notify_custom("P2", "test2", "body")
            notifier.notify_custom("P2", "test3", "body")
            notifier.notify_custom("P2", "test4", "body")
            notifier.notify_custom("P2", "test5", "body")
            # 6th should be rate limited (5 per day)
            notifier.notify_custom("P2", "test6", "body")
            assert notifier._sent_today[notifier._today] == 5

    def test_dedup(self):
        notifier = Notifier(serverchan_key="test")
        with patch("vortex.notify.notifier.send_serverchan", return_value=True):
            notifier.notify_custom("P2", "same_title", "body")
        assert "same_title" in notifier._sent_titles
        can = notifier._can_send("P2", "same_title")
        assert can is False  # already sent

    def test_disabled(self):
        """空 key 时底层 send_serverchan 返回 False"""
        notifier = Notifier(serverchan_key="")
        # notify_custom doesn't return a value, just check no exception
        notifier.notify_custom("P2", "test", "body")
