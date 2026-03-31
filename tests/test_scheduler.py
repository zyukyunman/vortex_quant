"""test_scheduler.py — 调度器单元测试"""
from unittest.mock import MagicMock, patch

import pytest

from vortex.core.scheduler import TaskScheduler


class TestTaskScheduler:
    @pytest.fixture
    def scheduler(self):
        ds = MagicMock()
        fh = MagicMock()
        bus = MagicMock()
        bus.flush.return_value = []
        runner = MagicMock()
        runner.run_all.return_value = []
        notifier = MagicMock()
        return TaskScheduler(ds, fh, bus, runner, notifier)

    def test_run_daily_pipeline(self, scheduler):
        result = scheduler.run_daily_pipeline("20250630")
        assert result["date"] == "20250630"
        assert "strategies_run" in result
        assert "errors" in result
        scheduler.runner.run_all.assert_called_once()

    def test_trigger_daily_pipeline(self, scheduler):
        result = scheduler.trigger("daily_pipeline")
        assert "date" in result

    def test_trigger_unknown_task(self, scheduler):
        result = scheduler.trigger("unknown")
        assert "error" in result

    def test_status_no_scheduler(self, scheduler):
        status = scheduler.status()
        assert status["running"] is False
        assert status["jobs"] == []


class TestTaskSchedulerStart:
    def test_start_without_apscheduler(self):
        """APScheduler 未安装时不崩溃"""
        ds = MagicMock()
        fh = MagicMock()
        bus = MagicMock()
        runner = MagicMock()
        scheduler = TaskScheduler(ds, fh, bus, runner)
        # 如果 APScheduler 未安装，start 只打 warning
        with patch.dict("sys.modules", {"apscheduler": None}):
            scheduler.start()
        # 不应抛异常
