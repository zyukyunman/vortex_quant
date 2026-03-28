"""
scheduler.py
调度编排 — 时间驱动的任务链

调度表:
  每交易日 06:30 — DataStore.update_daily()
  每交易日 07:00 — FactorHub.compute_all()
  每交易日 15:30 — StrategyRunner.run_all()
  每交易日 15:40 — SignalBus.flush() + Notifier
  每交易日 16:00 — RiskManager.monitor()

实现: APScheduler CronTrigger
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional

from app.core.datastore import DataStore
from app.core.factorhub import FactorHub
from app.core.signalbus import SignalBus
from app.models import SelectionResult
from app.notify.notifier import Notifier
from app.strategy.runner import StrategyRunner
from app.utils.date_utils import today_str

logger = logging.getLogger(__name__)


class TaskScheduler:
    """
    调度编排器

    两种模式:
      1. 手动触发: run_daily_pipeline(date) — 脚本或API手动调用
      2. 定时触发: start() — 启动 APScheduler 后台运行 (需要 FastAPI)
    """

    def __init__(
        self,
        ds: DataStore,
        fh: FactorHub,
        bus: SignalBus,
        runner: StrategyRunner,
        notifier: Optional[Notifier] = None,
    ):
        self.ds = ds
        self.fh = fh
        self.bus = bus
        self.runner = runner
        self.notifier = notifier
        self._scheduler = None  # APScheduler 实例 (延迟创建)

    def run_daily_pipeline(self, date: Optional[str] = None) -> Dict:
        """
        手动触发完整日终流水线

        Returns
        -------
        Dict
            执行摘要 {strategies: N, signals: N, errors: [...]}
        """
        date = date or today_str()
        logger.info("=" * 60)
        logger.info("日终流水线启动 @ %s", date)
        logger.info("=" * 60)

        errors: List[str] = []

        # Step 1: 计算因子
        logger.info("[Pipeline 1/4] 计算因子...")
        try:
            self.fh.compute_all(date)
        except Exception as e:
            errors.append(f"因子计算失败: {e}")
            logger.error("因子计算失败: %s", e)

        # Step 2: 执行全部策略
        logger.info("[Pipeline 2/4] 执行策略...")
        results = self.runner.run_all(date, parallel=False)

        # Step 3: 信号持久化
        logger.info("[Pipeline 3/4] 信号持久化...")
        flushed_signals = self.bus.flush(date)

        # Step 4: 通知推送
        logger.info("[Pipeline 4/4] 推送通知...")
        if self.notifier:
            for r in results:
                if r.signals:
                    self.notifier.notify_selection(r)

            if errors:
                self.notifier.notify_risk_alert(
                    "P1", "日终流水线有错误",
                    "\n".join(f"- {e}" for e in errors),
                )

        summary = {
            "date": date,
            "strategies_run": len(results),
            "total_signals": len(flushed_signals),
            "errors": errors,
        }
        logger.info("日终流水线完成: %s", summary)
        return summary

    def start(self):
        """启动 APScheduler 后台定时任务"""
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger
        except ImportError:
            logger.warning("APScheduler 未安装，定时任务不可用")
            return

        self._scheduler = BackgroundScheduler()

        # 每交易日 15:35 执行日终流水线
        self._scheduler.add_job(
            func=self.run_daily_pipeline,
            trigger=CronTrigger(hour=15, minute=35, day_of_week="mon-fri"),
            id="daily_pipeline",
            name="日终流水线",
            replace_existing=True,
        )

        self._scheduler.start()
        logger.info("调度器已启动 (每交易日 15:35 执行)")

    def stop(self):
        """停止调度器"""
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            logger.info("调度器已停止")

    def trigger(self, task_name: str) -> Dict:
        """手动触发指定任务"""
        if task_name == "daily_pipeline":
            return self.run_daily_pipeline()
        else:
            return {"error": f"未知任务: {task_name}"}

    def status(self) -> Dict:
        """获取调度器状态"""
        if not self._scheduler:
            return {"running": False, "jobs": []}

        jobs = []
        for job in self._scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time) if job.next_run_time else None,
            })
        return {"running": self._scheduler.running, "jobs": jobs}
