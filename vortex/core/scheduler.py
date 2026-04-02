"""
scheduler.py
调度编排 — 时间驱动的任务链

调度表:
  每交易日 15:35 — 日终流水线 (因子→策略→信号→通知)
  每交易日 16:30 — 增量数据同步
  每周六   08:00 — 全量同步检查 (补缺 + 完整性校验)

实现: APScheduler CronTrigger
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional

from vortex.core.data.datastore import DataStore
from vortex.core.factorhub import FactorHub
from vortex.core.signalbus import SignalBus
from vortex.models import SelectionResult
from vortex.notify.notifier import Notifier
from vortex.strategy.runner import StrategyRunner
from vortex.utils.date_utils import today_str

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

        # 每交易日 16:30 增量同步数据
        self._scheduler.add_job(
            func=self._run_daily_sync,
            trigger=CronTrigger(hour=16, minute=30, day_of_week="mon-fri"),
            id="daily_sync",
            name="每日数据同步",
            replace_existing=True,
        )

        # 每交易日 15:35 执行日终流水线
        self._scheduler.add_job(
            func=self.run_daily_pipeline,
            trigger=CronTrigger(hour=15, minute=35, day_of_week="mon-fri"),
            id="daily_pipeline",
            name="日终流水线",
            replace_existing=True,
        )

        # 每周六 08:00 全量同步检查
        self._scheduler.add_job(
            func=self._run_weekly_full_sync,
            trigger=CronTrigger(hour=8, minute=0, day_of_week="sat"),
            id="weekly_full_sync",
            name="每周全量同步检查",
            replace_existing=True,
        )

        self._scheduler.start()
        logger.info("调度器已启动 (16:30 增量同步, 15:35 日终流水线, 周六08:00 全量检查)")

    def stop(self):
        """停止调度器"""
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            logger.info("调度器已停止")

    def trigger(self, task_name: str) -> Dict:
        """手动触发指定任务"""
        if task_name == "daily_pipeline":
            return self.run_daily_pipeline()
        elif task_name == "daily_sync":
            return self._run_daily_sync()
        elif task_name == "weekly_full_sync":
            return self._run_weekly_full_sync()
        else:
            return {"error": f"未知任务: {task_name}"}

    def _run_daily_sync(self) -> Dict:
        """每日增量数据同步"""
        logger.info("[DailySync] 开始每日数据同步...")
        try:
            from vortex.core.data.syncer import DataSyncer
            syncer = DataSyncer(self.ds, start_year=2005, user_points=5000)
            results = syncer.sync_daily()
            summary = {
                "total": len(results),
                "success": sum(1 for r in results if r.status.value == "success"),
                "skipped": sum(1 for r in results if r.status.value == "skipped"),
                "failed": sum(1 for r in results if r.status.value == "failed"),
            }
            logger.info("[DailySync] 完成: %s", summary)

            # 同步失败时推送通知
            failed = [r for r in results if r.status.value == "failed"]
            if failed and self.notifier:
                msg = "\n".join(f"- {r.name}: {r.message[:60]}" for r in failed)
                self.notifier.notify_risk_alert(
                    "P2", "数据同步部分失败", msg,
                )
            return summary
        except Exception as e:
            logger.error("[DailySync] 异常: %s", e)
            return {"error": str(e)}

    def _run_weekly_full_sync(self) -> Dict:
        """每周六全量同步检查 — 补齐缺失数据、校验完整性"""
        logger.info("[WeeklySync] 开始每周全量同步检查...")
        try:
            from vortex.core.data.syncer import DataSyncer
            syncer = DataSyncer(self.ds, start_year=2005, user_points=5000)
            results = syncer.sync_all()
            summary = {
                "total": len(results),
                "success": sum(1 for r in results if r.status.value == "success"),
                "skipped": sum(1 for r in results if r.status.value == "skipped"),
                "failed": sum(1 for r in results if r.status.value == "failed"),
            }
            logger.info("[WeeklySync] 完成: %s", summary)

            failed = [r for r in results if r.status.value == "failed"]
            if failed and self.notifier:
                msg = "\n".join(f"- {r.name}: {r.message[:60]}" for r in failed)
                self.notifier.notify_risk_alert(
                    "P2", "每周全量同步部分失败", msg,
                )
            return summary
        except Exception as e:
            logger.error("[WeeklySync] 异常: %s", e)
            return {"error": str(e)}

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
