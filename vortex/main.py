"""
QuantPilot FastAPI 应用入口

启动: uvicorn vortex.main:app --reload --port 8000
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from vortex.config.settings import get_settings, setup_logging

logger = logging.getLogger(__name__)


def build_components():
    """构建全部核心组件 — 单例"""
    from vortex.core.datastore import DataStore
    from vortex.core.factorhub import FactorHub
    from vortex.core.scheduler import TaskScheduler
    from vortex.core.signalbus import SignalBus
    from vortex.notify.notifier import Notifier
    from vortex.strategy.dividend import DividendQualityFCFStrategy
    from vortex.strategy.runner import StrategyRunner

    cfg = get_settings()
    ds = DataStore(cfg)
    fh = FactorHub(ds)
    fh.register_all_defaults()
    bus = SignalBus(cfg.data_dir)
    runner = StrategyRunner(ds, fh, bus)

    # 注册策略
    dividend = DividendQualityFCFStrategy(ds, fh, bus)
    runner.register(dividend)

    # 通知器
    notifier = Notifier(serverchan_key=cfg.serverchan_key)

    # 调度器
    scheduler = TaskScheduler(ds, fh, bus, runner, notifier)

    return {
        "settings": cfg,
        "ds": ds,
        "fh": fh,
        "bus": bus,
        "runner": runner,
        "notifier": notifier,
        "scheduler": scheduler,
    }


# 全局状态容器
_components = {}


def get_component(name: str):
    """获取全局组件"""
    if not _components:
        _components.update(build_components())
    return _components[name]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    setup_logging()
    logger.info("QuantPilot 启动中...")

    # 初始化组件
    _components.update(build_components())

    # 启动调度器
    scheduler = _components["scheduler"]
    scheduler.start()
    logger.info("QuantPilot 就绪")

    yield

    # 关闭
    scheduler.stop()
    logger.info("QuantPilot 已关闭")


app = FastAPI(
    title="QuantPilot",
    description="A股量化选股系统 — 红利质量现金流复合策略",
    version="1.0.0",
    lifespan=lifespan,
)


# 注册路由
from vortex.api.routes import router  # noqa: E402
app.include_router(router, prefix="/api/v1")


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}
