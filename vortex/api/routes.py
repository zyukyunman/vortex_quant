"""
API 路由汇总

全部路由挂在 /api/v1 下:
  GET  /strategy/list         — 策略列表
  POST /strategy/run           — 执行策略
  GET  /factor/list            — 因子列表
  POST /factor/compute         — 计算因子
  POST /signal/flush           — 信号持久化
  GET  /signal/history         — 信号历史
  POST /scheduler/trigger      — 触发调度任务
  GET  /scheduler/status       — 调度器状态
  POST /notify/test            — 测试推送通知
  GET  /stock/profile          — 个股画像
  POST /backtest/run           — 执行回测
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()

# ---- 认证 ----
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: Optional[str] = Depends(api_key_header)):
    from vortex.main import get_component
    expected = get_component("settings").api_key
    if expected and api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


# ---- Request Models ----
class RunStrategyRequest(BaseModel):
    date: str
    strategy: Optional[str] = None
    weight_method: str = "fixed"


class ComputeFactorRequest(BaseModel):
    date: str
    factor_name: Optional[str] = None


class BacktestRequest(BaseModel):
    start_date: str
    end_date: str
    freq: str = "M"


# ---- 策略路由 ----
@router.get("/strategy/list")
async def strategy_list(key=Depends(verify_api_key)):
    from vortex.main import get_component
    runner = get_component("runner")
    return {"strategies": runner.list_strategies()}


@router.post("/strategy/run")
async def strategy_run(req: RunStrategyRequest, key=Depends(verify_api_key)):
    from vortex.main import get_component
    runner = get_component("runner")

    if req.strategy:
        result = runner.run_one(req.strategy, req.date)
        if result is None:
            raise HTTPException(404, f"策略 '{req.strategy}' 不存在或执行失败")
        return {
            "date": result.date,
            "strategy": result.strategy,
            "top_n": result.top_n,
            "signals": [
                {"ts_code": s.ts_code, "name": s.name,
                 "weight": round(s.weight, 4), "score": round(s.score, 4)}
                for s in result.signals
            ],
        }
    else:
        results = runner.run_all(req.date)
        return {
            "date": req.date,
            "strategies_run": len(results),
            "total_signals": sum(len(r.signals) for r in results),
        }


# ---- 因子路由 ----
@router.get("/factor/list")
async def factor_list(key=Depends(verify_api_key)):
    from vortex.main import get_component
    fh = get_component("fh")
    return {"factors": fh.list_factors()}


@router.post("/factor/compute")
async def factor_compute(req: ComputeFactorRequest, key=Depends(verify_api_key)):
    from vortex.main import get_component
    fh = get_component("fh")

    if req.factor_name:
        result = fh.compute(req.factor_name, req.date)
        top = result.sort_values(ascending=False).head(10)
        return {
            "factor": req.factor_name,
            "date": req.date,
            "n_stocks": len(result.dropna()),
            "top_10": top.to_dict(),
        }
    else:
        results = fh.compute_all(req.date)
        return {
            "date": req.date,
            "computed": len(results),
            "factors": {k: len(v.dropna()) for k, v in results.items()},
        }


# ---- 信号路由 ----
@router.post("/signal/flush")
async def signal_flush(date: str = Query(...), key=Depends(verify_api_key)):
    from vortex.main import get_component
    bus = get_component("bus")
    signals = bus.flush(date)
    return {"date": date, "flushed": len(signals)}


@router.get("/signal/history")
async def signal_history(
    year: str = Query("2025"),
    key=Depends(verify_api_key),
):
    import pandas as pd
    from vortex.main import get_component
    cfg = get_component("settings")
    path = cfg.data_dir / "signal" / f"{year}.parquet"
    if not path.exists():
        return {"year": year, "signals": []}
    df = pd.read_parquet(path)
    return {"year": year, "count": len(df), "signals": df.tail(50).to_dict("records")}


# ---- 调度路由 ----
@router.post("/scheduler/trigger")
async def scheduler_trigger(
    task: str = Query("daily_pipeline"),
    key=Depends(verify_api_key),
):
    from vortex.main import get_component
    scheduler = get_component("scheduler")
    result = scheduler.trigger(task)
    return result


@router.get("/scheduler/status")
async def scheduler_status(key=Depends(verify_api_key)):
    from vortex.main import get_component
    scheduler = get_component("scheduler")
    return scheduler.status()


# ---- 通知路由 ----
@router.post("/notify/test")
async def notify_test(
    title: str = Query("QuantPilot 测试"),
    key=Depends(verify_api_key),
):
    from vortex.main import get_component
    notifier = get_component("notifier")
    success = notifier.notify_custom("P2", title, "这是一条测试消息。如果你看到了，说明通知配置正确。")
    return {"sent": success}


# ---- 个股画像 ----
@router.get("/stock/profile")
async def stock_profile(
    ts_code: str = Query(..., description="股票代码 e.g. 000651.SZ"),
    key=Depends(verify_api_key),
):
    from vortex.analysis.analyzer import StockAnalyzer
    from vortex.main import get_component
    ds = get_component("ds")
    analyzer = StockAnalyzer(ds)
    return analyzer.profile(ts_code)


# ---- 回测路由 ----
@router.post("/backtest/run")
async def backtest_run(req: BacktestRequest, key=Depends(verify_api_key)):
    from vortex.executor.backtest import BacktestEngine
    from vortex.main import get_component

    ds = get_component("ds")
    fh = get_component("fh")
    bus = get_component("bus")

    from vortex.strategy.dividend import DividendQualityFCFStrategy
    strategy = DividendQualityFCFStrategy(ds, fh, bus)
    engine = BacktestEngine(ds)

    try:
        result = engine.run(strategy, req.start_date, req.end_date, req.freq)
        return {
            "metrics": result.metrics,
            "n_rebalance": len(result.rebalance_dates),
            "final_nav": float(result.nav_series.iloc[-1]) if not result.nav_series.empty else 1.0,
        }
    except Exception as e:
        raise HTTPException(500, str(e))
