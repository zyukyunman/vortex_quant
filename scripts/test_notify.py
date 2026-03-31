"""Server酱推送测试脚本"""
import sys
sys.path.insert(0, ".")

from vortex.notify.serverchan import send_serverchan

KEY = "SCT281388T1heazXaxMKfgBQfCS9MZ1P9g"

title = "QuantPilot 系统就绪"
desp = """# QuantPilot 部署完成

## 已完成模块
- L1 DataStore: Tushare + Parquet + DuckDB
- L2 FactorHub: 16个因子 (价值x6 + 质量x6 + 现金流x4)
- L4 SignalBus: 信号收集、去重、持久化
- L5 StrategyRunner: 并行策略执行
- L6 PortfolioEngine: 组合构建和再平衡
- L7 BacktestEngine: 回测引擎
- L8 RiskManager: 风控管理
- FilterPipeline: 12个可复用筛选器
- WeightOptimizer: Fixed/Equal/IC/ICIR 四种权重方案
- Scheduler: APScheduler 调度器
- Notifier: Server酱推送 (收到此消息说明配置正确)
- FastAPI: REST API 服务
- UnitTests: 79个测试全部通过

## 数据状态
- 日线行情 2023-2026: OK
- 每日估值 2023-2026: OK
- 财务数据: 下载中
- 分红数据: 待下载

## 下一步
数据下载完成后将自动执行策略选股和权重调优
"""

result = send_serverchan(KEY, title, desp)
print(f"发送结果: {result}")
