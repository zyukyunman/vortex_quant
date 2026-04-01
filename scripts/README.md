# scripts/ 脚本说明

> **维护规则**: 每次新增脚本或修改现有脚本功能时，必须同步更新本文件。

## 日常运行方式

| 方式 | 命令 | 说明 |
|------|------|------|
| **后台服务 (推荐)** | `uvicorn vortex.main:app --port 8000` | 启动后自动执行定时任务：06:30 数据同步、15:35 日终流水线 |
| **手动脚本** | `python scripts/xxx.py` | 按需手动执行，详见下方各脚本说明 |

---

## 脚本清单

### 数据同步

| 脚本 | 用途 |
|------|------|
| `run_sync.py` | Tushare 全量数据同步引擎 CLI，覆盖 89 个接口 |

```bash
python scripts/run_sync.py                        # 全量同步 (跳过已有)
python scripts/run_sync.py --daily                 # 每日增量同步
python scripts/run_sync.py --category macro        # 只同步宏观数据
python scripts/run_sync.py --one moneyflow_hsgt    # 只同步单个接口
python scripts/run_sync.py --force                 # 强制重新下载
python scripts/run_sync.py --list                  # 列出全部任务
python scripts/run_sync.py --points 5000           # 声明积分
python scripts/run_sync.py --start-year 2017       # 指定起始年份
```

### 策略执行

| 脚本 | 用途 |
|------|------|
| `run_strategy.py` | 单次选股执行，输出持仓信号 |

```bash
python scripts/run_strategy.py                     # 最近一个交易日
python scripts/run_strategy.py --date 20260327     # 指定日期
python scripts/run_strategy.py --top 20            # 选 Top 20
python scripts/run_strategy.py --weight-method ic  # IC 加权
```

### 回测

| 脚本 | 用途 |
|------|------|
| `run_backtest_full.py` | 多段 + 全量回测，生成 HTML 报告 (含基准对比) |
| `run_backtest_sa.py` | 敏感性分析回测 (2023-2026 半年调仓) |

```bash
python scripts/run_backtest_full.py                # 全量 2017~now
python scripts/run_backtest_full.py --freq Q       # 季度调仓
python scripts/run_backtest_full.py --start 20190101

python scripts/run_backtest_sa.py                  # 默认 2023-2026 SA
python scripts/run_backtest_sa.py --start 20200101 --end 20260328 --freq Q
```

### 因子研究

| 脚本 | 用途 |
|------|------|
| `run_factor_test.py` | IC 分析 + 权重优化 + 多组权重回测对比 |

```bash
python scripts/run_factor_test.py                  # 默认 36 个月 IC
python scripts/run_factor_test.py --months 60      # 60 个月窗口
```

### 报告生成

| 脚本 | 用途 |
|------|------|
| `gen_charts.py` | 轻量回测图表 (matplotlib, 分阶段隔离内存) |
| `gen_html_report.py` | 从 `_chart_data.json` 生成交互式 HTML 报告 (Chart.js) |
| `gen_qs_report.py` | QuantStats 专业级回测报告 |

```bash
python scripts/gen_charts.py
python scripts/gen_html_report.py
python scripts/gen_qs_report.py
```

---
