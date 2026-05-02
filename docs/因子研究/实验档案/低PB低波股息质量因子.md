---
tags: [vortex, vortex/research-domain, vortex/factor-archive]
aliases: [低PB低波股息质量因子, low_pb_lowvol_dividend_quality_factor]
created: 2026-05-01
updated: 2026-05-01
status: research_lead
factor_family: value_quality
data_sources: [valuation, bars]
artifact_root: /Users/zyukyunman/Documents/vortex_workspace/research/factor-reports/daily-cross-section-baseline
---

# 低PB低波股息质量因子

关联：[[价值质量因子]]、[[因子研究与评测全流程说明]]

## 一句话结论

低 PB、低波和股息率组合更稳健，但收益不足，适合作为组合防守腿或风险过滤线索。

## 研究假设

估值便宜、波动较低、股息率较高的公司，在弱市中更抗跌，能改善组合风险收益。

## 字段映射

| 数据集 | 字段 | 用途 |
|---|---|---|
| `valuation` | `pb`, `dv_ttm` | 估值和股息 |
| `bars` | `close` | 波动率和收益 |
| `bars` | `amount` | 流动性过滤 |

## 关键指标

| 指标 | 数值 |
|---|---:|
| 20d IC mean | 0.084282 |
| 20d ICIR | 0.363134 |
| 20d 正 IC 占比 | 65.15% |
| Top30 年化 | 15.26% |
| Top30 最大回撤 | -14.76% |
| 市场门控 Top30 年化 | 10.54% |
| 市场门控 Top30 最大回撤 | -11.48% |
| 与 QGV 50/50 叠加年化（2023-2026 overlay） | 31.89% |
| 与 QGV 50/50 叠加最大回撤（2023-2026 overlay） | -13.53% |
| 与 QGV 50/50 弱市 p05（2023-2026 overlay） | -2.47% |

## 阶段判断

`research_lead`。

## 失败或保留原因

保留：比小市值低波反转更稳，回撤明显低。
失败：收益不足，距离 30% 年化 / 5% 回撤目标很远。

## 可复用经验

价值质量类因子可能更适合做组合防守腿、风险预算或过滤器，不应单独承担高 alpha 目标。 2026-05-01 overlay 显示，它与 `quality_growth_value` 50/50 叠加在 2023-2026 重叠样本把 QGV 回撤从约 -23.47% 降到 -13.53%，值得做持仓级复验。

## 产物路径

```text
/Users/zyukyunman/Documents/vortex_workspace/research/factor-reports/daily-cross-section-baseline/
```
