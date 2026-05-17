---
name: factor-quality-gate
description: "因子质量门禁流程。适用场景： 新候选因子、模型生成因子、CogAlpha 候选或代码因子需要进入评测前的失败即阻断检查。"
argument-hint: "提供候选契约、表达式/代码、字段可见时间、样本覆盖、数值指标、版本血统和已有因子相关性"
tags: [vortex, vortex/skill, vortex/research-domain, vortex/ai-quant-os]
obsidian_links:
  - "[[CogAlpha Quality Gate与Fitness规范]]"
  - "[[运行与产物契约]]"
---

# 因子质量门禁

目标是在评测前挡掉不可采信候选。

## 检查项

1. 契约必填字段。
2. 字段白名单和可见时间。
3. 算子/代码安全。
4. PIT 和未来函数。
5. 覆盖率、缺失/无穷值、唯一值比例。
6. 与已有因子重复和高相关。

## 输出

```text
门禁结论：通过 / 拒绝 / 需要补证据
阻断问题：
警告：
指标：
需要跟进：
```

## 规则

1. 有硬伤时不允许“先跑跑看”。
2. LLM 解释不能替代数值检查。
3. 通过质量门禁仍不代表有预测力。
