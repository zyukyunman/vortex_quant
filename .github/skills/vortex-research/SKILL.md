---
name: vortex-research
description: 研究域技能。用于 Qlib 因子/模型/workflow/recorder 全流程，要求与多市场数据快照解耦并保持可复现。
---

# Vortex Research Skill

## 目标

1. 让因子研究与模型训练可复现、可比较、可上线。
2. 对输入数据源与存储后端解耦，只依赖标准快照协议。

## 必做清单

1. 使用 Qlib DataHandlerLP 分离 infer/learn。
2. 因子评估必须输出 IC、RankIC、ICIR、分组收益、多空收益。
3. 模型训练必须记录 params、metrics、artifacts。
4. 每次实验必须固化 workflow 配置快照。
5. 研究任务只读取 snapshot，不直接依赖原始 Provider。

## 输出产物

1. pred.pkl
2. ic_metrics.csv
3. signal_analysis.csv
4. workflow_config_snapshot.yaml

## 接口约定

- run_alpha_research(config)
- run_model_training(task_config)
- run_workflow(workflow_config)
- generate_signal_snapshot(as_of)
- update_online_prediction(to_date)
