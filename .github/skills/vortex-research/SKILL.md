---
name: vortex-research
description: 研究域技能。用于 Qlib DataHandler/Dataset/Model/Workflow/Recorder 全流程，因子研究与模型迭代。
---

# Vortex Research Skill

## 目标
- 让因子研究、模型训练、评估回测、实验记录全部标准化

## 必做清单
1. 使用 Qlib DataHandlerLP 分离 infer/learn 处理流程
2. 因子研究必须输出 IC、RankIC、ICIR、分组收益、多空收益
3. 模型训练必须记录 params、metrics、artifacts
4. 每次实验必须有可复现实验配置快照
5. 在线增量更新必须通过 OnlineManager/Updater 机制

## 输出产物
- pred.pkl
- ic_metrics.csv
- signal_analysis.csv
- workflow_config_snapshot.yaml

## 接口约定
- run_alpha_research(config)
- run_model_training(task_config)
- run_workflow(workflow_config)
- update_online_prediction(to_date)
