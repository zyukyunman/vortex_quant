"""Walk-forward 样本切分工具。

该模块只负责生成滚动训练/验证/测试窗口，避免策略或模型在全样本上调参。
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class WalkForwardConfig:
    """Walk-forward 窗口配置，单位为交易日数量。"""

    train_size: int
    validation_size: int
    test_size: int
    step_size: int
    expanding_train: bool = False


@dataclass(frozen=True)
class WalkForwardSplit:
    """单个 walk-forward 切分。"""

    split_id: int
    train: pd.DatetimeIndex
    validation: pd.DatetimeIndex
    test: pd.DatetimeIndex

    def to_dict(self) -> dict[str, object]:
        return {
            "split_id": self.split_id,
            "train_start": self.train[0].isoformat(),
            "train_end": self.train[-1].isoformat(),
            "validation_start": self.validation[0].isoformat(),
            "validation_end": self.validation[-1].isoformat(),
            "test_start": self.test[0].isoformat(),
            "test_end": self.test[-1].isoformat(),
        }


def generate_walk_forward_splits(
    dates: pd.DatetimeIndex,
    config: WalkForwardConfig,
) -> list[WalkForwardSplit]:
    """生成滚动 walk-forward 切分。"""

    _validate_config(config)
    dates = pd.DatetimeIndex(dates).sort_values().unique()
    if dates.empty:
        raise ValueError("dates 不能为空")

    total_window = config.train_size + config.validation_size + config.test_size
    splits: list[WalkForwardSplit] = []
    start = 0
    split_id = 1
    while start + total_window <= len(dates):
        train_start = 0 if config.expanding_train else start
        train_end = start + config.train_size
        validation_end = train_end + config.validation_size
        test_end = validation_end + config.test_size
        splits.append(
            WalkForwardSplit(
                split_id=split_id,
                train=dates[train_start:train_end],
                validation=dates[train_end:validation_end],
                test=dates[validation_end:test_end],
            )
        )
        split_id += 1
        start += config.step_size

    if not splits:
        raise ValueError("dates 长度不足以生成任何 walk-forward 切分")
    return splits


def _validate_config(config: WalkForwardConfig) -> None:
    if config.train_size <= 0:
        raise ValueError("train_size 必须为正整数")
    if config.validation_size <= 0:
        raise ValueError("validation_size 必须为正整数")
    if config.test_size <= 0:
        raise ValueError("test_size 必须为正整数")
    if config.step_size <= 0:
        raise ValueError("step_size 必须为正整数")

