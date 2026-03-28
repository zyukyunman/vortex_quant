"""
signalbus.py
L4 信号层 — 信号收集、去重、冲突标记、持久化

MVP 版本: 收集信号 + 持久化到 Parquet + 打印摘要。
冲突治理等高级功能后续迭代。
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List

import pandas as pd

from app.models import Signal

logger = logging.getLogger(__name__)


class SignalBus:
    """信号总线 — 收集、去重、分发"""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._signals: List[Signal] = []
        (self.data_dir / "signal").mkdir(parents=True, exist_ok=True)

    def publish(self, signal: Signal):
        """策略发布一个信号"""
        self._signals.append(signal)

    def publish_batch(self, signals: List[Signal]):
        """批量发布"""
        self._signals.extend(signals)

    def flush(self, date: str) -> List[Signal]:
        """
        日终汇总: 去重 → 持久化 → 返回有效信号

        去重规则: 同策略、同标的、同方向只保留最新（得分最高的）
        """
        if not self._signals:
            logger.info("SignalBus: 无信号")
            return []

        # 去重: 同 strategy + ts_code + action → 保留 score 最高
        seen = {}
        for sig in self._signals:
            key = (sig.strategy, sig.ts_code, sig.action)
            if key not in seen or sig.score > seen[key].score:
                seen[key] = sig
        deduped = list(seen.values())
        logger.info(
            "SignalBus: 原始 %d → 去重后 %d 信号",
            len(self._signals), len(deduped),
        )

        # 持久化
        self._save(deduped, date)

        # 清空缓冲
        self._signals.clear()
        return deduped

    def _save(self, signals: List[Signal], date: str):
        """保存信号到 Parquet"""
        if not signals:
            return
        records = []
        for s in signals:
            records.append({
                "date": s.date,
                "strategy": s.strategy,
                "ts_code": s.ts_code,
                "name": s.name,
                "action": s.action,
                "weight": s.weight,
                "score": s.score,
                "reason": s.reason,
                "confidence": s.confidence,
            })
        df = pd.DataFrame(records)
        year = date[:4]
        path = self.data_dir / "signal" / f"{year}.parquet"

        # 追加或新建
        if path.exists():
            existing = pd.read_parquet(path)
            # 去掉同日期同策略的旧信号
            existing = existing[
                ~((existing["date"] == date) & (existing["strategy"].isin(df["strategy"])))
            ]
            df = pd.concat([existing, df], ignore_index=True)

        df.to_parquet(path, index=False)
        logger.info("信号已保存: %s (%d 条)", path, len(records))
