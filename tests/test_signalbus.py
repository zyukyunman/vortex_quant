"""test_signalbus.py — 信号总线单元测试"""
import pandas as pd
import pytest

from vortex.core.signalbus import SignalBus
from vortex.models import Signal


class TestSignalBus:
    def test_publish_and_flush(self, tmp_data_dir):
        bus = SignalBus(tmp_data_dir)
        sig = Signal(
            date="20250630", strategy="test", ts_code="000651.SZ",
            name="格力电器", action="buy", weight=0.05, score=0.85,
            reason="测试",
        )
        bus.publish(sig)
        flushed = bus.flush("20250630")
        assert len(flushed) == 1
        assert flushed[0].ts_code == "000651.SZ"

    def test_dedup(self, tmp_data_dir):
        """同策略同标的同方向只保留最高分"""
        bus = SignalBus(tmp_data_dir)
        bus.publish(Signal(
            date="20250630", strategy="s1", ts_code="000651.SZ",
            name="格力电器", action="buy", weight=0.03, score=0.60,
            reason="低分",
        ))
        bus.publish(Signal(
            date="20250630", strategy="s1", ts_code="000651.SZ",
            name="格力电器", action="buy", weight=0.05, score=0.90,
            reason="高分",
        ))
        flushed = bus.flush("20250630")
        assert len(flushed) == 1
        assert flushed[0].score == 0.90

    def test_batch_publish(self, tmp_data_dir):
        bus = SignalBus(tmp_data_dir)
        signals = [
            Signal(date="20250630", strategy="s1", ts_code=f"00000{i}.SZ",
                   name=f"stock{i}", action="buy", weight=0.01, score=0.5,
                   reason="batch")
            for i in range(5)
        ]
        bus.publish_batch(signals)
        flushed = bus.flush("20250630")
        assert len(flushed) == 5

    def test_empty_flush(self, tmp_data_dir):
        bus = SignalBus(tmp_data_dir)
        flushed = bus.flush("20250630")
        assert flushed == []

    def test_persistence(self, tmp_data_dir):
        """验证信号保存到 Parquet"""
        bus = SignalBus(tmp_data_dir)
        bus.publish(Signal(
            date="20250630", strategy="s1", ts_code="000651.SZ",
            name="格力电器", action="buy", weight=0.05, score=0.85,
            reason="测试持久化",
        ))
        bus.flush("20250630")

        path = tmp_data_dir / "signal" / "2025.parquet"
        assert path.exists()
        df = pd.read_parquet(path)
        assert len(df) == 1
        assert df.iloc[0]["ts_code"] == "000651.SZ"
