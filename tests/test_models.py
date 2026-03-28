"""test_models.py — 数据模型单元测试"""
import pytest

from app.models import FactorExposure, SelectionResult, Signal


class TestSignal:
    def test_create(self):
        sig = Signal(
            date="20250630", strategy="test", ts_code="000651.SZ",
            name="格力电器", action="buy", weight=0.05, score=0.85,
            reason="高股息",
        )
        assert sig.ts_code == "000651.SZ"
        assert sig.action == "buy"
        assert sig.confidence == 1.0  # default value

    def test_metadata_default(self):
        sig = Signal(
            date="20250630", strategy="test", ts_code="000651.SZ",
            name="格力电器", action="buy", weight=0.05, score=0.85,
            reason="测试",
        )
        assert sig.metadata == {}


class TestSelectionResult:
    def test_summary(self, sample_signals):
        result = SelectionResult(
            date="20250630",
            strategy="test_strat",
            signals=sample_signals,
            universe_size=5000,
            after_filter_size=150,
            top_n=3,
        )
        text = result.summary()
        assert "test_strat" in text
        assert "20250630" in text
        assert "格力电器" in text
        assert "5000" in text

    def test_empty_result(self):
        result = SelectionResult(
            date="20250630", strategy="test", signals=[],
            universe_size=0, after_filter_size=0, top_n=0,
        )
        text = result.summary()
        assert "test" in text


class TestFactorExposure:
    def test_create(self):
        fe = FactorExposure(
            ts_code="000651.SZ", date="20250630",
            factor_name="dividend_yield", raw_value=0.06, z_score=1.5,
        )
        assert fe.raw_value == 0.06
        assert fe.z_score == 1.5
