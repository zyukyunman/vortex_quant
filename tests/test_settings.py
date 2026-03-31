"""test_settings.py — 配置管理单元测试"""
import os

import pytest


class TestSettings:
    def test_default_values(self, settings):
        assert settings.top_n == 30
        assert settings.max_weight_per_stock == 0.10
        assert settings.dividend_buy_threshold == 0.04
        assert settings.dividend_sell_threshold == 0.03

    def test_risk_params(self, settings):
        assert settings.max_daily_loss == 0.02
        assert settings.max_drawdown == 0.15
        assert settings.max_position_pct == 0.10
        assert settings.max_industry_pct == 0.30

    def test_validate_empty_token(self):
        from vortex.config.settings import Settings
        s = Settings(tushare_token="")
        with pytest.raises(ValueError, match="TUSHARE_TOKEN"):
            s.validate()

    def test_data_dir_created(self, mock_settings):
        assert mock_settings.data_dir.exists()
