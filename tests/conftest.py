"""conftest.py — pytest fixtures"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

# 确保项目根目录在 sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 设置环境变量避免真实加载 .env
os.environ.setdefault("TUSHARE_TOKEN", "test_token")
os.environ.setdefault("DATA_DIR", str(PROJECT_ROOT / "data"))
os.environ.setdefault("LOG_LEVEL", "WARNING")
os.environ.setdefault("SERVERCHAN_KEY", "")
os.environ.setdefault("API_KEY", "test_api_key")


@pytest.fixture
def settings():
    from vortex.config.settings import Settings
    return Settings()


@pytest.fixture
def tmp_data_dir(tmp_path):
    """创建临时数据目录结构"""
    for sub in [
        "market/daily", "market/adj_factor",
        "fundamental", "fundamental/valuation",
        "meta", "factor", "signal",
        "portfolio", "risk", "execution",
    ]:
        (tmp_path / sub).mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture
def mock_settings(tmp_data_dir):
    from vortex.config.settings import Settings
    return Settings(
        tushare_token="test_token",
        data_dir=tmp_data_dir,
        log_level="WARNING",
        serverchan_key="test_key",
        api_key="test_api_key",
    )


@pytest.fixture
def sample_stock_basic():
    """示例股票基本信息"""
    return pd.DataFrame({
        "ts_code": ["000651.SZ", "600519.SH", "000858.SZ", "601398.SH", "600036.SH"],
        "name": ["格力电器", "贵州茅台", "五粮液", "工商银行", "招商银行"],
        "industry": ["家用电器", "白酒", "白酒", "银行", "银行"],
        "list_date": ["19961118", "20010827", "19980427", "20061027", "20020409"],
        "market": ["主板", "主板", "主板", "主板", "主板"],
    })


@pytest.fixture
def sample_signals():
    """示例信号列表"""
    from vortex.models import Signal
    return [
        Signal(date="20250630", strategy="test_strat", ts_code="000651.SZ",
               name="格力电器", action="buy", weight=0.05, score=0.85,
               reason="高股息+高ROE"),
        Signal(date="20250630", strategy="test_strat", ts_code="600519.SH",
               name="贵州茅台", action="buy", weight=0.04, score=0.75,
               reason="高现金流"),
        Signal(date="20250630", strategy="test_strat", ts_code="000858.SZ",
               name="五粮液", action="buy", weight=0.03, score=0.65,
               reason="估值低"),
    ]


@pytest.fixture
def sample_factor_data():
    """示例因子数据"""
    index = ["000651.SZ", "600519.SH", "000858.SZ", "601398.SH", "600036.SH"]
    return {
        "dividend_yield": pd.Series([0.06, 0.01, 0.02, 0.05, 0.04], index=index),
        "fcf_yield": pd.Series([0.08, 0.12, 0.10, 0.03, 0.04], index=index),
        "roe_ttm": pd.Series([0.25, 0.35, 0.22, 0.12, 0.15], index=index),
        "delta_roe": pd.Series([0.02, 0.01, -0.01, 0.005, 0.01], index=index),
        "opcfd": pd.Series([1.2, 1.5, 1.1, 0.8, 0.9], index=index),
        "ep": pd.Series([0.10, 0.03, 0.05, 0.08, 0.07], index=index),
    }
