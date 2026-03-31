"""QuantPilot 配置管理"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ================================================================
#  策略专属配置 — 每个策略实例化时传入自己的 StrategyConfig
# ================================================================

@dataclass
class StrategyConfig:
    """
    策略级参数, 与全局 Settings 解耦。

    每个策略类可定义自己的默认值, 回测脚本可按需覆盖。
    新策略只需继承/实例化 StrategyConfig 即可, 无需改 Settings。
    """
    # ---- 选股约束 ----
    top_n: int = 30                         # 选股数量
    max_weight_per_stock: float = 0.10      # 单票上限
    max_weight_per_industry: float = 0.30   # 行业上限
    min_listed_days: int = 365              # 最少上市天数

    # ---- 红利策略专属 (其他策略可忽略) ----
    dividend_buy_threshold: float = 0.04    # 买入: 股息率 >= 4%
    dividend_sell_threshold: float = 0.03   # 卖出: 股息率 < 3%
    min_consecutive_dividend_years: int = 3 # 最少连续分红年数
    payout_ratio_range: tuple = (0.10, 1.0) # 分红比例范围


@dataclass
class Settings:
    """全局配置，从环境变量读取"""

    tushare_token: str = field(default_factory=lambda: os.getenv("TUSHARE_TOKEN", ""))
    data_dir: Path = field(
        default_factory=lambda: Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
    )
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    # ---- Server酱推送 ----
    serverchan_key: str = field(default_factory=lambda: os.getenv("SERVERCHAN_KEY", ""))

    # ---- API 认证 ----
    api_key: str = field(default_factory=lambda: os.getenv("API_KEY", ""))

    # ---- 全局风控参数 ----
    max_daily_loss: float = 0.02            # 单日最大亏损 2%
    max_drawdown: float = 0.15              # 最大回撤 15%
    max_position_pct: float = 0.10          # 单票最大仓位 10%
    max_industry_pct: float = 0.30          # 行业最大仓位 30%

    # ---- Tushare API 设置 ----
    tushare_retry: int = 3
    tushare_pause: float = 0.3              # 每次调用间隔秒数(防限频)

    # ---- 策略参数 (兼容旧代码, 新策略请用 StrategyConfig) ----
    dividend_buy_threshold: float = 0.04
    dividend_sell_threshold: float = 0.03
    top_n: int = 30
    max_weight_per_stock: float = 0.10
    max_weight_per_industry: float = 0.30
    min_consecutive_dividend_years: int = 3
    min_listed_days: int = 365
    payout_ratio_range: tuple = (0.10, 1.0)

    def __post_init__(self):
        self.data_dir = Path(self.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def validate(self):
        if not self.tushare_token or self.tushare_token == "your_token_here":
            raise ValueError(
                "请设置 TUSHARE_TOKEN，在 .env 文件中配置或设置环境变量。"
                "\n注册地址: https://tushare.pro/register"
            )


def get_settings() -> Settings:
    """获取全局配置单例"""
    return Settings()


def setup_logging(level: str = "INFO"):
    """配置日志格式"""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(name)-20s | %(levelname)-5s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
