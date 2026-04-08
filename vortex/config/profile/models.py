"""Profile 数据模型。

这一层只定义“配置对象长什么样”，不负责真正的解析流程。
换句话说，它是配置系统里的“数据结构定义层”：

- `store.py` 负责从磁盘读取 YAML
- `defaults.py` 负责提供默认值模板
- `merger.py` 负责把多种来源的配置合并成一个 dict
- `loader.py` 负责把 dict 变成这里定义的 dataclass
- `validator.py` 负责检查这些对象是否合法
- `resolver.py` 负责串起整条链路

对于刚接触 Python 的同学，可以把 dataclass 理解成“更省样板代码的配置类”。
它特别适合承载这种字段多、逻辑少的数据对象。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from vortex.data.provider.tushare_registry import get_default_tushare_datasets


@dataclass
class BaseProfile:
    """所有 Profile 的基类。

    目前只保留了所有域都共享的两个核心字段：

    - `name`：这个 profile 的逻辑名称，也是外部引用它的主键
    - `extends`：如果不为空，表示它要继承另一个 profile 的配置
    """

    # profile 的唯一名字；CLI 和其他模块通常通过这个名字引用配置。
    name: str
    # 继承的父 profile 名称。真正的继承展开逻辑不在这里做，而在 resolver/merger 中处理。
    extends: str | None = None  # 继承的 parent profile name


@dataclass
class DataProfile(BaseProfile):
    """数据域 Profile。

    它回答的是："如果我要建设/更新数据底座，需要哪些参数？"

    datasets 默认值展开为 provider 内置的默认全量 dataset 列表。
    用户通常不需要修改 datasets；如需排除某些数据集，使用 exclude_datasets。
    """

    # 使用哪个数据源适配器。当前默认是 tushare。
    provider: str = "tushare"
    # 需要同步哪些数据集。默认拉取 provider 内置的默认全量数据集。
    datasets: list[str] = field(
        default_factory=get_default_tushare_datasets
    )
    # 排除的数据集列表。高级用户可用此跳过不需要的数据。
    exclude_datasets: list[str] = field(default_factory=list)
    # 优先拉取的数据集（bootstrap 时优先处理这些，其余后台补全）。
    priority_datasets: list[str] = field(default_factory=list)
    # 历史数据起始日。
    history_start: str = "20170101"  # YYYYMMDD
    # 自动调度表达式；None 表示只允许手动触发。
    schedule: str | None = None  # cron expression, 如 "0 18 * * 1-5"
    # 质量规则包名称：决定跑哪些质量检查。
    quality_pack: str = "default"
    # PIT（Point-In-Time，时点对齐）规则包名称。
    pit_pack: str = "default"
    # 发布快照时采用哪套发布策略。
    publish_pack: str = "default"
    # 存储后端/存储布局的规则包名称。
    storage_pack: str = "default"
    # 域级通知配置。v2 统一采用 notification: dict，替代早期 notify 字段。
    notification: dict = field(default_factory=dict)

    @property
    def effective_datasets(self) -> list[str]:
        """实际要同步的数据集 = datasets - exclude_datasets。"""
        excluded = set(self.exclude_datasets)
        return [ds for ds in self.datasets if ds not in excluded]


@dataclass
class ResearchProfile(BaseProfile):
    """研究域 Profile。

    它描述因子评测、研究实验这类任务所需的配置，
    比如要评测哪份 snapshot、做几组分位、并发开多大等。
    """

    # 指定要消费哪一版数据快照；None 表示交给上层在运行时决定。
    snapshot: str | None = None
    # 研究市场标识，例如 cn_stock。
    market: str = "cn_stock"
    # 预测周期列表，例如 1/5/20 日收益。
    label_periods: list[int] = field(default_factory=lambda: [1, 5, 20])
    # 分组收益分析时默认分成多少组。
    n_groups: int = 5
    # 单机上同时评测的最大并发数。
    max_concurrent: int = 3


@dataclass
class StrategyProfile(BaseProfile):
    """策略域 Profile。

    它偏向“回测/组合构建/评分”场景：
    一套策略通常要绑定信号、流水线、回测参数和基准参数。
    """

    # 策略消费的数据快照版本。
    snapshot: str | None = None
    # 要使用的 signal 标识列表。
    signal_ids: list[str] = field(default_factory=list)
    # 策略四阶段流水线（Universe / Alpha / Portfolio / Risk）的配置容器。
    pipeline: dict = field(default_factory=dict)
    # 回测维度、窗口、频率等配置容器。
    backtest: dict = field(default_factory=dict)
    # 基准相关配置，例如指数基准、Alpha 基准等。
    benchmark: dict = field(default_factory=dict)


@dataclass
class TradeProfile(BaseProfile):
    """交易域 Profile。

    它面向实盘/仿真交易，字段关注点从“研究分析”转为“如何下单与风控”。
    """

    # 使用哪个交易网关，例如 paper（模拟）或 live（实盘）。
    gateway: str = "paper"
    # 订单生成策略，例如限价/市价、拆单方式等。
    order_policy: dict = field(default_factory=dict)
    # 风控规则包名称。
    risk_pack: str = "default"
    # 提交失败后的重试策略。
    retry_policy: dict = field(default_factory=dict)
    # 对账策略，例如日终核对、异常补偿等。
    reconcile_policy: dict = field(default_factory=dict)
