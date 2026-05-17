"""Trade domain adapters."""

from vortex.trade.broker import (
    BrokerHealth,
    CashSnapshot,
    FillRecord,
    OrderIntent,
    OrderRecord,
    PaperBrokerAdapter,
    PaperBrokerConfig,
    Position,
    Quote,
)
from vortex.trade.execution import (
    PaperRebalanceArtifacts,
    QmtRebalanceArtifacts,
    run_paper_rebalance,
    run_qmt_rebalance,
)
from vortex.trade.models import (
    ExecutionReport,
    Lineage,
    OrderPlan,
    ReconcileReport,
    RiskCheckResult,
    RiskRuleResult,
    TargetPortfolio,
    TargetPosition,
)
from vortex.trade.order_plan import OrderPlanConfig, generate_order_plan
from vortex.trade.qmt_bridge import QmtBridgeAdapter, QmtBridgeConfig, is_known_connection_status_bug
from vortex.trade.reconcile import reconcile_execution, reconcile_execution_report, write_reconcile_report
from vortex.trade.risk import PreTradeRiskConfig, run_pre_trade_risk_check
from vortex.trade.target_portfolio import TargetPortfolioBuildConfig, build_target_portfolio
from vortex.trade.xueqiu import (
    XueqiuAdapter,
    XueqiuAuthRequiredError,
    XueqiuConfig,
    XueqiuRebalanceArtifacts,
    build_xueqiu_rebalance_payload,
    check_xueqiu_auth,
    classify_xueqiu_exception,
    is_xueqiu_auth_error,
    run_xueqiu_rebalance,
)

__all__ = [
    "BrokerHealth",
    "CashSnapshot",
    "ExecutionReport",
    "FillRecord",
    "Lineage",
    "OrderIntent",
    "OrderPlan",
    "OrderPlanConfig",
    "OrderRecord",
    "PaperBrokerAdapter",
    "PaperBrokerConfig",
    "PaperRebalanceArtifacts",
    "Position",
    "PreTradeRiskConfig",
    "Quote",
    "QmtRebalanceArtifacts",
    "QmtBridgeAdapter",
    "QmtBridgeConfig",
    "is_known_connection_status_bug",
    "ReconcileReport",
    "RiskCheckResult",
    "RiskRuleResult",
    "TargetPortfolio",
    "TargetPortfolioBuildConfig",
    "TargetPosition",
    "build_target_portfolio",
    "generate_order_plan",
    "reconcile_execution",
    "reconcile_execution_report",
    "run_paper_rebalance",
    "run_qmt_rebalance",
    "run_pre_trade_risk_check",
    "write_reconcile_report",
    "XueqiuAdapter",
    "XueqiuAuthRequiredError",
    "XueqiuConfig",
    "XueqiuRebalanceArtifacts",
    "build_xueqiu_rebalance_payload",
    "check_xueqiu_auth",
    "classify_xueqiu_exception",
    "is_xueqiu_auth_error",
    "run_xueqiu_rebalance",
]
