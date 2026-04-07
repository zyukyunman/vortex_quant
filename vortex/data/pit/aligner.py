"""PitAligner — Point-In-Time 对齐器（01 §9.2, 06 §1.3.4）。

核心规则：
  1. 财报数据以 ann_date（公告日期）为准，NOT report_date
  2. effective_from = ann_date 的下一个交易日 09:30
  3. 若 ann_date 在交易时段内（09:30-15:00），当日 15:00 后生效
  4. 快报、预告、正式报各自独立 PIT，互不覆盖
  5. ann_date 为非交易日 → effective_from = 下一个交易日 09:30

失败模式（Fail-Closed）：
  - ann_date 缺失 → 阻断（不静默放行）
  - ann_date < report_date → 阻断（数据源异常）
  - 同 (symbol, report_date) 多条记录 → 保留最新 ann_date，记录 override
"""
from __future__ import annotations

from datetime import date, datetime, time

import pandas as pd

from vortex.data.pit.report import PitRecord, PitReport
from vortex.shared.calendar import TradingCalendar
from vortex.shared.logging import get_logger
from vortex.shared.timezone import MARKET_TZ

logger = get_logger(__name__)

# 交易时段边界
_MARKET_OPEN = time(9, 30)
_MARKET_CLOSE = time(15, 0)


def _parse_date(val: object) -> date | None:
    """将 YYYYMMDD 字符串或 date 对象转为 date，无效返回 None。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, date):
        return val
    s = str(val).strip().replace("-", "")[:8]
    if len(s) != 8 or not s.isdigit():
        return None
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def _compute_effective_from(
    ann_dt: date,
    calendar: TradingCalendar,
) -> datetime:
    """根据 PIT 规则计算 effective_from。

    规则 2/3/5 的统一实现：
      - 非交易日 → 下一个交易日 09:30
      - 交易日 → 下一个交易日 09:30（保守处理，确保不使用当日信息）
    """
    # 保守策略：始终取 ann_date 之后的下一个交易日 09:30
    # 这满足规则 2 和规则 5，且规则 3 的情况也被覆盖（当日 15:00 后生效
    # 等价于次交易日 09:30 可见）
    next_td = calendar.next_trading_day(ann_dt)
    return datetime.combine(next_td, _MARKET_OPEN, tzinfo=MARKET_TZ)


class PitAligner:
    """PIT 对齐器。对基本面数据执行 Point-In-Time 对齐。

    输入：含 ann_date, report_date 的基本面 DataFrame
    输出：附带 effective_from 列的 DataFrame + PitReport
    """

    def __init__(self, calendar: TradingCalendar) -> None:
        self._calendar = calendar

    def align(
        self,
        df: pd.DataFrame,
        ann_date_field: str = "ann_date",
        report_date_field: str = "report_date",
    ) -> tuple[pd.DataFrame, PitReport]:
        """执行 PIT 对齐。

        返回：(对齐后的 DataFrame, PitReport)
        对齐后 DataFrame 包含 effective_from 列，且已去重
        （同 symbol+report_date 仅保留最新 ann_date）。
        """
        if df.empty:
            return df.copy(), PitReport(overall_status="OK")

        records: list[PitRecord] = []
        blocked_details: list[dict] = []
        aligned_rows: list[int] = []

        for idx, row in df.iterrows():
            symbol = str(row.get("symbol", ""))
            ann_raw = row.get(ann_date_field)
            report_raw = row.get(report_date_field)

            ann_dt = _parse_date(ann_raw)
            report_dt = _parse_date(report_raw)

            # 规则：ann_date 缺失 → 阻断
            if ann_dt is None:
                rec = PitRecord(
                    symbol=symbol,
                    report_date=str(report_raw),
                    ann_date="",
                    effective_from="",
                    status="blocked",
                    reason="ann_date 缺失",
                )
                records.append(rec)
                blocked_details.append({
                    "index": idx, "symbol": symbol,
                    "reason": "ann_date 缺失",
                })
                continue

            # 规则：ann_date < report_date → 阻断（数据源异常）
            if report_dt is not None and ann_dt < report_dt:
                rec = PitRecord(
                    symbol=symbol,
                    report_date=str(report_raw),
                    ann_date=str(ann_raw),
                    effective_from="",
                    status="blocked",
                    reason=f"ann_date({ann_dt}) < report_date({report_dt})",
                )
                records.append(rec)
                blocked_details.append({
                    "index": idx, "symbol": symbol,
                    "reason": f"ann_date < report_date",
                })
                continue

            # 计算 effective_from
            try:
                eff = _compute_effective_from(ann_dt, self._calendar)
            except ValueError as exc:
                rec = PitRecord(
                    symbol=symbol,
                    report_date=str(report_raw),
                    ann_date=str(ann_raw),
                    effective_from="",
                    status="blocked",
                    reason=f"无法计算 effective_from: {exc}",
                )
                records.append(rec)
                blocked_details.append({
                    "index": idx, "symbol": symbol,
                    "reason": str(exc),
                })
                continue

            rec = PitRecord(
                symbol=symbol,
                report_date=str(report_raw),
                ann_date=str(ann_raw),
                effective_from=eff.isoformat(),
                status="aligned",
            )
            records.append(rec)
            aligned_rows.append(idx)  # type: ignore[arg-type]

        # 先去重：同 (symbol, report_date) 保留最新 ann_date，
        # 再计算 effective_from，确保存活行的 effective_from 正确
        if aligned_rows:
            result_df = df.loc[aligned_rows].copy()

            overridden_count = 0
            if ann_date_field in result_df.columns:
                before_len = len(result_df)
                result_df = result_df.sort_values(
                    [ann_date_field], ascending=False,
                ).drop_duplicates(
                    subset=["symbol", report_date_field], keep="first",
                ).sort_values(
                    [ann_date_field, "symbol"], ascending=True,
                )
                overridden_count = before_len - len(result_df)

                # 标记被覆盖的记录到 PIT report
                if overridden_count > 0:
                    surviving_keys = set(
                        zip(result_df["symbol"], result_df[report_date_field])
                    )
                    for rec in records:
                        if rec.status == "aligned":
                            key = (rec.symbol, rec.report_date)
                            # 检查此记录是否被更新的 ann_date 覆盖
                            surviving_ann = result_df.loc[
                                (result_df["symbol"] == rec.symbol)
                                & (result_df[report_date_field].astype(str) == rec.report_date),
                                ann_date_field,
                            ]
                            if (
                                not surviving_ann.empty
                                and str(surviving_ann.iloc[0]) != rec.ann_date
                            ):
                                rec.status = "overridden"
                                rec.reason = (
                                    f"被更新 ann_date={surviving_ann.iloc[0]} 覆盖"
                                )
            else:
                overridden_count = 0

            # 为存活行计算正确的 effective_from
            eff_values: list[str] = []
            for _, row in result_df.iterrows():
                ann_dt = _parse_date(row.get(ann_date_field))
                if ann_dt is not None:
                    try:
                        eff = _compute_effective_from(ann_dt, self._calendar)
                        eff_values.append(eff.isoformat())
                    except ValueError:
                        eff_values.append("")
                else:
                    eff_values.append("")
            result_df["effective_from"] = eff_values
        else:
            result_df = df.iloc[:0].copy()
            result_df["effective_from"] = pd.Series(dtype="str")
            overridden_count = 0

        blocked_count = sum(1 for r in records if r.status == "blocked")
        aligned_count = sum(1 for r in records if r.status == "aligned")
        overall = "BLOCKED" if blocked_count > 0 else "OK"

        report = PitReport(
            overall_status=overall,
            total_records=len(df),
            aligned_count=aligned_count,
            blocked_count=blocked_count,
            overridden_count=overridden_count,
            records=records,
            blocked_details=blocked_details,
        )

        if blocked_count:
            logger.warning(
                "PIT 对齐: %d/%d 条记录被阻断",
                blocked_count, len(df),
            )
        else:
            logger.info(
                "PIT 对齐完成: %d 条对齐, %d 条去重覆盖",
                aligned_count, overridden_count,
            )

        return result_df, report
