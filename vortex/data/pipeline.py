"""DataPipeline — 数据域主流程编排器（06 §3.4）。

生命周期流程（严格顺序）：
  采集(fetch) → 标准化(normalize) → 质量校验(quality) → PIT 对齐(pit)
    → 衍生指标(derived) → 快照发布(publish)

四级进度模型：run → phase → dataset → chunk
"""
from __future__ import annotations

import inspect
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

import pandas as pd

from vortex.config.profile.models import DataProfile
from vortex.data.calendar import DataCalendar
from vortex.data.derived import DerivedMetricCalculator
from vortex.data.manifest import SyncManifest
from vortex.data.pit.aligner import PitAligner
from vortex.data.pit.report import PitReport
from vortex.data.provider.base import DataProvider
from vortex.data.quality.engine import QualityEngine
from vortex.data.quality.report import QualityContext, QualityReport
from vortex.data.storage.base import StorageBackend
from vortex.shared.calendar import TradingCalendar
from vortex.shared.errors import DataError
from vortex.shared.ids import generate_run_id
from vortex.shared.logging import get_logger

logger = get_logger(__name__)

# DataProfile 即为当前阶段的 ResolvedProfile
# 完整的 profile 合并解析将在 config 域后续迭代中实现
ResolvedProfile = DataProfile


def _map_quality_status(report: QualityReport | None) -> str:
    """将 QualityReport 状态映射为 DDL 契约值（passed | failed | skipped）。"""
    if report is None:
        return "skipped"
    if report.overall_status == "FAILED":
        return "failed"
    # PASSED 和 WARNING 都视为通过（warning 不阻断发布）
    return "passed"


def _ordered_datasets(profile: ResolvedProfile) -> list[str]:
    """返回本次运行的数据集顺序。

    规则：
    1. 先应用 exclude_datasets，得到 effective_datasets
    2. priority_datasets 中存在且未被排除的数据集优先
    3. 其余数据集按 datasets 原有顺序补齐
    """
    effective = profile.effective_datasets
    effective_set = set(effective)
    ordered: list[str] = []
    seen: set[str] = set()

    for name in profile.priority_datasets:
        if name in effective_set and name not in seen:
            ordered.append(name)
            seen.add(name)

    for name in effective:
        if name not in seen:
            ordered.append(name)
            seen.add(name)

    return ordered


@dataclass
class RunReport:
    """单次运行报告。"""

    run_id: str
    action: str
    status: str  # "success" | "partial_success" | "failed"
    total_rows: int = 0
    quality_report: QualityReport | None = None
    pit_report: PitReport | None = None
    snapshot_id: str | None = None
    error: str | None = None
    detail: dict = field(default_factory=dict)


@dataclass
class DatasetSyncOutcome:
    rows_written: int = 0
    pit_report: PitReport | None = None
    quality_candidate: str | None = None


@dataclass(frozen=True)
class DatasetFetchPlan:
    start: date
    end: date
    trading_days: list[date]
    skip_reason: str | None = None
    partition_key: str | None = None
    target_partitions: int = 0
    existing_partitions: int = 0
    covered_partitions: int = 0
    missing_partitions: int = 0
    missing_partition_values: tuple[str, ...] = ()


_DATASET_MAX_ATTEMPTS = 3
_DATASET_RETRY_COOLDOWN_SECONDS = 120.0
_NON_RETRYABLE_DATASET_ERROR_CODES = {
    "DATA_PROVIDER_PERMISSION_DENIED",
    "DATA_PROVIDER_PERMISSION_REQUIRED",
    "DATA_PROVIDER_API_NOT_FOUND",
    "DATA_PROVIDER_UNSUPPORTED_FETCH_MODE",
    "DATA_PROVIDER_UNSUPPORTED_REFERENCE",
}


class DataPipeline:
    """数据域主流程编排器。

    协调 Provider → Storage → Quality → PIT → Derived → Publish 全流程。
    """

    def __init__(
        self,
        provider: DataProvider,
        storage: StorageBackend,
        quality_engine: QualityEngine,
        manifest: SyncManifest,
        calendar: DataCalendar | None = None,
        derived: DerivedMetricCalculator | None = None,
        progress_callback: Callable[..., None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> None:
        self._provider = provider
        self._storage = storage
        self._quality = quality_engine
        self._manifest = manifest
        self._calendar = calendar
        self._derived = derived
        self._progress_callback = progress_callback
        self._cancel_check = cancel_check

    def _raise_if_cancelled(self) -> None:
        if self._cancel_check and self._cancel_check():
            raise DataError(
                code="DATA_TASK_CANCELLED",
                message="数据任务已取消",
            )

    def _emit_progress(self, *, force: bool = False, **kwargs: object) -> None:
        self._raise_if_cancelled()
        if self._progress_callback is not None:
            self._progress_callback(force=force, **kwargs)

    def _make_dataset_progress_callback(
        self,
        *,
        stage: str,
        stage_index: int,
        dataset: str,
        dataset_index: int,
        total_datasets: int,
        written_rows: int,
    ) -> Callable[[int, int, str], None]:
        def _callback(current: int, total: int, label: str) -> None:
            self._emit_progress(
                current_stage=stage,
                total_stages=5,
                completed_stages=stage_index,
                current_dataset=dataset,
                total_datasets=total_datasets,
                completed_datasets=dataset_index - 1,
                current_chunk=current,
                total_chunks=total,
                written_rows=written_rows,
                message=label,
            )

        return _callback

    def _fetch_dataset_with_compat(
        self,
        dataset: str,
        market: str,
        start: date,
        end: date,
        *,
        symbols: list[str] | None,
        trading_days: list[date] | None,
        partition_values: list[str] | None,
        progress_callback: Callable[[int, int, str], None] | None,
    ) -> pd.DataFrame:
        """兼容旧 provider：仅在对方声明时才传递新增进度/取消参数。"""
        kwargs: dict[str, object] = {
            "symbols": symbols,
            "trading_days": trading_days,
        }
        params = inspect.signature(self._provider.fetch_dataset).parameters
        if "partition_values" in params:
            kwargs["partition_values"] = partition_values
        if "progress_callback" in params:
            kwargs["progress_callback"] = progress_callback
        if "cancel_check" in params:
            kwargs["cancel_check"] = self._cancel_check
        return self._provider.fetch_dataset(
            dataset,
            market,
            start,
            end,
            **kwargs,
        )

    def bootstrap(
        self,
        profile: ResolvedProfile,
        dry_run: bool = False,
        *,
        run_id: str | None = None,
        action: str = "bootstrap",
    ) -> RunReport:
        """全量初始化：history_start → 最新交易日。

        流程：
          1. 获取交易日历 + 标的列表
          2. 按 dataset 分组拉取（可并行）
          3. PIT 对齐（基本面数据）
          4. 衍生指标计算
          5. 质量门禁
          6. auto_publish（若配置启用）
        """
        run_id = run_id or generate_run_id("data")
        self._manifest.create_run(run_id, profile.name, action)
        self._manifest.update_status(run_id, "running")

        try:
            self._emit_progress(
                current_stage="prepare",
                total_stages=5,
                completed_stages=0,
                message=f"{action}：准备交易日历与标的列表",
                force=True,
            )
            start = self._parse_date(profile.history_start)
            end = date.today()

            report = self._run_sync(
                run_id=run_id,
                profile=profile,
                start=start,
                end=end,
                action=action,
                dry_run=dry_run,
            )

            status = report.status
            self._manifest.update_status(
                run_id, status,
                total_rows=report.total_rows,
                quality_status=_map_quality_status(report.quality_report),
                snapshot_id=report.snapshot_id,
                as_of_start=start.isoformat(),
                as_of_end=end.isoformat(),
                error_message=report.error,
            )
            return report

        except DataError as exc:
            if exc.code == "DATA_TASK_CANCELLED":
                self._manifest.update_status(
                    run_id, "cancelled", error_message=str(exc),
                )
                logger.warning("%s 已取消: %s", action, exc)
                raise
            self._manifest.update_status(
                run_id, "failed", error_message=str(exc),
            )
            logger.error("bootstrap 失败: %s", exc, exc_info=True)
            return RunReport(
                run_id=run_id, action=action, status="failed",
                error=str(exc),
            )
        except Exception as exc:
            self._manifest.update_status(
                run_id, "failed", error_message=str(exc),
            )
            logger.error("bootstrap 失败: %s", exc, exc_info=True)
            return RunReport(
                run_id=run_id, action=action, status="failed",
                error=str(exc),
            )

    def update(
        self,
        profile: ResolvedProfile,
        dry_run: bool = False,
        *,
        run_id: str | None = None,
        action: str = "update",
    ) -> RunReport:
        """增量更新：上次 as_of + 1 → 最新交易日。

        幂等：重复执行覆盖写，不产生副作用。
        """
        run_id = run_id or generate_run_id("data")
        self._manifest.create_run(run_id, profile.name, action)
        self._manifest.update_status(run_id, "running")

        try:
            self._emit_progress(
                current_stage="prepare",
                total_stages=5,
                completed_stages=0,
                message=f"{action}：计算增量范围",
                force=True,
            )
            # 查找最近一次成功同步的结束日期（含 bootstrap 和 update）
            latest = self._manifest.get_latest_run(profile.name)
            if latest and latest.get("status") == "success" and latest.get("as_of_end"):
                last_end = self._parse_date(latest["as_of_end"])
                start = last_end + timedelta(days=1)
            else:
                # 无成功历史，退化为从 history_start 开始
                start = self._parse_date(profile.history_start)

            end = date.today()

            if start > end:
                logger.info("无需更新：数据已是最新")
                self._manifest.update_status(run_id, "success", total_rows=0)
                self._emit_progress(
                    current_stage="finished",
                    total_stages=5,
                    completed_stages=5,
                    total_datasets=0,
                    completed_datasets=0,
                    current_chunk=0,
                    total_chunks=0,
                    written_rows=0,
                    message="无需更新：数据已是最新",
                    force=True,
                )
                return RunReport(
                    run_id=run_id, action=action, status="success",
                )

            report = self._run_sync(
                run_id=run_id,
                profile=profile,
                start=start,
                end=end,
                action=action,
                dry_run=dry_run,
            )

            status = report.status
            self._manifest.update_status(
                run_id, status,
                total_rows=report.total_rows,
                quality_status=_map_quality_status(report.quality_report),
                snapshot_id=report.snapshot_id,
                as_of_start=start.isoformat(),
                as_of_end=end.isoformat(),
                error_message=report.error,
            )
            return report

        except DataError as exc:
            if exc.code == "DATA_TASK_CANCELLED":
                self._manifest.update_status(
                    run_id, "cancelled", error_message=str(exc),
                )
                logger.warning("%s 已取消: %s", action, exc)
                raise
            self._manifest.update_status(
                run_id, "failed", error_message=str(exc),
            )
            logger.error("update 失败: %s", exc, exc_info=True)
            return RunReport(
                run_id=run_id, action=action, status="failed",
                error=str(exc),
            )
        except Exception as exc:
            self._manifest.update_status(
                run_id, "failed", error_message=str(exc),
            )
            logger.error("update 失败: %s", exc, exc_info=True)
            return RunReport(
                run_id=run_id, action=action, status="failed",
                error=str(exc),
            )

    def repair(
        self,
        profile: ResolvedProfile,
        date_range: tuple[date, date],
        *,
        run_id: str | None = None,
        action: str = "repair",
    ) -> RunReport:
        """修复指定日期范围的数据。"""
        run_id = run_id or generate_run_id("data")
        self._manifest.create_run(run_id, profile.name, action)
        self._manifest.update_status(run_id, "running")

        try:
            self._emit_progress(
                current_stage="prepare",
                total_stages=5,
                completed_stages=0,
                message=f"{action}：准备重跑范围",
                force=True,
            )
            start, end = date_range
            report = self._run_sync(
                run_id=run_id,
                profile=profile,
                start=start,
                end=end,
                action=action,
                dry_run=False,
            )

            status = report.status
            self._manifest.update_status(
                run_id, status,
                total_rows=report.total_rows,
                as_of_start=start.isoformat(),
                as_of_end=end.isoformat(),
                error_message=report.error,
            )
            return report

        except DataError as exc:
            if exc.code == "DATA_TASK_CANCELLED":
                self._manifest.update_status(
                    run_id, "cancelled", error_message=str(exc),
                )
                logger.warning("%s 已取消: %s", action, exc)
                raise
            self._manifest.update_status(
                run_id, "failed", error_message=str(exc),
            )
            logger.error("repair 失败: %s", exc, exc_info=True)
            return RunReport(
                run_id=run_id, action=action, status="failed",
                error=str(exc),
            )
        except Exception as exc:
            self._manifest.update_status(
                run_id, "failed", error_message=str(exc),
            )
            logger.error("repair 失败: %s", exc, exc_info=True)
            return RunReport(
                run_id=run_id, action=action, status="failed",
                error=str(exc),
            )

    def publish(
        self,
        profile: ResolvedProfile,
        as_of: date | None = None,
        *,
        run_id: str | None = None,
        action: str = "publish",
    ) -> str:
        """发布快照。返回 snapshot_id。

        前置条件：最近一次质量检查通过。
        """
        if as_of is None:
            as_of = date.today()

        # 检查前置条件：最近一次 run 的质量状态
        latest_run = self._manifest.get_latest_run(profile.name)
        if latest_run and latest_run.get("quality_status") == "failed":
            raise DataError(
                code="DATA_PUBLISH_QUALITY_FAILED",
                message="最近一次质量检查未通过，阻断发布",
                detail={"run_id": latest_run.get("run_id")},
            )

        # 创建 manifest run 记录（确保 FK 有效）
        run_id = run_id or generate_run_id("data")
        self._manifest.create_run(run_id, profile.name, action)
        self._manifest.update_status(run_id, "running")

        try:
            self._emit_progress(
                current_stage="publish",
                total_stages=5,
                completed_stages=4,
                message=f"publish：准备发布 {as_of:%Y%m%d} 快照",
                force=True,
            )
            self._raise_if_cancelled()
            snapshot_id = self._storage.snapshot(profile.name, as_of)

            self._manifest.create_snapshot(
                snapshot_id=snapshot_id,
                profile=profile.name,
                as_of=as_of.strftime("%Y%m%d"),
                run_id=run_id,
                storage_path=f"data/authoritative/{profile.name}",
            )
            self._manifest.update_status(
                run_id, "success", snapshot_id=snapshot_id,
            )

            logger.info("快照已发布: %s", snapshot_id)
            return snapshot_id
        except DataError as exc:
            if exc.code == "DATA_TASK_CANCELLED":
                self._manifest.update_status(
                    run_id, "cancelled", error_message=str(exc),
                )
                logger.warning("%s 已取消: %s", action, exc)
                raise
            self._manifest.update_status(
                run_id, "failed", error_message=str(exc),
            )
            raise
        except Exception as exc:
            self._manifest.update_status(
                run_id, "failed", error_message=str(exc),
            )
            raise

    # ------------------------------------------------------------------
    # 核心同步逻辑
    # ------------------------------------------------------------------

    def _run_sync(
        self,
        run_id: str,
        profile: ResolvedProfile,
        start: date,
        end: date,
        action: str,
        dry_run: bool,
    ) -> RunReport:
        """执行数据同步的核心流程。"""
        market = "cn_stock"
        total_rows = 0
        quality_report: QualityReport | None = None
        pit_report: PitReport | None = None
        snapshot_id: str | None = None

        # 1. 获取交易日历
        trading_days: list[date] = []
        if self._calendar:
            trading_days = self._calendar.load_or_fetch(market, start, end)
        else:
            trading_days = self._provider.fetch_calendar(market, start, end)
        self._raise_if_cancelled()

        # 2. 获取标的列表
        instruments = self._provider.fetch_instruments(market)
        symbols = instruments["symbol"].tolist() if not instruments.empty else []
        self._raise_if_cancelled()

        if dry_run:
            logger.info(
                "dry_run: 交易日 %d 天, 标的 %d 个, 跳过实际拉取",
                len(trading_days), len(symbols),
            )
            self._emit_progress(
                current_stage="finished",
                total_stages=1,
                completed_stages=1,
                total_datasets=0,
                completed_datasets=0,
                current_chunk=0,
                total_chunks=0,
                written_rows=0,
                message=f"dry_run：交易日 {len(trading_days)} 天，标的 {len(symbols)} 个",
                force=True,
            )
            return RunReport(
                run_id=run_id, action=action, status="success",
                detail={"trading_days": len(trading_days), "symbols": len(symbols)},
            )

        # 3. 按 dataset 拉取数据
        datasets = self._canonicalize_datasets(_ordered_datasets(profile))
        self._emit_progress(
            current_stage="fetch",
            total_stages=5,
            completed_stages=1,
            total_datasets=len(datasets),
            completed_datasets=0,
            current_chunk=0,
            total_chunks=0,
            written_rows=0,
            message=f"开始同步 {len(datasets)} 个 dataset",
            force=True,
        )

        quality_dataset: str | None = None
        skipped_datasets: list[dict[str, str]] = []
        for index, dataset in enumerate(datasets, start=1):
            self._raise_if_cancelled()
            meta = self._dataset_meta(dataset)
            access = self._dataset_access(dataset)

            if not bool(access.get("allowed", True)):
                reason = str(access.get("reason") or "当前账号不可访问")
                logger.warning("跳过 dataset=%s：%s", dataset, reason)
                skipped_datasets.append({"dataset": dataset, "reason": reason})
                self._emit_progress(
                    current_stage="fetch",
                    total_stages=5,
                    completed_stages=1,
                    current_dataset=dataset,
                    total_datasets=len(datasets),
                    completed_datasets=index,
                    current_chunk=0,
                    total_chunks=0,
                    written_rows=total_rows,
                    message=f"跳过 {dataset}：{reason}",
                    force=True,
                )
                continue

            if meta.get("requires_symbols") and not symbols:
                logger.warning("跳过 dataset=%s：当前无可用标的", dataset)
                skipped_datasets.append({"dataset": dataset, "reason": "当前无可用标的"})
                self._emit_progress(
                    current_stage="fetch",
                    total_stages=5,
                    completed_stages=1,
                    current_dataset=dataset,
                    total_datasets=len(datasets),
                    completed_datasets=index,
                    current_chunk=0,
                    total_chunks=0,
                    written_rows=total_rows,
                    message=f"跳过 {dataset}：当前无可用标的",
                    force=True,
                )
                continue

            last_error: str | None = None
            completed = False
            for attempt in range(1, _DATASET_MAX_ATTEMPTS + 1):
                self._raise_if_cancelled()
                try:
                    outcome = self._sync_single_dataset(
                        run_id,
                        dataset,
                        market=market,
                        start=start,
                        end=end,
                        action=action,
                        symbols=symbols,
                        trading_days=trading_days,
                        dataset_index=index,
                        total_datasets=len(datasets),
                        total_rows=total_rows,
                    )
                    total_rows += outcome.rows_written
                    pit_report = outcome.pit_report or pit_report
                    if quality_dataset is None and outcome.quality_candidate is not None:
                        quality_dataset = outcome.quality_candidate
                    completed = True
                    break
                except DataError as exc:
                    if exc.code == "DATA_TASK_CANCELLED":
                        raise
                    last_error = str(exc)
                    retryable = self._is_retryable_dataset_exception(exc)
                except Exception as exc:
                    last_error = str(exc)
                    retryable = True

                if not retryable:
                    logger.warning("dataset=%s 不可重试，直接跳过: %s", dataset, last_error)
                    break
                if attempt >= _DATASET_MAX_ATTEMPTS:
                    logger.error(
                        "dataset=%s 连续失败 %d 次，跳过该 dataset: %s",
                        dataset,
                        _DATASET_MAX_ATTEMPTS,
                        last_error,
                    )
                    break

                logger.warning(
                    "dataset=%s 第 %d/%d 次失败，%.0fs 后重试: %s",
                    dataset,
                    attempt,
                    _DATASET_MAX_ATTEMPTS,
                    _DATASET_RETRY_COOLDOWN_SECONDS,
                    last_error,
                )
                self._emit_progress(
                    current_stage="fetch",
                    total_stages=5,
                    completed_stages=1,
                    current_dataset=dataset,
                    total_datasets=len(datasets),
                    completed_datasets=index - 1,
                    current_chunk=0,
                    total_chunks=0,
                    written_rows=total_rows,
                    message=(
                        f"{dataset} 失败，{int(_DATASET_RETRY_COOLDOWN_SECONDS)}s 后重试 "
                        f"({attempt}/{_DATASET_MAX_ATTEMPTS})"
                    ),
                    force=True,
                )
                time.sleep(_DATASET_RETRY_COOLDOWN_SECONDS)

            if completed:
                continue

            skip_reason = last_error or "dataset 执行失败"
            skipped_datasets.append({"dataset": dataset, "reason": skip_reason})
            self._emit_progress(
                current_stage="fetch",
                total_stages=5,
                completed_stages=1,
                current_dataset=dataset,
                total_datasets=len(datasets),
                completed_datasets=index,
                current_chunk=0,
                total_chunks=0,
                written_rows=total_rows,
                message=f"跳过 {dataset}：{skip_reason}",
                force=True,
            )

        # 4. 衍生指标（Phase 1A stub）
        if self._derived:
            self._emit_progress(
                current_stage="quality",
                total_stages=5,
                completed_stages=3,
                total_datasets=len(datasets),
                completed_datasets=len(datasets),
                current_chunk=0,
                total_chunks=0,
                written_rows=total_rows,
                message="执行衍生指标计算",
                force=True,
            )
            self._derived.compute_all()

        # 5. 质量门禁（当前默认对 bars 做检查；后续可扩展到更多 dataset）
        if quality_dataset is not None:
            self._emit_progress(
                current_stage="quality",
                total_stages=5,
                completed_stages=3,
                current_dataset=quality_dataset,
                total_datasets=len(datasets),
                completed_datasets=len(datasets),
                current_chunk=1,
                total_chunks=1,
                written_rows=total_rows,
                message=f"执行质量检查：{quality_dataset}",
                force=True,
            )
            quality_data = self._storage.read(quality_dataset)
            ctx = QualityContext(
                dataset=quality_dataset,
                profile=profile.name,
                market=market,
                trading_days=trading_days,
            )
            quality_report = self._quality.run(quality_dataset, quality_data, ctx)

        # 6. 自动发布
        self._emit_progress(
            current_stage="publish",
            total_stages=5,
            completed_stages=4,
            total_datasets=len(datasets),
            completed_datasets=len(datasets),
            current_chunk=0,
            total_chunks=0,
            written_rows=total_rows,
            message="准备自动发布快照",
            force=True,
        )
        if quality_report and quality_report.passed and not skipped_datasets:
            try:
                self._raise_if_cancelled()
                snapshot_id = self._storage.snapshot(profile.name, end)
                self._manifest.create_snapshot(
                    snapshot_id=snapshot_id,
                    profile=profile.name,
                    as_of=end.strftime("%Y%m%d"),
                    run_id=run_id,
                    storage_path=f"data/authoritative/{profile.name}",
                    row_count=total_rows,
                    datasets=datasets,
                )
            except Exception as exc:
                logger.warning("自动发布失败: %s", exc)

        status = "success"
        summary_error: str | None = None
        if quality_report and not quality_report.passed:
            status = "failed"
            logger.warning("质量门禁未通过，阻断发布")
            summary_error = "质量门禁未通过，阻断发布"
        elif skipped_datasets:
            status = "partial_success"
            summary_error = f"{len(skipped_datasets)} 个 dataset 被跳过"
            logger.warning("本次运行部分完成：%s", summary_error)

        self._emit_progress(
            current_stage="finished",
            total_stages=5,
            completed_stages=5,
            total_datasets=len(datasets),
            completed_datasets=len(datasets),
            current_chunk=0,
            total_chunks=0,
            written_rows=total_rows,
            message=summary_error or f"{action} {status}",
            force=True,
        )
        return RunReport(
            run_id=run_id,
            action=action,
            status=status,
            total_rows=total_rows,
            quality_report=quality_report,
            pit_report=pit_report,
            snapshot_id=snapshot_id,
            error=summary_error,
            detail={"skipped_datasets": skipped_datasets},
        )

    @staticmethod
    def _parse_date(s: str) -> date:
        """解析 YYYYMMDD 或 YYYY-MM-DD 格式日期。"""
        s = s.strip().replace("-", "")[:8]
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

    def _canonicalize_datasets(self, datasets: list[str]) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for dataset in datasets:
            canonical = self._provider.resolve_dataset(dataset)
            if canonical not in seen:
                ordered.append(canonical)
                seen.add(canonical)
        return ordered

    def _dataset_meta(self, dataset: str) -> dict[str, object]:
        meta = self._provider.dataset_registry.get(dataset)
        if meta is None:
            raise DataError(
                code="DATA_PIPELINE_UNKNOWN_DATASET",
                message=f"Provider 未声明 dataset: {dataset}",
                detail={"dataset": dataset},
            )
        return meta

    def _dataset_access(self, dataset: str) -> dict[str, object]:
        checker = getattr(self._provider, "describe_dataset_access", None)
        if callable(checker):
            result = checker(dataset)
            if isinstance(result, dict):
                return result
        return {"dataset": dataset, "allowed": True}

    @staticmethod
    def _is_retryable_dataset_exception(exc: Exception) -> bool:
        if not isinstance(exc, DataError):
            return True
        return exc.code not in _NON_RETRYABLE_DATASET_ERROR_CODES

    def _sync_single_dataset(
        self,
        run_id: str,
        dataset: str,
        *,
        market: str,
        start: date,
        end: date,
        action: str,
        symbols: list[str],
        trading_days: list[date],
        dataset_index: int,
        total_datasets: int,
        total_rows: int,
    ) -> DatasetSyncOutcome:
        dataset_started_at = time.perf_counter()
        fetch_elapsed = 0.0
        pit_elapsed = 0.0
        write_elapsed = 0.0
        meta = self._dataset_meta(dataset)
        fetch_plan = self._plan_dataset_fetch(
            dataset,
            meta,
            start=start,
            end=end,
            trading_days=trading_days,
            action=action,
        )

        logger.info(
            "开始同步 dataset=%s (%d/%d)",
            dataset,
            dataset_index,
            total_datasets,
        )
        self._log_fetch_plan(dataset, fetch_plan)
        if (
            fetch_plan.skip_reason is None
            and (
                fetch_plan.start != start
                or fetch_plan.end != end
                or len(fetch_plan.trading_days) != len(trading_days)
            )
        ):
            logger.info(
                "dataset=%s 去重决策: 跳过 %d 个已存在分区，沿用 %d 个已登记覆盖分区，仅抓取 %d 个缺失分区；范围 %s~%s -> %s~%s, trading_days %d -> %d",
                dataset,
                fetch_plan.existing_partitions,
                fetch_plan.covered_partitions,
                fetch_plan.missing_partitions,
                start.isoformat(),
                end.isoformat(),
                fetch_plan.start.isoformat(),
                fetch_plan.end.isoformat(),
                len(trading_days),
                len(fetch_plan.trading_days),
            )
        if fetch_plan.skip_reason is not None:
            logger.info("dataset=%s 复用已有数据，跳过抓取: %s", dataset, fetch_plan.skip_reason)
            logger.info(
                "dataset=%s 跳过完成: total_elapsed=%.2fs",
                dataset,
                time.perf_counter() - dataset_started_at,
            )
            self._emit_progress(
                current_stage="fetch",
                total_stages=5,
                completed_stages=1,
                current_dataset=dataset,
                total_datasets=total_datasets,
                completed_datasets=dataset_index,
                current_chunk=0,
                total_chunks=0,
                written_rows=total_rows,
                message=f"{dataset}：{fetch_plan.skip_reason}",
                force=True,
            )
            return DatasetSyncOutcome(
                quality_candidate=dataset if meta.get("quality_check") else None,
            )
        self._emit_progress(
            current_stage="fetch",
            total_stages=5,
            completed_stages=1,
            current_dataset=dataset,
            total_datasets=total_datasets,
            completed_datasets=dataset_index - 1,
            current_chunk=0,
            total_chunks=0,
            written_rows=total_rows,
            message=f"开始抓取 {dataset} ({dataset_index}/{total_datasets})",
            force=True,
        )
        fetch_started_at = time.perf_counter()
        df = self._fetch_dataset_with_compat(
            dataset,
            market,
            fetch_plan.start,
            fetch_plan.end,
            symbols=symbols,
            trading_days=fetch_plan.trading_days,
            partition_values=list(fetch_plan.missing_partition_values) or None,
            progress_callback=self._make_dataset_progress_callback(
                stage="fetch",
                stage_index=1,
                dataset=dataset,
                dataset_index=dataset_index,
                total_datasets=total_datasets,
                written_rows=total_rows,
            ),
        )
        fetch_elapsed = time.perf_counter() - fetch_started_at
        raw_df = df
        if df.empty:
            self._record_dataset_partition_coverage(
                run_id=run_id,
                dataset=dataset,
                meta=meta,
                fetch_plan=fetch_plan,
                as_of_end=end,
                source_df=raw_df,
                materialized_df=raw_df,
                pit_applied=False,
            )
            self._record_dataset_range_coverage(
                run_id=run_id,
                dataset=dataset,
                meta=meta,
                start=start,
                end=end,
                source_df=raw_df,
                materialized_df=raw_df,
            )
            logger.info("dataset=%s 无可写入数据", dataset)
            logger.info(
                "dataset=%s 完成: fetch_elapsed=%.2fs, pit_elapsed=%.2fs, write_elapsed=%.2fs, total_elapsed=%.2fs, rows=0",
                dataset,
                fetch_elapsed,
                pit_elapsed,
                write_elapsed,
                time.perf_counter() - dataset_started_at,
            )
            self._emit_progress(
                current_stage="fetch",
                total_stages=5,
                completed_stages=1,
                current_dataset=dataset,
                total_datasets=total_datasets,
                completed_datasets=dataset_index,
                current_chunk=1,
                total_chunks=1,
                written_rows=total_rows,
                message=f"{dataset} 无可写入数据",
                force=True,
            )
            return DatasetSyncOutcome()

        current_pit: PitReport | None = None
        if meta.get("pit_required") or meta.get("use_pit"):
            pit_started_at = time.perf_counter()
            self._emit_progress(
                current_stage="write",
                total_stages=5,
                completed_stages=2,
                current_dataset=dataset,
                total_datasets=total_datasets,
                completed_datasets=dataset_index - 1,
                current_chunk=0,
                total_chunks=0,
                written_rows=total_rows,
                message=f"{dataset}：执行 PIT 对齐",
                force=True,
            )
            df, current_pit = self._apply_pit_alignment(df, trading_days)
            pit_elapsed = time.perf_counter() - pit_started_at

        self._emit_progress(
            current_stage="write",
            total_stages=5,
            completed_stages=2,
            current_dataset=dataset,
            total_datasets=total_datasets,
            completed_datasets=dataset_index - 1,
            current_chunk=0,
            total_chunks=0,
            written_rows=total_rows,
            message=f"{dataset}：开始落盘",
            force=True,
        )
        write_started_at = time.perf_counter()
        rows = self._write_dataset(
            dataset,
            df,
            meta,
            progress_callback=self._make_dataset_progress_callback(
                stage="write",
                stage_index=2,
                dataset=dataset,
                dataset_index=dataset_index,
                total_datasets=total_datasets,
                written_rows=total_rows,
            ),
        )
        write_elapsed = time.perf_counter() - write_started_at
        self._record_dataset_partition_coverage(
            run_id=run_id,
            dataset=dataset,
            meta=meta,
            fetch_plan=fetch_plan,
            as_of_end=end,
            source_df=raw_df,
            materialized_df=df,
            pit_applied=current_pit is not None,
        )
        self._record_dataset_range_coverage(
            run_id=run_id,
            dataset=dataset,
            meta=meta,
            start=start,
            end=end,
            source_df=raw_df,
            materialized_df=df,
        )
        logger.info(
            "dataset=%s 已写入 %d 行（累计 %d 行）",
            dataset,
            rows,
            total_rows + rows,
        )
        logger.info(
            "dataset=%s 完成: fetch_elapsed=%.2fs, pit_elapsed=%.2fs, write_elapsed=%.2fs, total_elapsed=%.2fs, rows=%d",
            dataset,
            fetch_elapsed,
            pit_elapsed,
            write_elapsed,
            time.perf_counter() - dataset_started_at,
            rows,
        )
        self._emit_progress(
            current_stage="write",
            total_stages=5,
            completed_stages=2,
            current_dataset=dataset,
            total_datasets=total_datasets,
            completed_datasets=dataset_index,
            current_chunk=1,
            total_chunks=1,
            written_rows=total_rows + rows,
            message=f"{dataset} 已写入 {rows} 行",
            force=True,
        )
        quality_candidate = dataset if meta.get("quality_check") else None
        return DatasetSyncOutcome(
            rows_written=rows,
            pit_report=current_pit,
            quality_candidate=quality_candidate,
        )

    def _plan_dataset_fetch(
        self,
        dataset: str,
        meta: dict[str, object],
        *,
        start: date,
        end: date,
        trading_days: list[date],
        action: str,
    ) -> DatasetFetchPlan:
        fetch_mode = str(meta.get("fetch_mode") or "").strip()
        partition_by = str(meta.get("partition_by") or "").strip()

        if (
            action == "bootstrap"
            and fetch_mode == "symbol_once"
            and partition_by == "date"
            and self._has_exact_range_coverage(dataset, start=start, end=end, as_of_end=end)
        ):
            return DatasetFetchPlan(
                start=start,
                end=end,
                trading_days=trading_days,
                skip_reason="目标范围已完成全量扫描",
            )

        if (
            action == "bootstrap"
            and fetch_mode in {"stock_reference", "calendar", "reference_once", "fund_reference", "index_reference"}
            and self._dataset_has_materialized_data(dataset, meta)
        ):
            return DatasetFetchPlan(
                start=start,
                end=end,
                trading_days=trading_days,
                skip_reason="已存在本地缓存，跳过重复抓取",
            )

        if fetch_mode == "trade_day_all" and partition_by == "date" and trading_days:
            existing_dates = self._existing_partition_values(dataset, "date")
            existing_target_days = [
                day for day in trading_days
                if day.strftime("%Y%m%d") in existing_dates
            ]
            missing_days = [
                day for day in trading_days
                if day.strftime("%Y%m%d") not in existing_dates
            ]
            if not missing_days:
                return DatasetFetchPlan(
                    start=start,
                    end=end,
                    trading_days=[],
                    skip_reason="目标日期分区已全部存在",
                    partition_key="date",
                    target_partitions=len(trading_days),
                    existing_partitions=len(existing_target_days),
                    missing_partitions=0,
                )
            return DatasetFetchPlan(
                start=missing_days[0],
                end=missing_days[-1],
                trading_days=missing_days,
                partition_key="date",
                target_partitions=len(trading_days),
                existing_partitions=len(existing_target_days),
                missing_partitions=len(missing_days),
            )

        if fetch_mode == "symbol_range" and partition_by == "date" and trading_days:
            expected_dates = self._expected_date_partition_values(meta, trading_days)
            if expected_dates:
                existing_dates = self._existing_partition_values(dataset, "date")
                existing_target_dates = [
                    value for value in expected_dates if value in existing_dates
                ]
                missing_dates = [
                    value for value in expected_dates if value not in existing_dates
                ]
                if not missing_dates:
                    return DatasetFetchPlan(
                        start=start,
                        end=end,
                        trading_days=[],
                        skip_reason="目标日期分区已全部存在",
                        partition_key="date",
                        target_partitions=len(expected_dates),
                        existing_partitions=len(existing_target_dates),
                        missing_partitions=0,
                    )
                return DatasetFetchPlan(
                    start=self._parse_date(missing_dates[0]),
                    end=self._parse_date(missing_dates[-1]),
                    trading_days=[self._parse_date(value) for value in missing_dates],
                    partition_key="date",
                    target_partitions=len(expected_dates),
                    existing_partitions=len(existing_target_dates),
                    missing_partitions=len(missing_dates),
                    missing_partition_values=tuple(missing_dates),
                )

        if fetch_mode == "symbol_quarter_range" and partition_by == "report_date":
            partition_key = "end_date" if dataset == "fundamental" else "report_date"
            expected_quarters = self._expected_quarter_partition_values(start, end)
            if expected_quarters:
                existing_quarters = self._existing_partition_values(dataset, partition_key)
                covered_quarters = self._covered_partition_values(
                    dataset,
                    partition_key,
                    end,
                )
                existing_target_quarters = [
                    value for value in expected_quarters if value in existing_quarters
                ]
                covered_target_quarters = [
                    value
                    for value in expected_quarters
                    if value not in existing_quarters and value in covered_quarters
                ]
                missing_quarters = [
                    value
                    for value in expected_quarters
                    if value not in existing_quarters and value not in covered_quarters
                ]
                if not missing_quarters:
                    return DatasetFetchPlan(
                        start=start,
                        end=end,
                        trading_days=trading_days,
                        skip_reason=(
                            "目标季度分区已全部存在"
                            if not covered_target_quarters
                            else "目标季度分区已全部存在或已登记覆盖"
                        ),
                        partition_key=partition_key,
                        target_partitions=len(expected_quarters),
                        existing_partitions=len(existing_target_quarters),
                        covered_partitions=len(covered_target_quarters),
                        missing_partitions=0,
                    )
                return DatasetFetchPlan(
                    start=self._parse_date(missing_quarters[0]),
                    end=self._parse_date(missing_quarters[-1]),
                    trading_days=trading_days,
                    partition_key=partition_key,
                    target_partitions=len(expected_quarters),
                    existing_partitions=len(existing_target_quarters),
                    covered_partitions=len(covered_target_quarters),
                    missing_partitions=len(missing_quarters),
                    missing_partition_values=tuple(missing_quarters),
                )

        return DatasetFetchPlan(start=start, end=end, trading_days=trading_days)

    @staticmethod
    def _log_fetch_plan(dataset: str, fetch_plan: DatasetFetchPlan) -> None:
        if fetch_plan.partition_key:
            logger.info(
                "dataset=%s 去重判断: partition_key=%s, target_partitions=%d, existing_partitions=%d, missing_partitions=%d",
                dataset,
                fetch_plan.partition_key,
                fetch_plan.target_partitions,
                fetch_plan.existing_partitions,
                fetch_plan.missing_partitions,
            )
        if fetch_plan.covered_partitions:
            logger.info(
                "dataset=%s 去重覆盖: partition_key=%s, covered_partitions=%d",
                dataset,
                fetch_plan.partition_key,
                fetch_plan.covered_partitions,
            )
        if fetch_plan.skip_reason is not None:
            logger.info("dataset=%s 去重决策: %s", dataset, fetch_plan.skip_reason)

    def _dataset_has_materialized_data(self, dataset: str, meta: dict[str, object]) -> bool:
        partition_by = str(meta.get("partition_by") or "").strip()
        if partition_by:
            return bool(self._storage.list_partitions(dataset))
        existing = self._storage.read(dataset)
        return not existing.empty

    def _existing_partition_values(self, dataset: str, partition_key: str) -> set[str]:
        values: set[str] = set()
        prefix = f"{partition_key}="
        for raw in self._storage.list_partitions(dataset):
            for segment in str(raw).split("/"):
                if segment.startswith(prefix):
                    values.add(segment[len(prefix):])
                    break
        return values

    def _covered_partition_values(
        self,
        dataset: str,
        partition_key: str,
        as_of_end: date,
    ) -> set[str]:
        return self._manifest.list_partition_coverages(
            dataset=dataset,
            partition_key=partition_key,
            as_of_end=as_of_end.isoformat(),
            statuses=("pit_blocked", "source_empty"),
        )

    def _has_exact_range_coverage(
        self,
        dataset: str,
        *,
        start: date,
        end: date,
        as_of_end: date,
    ) -> bool:
        return self._range_coverage_value(start, end) in self._manifest.list_partition_coverages(
            dataset=dataset,
            partition_key="__range__",
            as_of_end=as_of_end.isoformat(),
            statuses=("range_complete",),
        )

    @staticmethod
    def _range_coverage_value(start: date, end: date) -> str:
        return f"{start.strftime('%Y%m%d')}:{end.strftime('%Y%m%d')}"

    @staticmethod
    def _expected_quarter_partition_values(start: date, end: date) -> list[str]:
        values: list[str] = []
        current = date(start.year, ((start.month - 1) // 3) * 3 + 1, 1)
        while current <= end:
            quarter_end_month = ((current.month - 1) // 3 + 1) * 3
            if quarter_end_month == 12:
                quarter_end = date(current.year, 12, 31)
            else:
                quarter_end = date(current.year, quarter_end_month + 1, 1) - timedelta(days=1)
            if start <= quarter_end <= end:
                values.append(quarter_end.strftime("%Y%m%d"))
            current = quarter_end + timedelta(days=1)
        return values

    @staticmethod
    def _expected_date_partition_values(
        meta: dict[str, object],
        trading_days: list[date],
    ) -> list[str]:
        if not trading_days:
            return []

        mode = str(meta.get("date_partition_mode") or "trade_day").strip()
        if mode == "trade_day":
            return [day.strftime("%Y%m%d") for day in trading_days]

        buckets: dict[tuple[int, int], date] = {}
        if mode == "week_end":
            for day in trading_days:
                iso = day.isocalendar()
                buckets[(iso.year, iso.week)] = day
            return [
                buckets[key].strftime("%Y%m%d")
                for key in sorted(buckets)
            ]

        if mode == "month_end":
            for day in trading_days:
                buckets[(day.year, day.month)] = day
            return [
                buckets[key].strftime("%Y%m%d")
                for key in sorted(buckets)
            ]

        return [day.strftime("%Y%m%d") for day in trading_days]

    def _apply_pit_alignment(
        self,
        df: pd.DataFrame,
        trading_days: list[date],
    ) -> tuple[pd.DataFrame, PitReport | None]:
        if not trading_days:
            raise DataError(
                code="DATA_PIT_NO_CALENDAR",
                message="基本面数据需要 PIT 对齐，但交易日历不可用",
            )

        cal = TradingCalendar()
        cal._trading_days = set(trading_days)
        cal._sorted_days = sorted(trading_days)
        aligner = PitAligner(cal)
        aligned, report = aligner.align(df)

        if report and not report.passed:
            logger.warning(
                "PIT 对齐存在阻断记录（%d 条），仅写入已对齐数据",
                report.blocked_count,
            )
        return aligned, report

    def _record_dataset_partition_coverage(
        self,
        *,
        run_id: str,
        dataset: str,
        meta: dict[str, object],
        fetch_plan: DatasetFetchPlan,
        as_of_end: date,
        source_df: pd.DataFrame,
        materialized_df: pd.DataFrame,
        pit_applied: bool,
    ) -> None:
        if not fetch_plan.partition_key or not fetch_plan.missing_partition_values:
            return
        source_column = str(meta.get("partition_by") or "").strip()
        if not source_column:
            return

        source_counts = self._partition_row_counts(source_df, source_column)
        materialized_counts = self._partition_row_counts(materialized_df, source_column)
        for partition_value in fetch_plan.missing_partition_values:
            source_rows = source_counts.get(partition_value, 0)
            materialized_rows = materialized_counts.get(partition_value, 0)
            if materialized_rows > 0:
                status = "materialized"
            elif source_rows > 0 and pit_applied:
                status = "pit_blocked"
            else:
                status = "source_empty"
            self._manifest.record_partition_coverage(
                run_id=run_id,
                dataset=dataset,
                partition_key=fetch_plan.partition_key,
                partition_value=partition_value,
                as_of_end=as_of_end.isoformat(),
                status=status,
                source_rows=source_rows,
                materialized_rows=materialized_rows,
                detail={
                    "pit_applied": pit_applied,
                    "source_column": source_column,
                },
            )

    @staticmethod
    def _partition_row_counts(df: pd.DataFrame, column: str) -> dict[str, int]:
        if df.empty or column not in df.columns:
            return {}
        counts = df[column].astype(str).value_counts(dropna=False).to_dict()
        return {str(key): int(value) for key, value in counts.items()}

    def _record_dataset_range_coverage(
        self,
        *,
        run_id: str,
        dataset: str,
        meta: dict[str, object],
        start: date,
        end: date,
        source_df: pd.DataFrame,
        materialized_df: pd.DataFrame,
    ) -> None:
        fetch_mode = str(meta.get("fetch_mode") or "").strip()
        partition_by = str(meta.get("partition_by") or "").strip()
        if fetch_mode != "symbol_once" or partition_by != "date":
            return
        self._manifest.record_partition_coverage(
            run_id=run_id,
            dataset=dataset,
            partition_key="__range__",
            partition_value=self._range_coverage_value(start, end),
            as_of_end=end.isoformat(),
            status="range_complete",
            source_rows=len(source_df.index),
            materialized_rows=len(materialized_df.index),
            detail={
                "fetch_mode": fetch_mode,
                "partition_by": partition_by,
            },
        )

    def _write_dataset(
        self,
        dataset: str,
        df: pd.DataFrame,
        meta: dict[str, object],
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
    ) -> int:
        partition_by = str(meta.get("partition_by") or "").strip()
        if not partition_by or partition_by not in df.columns:
            if progress_callback is not None:
                progress_callback(1, 1, f"{dataset} write")
            return self._storage.upsert(dataset, df, {})

        partition_key = "end_date" if dataset == "fundamental" and partition_by == "report_date" else partition_by
        total = 0
        total_groups = max(int(df[partition_by].nunique(dropna=False)), 1)
        for index, (partition_value, group) in enumerate(df.groupby(partition_by), start=1):
            total += self._storage.upsert(
                dataset,
                group,
                {partition_key: str(partition_value)},
            )
            if progress_callback is not None:
                progress_callback(
                    index,
                    total_groups,
                    f"{dataset} {partition_key}={partition_value}",
                )
        return total
