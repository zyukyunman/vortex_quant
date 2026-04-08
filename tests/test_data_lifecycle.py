"""Data 域闭环场景测试。

这些测试不替代 provider / pipeline / cli 的细粒度单元回归，
而是把用户真正关心的流程闭环单独拉出来验证：

1. bootstrap 运行中 cancel 后，已安全落盘的数据不能被重复抓取；
2. 未完成的稀疏事件表必须为数据安全重新抓取，不能误判为完成；
3. 同一范围完整成功后，再次 rerun 应命中 dedup / exact-range coverage；
4. server start / status / cancel 的控制面状态必须闭环一致。
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date

import pandas as pd
import pytest

import vortex.cli as cli
from vortex.cli import _collect_data_status, _submit_data_background_task
from vortex.config.profile.models import DataProfile
from vortex.data.manifest import SyncManifest
from vortex.data.pipeline import DataPipeline
from vortex.data.quality.engine import QualityEngine
from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.runtime.database import Database
from vortex.runtime.server import Server
from vortex.runtime.task_queue import TaskProgress, TaskQueue, TaskStatus
from vortex.runtime.workspace import Workspace
from vortex.shared.errors import DataError


class ScenarioLifecycleProvider:
    """用最小代表集模拟 Data 生命周期。

    代表三类关键数据：
    - bars: `trade_day_all + date`
    - fundamental: `symbol_quarter_range + report_date`
    - events: `symbol_once + date` 稀疏事件表
    """

    def __init__(self, *, cancel_events_on_attempt: int | None = None) -> None:
        self.cancel_events_on_attempt = cancel_events_on_attempt
        self.calls: list[dict[str, object]] = []
        self._events_attempts = 0

    @property
    def name(self) -> str:
        return "scenario"

    @property
    def supported_markets(self) -> list[str]:
        return ["cn_stock"]

    @property
    def dataset_registry(self) -> dict[str, dict[str, object]]:
        return {
            "bars": {
                "api": "daily",
                "description": "A 股日线行情",
                "phase": "1A",
                "fetch_mode": "trade_day_all",
                "partition_by": "date",
            },
            "fundamental": {
                "api": "income",
                "description": "利润表",
                "phase": "1A",
                "fetch_mode": "symbol_quarter_range",
                "partition_by": "report_date",
            },
            "events": {
                "api": "dividend",
                "description": "分红事件",
                "phase": "1A",
                "fetch_mode": "symbol_once",
                "partition_by": "date",
            },
        }

    def resolve_dataset(self, dataset: str) -> str:
        return dataset

    def smoke_test(self) -> bool:
        return True

    def fetch_instruments(self, market: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "symbol": ["000001.SZ"],
                "name": ["平安银行"],
                "list_date": ["19910403"],
                "delist_date": [None],
                "industry": ["银行"],
                "market_cap": [None],
            }
        )

    def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
        return [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 3), date(2026, 4, 8)]

    def fetch_dataset(
        self,
        dataset: str,
        market: str,
        start: date,
        end: date,
        *,
        symbols: list[str] | None = None,
        trading_days: list[date] | None = None,
        partition_values: list[str] | None = None,
        progress_callback=None,
        cancel_check=None,
    ) -> pd.DataFrame:
        self.calls.append(
            {
                "dataset": dataset,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "trading_days": [
                    day.strftime("%Y%m%d") for day in (trading_days or [])
                ],
                "partition_values": list(partition_values or []),
            }
        )

        if dataset == "bars":
            days = trading_days or []
            return pd.DataFrame(
                {
                    "symbol": ["000001.SZ"] * len(days),
                    "date": [day.strftime("%Y%m%d") for day in days],
                    "close": [10.0 + idx for idx, _day in enumerate(days)],
                    "volume": [1000] * len(days),
                }
            )

        if dataset == "fundamental":
            quarter = (partition_values or ["20260331"])[0]
            return pd.DataFrame(
                {
                    "symbol": ["000001.SZ"],
                    "report_date": [quarter],
                    "ann_date": ["20260402"],
                    "revenue": [100.0],
                }
            )

        if dataset == "events":
            self._events_attempts += 1
            if self.cancel_events_on_attempt == self._events_attempts:
                raise DataError(
                    code="DATA_TASK_CANCELLED",
                    message="数据任务已取消",
                )
            return pd.DataFrame(
                {
                    "symbol": ["000001.SZ", "000001.SZ"],
                    "date": ["20260402", "20260408"],
                    "cash_div": [0.5, 0.8],
                }
            )

        raise AssertionError(f"unexpected dataset: {dataset}")

    def call_count(self, dataset: str) -> int:
        return sum(1 for item in self.calls if item["dataset"] == dataset)


def build_lifecycle_pipeline(
    tmp_path,
    *,
    cancel_events_on_attempt: int | None = None,
) -> tuple[ScenarioLifecycleProvider, ParquetDuckDBBackend, SyncManifest, DataPipeline, DataProfile]:
    provider = ScenarioLifecycleProvider(
        cancel_events_on_attempt=cancel_events_on_attempt,
    )
    storage = ParquetDuckDBBackend(tmp_path / "data")
    storage.initialize()
    manifest = SyncManifest(tmp_path / "manifest.db")
    pipeline = DataPipeline(
        provider=provider,
        storage=storage,
        quality_engine=QualityEngine(rules=[]),
        manifest=manifest,
    )
    profile = DataProfile(
        name="default",
        datasets=["bars", "fundamental", "events"],
        history_start="20260101",
    )
    return provider, storage, manifest, pipeline, profile


class TestDataLifecyclePipeline:
    RANGE_START = date(2026, 1, 1)
    RANGE_END = date(2026, 4, 8)

    def test_bootstrap_cancel_then_rerun_skips_completed_datasets_but_refetches_incomplete_events(
        self,
        tmp_path,
        caplog,
    ):
        provider, storage, manifest, pipeline, profile = build_lifecycle_pipeline(
            tmp_path,
            cancel_events_on_attempt=1,
        )
        caplog.set_level(logging.INFO)

        with pytest.raises(DataError) as exc_info:
            pipeline.repair(
                profile,
                (self.RANGE_START, self.RANGE_END),
                action="bootstrap",
                run_id="run_cancelled",
            )

        assert exc_info.value.code == "DATA_TASK_CANCELLED"
        assert len(storage.read("bars")) == 4
        assert len(storage.read("fundamental")) == 1
        assert storage.read("events").empty

        cancelled_run = manifest.get_run("run_cancelled")
        assert cancelled_run is not None
        assert cancelled_run["status"] == "cancelled"

        caplog.clear()
        report = pipeline.repair(
            profile,
            (self.RANGE_START, self.RANGE_END),
            action="bootstrap",
            run_id="run_resume",
        )

        assert report.status == "success"
        assert provider.call_count("bars") == 1
        assert provider.call_count("fundamental") == 1
        assert provider.call_count("events") == 2
        assert "dataset=bars 复用已有数据，跳过抓取" in caplog.text
        assert "dataset=fundamental 复用已有数据，跳过抓取" in caplog.text
        assert len(storage.read("events")) == 2

        exact_range = manifest.list_partition_coverages(
            dataset="events",
            partition_key="__range__",
            as_of_end="2026-04-08",
            statuses=("range_complete",),
        )
        assert exact_range == {"20260101:20260408"}

    def test_bootstrap_rerun_after_complete_range_skips_events_and_materialized_datasets(
        self,
        tmp_path,
        caplog,
    ):
        provider, storage, manifest, pipeline, profile = build_lifecycle_pipeline(tmp_path)
        caplog.set_level(logging.INFO)

        first = pipeline.repair(
            profile,
            (self.RANGE_START, self.RANGE_END),
            action="bootstrap",
            run_id="run_first",
        )
        assert first.status == "success"

        caplog.clear()
        second = pipeline.repair(
            profile,
            (self.RANGE_START, self.RANGE_END),
            action="bootstrap",
            run_id="run_second",
        )

        assert second.status == "success"
        assert provider.call_count("bars") == 1
        assert provider.call_count("fundamental") == 1
        assert provider.call_count("events") == 1
        assert "dataset=bars 复用已有数据，跳过抓取" in caplog.text
        assert "dataset=fundamental 复用已有数据，跳过抓取" in caplog.text
        assert "dataset=events 复用已有数据，跳过抓取: 目标范围已完成全量扫描" in caplog.text

        run_second = manifest.get_run("run_second")
        assert run_second is not None
        assert run_second["status"] == "success"
        assert len(storage.read("bars")) == 4
        assert len(storage.read("fundamental")) == 1
        assert len(storage.read("events")) == 2


class TestDataLifecycleControlPlane:
    def test_server_status_cancel_form_a_closed_loop_for_running_bootstrap(
        self,
        monkeypatch,
        tmp_path,
        capsys,
    ):
        class _Proc:
            pid = 45678

        kill_state = {"alive": True, "signals": []}

        def _fake_kill(pid, sig):
            kill_state["signals"].append((int(pid), int(sig)))
            if int(pid) == _Proc.pid:
                kill_state["alive"] = False

        monkeypatch.setattr(
            cli,
            "_launch_background_process",
            lambda command, log_path: _Proc(),
        )
        monkeypatch.setattr(cli.os, "kill", _fake_kill)
        monkeypatch.setattr(cli.time, "sleep", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(
            cli,
            "_is_pid_alive",
            lambda pid: kill_state["alive"] if int(pid) == _Proc.pid else False,
        )
        monkeypatch.setattr(
            Server,
            "_is_pid_alive",
            lambda self, pid: int(pid) in {_Proc.pid, os.getpid()},
        )

        root = tmp_path / "workspace"
        Workspace(root).initialize()
        result = _submit_data_background_task(
            root=root,
            profile_name="default",
            action="bootstrap",
            fmt="json",
        )
        capsys.readouterr()

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task_queue = TaskQueue(db)
        task_queue.update_status(result["task_id"], TaskStatus.RUNNING)
        task_queue.update_progress(
            result["task_id"],
            TaskProgress(
                run_id=result["run_id"],
                current_stage="fetch",
                total_stages=5,
                completed_stages=1,
                current_dataset="events",
                total_datasets=52,
                completed_datasets=4,
                current_chunk=1200,
                total_chunks=5499,
                written_rows=1024,
                message="events 300750.SZ",
                log_path=result["log_path"],
                pid=_Proc.pid,
            ),
        )
        db.close()

        manifest = SyncManifest(root / "state" / "manifests" / "default" / "sync_manifest.db")
        manifest.create_run(str(result["run_id"]), "default", "bootstrap")
        manifest.update_status(str(result["run_id"]), "running")
        manifest.close()

        server = Server(root)
        server.start()

        status_before = _collect_data_status(root, "default")
        assert len(status_before["active_tasks"]) == 1
        assert status_before["active_tasks"][0]["task_id"] == result["task_id"]
        assert status_before["active_tasks"][0]["current_dataset"] == "events"

        cli._cancel_data_task(root, "default", task_id=None, fmt="json")
        payload = json.loads(capsys.readouterr().out)
        assert payload["task_id"] == result["task_id"]
        assert payload["signal_sent"] is True
        assert kill_state["signals"]

        status_after = _collect_data_status(root, "default")
        assert status_after["active_tasks"] == []
        assert status_after["latest_run"]["status"] == "cancelled"

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task = TaskQueue(db).get_task(result["task_id"])
        db.close()
        assert task is not None
        assert task["status"] == "cancelled"

        manifest = SyncManifest(root / "state" / "manifests" / "default" / "sync_manifest.db")
        run = manifest.get_run(str(result["run_id"]))
        manifest.close()
        assert run is not None
        assert run["status"] == "cancelled"

        server.stop()
