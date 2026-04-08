"""Vortex 服务运行时。

当前为进程内编排层（无 HTTP/RPC），管理 workspace、任务队列与事件总线。
支持信号优雅退出、PID 文件管理、崩溃恢复。
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path

from vortex.runtime.database import Database
from vortex.runtime.task_queue import TaskProgress, TaskQueue, TaskStatus
from vortex.runtime.workspace import Workspace
from vortex.shared.events import get_event_bus
from vortex.shared.logging import get_logger
from vortex.shared.types import Domain

logger = get_logger(__name__)

# 优雅退出等待时长（秒）
_DRAIN_TIMEOUT = 60


class Server:
    """Vortex 服务运行时。管理 workspace、任务队列、事件总线。

    生命周期：
    1. __init__: 构造对象，不做 I/O
    2. start(): 初始化 workspace/DB，写 PID 文件，注册信号处理，恢复崩溃任务
    3. run_forever(): 阻塞式主循环（用于 `vortex server start`）
    4. stop(): 设置 draining，等待任务完成，清理 PID 文件
    """

    def __init__(self, workspace_root: Path, max_concurrent: int = 3) -> None:
        self.workspace = Workspace(workspace_root)
        self.db = Database(self.workspace.db_path)
        self.task_queue = TaskQueue(self.db, max_concurrent)
        self.event_bus = get_event_bus()
        self._draining = False
        self._running = False

    # ------------------------------------------------------------------
    # PID 文件管理
    # ------------------------------------------------------------------

    @property
    def pid_path(self) -> Path:
        return self.workspace.state_dir / "vortex.pid"

    def _write_pid(self) -> None:
        """写入当前进程 PID。"""
        self.pid_path.parent.mkdir(parents=True, exist_ok=True)
        self.pid_path.write_text(str(os.getpid()))
        logger.debug("PID 文件已写入: %s (pid=%d)", self.pid_path, os.getpid())

    def _remove_pid(self) -> None:
        """移除 PID 文件。"""
        if self.pid_path.exists():
            self.pid_path.unlink(missing_ok=True)
            logger.debug("PID 文件已移除")

    def _read_pid(self) -> int | None:
        """读取现有 PID 文件，返回 PID 或 None。"""
        if not self.pid_path.exists():
            return None
        try:
            return int(self.pid_path.read_text().strip())
        except (ValueError, OSError):
            return None

    def _is_pid_alive(self, pid: int) -> bool:
        """检查指定 PID 的进程是否还在运行。"""
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False

    # ------------------------------------------------------------------
    # 信号处理
    # ------------------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        """注册 SIGINT/SIGTERM 处理器，触发优雅退出。"""
        def _handler(signum: int, _frame: object) -> None:
            sig_name = signal.Signals(signum).name
            logger.info("收到信号 %s，开始优雅退出…", sig_name)
            self._draining = True
            self._running = False

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    # ------------------------------------------------------------------
    # 崩溃恢复
    # ------------------------------------------------------------------

    @staticmethod
    def _task_worker_pid(task: dict) -> int | None:
        progress_json = task.get("progress_json")
        if not progress_json:
            return None
        try:
            payload = json.loads(str(progress_json))
        except (TypeError, ValueError):
            return None
        progress = TaskProgress.from_dict(payload)
        if progress.pid is None:
            return None
        try:
            return int(progress.pid)
        except (TypeError, ValueError):
            return None

    def _mark_run_interrupted(self, task: dict, error: str) -> None:
        from vortex.data.manifest import SyncManifest

        profile = task.get("profile")
        run_id = task.get("run_id")
        if not profile or not run_id:
            return

        manifest_path = self.workspace.state_dir / "manifests" / str(profile) / "sync_manifest.db"
        if not manifest_path.exists():
            return

        manifest = SyncManifest(manifest_path)
        try:
            run = manifest.get_run(str(run_id))
            if run is None:
                return
            if run.get("status") not in {"pending", "running"}:
                return
            manifest.update_status(str(run_id), "failed", error_message=error)
        finally:
            manifest.close()

    def _recover_stale_tasks(self) -> int:
        """检测上次崩溃遗留的 RUNNING 任务，标记为 interrupted。

        返回恢复的任务数。
        """
        running_tasks = self.task_queue.list_tasks(status=TaskStatus.RUNNING)
        count = 0
        for task in running_tasks:
            task_id = task["task_id"]
            worker_pid = self._task_worker_pid(task)
            if worker_pid is not None and self._is_pid_alive(worker_pid):
                logger.info(
                    "崩溃恢复: 任务 %s 的 worker 仍在运行 (pid=%d)，保留 running 状态",
                    task_id,
                    worker_pid,
                )
                continue
            error = "interrupted: server crashed"
            self.task_queue.update_status(
                task_id, TaskStatus.FAILED, error=error
            )
            self._mark_run_interrupted(task, error)
            logger.warning("崩溃恢复: 任务 %s 已标记为 interrupted", task_id)
            count += 1
        return count

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def start(self) -> None:
        """初始化 workspace 和数据库，写 PID 文件，恢复崩溃任务。"""
        # 检查是否已有实例运行
        existing_pid = self._read_pid()
        if existing_pid is not None and self._is_pid_alive(existing_pid):
            logger.error(
                "另一个 Vortex 实例正在运行 (pid=%d)，请先停止", existing_pid
            )
            raise RuntimeError(
                f"Another Vortex server is already running (pid={existing_pid})"
            )

        self.workspace.initialize()
        self.db.initialize_tables()
        self._write_pid()
        self._install_signal_handlers()

        # 崩溃恢复
        recovered = self._recover_stale_tasks()
        if recovered:
            logger.info("崩溃恢复: 已标记 %d 个残留任务为 interrupted", recovered)

        logger.info("Vortex Server 已启动，工作区: %s", self.workspace.root)

    def run_forever(self) -> None:
        """阻塞式主循环。SIGINT/SIGTERM 触发退出。

        此方法在 `vortex server start` 时调用。
        循环体当前只做心跳检测；实际任务调度由 submit_task 触发。
        """
        self._running = True
        logger.info("进入主循环，按 Ctrl+C 退出…")
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def stop(self) -> None:
        """优雅退出：等待运行中任务完成（最多 %d 秒），然后清理资源。""" % _DRAIN_TIMEOUT
        self._draining = True
        self._running = False

        # 等待运行中的任务完成
        deadline = time.monotonic() + _DRAIN_TIMEOUT
        while self.task_queue.running_count > 0:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning(
                    "等待超时 (%ds)，仍有 %d 个任务运行中，强制退出",
                    _DRAIN_TIMEOUT,
                    self.task_queue.running_count,
                )
                break
            logger.info(
                "等待 %d 个运行中任务完成 (剩余 %.0fs)…",
                self.task_queue.running_count,
                remaining,
            )
            time.sleep(1)

        self._remove_pid()
        self.db.close()
        self.event_bus.clear()
        self._draining = False
        logger.info("Vortex Server 已停止")

    @property
    def is_draining(self) -> bool:
        """服务是否正在优雅退出。新任务提交应被拒绝。"""
        return self._draining

    def status(self) -> dict:
        """返回服务状态信息。"""
        running = self.task_queue.running_count
        pending_rows = self.task_queue.list_tasks(status=TaskStatus.PENDING)
        existing_pid = self._read_pid()
        return {
            "workspace": str(self.workspace.root),
            "db_exists": self.workspace.db_path.exists(),
            "pid": existing_pid,
            "pid_alive": existing_pid is not None and self._is_pid_alive(existing_pid),
            "draining": self._draining,
            "running_tasks": running,
            "pending_tasks": len(pending_rows),
            "can_accept_task": self.task_queue.can_run() and not self._draining,
        }

    def submit_task(
        self, domain: Domain, action: str, profile: str, run_id: str
    ) -> str:
        if self._draining:
            raise RuntimeError("Server 正在关闭，不接受新任务")
        return self.task_queue.submit(domain, action, profile, run_id)

    def get_task_progress(self, task_id: str) -> TaskProgress | None:
        row = self.task_queue.get_task(task_id)
        if row is None:
            return None
        import json

        raw = row.get("progress_json")
        if raw:
            return TaskProgress.from_dict(json.loads(raw))
        return TaskProgress(run_id=row.get("run_id", ""))
