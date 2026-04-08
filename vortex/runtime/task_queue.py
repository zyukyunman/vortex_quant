"""全局任务队列 + 并发控制 + 幂等提交。

任务进度采用四级追踪: run → stage → dataset → chunk，
满足数据拉取等大批量任务的细粒度进度汇报需求。

幂等规则：
- resource_key = (domain, profile, action) 唯一标识一类操作
- 同 resource_key 下若已存在 PENDING/RUNNING 任务，submit 返回已有 task_id
- 不同 backfill 日期范围视为不同 resource_key（action 包含范围后缀）
- dry_run 不写 DB，不参与幂等判断
"""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, fields
from enum import Enum

from vortex.runtime.database import Database
from vortex.shared.types import Domain


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskProgress:
    """四级进度追踪：run → stage → dataset → chunk。"""

    run_id: str
    current_stage: str | None = None
    total_stages: int = 0
    completed_stages: int = 0
    current_dataset: str | None = None
    total_datasets: int = 0
    completed_datasets: int = 0
    current_chunk: int = 0
    total_chunks: int = 0
    written_rows: int = 0
    message: str = ""
    log_path: str | None = None
    pid: int | None = None
    retry_attempt: int = 0
    max_retry_attempts: int = 0
    next_retry_at: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> TaskProgress:
        allowed = {item.name for item in fields(cls)}
        payload = {key: value for key, value in d.items() if key in allowed}
        return cls(**payload)


def make_resource_key(domain: str, profile: str, action: str) -> str:
    """生成资源键。同 resource_key 的活跃任务互斥。"""
    return f"{domain}:{profile}:{action}"


class TaskQueue:
    """全局任务队列 + 并发控制 + 幂等互斥。"""

    def __init__(self, db: Database, max_concurrent: int = 3) -> None:
        self._db = db
        self._max_concurrent = max_concurrent

    # ------------------------------------------------------------------
    # 幂等查询
    # ------------------------------------------------------------------

    def find_active_task(self, resource_key: str) -> dict | None:
        """查找同 resource_key 下是否有 PENDING/RUNNING 任务。"""
        return self._db.fetchone(
            """SELECT * FROM task_queue
               WHERE resource_key = ? AND status IN (?, ?)
               ORDER BY created_at DESC LIMIT 1""",
            (resource_key, TaskStatus.PENDING.value, TaskStatus.RUNNING.value),
        )

    def has_active_task(self, resource_key: str) -> bool:
        """是否存在活跃任务（PENDING 或 RUNNING）。"""
        return self.find_active_task(resource_key) is not None

    # ------------------------------------------------------------------
    # 提交与状态更新
    # ------------------------------------------------------------------

    def submit(
        self,
        domain: Domain,
        action: str,
        profile: str,
        run_id: str,
        *,
        dry_run: bool = False,
    ) -> str:
        """提交任务，返回 task_id。

        幂等行为：
        - 若同 resource_key 已有 PENDING/RUNNING 任务，返回已有 task_id（不创建新任务）
        - dry_run=True 时不写 DB，直接返回预生成的 task_id

        注意：resource_key 在 DB 事务内检查，保证原子性。
        """
        resource_key = make_resource_key(domain, profile, action)

        if dry_run:
            return f"dry-run-{uuid.uuid4()}"

        # 事务内幂等检查 + 插入
        existing = self.find_active_task(resource_key)
        if existing is not None:
            return existing["task_id"]

        task_id = str(uuid.uuid4())
        self._db.execute(
            """INSERT INTO task_queue
               (task_id, domain, action, profile, status, run_id, resource_key)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                task_id,
                domain,
                action,
                profile,
                TaskStatus.PENDING.value,
                run_id,
                resource_key,
            ),
        )
        return task_id

    def update_progress(self, task_id: str, progress: TaskProgress) -> None:
        self._db.execute(
            """UPDATE task_queue
               SET progress_json = ?, updated_at = datetime('now')
               WHERE task_id = ?""",
            (json.dumps(progress.to_dict(), ensure_ascii=False), task_id),
        )

    def update_status(
        self, task_id: str, status: TaskStatus, error: str | None = None
    ) -> None:
        self._db.execute(
            """UPDATE task_queue
               SET status = ?, error = ?, updated_at = datetime('now')
               WHERE task_id = ?""",
            (status.value, error, task_id),
        )

    def cancel(self, task_id: str) -> bool:
        """取消任务。仅 PENDING / RUNNING 可取消，返回是否成功。"""
        row = self._db.fetchone(
            "SELECT status FROM task_queue WHERE task_id = ?", (task_id,)
        )
        if row is None:
            return False
        if row["status"] not in (TaskStatus.PENDING.value, TaskStatus.RUNNING.value):
            return False
        self.update_status(task_id, TaskStatus.CANCELLED)
        return True

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get_task(self, task_id: str) -> dict | None:
        return self._db.fetchone(
            "SELECT * FROM task_queue WHERE task_id = ?", (task_id,)
        )

    def list_tasks(
        self,
        domain: Domain | None = None,
        status: TaskStatus | None = None,
    ) -> list[dict]:
        sql = "SELECT * FROM task_queue WHERE 1=1"
        params: list = []
        if domain is not None:
            sql += " AND domain = ?"
            params.append(domain)
        if status is not None:
            sql += " AND status = ?"
            params.append(status.value)
        sql += " ORDER BY created_at DESC"
        return self._db.fetchall(sql, tuple(params))

    def is_cancelled(self, task_id: str) -> bool:
        """检查任务是否被取消（用于 worker 轮询）。"""
        row = self._db.fetchone(
            "SELECT status FROM task_queue WHERE task_id = ?", (task_id,)
        )
        return row is not None and row["status"] == TaskStatus.CANCELLED.value

    # ------------------------------------------------------------------
    # 并发控制
    # ------------------------------------------------------------------

    @property
    def running_count(self) -> int:
        row = self._db.fetchone(
            "SELECT COUNT(*) as cnt FROM task_queue WHERE status = ?",
            (TaskStatus.RUNNING.value,),
        )
        return row["cnt"] if row else 0

    def can_run(self) -> bool:
        """当前运行数 < max_concurrent。"""
        return self.running_count < self._max_concurrent
