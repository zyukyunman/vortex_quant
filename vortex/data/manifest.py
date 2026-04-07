"""SyncManifest — 数据同步清单管理（06 §3.6）。

控制面 SQLite 操作：管理 sync_manifest 和 snapshot_descriptors 表。
每个 profile 有独立的 manifest DB（state/manifests/{profile}/sync_manifest.db）。

重要：数据面用 Parquet + DuckDB，控制面用 SQLite，二者严格分离。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from vortex.shared.logging import get_logger

logger = get_logger(__name__)

_SYNC_MANIFEST_DDL = """\
CREATE TABLE IF NOT EXISTS sync_manifest (
    run_id          TEXT PRIMARY KEY,
    profile         TEXT NOT NULL,
    action          TEXT NOT NULL,
    status          TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    as_of_start     TEXT,
    as_of_end       TEXT,
    datasets        TEXT,
    total_rows      INTEGER DEFAULT 0,
    quality_status  TEXT,
    snapshot_id     TEXT,
    vortex_version  TEXT NOT NULL,
    reproducibility_key TEXT,
    error_message   TEXT,
    schema_version  TEXT DEFAULT '1'
);
"""

_SNAPSHOT_DESCRIPTORS_DDL = """\
CREATE TABLE IF NOT EXISTS snapshot_descriptors (
    snapshot_id     TEXT PRIMARY KEY,
    profile         TEXT NOT NULL,
    as_of           TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    status          TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    quality_report  TEXT,
    row_count       INTEGER,
    datasets        TEXT,
    storage_path    TEXT NOT NULL,
    schema_version  TEXT DEFAULT '1',
    FOREIGN KEY (run_id) REFERENCES sync_manifest(run_id)
);
"""

_HEALTH_REPORT_DDL = """\
CREATE TABLE IF NOT EXISTS health_report (
    report_id       TEXT PRIMARY KEY,
    profile         TEXT NOT NULL,
    check_time      TEXT NOT NULL,
    overall_status  TEXT NOT NULL,
    details         TEXT,
    schema_version  TEXT DEFAULT '1'
);
"""


class SyncManifest:
    """数据同步清单管理。控制面 SQLite 数据库操作。

    db_path 指向 state/manifests/{profile}/sync_manifest.db
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._ensure_tables()

    # ------------------------------------------------------------------
    # 连接管理
    # ------------------------------------------------------------------

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self._db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _ensure_tables(self) -> None:
        """创建控制面 DDL 表（幂等）。"""
        with self.conn:
            self.conn.executescript(
                _SYNC_MANIFEST_DDL + _SNAPSHOT_DESCRIPTORS_DDL + _HEALTH_REPORT_DDL
            )

    # ------------------------------------------------------------------
    # sync_manifest CRUD
    # ------------------------------------------------------------------

    def create_run(self, run_id: str, profile: str, action: str) -> None:
        """创建新的同步运行记录。"""
        now = datetime.now().isoformat()
        with self.conn:
            self.conn.execute(
                """INSERT INTO sync_manifest
                   (run_id, profile, action, status, started_at, vortex_version)
                   VALUES (?, ?, ?, 'pending', ?, ?)""",
                (run_id, profile, action, now, "0.1.0"),
            )
        logger.info("创建运行记录: run_id=%s, action=%s", run_id, action)

    def update_status(self, run_id: str, status: str, **kwargs: object) -> None:
        """更新运行记录状态及附加字段。

        支持的 kwargs: finished_at, as_of_start, as_of_end, datasets,
                       total_rows, quality_status, snapshot_id, error_message
        """
        set_parts = ["status = ?"]
        params: list[object] = [status]

        for key, val in kwargs.items():
            if key == "datasets" and isinstance(val, list):
                val = json.dumps(val, ensure_ascii=False)
            set_parts.append(f"{key} = ?")
            params.append(val)

        if status in ("success", "partial_success", "failed", "cancelled"):
            set_parts.append("finished_at = ?")
            params.append(datetime.now().isoformat())

        params.append(run_id)
        sql = f"UPDATE sync_manifest SET {', '.join(set_parts)} WHERE run_id = ?"

        with self.conn:
            self.conn.execute(sql, tuple(params))
        logger.debug("更新运行状态: run_id=%s, status=%s", run_id, status)

    def get_latest_run(
        self, profile: str, action: str | None = None,
    ) -> dict | None:
        """获取最近一次运行记录。"""
        if action:
            sql = (
                "SELECT * FROM sync_manifest "
                "WHERE profile = ? AND action = ? "
                "ORDER BY started_at DESC LIMIT 1"
            )
            cursor = self.conn.execute(sql, (profile, action))
        else:
            sql = (
                "SELECT * FROM sync_manifest "
                "WHERE profile = ? "
                "ORDER BY started_at DESC LIMIT 1"
            )
            cursor = self.conn.execute(sql, (profile,))

        row = cursor.fetchone()
        return dict(row) if row else None

    def get_run(self, run_id: str) -> dict | None:
        """按 run_id 获取运行记录。"""
        cursor = self.conn.execute(
            "SELECT * FROM sync_manifest WHERE run_id = ?", (run_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # snapshot_descriptors
    # ------------------------------------------------------------------

    def create_snapshot(
        self,
        snapshot_id: str,
        profile: str,
        as_of: str,
        run_id: str,
        storage_path: str,
        quality_report: str = "",
        row_count: int = 0,
        datasets: list[str] | None = None,
    ) -> None:
        """创建快照描述符记录。"""
        now = datetime.now().isoformat()
        datasets_json = json.dumps(datasets or [], ensure_ascii=False)

        # 同 profile + as_of 旧记录标记为 superseded
        with self.conn:
            self.conn.execute(
                """UPDATE snapshot_descriptors
                   SET status = 'superseded'
                   WHERE profile = ? AND as_of = ? AND status = 'published'""",
                (profile, as_of),
            )
            self.conn.execute(
                """INSERT INTO snapshot_descriptors
                   (snapshot_id, profile, as_of, created_at, status, run_id,
                    quality_report, row_count, datasets, storage_path)
                   VALUES (?, ?, ?, ?, 'published', ?, ?, ?, ?, ?)""",
                (snapshot_id, profile, as_of, now, run_id,
                 quality_report, row_count, datasets_json, storage_path),
            )
        logger.info("快照描述符已创建: %s", snapshot_id)

    def get_latest_snapshot(self, profile: str) -> dict | None:
        """获取最新的已发布快照。"""
        cursor = self.conn.execute(
            """SELECT * FROM snapshot_descriptors
               WHERE profile = ? AND status = 'published'
               ORDER BY created_at DESC LIMIT 1""",
            (profile,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None
