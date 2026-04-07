"""控制面 SQLite 数据库管理。

提供统一的 SQL 执行接口，结果以 dict 形式返回，便于上层直接使用。
内置 schema 版本管理：每次启动自动检测并执行增量迁移，保证前后版本兼容。
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

# 当前 schema 版本，每次变更 DDL 时递增
SCHEMA_VERSION = 2

# ------------------------------------------------------------------
# 迁移注册表：key = 目标版本号，value = SQL 列表
# 从 version (key-1) 迁移到 version (key)
# ------------------------------------------------------------------
_MIGRATIONS: dict[int, list[str]] = {
    # v0 → v1: 初始建表
    1: [
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            domain      TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            level       TEXT NOT NULL DEFAULT 'info',
            payload_json TEXT,
            run_id      TEXT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS task_queue (
            task_id     TEXT PRIMARY KEY,
            domain      TEXT NOT NULL,
            action      TEXT NOT NULL,
            profile     TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            progress_json TEXT,
            error       TEXT,
            run_id      TEXT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_task_queue_status ON task_queue(status)",
        "CREATE INDEX IF NOT EXISTS idx_task_queue_domain ON task_queue(domain)",
        "CREATE INDEX IF NOT EXISTS idx_audit_log_domain  ON audit_log(domain)",
    ],
    # v1 → v2: task_queue 增加 resource_key（任务幂等）
    2: [
        # ALTER TABLE ADD COLUMN 对已有列不报错需先检测，此处用安全写法
        "_migrate_add_column:task_queue:resource_key:TEXT",
        "CREATE INDEX IF NOT EXISTS idx_task_queue_resource_key ON task_queue(resource_key, status)",
    ],
}


class Database:
    """控制面 SQLite 数据库，内置 schema 版本管理。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

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

    # ------------------------------------------------------------------
    # Schema 版本管理
    # ------------------------------------------------------------------

    def _get_schema_version(self) -> int:
        """读取当前 schema 版本号，首次使用返回 0。"""
        # schema_version 表可能不存在（v0 旧库或全新库）
        try:
            row = self.conn.execute(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
            ).fetchone()
            return int(row["version"]) if row else 0
        except sqlite3.OperationalError:
            return 0

    def _set_schema_version(self, version: int) -> None:
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version "
            "(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT (datetime('now')))"
        )
        self.conn.execute(
            "INSERT OR REPLACE INTO schema_version (version) VALUES (?)",
            (version,),
        )

    def _detect_legacy_version(self) -> int:
        """检测没有 schema_version 表的旧库实际处于哪个版本。

        通过已有表结构推断，避免对旧库重复执行已完成的迁移。
        """
        try:
            tables = {
                row[0]
                for row in self.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        except sqlite3.OperationalError:
            return 0

        if "task_queue" not in tables:
            return 0  # 全新库

        # task_queue 存在，检查是否已有 resource_key 列（v2 特征）
        cols = {
            row[1]
            for row in self.conn.execute("PRAGMA table_info(task_queue)").fetchall()
        }
        if "resource_key" in cols:
            return 2
        return 1  # 有 task_queue 但没有 resource_key

    def _execute_migration_step(self, sql: str) -> None:
        """执行单条迁移指令，支持特殊前缀命令。"""
        if sql.startswith("_migrate_add_column:"):
            # 格式: _migrate_add_column:表名:列名:类型
            _, table, column, col_type = sql.split(":")
            cols = {
                row[1]
                for row in self.conn.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall()
            }
            if column not in cols:
                self.conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"
                )
        else:
            self.conn.execute(sql)

    def initialize_tables(self) -> None:
        """创建或迁移所有控制面基表到最新 schema 版本。

        逻辑：
        1. 读取当前版本（schema_version 表或结构推断）
        2. 依序执行从当前版本到 SCHEMA_VERSION 的全部迁移
        3. 记录新版本号
        """
        current = self._get_schema_version()

        # 旧库没有 schema_version 表时，通过结构推断
        if current == 0:
            current = self._detect_legacy_version()

        if current >= SCHEMA_VERSION:
            return  # 已是最新

        logger.info(
            "控制面数据库迁移: v%d → v%d (%s)",
            current, SCHEMA_VERSION, self._db_path.name,
        )

        with self.conn:
            for target_ver in range(current + 1, SCHEMA_VERSION + 1):
                steps = _MIGRATIONS.get(target_ver, [])
                for sql in steps:
                    self._execute_migration_step(sql)
                logger.debug("  迁移完成: v%d → v%d", target_ver - 1, target_ver)

            self._set_schema_version(SCHEMA_VERSION)

    # ------------------------------------------------------------------
    # 通用 SQL 执行
    # ------------------------------------------------------------------

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self.conn:
            return self.conn.execute(sql, params)

    def executemany(self, sql: str, params_list: list[tuple]) -> sqlite3.Cursor:
        with self.conn:
            return self.conn.executemany(sql, params_list)

    def fetchone(self, sql: str, params: tuple = ()) -> dict | None:
        cursor = self.conn.execute(sql, params)
        row = cursor.fetchone()
        return dict(row) if row else None

    def fetchall(self, sql: str, params: tuple = ()) -> list[dict]:
        cursor = self.conn.execute(sql, params)
        return [dict(r) for r in cursor.fetchall()]
