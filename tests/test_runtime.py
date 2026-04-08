"""Phase 0 — runtime 模块测试（workspace / database / task_queue / server）。"""
from __future__ import annotations

import json
import os
import signal
from pathlib import Path

import pytest

from vortex.data.manifest import SyncManifest
from vortex.runtime.database import Database
from vortex.runtime.task_queue import TaskProgress, TaskQueue, TaskStatus, make_resource_key
from vortex.runtime.workspace import Workspace


class TestWorkspace:
    def test_init_creates_directories(self, tmp_path):
        ws = Workspace(tmp_path / "ws")
        ws.initialize()
        assert ws.state_dir.is_dir()
        assert ws.data_dir.is_dir()
        assert ws.research_dir.is_dir()
        assert ws.strategy_dir.is_dir()

    def test_double_init_is_idempotent(self, tmp_path):
        ws = Workspace(tmp_path / "ws")
        ws.initialize()
        ws.initialize()  # 不应抛出异常

    def test_db_path_under_state(self, tmp_path):
        ws = Workspace(tmp_path / "ws")
        ws.initialize()
        assert "state" in str(ws.db_path)


class TestDatabase:
    def test_initialize_tables(self, tmp_path):
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        rows = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = {r[0] for r in rows}
        assert "audit_log" in table_names
        assert "task_queue" in table_names

    def test_write_and_read(self, tmp_path):
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        db.execute(
            "INSERT INTO audit_log (domain, event_type, level, payload_json, run_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("data", "sync.completed", "info", "{}", "run_001", "2026-04-01T00:00:00"),
        )
        row = db.execute("SELECT * FROM audit_log WHERE run_id = ?", ("run_001",)).fetchone()
        assert row is not None

    def test_task_queue_has_resource_key_column(self, tmp_path):
        """验证 task_queue 表包含 resource_key 列。"""
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        info = db.fetchall("PRAGMA table_info(task_queue)")
        column_names = {row["name"] for row in info}
        assert "resource_key" in column_names

    def test_schema_version_recorded(self, tmp_path):
        """初始化后 schema_version 表应记录当前版本。"""
        from vortex.runtime.database import SCHEMA_VERSION
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        row = db.fetchone("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
        assert row is not None
        assert row["version"] == SCHEMA_VERSION

    def test_migrate_v1_to_v2(self, tmp_path):
        """模拟 v1 旧库（无 resource_key），调用 initialize 后应自动迁移到 v2。"""
        import sqlite3
        db_path = tmp_path / "legacy.db"
        # 手工创建 v1 schema（没有 resource_key 列、没有 schema_version 表）
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            CREATE TABLE audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                event_type TEXT NOT NULL,
                level TEXT NOT NULL DEFAULT 'info',
                payload_json TEXT,
                run_id TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE task_queue (
                task_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                action TEXT NOT NULL,
                profile TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                progress_json TEXT,
                error TEXT,
                run_id TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            INSERT INTO task_queue (task_id, domain, action, profile, status, run_id)
            VALUES ('old_task', 'data', 'bootstrap', 'default', 'success', 'run_old');
        """)
        conn.close()

        # 用 Database 打开旧库，应自动迁移
        db = Database(db_path)
        db.initialize_tables()

        # 验证: resource_key 列已添加
        cols = {row["name"] for row in db.fetchall("PRAGMA table_info(task_queue)")}
        assert "resource_key" in cols

        # 验证: 旧数据未丢失
        old = db.fetchone("SELECT * FROM task_queue WHERE task_id = 'old_task'")
        assert old is not None
        assert old["domain"] == "data"

        # 验证: schema_version 已更新
        from vortex.runtime.database import SCHEMA_VERSION
        ver = db.fetchone("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
        assert ver["version"] == SCHEMA_VERSION
        db.close()

    def test_idempotent_migration(self, tmp_path):
        """多次调用 initialize_tables 不报错。"""
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        db.initialize_tables()  # 第二次应直接跳过
        db.initialize_tables()  # 第三次也不报错
        from vortex.runtime.database import SCHEMA_VERSION
        ver = db.fetchone("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
        assert ver["version"] == SCHEMA_VERSION


class TestTaskQueue:
    def test_submit_and_status(self, tmp_path):
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        tq = TaskQueue(db)
        task_id = tq.submit("data", "bootstrap", "test", "run_001")
        assert task_id is not None
        task = tq.get_task(task_id)
        assert task is not None

    def test_concurrency_limit(self, tmp_path):
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        tq = TaskQueue(db, max_concurrent=2)
        t1 = tq.submit("data", "bootstrap", "p1", "run_a")
        t2 = tq.submit("data", "update", "p2", "run_b")
        t3 = tq.submit("data", "backfill", "p3", "run_c")
        assert t3 is not None

    def test_idempotent_submit_returns_same_task(self, tmp_path):
        """同 resource_key 的活跃任务，submit 应返回已有 task_id。"""
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        tq = TaskQueue(db)
        t1 = tq.submit("data", "bootstrap", "default", "run_1")
        t2 = tq.submit("data", "bootstrap", "default", "run_2")
        assert t1 == t2  # 幂等：返回已有任务

    def test_different_action_creates_new_task(self, tmp_path):
        """不同 action 应创建不同任务。"""
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        tq = TaskQueue(db)
        t1 = tq.submit("data", "bootstrap", "default", "run_1")
        t2 = tq.submit("data", "update", "default", "run_2")
        assert t1 != t2

    def test_completed_task_allows_new_submit(self, tmp_path):
        """已完成的任务不阻止新的同 resource_key 提交。"""
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        tq = TaskQueue(db)
        t1 = tq.submit("data", "bootstrap", "default", "run_1")
        tq.update_status(t1, TaskStatus.SUCCESS)
        t2 = tq.submit("data", "bootstrap", "default", "run_2")
        assert t1 != t2  # 成功后允许新提交

    def test_dry_run_does_not_persist(self, tmp_path):
        """dry_run 不应写入 DB。"""
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        tq = TaskQueue(db)
        t1 = tq.submit("data", "bootstrap", "default", "run_1", dry_run=True)
        assert t1.startswith("dry-run-")
        assert tq.get_task(t1) is None  # DB 中没有

    def test_resource_key_stored_in_db(self, tmp_path):
        """resource_key 应被存储到 DB 记录中。"""
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        tq = TaskQueue(db)
        t1 = tq.submit("data", "bootstrap", "default", "run_1")
        task = tq.get_task(t1)
        assert task["resource_key"] == "data:default:bootstrap"

    def test_has_active_task(self, tmp_path):
        db = Database(tmp_path / "test.db")
        db.initialize_tables()
        tq = TaskQueue(db)
        rk = make_resource_key("data", "default", "bootstrap")
        assert not tq.has_active_task(rk)
        tq.submit("data", "bootstrap", "default", "run_1")
        assert tq.has_active_task(rk)


class TestServer:
    def test_start_creates_pid_file(self, tmp_path):
        from vortex.runtime.server import Server

        server = Server(tmp_path / "ws")
        server.start()
        assert server.pid_path.exists()
        assert int(server.pid_path.read_text().strip()) == os.getpid()
        server.stop()
        assert not server.pid_path.exists()

    def test_status_includes_pid_and_draining(self, tmp_path):
        from vortex.runtime.server import Server

        server = Server(tmp_path / "ws")
        server.start()
        info = server.status()
        assert "pid" in info
        assert "draining" in info
        assert info["draining"] is False
        server.stop()

    def test_draining_rejects_new_tasks(self, tmp_path):
        from vortex.runtime.server import Server

        server = Server(tmp_path / "ws")
        server.start()
        server._draining = True
        with pytest.raises(RuntimeError, match="正在关闭"):
            server.submit_task("data", "bootstrap", "default", "run_1")
        server._draining = False
        server.stop()

    def test_stale_task_recovery_marks_dead_worker_failed_and_syncs_manifest(
        self, monkeypatch, tmp_path
    ):
        """worker 已死时，server 启动应回收 RUNNING 任务并同步 manifest。"""
        from vortex.runtime.server import Server

        root = tmp_path / "ws"
        Workspace(root).initialize()

        manifest = SyncManifest(root / "state" / "manifests" / "default" / "sync_manifest.db")
        manifest.create_run("run_x", "default", "bootstrap")
        manifest.update_status("run_x", "running")
        manifest.close()

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        db.execute(
            """INSERT INTO task_queue (task_id, domain, action, profile, status, run_id, resource_key)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("stale-task", "data", "bootstrap", "default", "running", "run_x", "data:default:bootstrap"),
        )
        db.execute(
            "UPDATE task_queue SET progress_json = ? WHERE task_id = ?",
            (
                json.dumps(TaskProgress(run_id="run_x", pid=43210).to_dict(), ensure_ascii=False),
                "stale-task",
            ),
        )
        db.close()

        monkeypatch.setattr(Server, "_is_pid_alive", lambda self, pid: False)

        server = Server(root)
        server.start()
        task = server.task_queue.get_task("stale-task")
        assert task["status"] == "failed"
        assert "interrupted" in task["error"]

        manifest = SyncManifest(root / "state" / "manifests" / "default" / "sync_manifest.db")
        run = manifest.get_run("run_x")
        manifest.close()
        assert run is not None
        assert run["status"] == "failed"
        assert "interrupted" in str(run["error_message"])

        server.stop()

    def test_stale_task_recovery_keeps_alive_worker_running(self, monkeypatch, tmp_path):
        """worker 仍存活时，server 启动不应误判 interrupted。"""
        from vortex.runtime.server import Server

        root = tmp_path / "ws"
        Workspace(root).initialize()

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        db.execute(
            """INSERT INTO task_queue (task_id, domain, action, profile, status, run_id, resource_key)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("alive-task", "data", "bootstrap", "default", "running", "run_alive", "data:default:bootstrap"),
        )
        db.execute(
            "UPDATE task_queue SET progress_json = ? WHERE task_id = ?",
            (
                json.dumps(TaskProgress(run_id="run_alive", pid=43210).to_dict(), ensure_ascii=False),
                "alive-task",
            ),
        )
        db.close()

        monkeypatch.setattr(Server, "_is_pid_alive", lambda self, pid: int(pid) == 43210)

        server = Server(root)
        server.start()
        task = server.task_queue.get_task("alive-task")
        assert task["status"] == "running"
        assert task["error"] is None

        server.task_queue.update_status("alive-task", TaskStatus.CANCELLED)
        server.stop()

    def test_double_start_fails(self, tmp_path):
        """同一 workspace 不允许启动两个实例（PID 文件互斥）。"""
        from vortex.runtime.server import Server

        server1 = Server(tmp_path / "ws")
        server1.start()
        server2 = Server(tmp_path / "ws")
        with pytest.raises(RuntimeError, match="already running"):
            server2.start()
        server1.stop()
