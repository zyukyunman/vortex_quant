"""Vortex CLI 入口。

使用 argparse 提供子命令：init / server / profile / data。
init 支持交互式和非交互式两种模式。
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import unicodedata
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import yaml

from vortex import __version__
from vortex.shared.logging import get_logger, setup_logging

logger = get_logger(__name__)

DEFAULT_WORKSPACE = "~/Documents/vortex_workspace"
DEFAULT_DATA_PROFILE_NAME = "default"
DEFAULT_WORKDAY_UPDATE_FREQUENCIES = ["daily", "intraday"]
DEFAULT_WEEKEND_UPDATE_FREQUENCIES = ["weekly", "monthly", "quarterly", "other"]


class InitCancelled(Exception):
    """用户主动取消 init 流程。"""


# ------------------------------------------------------------------
# 交互式初始化辅助
# ------------------------------------------------------------------


def _is_interactive() -> bool:
    """检测当前终端是否支持交互。"""
    return sys.stdin.isatty() and sys.stdout.isatty()


def _prompt(message: str, default: str = "") -> str:
    """带默认值的交互提示。"""
    suffix = f" [{default}]" if default else ""
    try:
        answer = input(f"{message}{suffix}: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        raise InitCancelled
    return answer or default


def _prompt_yes_no(message: str, default: bool = True) -> bool:
    """是/否提问。"""
    hint = "Y/n" if default else "y/N"
    answer = _prompt(f"{message} ({hint})", "y" if default else "n")
    return answer.lower() in ("y", "yes")


def _prompt_choice(message: str, choices: list[str], default: str) -> str:
    """多选一提问。"""
    print(message)
    for i, c in enumerate(choices, 1):
        marker = "→ " if c == default else "  "
        print(f"  {marker}{i}. {c}")
    answer = _prompt(f"请选择 (1-{len(choices)})", str(choices.index(default) + 1))
    try:
        idx = int(answer) - 1
        if 0 <= idx < len(choices):
            return choices[idx]
    except ValueError:
        pass
    return default


def _apply_multi_select_command(
    current: list[str],
    options: list[str],
    defaults: list[str],
    answer: str,
) -> tuple[list[str], str | None]:
    """应用多选命令。

    支持：
    - `a` / `all`: 全选
    - `n` / `none`: 全不选
    - `d` / `default`: 恢复默认
    - 编号序列：切换对应选项，支持空格或逗号分隔，例如 `1 3 5` / `1,3,5`
    """
    selected = list(current)
    normalized = answer.strip().lower()
    if normalized in {"a", "all"}:
        return list(options), None
    if normalized in {"n", "none"}:
        return [], None
    if normalized in {"d", "default"}:
        return list(defaults), None

    tokens = [tok for tok in normalized.replace(",", " ").split() if tok]
    if not tokens:
        return selected, None

    indices: list[int] = []
    for token in tokens:
        if not token.isdigit():
            return current, f"无法识别的输入: {token}"
        idx = int(token) - 1
        if idx < 0 or idx >= len(options):
            return current, f"编号超出范围: {token}"
        indices.append(idx)

    selected_set = set(selected)
    for idx in indices:
        option = options[idx]
        if option in selected_set:
            selected_set.remove(option)
        else:
            selected_set.add(option)
    return [opt for opt in options if opt in selected_set], None


def _apply_multi_select_key(
    cursor: int,
    current: list[str],
    options: list[str],
    defaults: list[str],
    key: str,
) -> tuple[int, list[str], bool]:
    """应用键盘多选操作。

    支持：
    - up/down: 移动当前光标
    - space: 切换当前项
    - a: 全选
    - n: 全不选
    - d: 恢复默认
    - enter: 确认
    """
    if not options:
        return 0, [], True

    selected_set = set(current)
    next_cursor = cursor
    done = False

    if key == "up":
        next_cursor = (cursor - 1) % len(options)
    elif key == "down":
        next_cursor = (cursor + 1) % len(options)
    elif key == "space":
        option = options[cursor]
        if option in selected_set:
            selected_set.remove(option)
        else:
            selected_set.add(option)
    elif key == "a":
        selected_set = set(options)
    elif key == "n":
        selected_set = set()
    elif key == "d":
        selected_set = set(defaults)
    elif key == "enter":
        done = True

    selected = [opt for opt in options if opt in selected_set]
    return next_cursor, selected, done


def _multi_select_window(
    total_options: int,
    cursor: int,
    visible_count: int,
) -> tuple[int, int]:
    """根据当前光标计算可见窗口。"""
    if total_options <= visible_count:
        return 0, total_options

    half = visible_count // 2
    start = max(0, cursor - half)
    end = start + visible_count
    if end > total_options:
        end = total_options
        start = max(0, end - visible_count)
    return start, end


def _terminal_display_width(text: str) -> int:
    """估算字符串在终端中的显示宽度。"""
    width = 0
    for ch in text:
        width += 2 if unicodedata.east_asian_width(ch) in {"W", "F"} else 1
    return width


def _truncate_terminal_line(text: str, max_columns: int) -> str:
    """按终端显示宽度截断文本，避免自动换行把交互布局打乱。"""
    if max_columns <= 0 or _terminal_display_width(text) <= max_columns:
        return text

    if max_columns <= 3:
        return text[:max_columns]

    target_width = max_columns - 3
    current_width = 0
    result: list[str] = []
    for ch in text:
        ch_width = 2 if unicodedata.east_asian_width(ch) in {"W", "F"} else 1
        if current_width + ch_width > target_width:
            break
        result.append(ch)
        current_width += ch_width
    return "".join(result).rstrip() + "..."


def _multi_select_lines(
    message: str,
    options: list[str],
    selected: list[str],
    cursor: int,
) -> list[str]:
    """渲染多选 UI 的文本行。"""
    terminal_columns, terminal_rows = shutil.get_terminal_size(fallback=(100, 24))
    visible_count = min(16, max(8, terminal_rows - 8))
    start, end = _multi_select_window(len(options), cursor, visible_count)

    selected_set = set(selected)
    lines = [
        message,
        "  操作：↑↓ 移动；空格 勾选/取消；回车 确认",
        "  快捷键：a 全选；n 全不选；d 恢复默认",
        f"  已选 {len(selected)}/{len(options)} 项；当前显示 {start + 1}-{end}/{len(options)}",
    ]

    if start > 0:
        lines.append("  ↑ 上方还有更多项目")

    for idx in range(start, end):
        opt = options[idx]
        i = idx + 1
        pointer = "❯" if idx == cursor else " "
        marker = "✓" if opt in selected_set else " "
        lines.append(f"  {pointer} [{marker}] {i}. {opt}")

    if end < len(options):
        lines.append("  ↓ 下方还有更多项目")

    max_columns = max(40, terminal_columns - 1)
    return [_truncate_terminal_line(line, max_columns) for line in lines]


def _redraw_multi_select(lines: list[str]) -> None:
    """在终端中重绘多选 UI。"""
    sys.stdout.write("\x1b[2J\x1b[H")
    for line in lines:
        sys.stdout.write(line)
        # raw terminal 模式会关闭 ONLCR，必须显式写入 CRLF 才能回到行首。
        sys.stdout.write("\r\n")
    sys.stdout.flush()


@contextmanager
def _raw_terminal_mode() -> object:
    """在 POSIX TTY 上临时启用 raw 模式。"""
    import termios
    import tty

    fd = sys.stdin.fileno()
    original = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        yield
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, original)


def _read_multi_select_key() -> str:
    """读取一枚键盘按键并归一化。"""
    ch = sys.stdin.read(1)
    if ch in ("\r", "\n"):
        return "enter"
    if ch == " ":
        return "space"
    if ch == "\x03":
        raise KeyboardInterrupt
    if ch == "\x1b":
        next_ch = sys.stdin.read(1)
        if next_ch == "[":
            direction = sys.stdin.read(1)
            if direction == "A":
                return "up"
            if direction == "B":
                return "down"
        return "escape"
    return ch.lower()


def _prompt_multi_select_interactive(
    message: str,
    options: list[str],
    defaults: list[str],
) -> list[str]:
    """键盘交互式多选。"""
    selected = list(defaults)
    cursor = 0
    if defaults:
        first_default = defaults[0]
        if first_default in options:
            cursor = options.index(first_default)

    try:
        with _raw_terminal_mode():
            sys.stdout.write("\x1b[?1049h\x1b[?25l")
            sys.stdout.flush()
            while True:
                _redraw_multi_select(
                    _multi_select_lines(message, options, selected, cursor),
                )
                key = _read_multi_select_key()
                cursor, selected, done = _apply_multi_select_key(
                    cursor=cursor,
                    current=selected,
                    options=options,
                    defaults=defaults,
                    key=key,
                )
                if done:
                    return selected
    except KeyboardInterrupt:
        print()
        raise InitCancelled
    finally:
        sys.stdout.write("\x1b[?25h\x1b[?1049l")
        sys.stdout.flush()


def _prompt_multi_select_fallback(
    message: str,
    options: list[str],
    defaults: list[str],
) -> list[str]:
    """文本回退版多选。

    通过“查看当前选择 → 切换编号 → 回车确认”的循环交互，降低一次性输错的概率。
    """
    print(message)
    print("  操作说明：输入编号切换；直接回车确认；a=全选；n=全不选；d=恢复默认")
    selected = list(defaults)

    while True:
        selected_set = set(selected)
        for i, opt in enumerate(options, 1):
            marker = "✓" if opt in selected_set else " "
            print(f"  [{marker}] {i}. {opt}")

        answer = _prompt("请选择", "")
        if not answer:
            return selected

        updated, error = _apply_multi_select_command(
            current=selected,
            options=options,
            defaults=defaults,
            answer=answer,
        )
        if error is not None:
            print(f"⚠️  {error}，请重新输入。")
            continue
        selected = updated
        print()


def _prompt_multi_select(message: str, options: list[str], defaults: list[str]) -> list[str]:
    """多选提问。

    优先使用键盘交互式多选；终端不支持时回退到文本命令模式。
    """
    if not options:
        return []

    if os.name == "posix" and _is_interactive():
        try:
            return _prompt_multi_select_interactive(message, options, defaults)
        except InitCancelled:
            raise
        except Exception as exc:
            logger.warning("键盘多选不可用，回退为文本模式: %s", exc)

    return _prompt_multi_select_fallback(message, options, defaults)


def _managed_workspace_dirs(root: Path) -> list[Path]:
    """返回 init 过程中会创建的工作区目录。"""
    return [
        root / "state",
        root / "data",
        root / "research",
        root / "strategy",
        root / "trade",
        root / "profiles",
        root / "resolved",
    ]


def _capture_init_state(
    root: Path,
    default_profile: Path,
    env_file: Path,
) -> dict[str, object]:
    """记录 init 落盘前的状态，用于取消时回滚。"""
    return {
        "root_existed": root.exists(),
        "managed_dirs": {str(path): path.exists() for path in _managed_workspace_dirs(root)},
        "default_profile_existed": default_profile.exists(),
        "default_profile_bytes": default_profile.read_bytes() if default_profile.exists() else None,
        "env_existed": env_file.exists(),
        "env_bytes": env_file.read_bytes() if env_file.exists() else None,
    }


def _rollback_init_state(
    root: Path,
    default_profile: Path,
    env_file: Path,
    state: dict[str, object] | None,
) -> None:
    """在 init 取消时尽量恢复到进入 init 之前的状态。"""
    if state is None:
        return

    root_existed = bool(state["root_existed"])
    if not root_existed:
        shutil.rmtree(root, ignore_errors=True)
        return

    if bool(state["default_profile_existed"]):
        default_profile.parent.mkdir(parents=True, exist_ok=True)
        default_profile.write_bytes(state["default_profile_bytes"] or b"")
    else:
        default_profile.unlink(missing_ok=True)

    if bool(state["env_existed"]):
        env_file.parent.mkdir(parents=True, exist_ok=True)
        env_file.write_bytes(state["env_bytes"] or b"")
    else:
        env_file.unlink(missing_ok=True)

    managed_dirs = state["managed_dirs"]
    assert isinstance(managed_dirs, dict)
    for path in _managed_workspace_dirs(root):
        if not managed_dirs.get(str(path), False) and path.exists():
            shutil.rmtree(path, ignore_errors=True)


def _print_cron_help() -> None:
    """打印 Cron 表达式的最小必要说明。"""
    print("  Cron 是 5 段格式：分 时 日 月 周")
    print("  例如：")
    print("    - 0 18 * * *   = 每天 18:00")
    print("    - 0 21 * * *   = 每天 21:00")
    print("    - 0 6 * * *    = 每天 06:00")
    print("  记忆方式：* 表示“每”，1-5 表示周一到周五")


def _resolve_init_schedule_choice(choice: str) -> str | None:
    """把 init 向导中的快捷选项映射为真正写入 profile 的 cron。

    这里故意把默认选项都映射成“每天触发”：
    - 工作日会由 `data update` 默认只跑 daily/intraday
    - 周末会由 `data update` 默认只跑 weekly/monthly/quarterly/other

    如果这里仍写成 `1-5`，周末桶就永远不会被自动调度到。
    """
    schedule_map = {
        "1": "0 18 * * *",
        "2": "0 21 * * *",
        "3": "0 6 * * *",
    }
    return schedule_map.get(choice)


def _build_default_data_config(
    *,
    history_start: str = "20170101",
    schedule: str | None = None,
) -> dict:
    """构建最小可读的默认数据配置。

    设计原则：
    - workspace 里沉淀的是“用户真正关心的输入”
    - provider 默认 datasets、质量/PIT/发布/存储 pack 属于框架内置能力，
      由 resolver 默认值补齐，不应直接展开到用户 YAML
    """
    config: dict[str, object] = {
        "name": DEFAULT_DATA_PROFILE_NAME,
        "type": "data",
        "provider": "tushare",
        "history_start": history_start,
    }
    if schedule:
        config["schedule"] = schedule
    return config


def _resolve_data_profile_name(raw: str | None) -> str:
    """兼容旧版 `--profile` 参数，但默认回落到工作区默认数据配置。

    Data CLI 当前面向“工作区唯一数据底座”：
    - 未传值：使用 `default`
    - 传入 `default`：直接使用
    - 传入 YAML 路径：提取 stem，如 `/x/default.yaml` -> `default`
    """
    if not raw:
        return DEFAULT_DATA_PROFILE_NAME

    candidate = raw.strip()
    if not candidate:
        return DEFAULT_DATA_PROFILE_NAME

    path_like = Path(candidate).expanduser()
    if path_like.suffix in {".yaml", ".yml"}:
        return path_like.stem or DEFAULT_DATA_PROFILE_NAME
    return candidate


def _format_selection_summary(selected: list[str], max_items: int = 5) -> str:
    """把多选结果格式化为简短摘要。"""
    if not selected:
        return "已选择 0 项"

    preview = ", ".join(selected[:max_items])
    if len(selected) > max_items:
        preview += " ..."
    return f"已选择 {len(selected)} 项：{preview}"


def _cli_runtime_cwd() -> Path:
    """返回后台子进程启动时使用的工作目录。"""
    return Path(__file__).resolve().parent.parent


def _tail_text_file(path: Path, max_lines: int = 10) -> str:
    """读取文本文件尾部，便于在启动失败时输出关键信息。"""
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-max_lines:])


def _refresh_latest_log_links(log_path: Path) -> None:
    """刷新日志目录下的 latest 软链接，便于快速定位当前最新日志。"""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    aliases = [log_path.parent / "latest.log"]

    stem = log_path.stem
    if "-" in stem:
        prefix = stem.rsplit("-", 1)[0]
        if prefix and prefix != "latest":
            aliases.append(log_path.parent / f"{prefix}-latest.log")

    for alias in aliases:
        try:
            if alias.exists() or alias.is_symlink():
                alias.unlink()
            alias.symlink_to(log_path.name)
        except OSError as exc:
            logger.warning(
                "刷新 latest 日志软链接失败: alias=%s target=%s error=%s",
                alias,
                log_path,
                exc,
            )


def _launch_background_process(command: list[str], log_path: Path) -> subprocess.Popen:
    """以 detached 子进程方式后台启动命令，并把 stdout/stderr 落到日志。"""
    _refresh_latest_log_links(log_path)
    with open(log_path, "ab") as log_file:
        return subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            cwd=str(_cli_runtime_cwd()),
            env=os.environ.copy(),
        )


def _load_workspace_env(root: Path) -> None:
    """从 workspace/.env 读取环境变量，但不覆盖显式导出的系统环境。"""
    env_file = root / ".env"
    if not env_file.exists():
        return

    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _parse_dataset_override(raw: str | None) -> list[str]:
    """解析逗号分隔的数据集覆盖参数。"""
    if raw is None:
        return []
    tokens = [token.strip() for token in raw.split(",")]
    return [token for token in tokens if token]


def _normalize_full_dataset_scope(
    root: Path,
    profile_name: str,
    datasets: list[str] | None,
) -> list[str]:
    """把“显式全量 dataset 列表”折叠回默认全量语义。

    典型场景是 init 里的“首次数据更新”：
    用户在交互界面里勾选了全部 dataset，本意仍然是“跑一次完整 bootstrap”。
    如果控制面继续把它记成 `bootstrap;datasets=...`，就会和普通 `bootstrap`
    变成两个 resource_key，导致同一份全量任务无法去重。
    """
    normalized = sorted({item.strip() for item in (datasets or []) if item.strip()})
    if not normalized:
        return []

    try:
        from vortex.config.profile.resolver import ProfileResolver
        from vortex.config.profile.store import ProfileStore
        from vortex.runtime.workspace import Workspace
        from vortex.shared.errors import ConfigError

        ws = Workspace(root)
        store = ProfileStore(ws.profiles_dir)
        resolver = ProfileResolver(store)
        profile, _ = resolver.resolve(profile_name, "data")
    except (ConfigError, FileNotFoundError):
        return normalized

    effective = sorted(
        {item.strip() for item in profile.effective_datasets if item.strip()}
    )
    if normalized == effective:
        return []
    return normalized


def _parse_update_frequency_override(raw: str | None) -> list[str]:
    """解析逗号分隔的更新频率过滤条件。"""
    if raw is None:
        return []

    from vortex.data.provider.tushare_registry import normalize_tushare_update_frequencies

    tokens = [token.strip() for token in raw.split(",")]
    return normalize_tushare_update_frequencies([token for token in tokens if token])


def _resolve_update_frequency_scope(
    action: str,
    datasets: list[str] | None,
    update_frequencies: list[str] | None,
    *,
    now: datetime | None = None,
) -> list[str]:
    """解析本次 data 命令真正要跑的频率子集。

    规则：
    1. 显式 `--frequencies` 优先
    2. 显式 `--datasets` 时，不再自动追加频率过滤
    3. `data update` 默认按自然日分桶：
       - 工作日：daily + intraday
       - 周末：weekly + monthly + quarterly + other
    4. 其余动作（bootstrap / backfill / repair / publish）默认不加频率过滤
    """
    if update_frequencies:
        return list(update_frequencies)
    if datasets:
        return []
    if action != "update":
        return []

    current = now or datetime.now()
    if current.weekday() < 5:
        return list(DEFAULT_WORKDAY_UPDATE_FREQUENCIES)
    return list(DEFAULT_WEEKEND_UPDATE_FREQUENCIES)


def _parse_data_filters(raw_filters: list[str] | None) -> dict[str, object]:
    """解析抽查命令的过滤表达式。

    支持：`col=value`、`col!=value`、`col>=value`、`col<=value`、`col>value`、`col<value`
    """
    if not raw_filters:
        return {}

    parsed: dict[str, object] = {}
    operators = [">=", "<=", "!=", ">", "<", "="]
    for raw in raw_filters:
        expression = raw.strip()
        if not expression:
            continue
        matched = None
        for op in operators:
            if op in expression:
                matched = op
                break
        if matched is None:
            raise ValueError(
                f"过滤条件格式错误: {expression}（需 col=value / col>=value 等）"
            )
        left, right = expression.split(matched, 1)
        column = left.strip()
        value = right.strip()
        if not column or not value:
            raise ValueError(
                f"过滤条件格式错误: {expression}（列名和值都不能为空）"
            )
        if column in parsed:
            raise ValueError(f"同一个字段暂不支持重复过滤: {column}")
        parsed[column] = value if matched == "=" else (matched, value)
    return parsed


def _parse_task_progress(raw: str | None):
    """解析 task_queue.progress_json。"""
    if not raw:
        return None
    try:
        from vortex.runtime.task_queue import TaskProgress

        return TaskProgress.from_dict(json.loads(raw))
    except Exception:
        return None


def _resolve_dataset_metadata(
    dataset: str,
) -> tuple[str, str | None, str | None, str | None, str | None, dict[str, str]]:
    """解析 dataset 的 canonical 名、底层 API、文档链接、说明、备注与字段文档。"""
    try:
        from vortex.data.provider.tushare_registry import (
            get_tushare_dataset_api_doc_url,
            get_tushare_dataset_field_docs,
            get_tushare_dataset_note,
            get_tushare_dataset_spec,
            resolve_tushare_dataset_name,
        )

        canonical = resolve_tushare_dataset_name(dataset)
        spec = get_tushare_dataset_spec(canonical)
        api_name = str(spec.get("api") or canonical).strip() or None
        api_doc_url = get_tushare_dataset_api_doc_url(canonical)
        description = str(spec.get("description") or "").strip() or None
        note = get_tushare_dataset_note(canonical)
        field_docs = get_tushare_dataset_field_docs(canonical)
        return canonical, api_name, api_doc_url, description, note, field_docs
    except Exception:
        return dataset, None, None, None, None, {}


def _collect_data_inspection(
    root: Path,
    *,
    dataset: str | None,
    columns: list[str],
    raw_filters: list[str],
    limit: int,
) -> dict[str, object]:
    """收集用户抽查某张表所需的元信息与样例数据。"""
    from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
    from vortex.runtime.workspace import Workspace

    ws = Workspace(root)
    ws.ensure_initialized()
    storage = ParquetDuckDBBackend(ws.data_dir)

    if dataset is None:
        catalog = []
        for name in storage.list_datasets():
            canonical, api_name, api_doc_url, description, note, _field_docs = (
                _resolve_dataset_metadata(name)
            )
            partitions = [part for part in storage.list_partitions(name) if part != "."]
            catalog.append(
                {
                    "dataset": name,
                    "canonical_dataset": canonical,
                    "api": api_name,
                    "api_doc_url": api_doc_url,
                    "description": description,
                    "note": note,
                    "storage_path": str(storage.dataset_path(name)),
                    "partition_count": len(partitions) if partitions else 1,
                }
            )
        return {
            "root": str(root),
            "mode": "catalog",
            "dataset_count": len(catalog),
            "datasets": catalog,
        }

    canonical, api_name, api_doc_url, description, note, field_docs = (
        _resolve_dataset_metadata(dataset)
    )
    filters = _parse_data_filters(raw_filters)
    schema = storage.schema(canonical)
    schema_with_docs = [
        {
            **item,
            "description": field_docs.get(str(item.get("name")), "未登记说明"),
        }
        for item in schema
    ]
    dataset_files = storage.list_partitions(canonical)
    materialized = bool(schema) or bool(dataset_files)
    visible_partitions = [part for part in dataset_files if part != "."]
    total_rows = storage.count_rows(canonical) if materialized else 0
    matching_rows = storage.count_rows(canonical, filters=filters) if materialized else 0
    preview = (
        storage.query(
            canonical,
            filters=filters,
            columns=columns or None,
            limit=max(limit, 0),
        )
        if materialized
        else None
    )
    preview_rows = (
        preview.to_dict(orient="records")
        if preview is not None and not preview.empty
        else []
    )
    return {
        "root": str(root),
        "mode": "dataset",
        "requested_dataset": dataset,
        "dataset": canonical,
        "api": api_name,
        "api_doc_url": api_doc_url,
        "description": description,
        "note": note,
        "storage_path": str(storage.dataset_path(canonical)),
        "parquet_glob": storage.parquet_glob(canonical),
        "materialized": materialized,
        "partition_count": (
            len(visible_partitions)
            if visible_partitions
            else (1 if materialized else 0)
        ),
        "partition_examples": visible_partitions[:5],
        "total_rows": total_rows,
        "matching_rows": matching_rows,
        "columns": schema_with_docs,
        "selected_columns": columns,
        "filter_expressions": raw_filters,
        "preview_rows": preview_rows,
    }


def _print_data_inspection(payload: dict[str, object], fmt: str) -> None:
    if fmt == "json":
        _print_result(payload, fmt)
        return

    mode = payload.get("mode")
    if mode == "catalog":
        datasets = payload.get("datasets", [])
        if not isinstance(datasets, list) or not datasets:
            print("ℹ️  当前还没有已落盘的数据表")
            return
        print(f"📚 当前已落盘 {payload.get('dataset_count', len(datasets))} 张表")
        for item in datasets:
            if not isinstance(item, dict):
                continue
            description = item.get("description") or "未登记说明"
            print(
                f"  - {item.get('dataset')}: {description} "
                f"(partitions={item.get('partition_count')})"
            )
            if item.get("api"):
                print(f"    api: {item.get('api')}")
            if item.get("api_doc_url"):
                print(f"    api_doc_url: {item.get('api_doc_url')}")
            if item.get("note"):
                print(f"    note: {item.get('note')}")
            print(f"    path: {item.get('storage_path')}")
        return

    print(f"🔎 dataset={payload.get('dataset')}")
    if payload.get("requested_dataset") != payload.get("dataset"):
        print(f"  alias: {payload.get('requested_dataset')} -> {payload.get('dataset')}")
    if payload.get("api"):
        print(f"  api: {payload.get('api')}")
    if payload.get("api_doc_url"):
        print(f"  api_doc_url: {payload.get('api_doc_url')}")
    print(f"  description: {payload.get('description') or '未登记说明'}")
    if payload.get("note"):
        print(f"  note: {payload.get('note')}")
    print(f"  materialized: {payload.get('materialized')}")
    print(f"  storage_path: {payload.get('storage_path')}")
    print(f"  parquet_glob: {payload.get('parquet_glob')}")
    print(f"  partition_count: {payload.get('partition_count')}")
    partition_examples = payload.get("partition_examples")
    if isinstance(partition_examples, list) and partition_examples:
        print(f"  partition_examples: {', '.join(str(item) for item in partition_examples)}")
    print(f"  total_rows: {payload.get('total_rows')}")
    print(f"  matching_rows: {payload.get('matching_rows')}")

    filter_expressions = payload.get("filter_expressions")
    if isinstance(filter_expressions, list) and filter_expressions:
        print(f"  filters: {', '.join(str(item) for item in filter_expressions)}")
    selected_columns = payload.get("selected_columns")
    if isinstance(selected_columns, list) and selected_columns:
        print(f"  selected_columns: {', '.join(str(item) for item in selected_columns)}")

    columns = payload.get("columns")
    if isinstance(columns, list) and columns:
        print("  columns:")
        for item in columns:
            if not isinstance(item, dict):
                continue
            print(
                f"    - {item.get('name')} ({item.get('type')}): "
                f"{item.get('description') or '未登记说明'}"
            )
    else:
        print("  columns: 尚无已落盘字段")

    preview_rows = payload.get("preview_rows")
    if isinstance(preview_rows, list) and preview_rows:
        try:
            import pandas as pd

            preview_df = pd.DataFrame(preview_rows)
            print("  preview:")
            print(preview_df.to_string(index=False))
        except Exception:
            print("  preview:")
            for row in preview_rows:
                print(f"    - {row}")
    else:
        print("  preview: 当前无匹配样例")


def _is_pid_alive(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def _task_summary_from_row(row: dict[str, object]) -> dict[str, object]:
    progress = _parse_task_progress(row.get("progress_json"))
    pid = getattr(progress, "pid", None)
    return {
        "task_id": row["task_id"],
        "action": row["action"],
        "status": row["status"],
        "run_id": getattr(progress, "run_id", None) or row.get("run_id"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "error": row.get("error"),
        "message": getattr(progress, "message", None),
        "log_path": getattr(progress, "log_path", None),
        "pid": pid,
        "pid_alive": _is_pid_alive(pid),
        "current_stage": getattr(progress, "current_stage", None),
        "total_stages": getattr(progress, "total_stages", 0),
        "completed_stages": getattr(progress, "completed_stages", 0),
        "current_dataset": getattr(progress, "current_dataset", None),
        "total_datasets": getattr(progress, "total_datasets", 0),
        "completed_datasets": getattr(progress, "completed_datasets", 0),
        "current_chunk": getattr(progress, "current_chunk", 0),
        "total_chunks": getattr(progress, "total_chunks", 0),
        "written_rows": getattr(progress, "written_rows", 0),
        "retry_attempt": getattr(progress, "retry_attempt", 0),
        "max_retry_attempts": getattr(progress, "max_retry_attempts", 0),
        "next_retry_at": getattr(progress, "next_retry_at", None),
    }


def _effective_active_data_tasks(
    tasks: list[dict[str, object]],
    latest_run: dict[str, object] | None,
) -> list[dict[str, object]]:
    active_tasks = [
        task for task in tasks if task.get("status") in {"pending", "running"}
    ]
    if active_tasks:
        return active_tasks

    if not latest_run or latest_run.get("status") != "running":
        return []

    run_id = latest_run.get("run_id")
    if not run_id:
        return []

    orphans: list[dict[str, object]] = []
    for task in tasks:
        if task.get("run_id") != run_id or not task.get("pid_alive"):
            continue
        payload = dict(task)
        payload["status"] = "running"
        message = str(payload.get("message") or "").strip()
        note = "worker alive; task_queue 状态已过期"
        payload["message"] = f"{message}; {note}" if message else note
        orphans.append(payload)
    return orphans


def _list_data_task_summaries(root: Path, profile_name: str) -> list[dict[str, object]]:
    from vortex.runtime.database import Database
    from vortex.runtime.task_queue import TaskQueue
    from vortex.runtime.workspace import Workspace

    ws = Workspace(root)
    ws.ensure_initialized()

    db = Database(ws.db_path)
    db.initialize_tables()
    task_queue = TaskQueue(db)
    try:
        summaries: list[dict[str, object]] = []
        for row in task_queue.list_tasks(domain="data"):
            payload = dict(row)
            if payload.get("profile") != profile_name:
                continue
            summaries.append(_task_summary_from_row(payload))
        return summaries
    finally:
        db.close()


def _resolve_data_task_summary(
    tasks: list[dict[str, object]],
    *,
    task_id: str | None = None,
    active_only: bool = False,
    prefer_active: bool = False,
    active_candidates: list[dict[str, object]] | None = None,
) -> dict[str, object] | None:
    if task_id:
        for task in tasks:
            if task.get("task_id") == task_id:
                return task
        return None

    active_tasks = (
        list(active_candidates)
        if active_candidates is not None
        else [task for task in tasks if task.get("status") in {"pending", "running"}]
    )

    if active_only:
        if len(active_tasks) > 1:
            joined = ", ".join(str(task["task_id"]) for task in active_tasks)
            raise ValueError(f"存在多个活跃任务，请显式指定 --task-id：{joined}")
        return active_tasks[0] if active_tasks else None

    if prefer_active and active_tasks:
        if len(active_tasks) > 1:
            joined = ", ".join(str(task["task_id"]) for task in active_tasks)
            raise ValueError(f"存在多个活跃任务，请显式指定 --task-id：{joined}")
        return active_tasks[0]

    return tasks[0] if tasks else None


def _format_progress_bar(current: int, total: int, width: int = 18) -> str:
    if total <= 0:
        return f"[{'-' * width}] --"
    ratio = max(0.0, min(float(current) / float(total), 1.0))
    filled = min(width, max(0, int(round(ratio * width))))
    bar = "#" * filled + "-" * (width - filled)
    return f"[{bar}] {current}/{total} ({ratio * 100:.1f}%)"


def _build_data_task_action(
    action: str,
    *,
    datasets: list[str] | None = None,
    update_frequencies: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    as_of: str | None = None,
) -> str:
    """构造 task_queue 的 action/resource_key，区分不同范围任务。"""
    scope_parts: list[str] = []
    if datasets:
        dataset_scope = sorted({item.strip() for item in datasets if item.strip()})
        if dataset_scope:
            scope_parts.append(f"datasets={','.join(dataset_scope)}")
    if update_frequencies:
        scope_parts.append(f"frequencies={','.join(update_frequencies)}")

    if action in {"backfill", "repair"} and start and end:
        action_key = f"{action}:{start}-{end}"
    elif action == "publish" and as_of:
        action_key = f"publish:{as_of}"
    else:
        action_key = action

    if scope_parts:
        return f"{action_key};{';'.join(scope_parts)}"
    return action_key


def _build_data_background_command(
    *,
    root: Path,
    profile_name: str,
    action: str,
    task_id: str,
    run_id: str,
    datasets: list[str] | None = None,
    update_frequencies: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    as_of: str | None = None,
    verbose: bool = False,
) -> list[str]:
    """构造后台 data worker 子进程命令。"""
    command = [
        sys.executable,
        "-m",
        "vortex",
        "data",
        action,
        "--root",
        str(root),
        "--format",
        "json",
        "--foreground",
        "--task-id",
        task_id,
        "--run-id",
        run_id,
        "--profile",
        profile_name,
    ]
    if verbose:
        command.append("--verbose")
    if datasets:
        command.extend(["--datasets", ",".join(datasets)])
    if update_frequencies:
        command.extend(["--frequencies", ",".join(update_frequencies)])
    if start:
        command.extend(["--start", start])
    if end:
        command.extend(["--end", end])
    if as_of:
        command.extend(["--as-of", as_of])
    return command


def _submit_data_background_task(
    *,
    root: Path,
    profile_name: str,
    action: str,
    fmt: str,
    emit_output: bool = True,
    datasets: list[str] | None = None,
    update_frequencies: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    as_of: str | None = None,
    verbose: bool = False,
) -> dict[str, object]:
    """提交 data 后台任务，并返回提交结果。"""
    from vortex.runtime.database import Database
    from vortex.runtime.task_queue import TaskProgress, TaskQueue
    from vortex.runtime.workspace import Workspace
    from vortex.shared.ids import generate_run_id

    ws = Workspace(root)
    ws.ensure_initialized()
    datasets = _normalize_full_dataset_scope(root, profile_name, datasets)

    db = Database(ws.db_path)
    db.initialize_tables()
    task_queue = TaskQueue(db)

    action_key = _build_data_task_action(
        action,
        datasets=datasets,
        update_frequencies=update_frequencies,
        start=start,
        end=end,
        as_of=as_of,
    )
    run_id = generate_run_id("data")
    task_id = task_queue.submit("data", action_key, profile_name, run_id)
    task = task_queue.get_task(task_id)
    assert task is not None

    try:
        existing = task["run_id"] != run_id
        progress = _parse_task_progress(task.get("progress_json"))
        if existing:
            payload = {
                "status": "deduplicated",
                "task_id": task_id,
                "run_id": task["run_id"],
                "action": task["action"],
                "profile": profile_name,
                "task_status": task["status"],
                "log_path": getattr(progress, "log_path", None),
                "pid": getattr(progress, "pid", None),
            }
            if fmt == "json":
                if emit_output:
                    _print_result(payload, fmt)
            elif emit_output:
                print("ℹ️  已存在同类运行中的任务，复用现有任务")
                print(f"   task_id: {payload['task_id']}")
                print(f"   run_id: {payload['run_id']}")
                if payload["pid"]:
                    print(f"   PID: {payload['pid']}")
                if payload["log_path"]:
                    print(f"   日志: {payload['log_path']}")
                print(f"   状态: vortex data status --root {root}")
                print(f"   跟踪日志: vortex data logs --root {root} --task-id {payload['task_id']} --follow")
                print(f"   取消任务: vortex data cancel --root {root} --task-id {payload['task_id']}")
            return payload

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = ws.state_dir / "logs" / f"data-{action}-{timestamp}.log"
        command = _build_data_background_command(
            root=root,
            profile_name=profile_name,
            action=action,
            task_id=task_id,
            run_id=run_id,
            datasets=datasets,
            update_frequencies=update_frequencies,
            start=start,
            end=end,
            as_of=as_of,
            verbose=verbose,
        )
        process = _launch_background_process(command, log_path)
        task_queue.update_progress(
            task_id,
            TaskProgress(
                run_id=run_id,
                current_stage="queued",
                total_stages=1,
                completed_stages=0,
                message=f"queued {action}",
                log_path=str(log_path),
                pid=process.pid,
            ),
        )

        payload = {
            "status": "submitted",
            "task_id": task_id,
            "run_id": run_id,
            "action": action,
            "profile": profile_name,
            "pid": process.pid,
            "log_path": str(log_path),
        }
        if fmt == "json":
            if emit_output:
                _print_result(payload, fmt)
        elif emit_output:
            print("🚀 已提交后台任务")
            print(f"   task_id: {task_id}")
            print(f"   run_id: {run_id}")
            print(f"   PID: {process.pid}")
            print(f"   日志: {log_path}")
            print(f"   状态: vortex data status --root {root}")
            print(f"   跟踪日志: vortex data logs --root {root} --task-id {task_id} --follow")
            print(f"   取消任务: vortex data cancel --root {root} --task-id {task_id}")
        return payload
    finally:
        db.close()


def _check_tushare_token() -> str | None:
    """检测并验证 TUSHARE_TOKEN。返回可用 token 或 None。"""
    token = os.environ.get("TUSHARE_TOKEN")
    if token:
        print(f"✅ 检测到 TUSHARE_TOKEN (前8位: {token[:8]}...)")
        return token

    print("⚠️  未检测到 TUSHARE_TOKEN 环境变量")
    print("   注册地址: https://tushare.pro/register")
    token = _prompt("请输入 Tushare Token（留空跳过）")
    if not token:
        return None
    return token


def _smoke_test_tushare(token: str) -> bool:
    """冒烟测试 Tushare Token 是否有效。"""
    try:
        import tushare as ts
    except ImportError:
        print("❌ 当前 vortex 运行环境未安装 tushare")
        print(f"   当前解释器: {sys.executable}")
        print(f"   请运行: {sys.executable} -m pip install tushare")
        print("   注意：给其他 Python 执行 pip install 不会影响当前 vortex。")
        return False

    try:
        ts.set_token(token)
        pro = ts.pro_api()
        df = pro.trade_cal(exchange="SSE", start_date="20240101", end_date="20240110")
        if df is not None and not df.empty:
            print("✅ Tushare Token 验证通过")
            return True
        print("❌ Tushare Token 验证失败: 返回数据为空")
        return False
    except Exception as e:
        print(f"❌ Tushare Token 验证失败: {e}")
        return False


# ------------------------------------------------------------------
# 通知 / Agent 交互配置
# ------------------------------------------------------------------


def _init_step_feishu() -> dict[str, str]:
    """Init 向导 Step 5: 飞书通知配置。

    逐项引导用户填写飞书开放平台的凭证信息。
    任何一项留空都视为跳过整个飞书配置。

    Returns:
        配置成功时返回环境变量字典，跳过时返回空字典。
    """
    print("📌 Step 5/6: 飞书通知配置")
    print("   飞书通知可以在数据更新完成、失败等事件时自动推送消息。")
    print("   如需配置，请先在飞书开放平台 (open.feishu.cn) 创建一个应用。")
    print()
    if not _prompt_yes_no("是否配置飞书通知？", default=False):
        print("   已跳过飞书通知配置。")
        return {}

    print()
    print("   逐项填写飞书配置（任意一项留空即跳过整个飞书配置）：")
    print()

    app_id = _prompt(
        "   App ID\n"
        "   （在飞书开放平台 → 应用管理 → 凭证与基础信息页面获取）",
    )
    if not app_id:
        print("   已跳过飞书通知配置。")
        return {}

    app_secret = _prompt(
        "   App Secret\n"
        "   （同上页面获取，注意保密，不要提交到版本控制）",
    )
    if not app_secret:
        print("   已跳过飞书通知配置。")
        return {}

    receive_id = _prompt(
        "   默认接收人 ID\n"
        "   （在飞书客户端打开目标用户/群组的资料页 → 复制 Open ID 或 Chat ID）",
    )
    if not receive_id:
        print("   已跳过飞书通知配置。")
        return {}

    receive_id_type = _prompt(
        "   接收人 ID 类型\n"
        "   可选值: open_id / user_id / chat_id / email",
        "open_id",
    )

    print("   ✅ 飞书通知已配置")
    return {
        "FEISHU_APP_ID": app_id,
        "FEISHU_APP_SECRET": app_secret,
        "FEISHU_DEFAULT_RECEIVE_ID": receive_id,
        "FEISHU_DEFAULT_RECEIVE_ID_TYPE": receive_id_type,
    }


def _init_step_agent(root: Path) -> dict[str, str]:
    """Init 向导 Step 6: AI Agent 配置。

    引导用户配置 AI Agent 后端（当前仅支持 Copilot CLI）。
    会自动检测 copilot 命令是否可用，不可用时提示安装方法。

    Args:
        root: 工作区根目录，用作默认 scope

    Returns:
        配置成功时返回环境变量字典，跳过时返回空字典。
    """
    print("📌 Step 6/6: AI Agent 配置")
    print("   AI Agent 可以在特定事件发生时自动调用 Copilot CLI 进行分析或修复。")
    print()
    if not _prompt_yes_no("是否配置 AI Agent？", default=False):
        print("   已跳过 Agent 配置。")
        return {}

    # 检测 copilot CLI 是否可用
    copilot_path = shutil.which("copilot")
    if copilot_path:
        print(f"   ✅ 检测到 copilot CLI: {copilot_path}")
    else:
        print("   ⚠️  未检测到 copilot 命令。")
        print("   安装方式：")
        print("     npm install -g @githubnext/github-copilot-cli")
        print("   安装后需完成认证：")
        print("     copilot auth")
        print()
        if not _prompt_yes_no("是否仍要配置（安装后即可使用）？", default=True):
            print("   已跳过 Agent 配置。")
            return {}

    print()
    scope = _prompt(
        "   工作目录范围\n"
        "   （Agent 执行时的工作目录，通常设为仓库根目录）",
        str(root),
    )

    effort = _prompt_choice(
        "   推理强度：",
        ["high", "medium", "low"],
        "high",
    )

    print(f"   ✅ AI Agent 已配置 (后端=copilot, 强度={effort})")
    return {
        "VORTEX_AGENT_ENABLED": "true",
        "VORTEX_AGENT_BACKEND": "copilot",
        "VORTEX_AGENT_SCOPE": scope,
        "VORTEX_AGENT_EFFORT": effort,
    }


def _merge_env_file(env_file: Path, new_vars: dict[str, str]) -> None:
    """将环境变量合并写入 .env 文件。

    如果 .env 已存在，读取现有内容并更新/追加新变量；
    如果不存在，创建新文件。保留注释行和空行。

    Args:
        env_file: .env 文件路径
        new_vars: 要写入的环境变量字典
    """
    lines: list[str] = []
    existing_keys: set[str] = set()

    if env_file.exists():
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            stripped = raw_line.strip()
            # 保留注释行和空行
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                lines.append(raw_line)
                continue
            key = stripped.split("=", 1)[0].strip()
            if key in new_vars:
                # 用新值替换已有的同名变量
                lines.append(f"{key}={new_vars[key]}")
                existing_keys.add(key)
            else:
                lines.append(raw_line)

    # 追加 .env 中不存在的新变量
    for key, value in new_vars.items():
        if key not in existing_keys:
            lines.append(f"{key}={value}")

    env_file.parent.mkdir(parents=True, exist_ok=True)
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ------------------------------------------------------------------
# 子命令实现
# ------------------------------------------------------------------


def cmd_init(args: argparse.Namespace) -> None:
    """初始化工作区。

    交互模式（默认，TTY 下）：
      1. 检测/输入 TUSHARE_TOKEN 并冒烟测试
      2. 选择历史数据起始日
      3. 选择是否立刻开始首次数据更新；若是，则选择本次先同步的数据集
      4. 配置自动更新计划
      5. 飞书通知配置（可选，逐项填写凭证）
      6. AI Agent 配置（可选，检测 Copilot CLI 并设置参数）

    非交互模式（--non-interactive 或管道输入）：
      使用默认配置，适合 CI/脚本。
    """
    from vortex.runtime.workspace import Workspace

    root = Path(args.root).expanduser().resolve()
    _load_workspace_env(root)
    ws = Workspace(root)
    non_interactive = getattr(args, "non_interactive", False) or not _is_interactive()
    default_profile = ws.profiles_dir / f"{DEFAULT_DATA_PROFILE_NAME}.yaml"
    env_file = ws.root / ".env"

    # 默认数据集列表：使用 provider registry 统一口径。
    from vortex.data.provider.tushare_registry import (
        DEFAULT_TUSHARE_PRIORITY_DATASETS,
        get_default_tushare_datasets,
    )

    all_datasets = get_default_tushare_datasets()
    default_priority = [name for name in DEFAULT_TUSHARE_PRIORITY_DATASETS if name in all_datasets]

    # 构建初始化配置
    config = _build_default_data_config()

    tushare_token = None
    run_bootstrap_now = False
    bootstrap_datasets: list[str] = []
    write_profile = True
    init_state: dict[str, object] | None = None
    # 通知/Agent 交互结果，init 成功后统一写入 .env
    extra_env_vars: dict[str, str] = {}

    try:
        if not non_interactive:
            print()
            print("=" * 50)
            print("  Vortex 工作区初始化向导")
            print("=" * 50)
            print()

            # Step 1: TUSHARE_TOKEN
            print("📌 Step 1/6: 数据源配置")
            tushare_token = _check_tushare_token()
            if tushare_token:
                if _smoke_test_tushare(tushare_token):
                    os.environ["TUSHARE_TOKEN"] = tushare_token
                else:
                    print("   Token 无效，你仍可继续初始化，但数据拉取会失败。")
                    if not _prompt_yes_no("是否继续？"):
                        print("已取消")
                        return
            else:
                print("   跳过 Token 配置。你可以稍后设置环境变量 TUSHARE_TOKEN。")
            print()

            # Step 2: 历史数据起始日
            print("📌 Step 2/6: 历史数据范围")
            history_start = _prompt("历史数据起始日 (YYYYMMDD)", "20170101")
            config["history_start"] = history_start
            print()

            # Step 3: 首次数据更新
            print("📌 Step 3/6: 首次数据更新")
            print("   默认情况下，初始化只写入配置；后续由你手动触发，或由自动调度更新全量数据集。")
            run_bootstrap_now = _prompt_yes_no("是否现在开始首次数据更新？", default=False)
            if run_bootstrap_now:
                if not os.environ.get("TUSHARE_TOKEN"):
                    print("   ⚠️  当前没有可用的 TUSHARE_TOKEN，无法立即更新，已跳过。")
                    run_bootstrap_now = False
                else:
                    priority = _prompt_multi_select(
                        "选择现在立刻更新的数据集（其余数据集后续自动/手动补齐）:",
                        all_datasets,
                        default_priority,
                    )
                    bootstrap_datasets = priority
                    print(f"   ✅ {_format_selection_summary(bootstrap_datasets)}")
            print()

            # Step 4: 自动更新计划
            print("📌 Step 4/6: 自动更新计划")
            enable_schedule = _prompt_yes_no("是否启用每日自动更新？", default=False)
            if enable_schedule:
                print("  选择更新时间:")
                print("  1. 每天 18:00（推荐；工作日跑日频，周末跑低频）")
                print("  2. 每天 21:00")
                print("  3. 每天 06:00（含非交易日）")
                print("  4. 自定义 Cron 表达式")
                _print_cron_help()
                choice = _prompt("请选择", "1")
                schedule = _resolve_init_schedule_choice(choice)
                if schedule:
                    config["schedule"] = schedule
                else:
                    print("  请输入自定义 Cron。")
                    _print_cron_help()
                    cron = _prompt("Cron 表达式（例如 0 18 * * *）", "0 18 * * *")
                    config["schedule"] = cron
            print()

            # Step 5: 飞书通知配置
            feishu_env = _init_step_feishu()
            if feishu_env:
                extra_env_vars.update(feishu_env)
                # 在 profile 配置中启用通知
                config["notification"] = {
                    "enabled": True,
                    "channel": "feishu",
                    "level": "warning",
                }
            print()

            # Step 6: AI Agent 配置
            agent_env = _init_step_agent(root)
            if agent_env:
                extra_env_vars.update(agent_env)
            print()

            if default_profile.exists():
                write_profile = _prompt_yes_no(
                    f"默认配置已存在 ({default_profile})，是否覆盖？",
                    default=False,
                )
                if not write_profile:
                    print("保留现有配置")

        init_state = _capture_init_state(root, default_profile, env_file)
        ws.initialize()

        if write_profile:
            _write_yaml(default_profile, config)
            logger.info("已写入默认 data profile: %s", default_profile)

        if run_bootstrap_now:
            print()
            try:
                _run_initial_bootstrap(root, DEFAULT_DATA_PROFILE_NAME, bootstrap_datasets)
            except Exception as exc:
                logger.warning("首次数据更新后台启动失败: %s", exc, exc_info=True)
                print(f"⚠️  首次数据更新启动失败: {exc}")
                print("   初始化已完成，你可以稍后手动执行 `vortex data bootstrap`。")

        # 仅在整个 init 成功收尾后再写入 .env，避免取消时留下半成品。
        env_to_write: dict[str, str] = {}
        if tushare_token:
            env_to_write["TUSHARE_TOKEN"] = tushare_token
        env_to_write.update(extra_env_vars)
        if env_to_write:
            _merge_env_file(env_file, env_to_write)
            written_keys = ", ".join(env_to_write.keys())
            print(f"💡 已将 {written_keys} 写入 {env_file}")
            print("   ⚠️  请确保 .env 已加入 .gitignore！")

        print()
        print(f"✅ 工作区已初始化: {root}")
        print(f"📄 默认数据配置: {default_profile}")
        print()
        print("后续操作:")
        print(f"  vortex server start --root {root}   # 后台启动服务")
        print(f"  vortex data bootstrap --root {root}  # 提交首次全量同步任务")
        print(f"  vortex data status --root {root}     # 查看同步状态")
    except (InitCancelled, KeyboardInterrupt):
        _rollback_init_state(root, default_profile, env_file, init_state)
        print("已取消初始化，未保留本次变更。")
        return


def _write_yaml(path: Path, data: dict) -> None:
    """写入 YAML 配置文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


def _build_data_pipeline(
    root: Path,
    profile_name: str,
    *,
    progress_callback=None,
    cancel_check=None,
):
    """构建 DataPipeline 及其依赖，供多个 CLI 入口复用。"""
    from vortex.config.profile.resolver import ProfileResolver
    from vortex.config.profile.store import ProfileStore
    from vortex.data.calendar import DataCalendar
    from vortex.data.manifest import SyncManifest
    from vortex.data.pipeline import DataPipeline
    from vortex.data.quality.engine import QualityEngine
    from vortex.data.quality.rules import ALL_RULES
    from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
    from vortex.runtime.workspace import Workspace

    ws = Workspace(root)
    ws.ensure_initialized()

    store = ProfileStore(ws.profiles_dir)
    resolver = ProfileResolver(store)
    profile, _ = resolver.resolve(profile_name, "data")

    manifest_dir = ws.state_dir / "manifests" / profile_name
    manifest = SyncManifest(manifest_dir / "sync_manifest.db")

    storage = ParquetDuckDBBackend(ws.data_dir)
    storage.initialize()
    quality_engine = QualityEngine(rules=ALL_RULES)

    from vortex.data.provider.tushare import TushareProvider

    provider = TushareProvider()
    calendar = DataCalendar(storage, provider)
    pipeline = DataPipeline(
        provider=provider,
        storage=storage,
        quality_engine=quality_engine,
        manifest=manifest,
        calendar=calendar,
        progress_callback=progress_callback,
        cancel_check=cancel_check,
    )
    return ws, manifest, pipeline, profile


def _run_initial_bootstrap(root: Path, profile_name: str, datasets: list[str]) -> None:
    """在 init 完成后后台启动一次限定数据集的 bootstrap。"""
    if not datasets:
        print("ℹ️  未选择立即更新的数据集，跳过首次数据更新。")
        return

    _submit_data_background_task(
        root=root,
        profile_name=profile_name,
        action="bootstrap",
        datasets=datasets,
        fmt="text",
    )
    print(f"   数据集: {_format_selection_summary(datasets)}")


def _apply_dataset_override(
    profile,
    datasets: list[str],
    update_frequencies: list[str] | None = None,
    *,
    frequency_resolver=None,
):
    """用 CLI 传入的数据集 / 更新频率覆盖 profile。"""
    import dataclasses

    update_frequencies = list(update_frequencies or [])
    if not datasets and not update_frequencies:
        return profile

    selected_datasets = list(datasets) if datasets else list(profile.effective_datasets)
    if update_frequencies:
        if frequency_resolver is None:
            raise ValueError("缺少 frequency_resolver，无法按更新频率筛选数据集")
        allowed = set(update_frequencies)
        selected_datasets = [
            dataset
            for dataset in selected_datasets
            if str(frequency_resolver(dataset) or "other").strip().lower() in allowed
        ]

    priority_datasets = (
        [dataset for dataset in datasets if dataset in selected_datasets]
        if datasets
        else [
            dataset
            for dataset in profile.priority_datasets
            if dataset in selected_datasets
        ]
    )

    return dataclasses.replace(
        profile,
        datasets=selected_datasets,
        exclude_datasets=[],
        priority_datasets=priority_datasets,
    )


def cmd_server(args: argparse.Namespace) -> None:
    """服务管理。"""
    from vortex.runtime.server import Server

    root = Path(getattr(args, "root", DEFAULT_WORKSPACE)).expanduser().resolve()
    _load_workspace_env(root)

    match args.server_action:
        case "start":
            if getattr(args, "foreground", False):
                server = Server(root)
                try:
                    server.start()
                    print("✅ Vortex Server 已启动，按 Ctrl+C 退出")
                    server.run_forever()
                except RuntimeError as e:
                    print(f"❌ {e}", file=sys.stderr)
                    sys.exit(1)
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_path = root / "state" / "logs" / f"server-{timestamp}.log"
            command = [
                sys.executable,
                "-m",
                "vortex",
                "server",
                "start",
                "--root",
                str(root),
                "--foreground",
            ]
            try:
                process = _launch_background_process(command, log_path)
            except OSError as e:
                print(f"❌ 启动失败: {e}", file=sys.stderr)
                sys.exit(1)

            time.sleep(0.5)
            exit_code = process.poll()
            if exit_code is not None:
                print("❌ Vortex Server 启动失败", file=sys.stderr)
                tail = _tail_text_file(log_path)
                if tail:
                    print(tail, file=sys.stderr)
                sys.exit(exit_code or 1)

            print("🚀 Vortex Server 已在后台启动")
            print(f"   PID: {process.pid}")
            print(f"   日志: {log_path}")
            print(f"   状态: vortex server status --root {root}")
            print(f"   停止: vortex server stop --root {root}")
        case "stop":
            # 读取 PID 文件，发送 SIGTERM
            import signal as _signal
            state_dir = root / "state"
            pid_path = state_dir / "vortex.pid"
            if not pid_path.exists():
                print("ℹ️  未找到 PID 文件，Server 可能未运行")
                return
            try:
                pid = int(pid_path.read_text().strip())
                os.kill(pid, _signal.SIGTERM)
                print(f"✅ 已向 Vortex Server (pid={pid}) 发送停止信号")
            except ProcessLookupError:
                print("ℹ️  Server 进程不存在，清理 PID 文件")
                pid_path.unlink(missing_ok=True)
            except Exception as e:
                print(f"❌ 停止失败: {e}", file=sys.stderr)
                sys.exit(1)
        case "status":
            server = Server(root)
            try:
                server.workspace.ensure_initialized()
                info = server.status()
                print(json.dumps(info, indent=2, ensure_ascii=False))
            except Exception as e:
                print(f"❌ {e}", file=sys.stderr)
                sys.exit(1)
        case _:
            print("用法: vortex server {start|stop|status}", file=sys.stderr)
            sys.exit(1)


def cmd_profile(args: argparse.Namespace) -> None:
    """配置管理。"""
    from vortex.config.profile.resolver import ProfileResolver
    from vortex.config.profile.store import ProfileStore

    root = Path(getattr(args, "root", DEFAULT_WORKSPACE)).expanduser().resolve()
    profiles_dir = root / "profiles"

    if not profiles_dir.exists():
        print(f"❌ profiles 目录不存在，请先运行 vortex init: {profiles_dir}", file=sys.stderr)
        sys.exit(1)

    store = ProfileStore(profiles_dir)
    resolver = ProfileResolver(store)

    match args.profile_action:
        case "explain":
            try:
                text = resolver.explain(args.name, args.type)
                print(text)
            except Exception as e:
                print(f"❌ {e}", file=sys.stderr)
                sys.exit(1)
        case "resolve":
            try:
                profile, sources = resolver.resolve(args.name, args.type)
                result = {
                    k: {"value": rf.value, "source": rf.source}
                    for k, rf in sources.items()
                }
                print(json.dumps(result, indent=2, ensure_ascii=False))
            except Exception as e:
                print(f"❌ {e}", file=sys.stderr)
                sys.exit(1)
        case _:
            print("用法: vortex profile {explain|resolve}", file=sys.stderr)
            sys.exit(1)


def _collect_data_status(root: Path, profile_name: str) -> dict[str, object]:
    """收集 data status 所需的最新运行和活跃任务信息。"""
    from vortex.data.manifest import SyncManifest
    from vortex.runtime.workspace import Workspace

    ws = Workspace(root)
    ws.ensure_initialized()

    manifest_dir = ws.state_dir / "manifests" / profile_name
    manifest = SyncManifest(manifest_dir / "sync_manifest.db")
    latest_run = manifest.get_latest_run(profile_name)
    manifest.close()

    tasks = _list_data_task_summaries(root, profile_name)
    active_tasks = _effective_active_data_tasks(tasks, latest_run)
    latest_task = tasks[0] if tasks else None

    return {
        "root": str(root),
        "profile": profile_name,
        "active_tasks": active_tasks,
        "latest_task": latest_task,
        "latest_run": latest_run,
    }


def _print_data_status(status: dict[str, object], fmt: str) -> None:
    """输出 data status。"""
    if fmt == "json":
        _print_result(status, fmt)
        return

    root = status.get("root")
    active_tasks = status.get("active_tasks", [])
    latest_task = status.get("latest_task")
    latest_run = status.get("latest_run")

    if isinstance(active_tasks, list) and active_tasks:
        print(f"⏳ 当前有 {len(active_tasks)} 个活跃任务")
        for task in active_tasks:
            if not isinstance(task, dict):
                continue
            print(
                f"  - task_id={task.get('task_id')} [{task.get('status')}] "
                f"action={task.get('action')} run_id={task.get('run_id')}"
            )
            if task.get("current_stage"):
                print(f"    stage: {task['current_stage']}")
            if task.get("total_datasets"):
                dataset_position = int(task.get("completed_datasets", 0) or 0)
                if (
                    task.get("current_dataset")
                    and dataset_position < int(task.get("total_datasets", 0) or 0)
                ):
                    dataset_position += 1
                print(
                    "    datasets: "
                    + _format_progress_bar(
                        dataset_position,
                        int(task.get("total_datasets", 0) or 0),
                    )
                )
                if task.get("current_dataset"):
                    print(f"    current_dataset: {task['current_dataset']}")
            if task.get("total_chunks"):
                print(
                    "    current_chunk: "
                    + _format_progress_bar(
                        int(task.get("current_chunk", 0) or 0),
                        int(task.get("total_chunks", 0) or 0),
                    )
                )
            if task.get("written_rows"):
                print(f"    written_rows: {task['written_rows']}")
            if task.get("max_retry_attempts"):
                print(
                    "    auto_recovery: "
                    f"{task.get('retry_attempt', 0)}/{task.get('max_retry_attempts', 0)}"
                )
            if task.get("next_retry_at"):
                print(f"    next_retry_at: {task['next_retry_at']}")
            if task.get("pid"):
                state = "alive" if task.get("pid_alive") else "dead"
                print(f"    pid: {task['pid']} ({state})")
            if task.get("updated_at"):
                print(f"    updated_at: {task['updated_at']}")
            if task.get("log_path"):
                print(f"    log: {task['log_path']}")
            if task.get("message"):
                print(f"    msg: {task['message']}")
            if root:
                print(
                    f"    logs: vortex data logs --root {root} --task-id {task.get('task_id')} --follow"
                )
                print(
                    f"    cancel: vortex data cancel --root {root} --task-id {task.get('task_id')}"
                )

    if isinstance(latest_run, dict) and latest_run:
        print("📌 最近一次同步")
        for key, value in latest_run.items():
            print(f"  {key}: {value}")
    elif isinstance(latest_task, dict) and latest_task:
        print("🧾 最近任务")
        print(
            f"  task_id={latest_task.get('task_id')} [{latest_task.get('status')}] "
            f"action={latest_task.get('action')} run_id={latest_task.get('run_id')}"
        )
        if latest_task.get("error"):
            print(f"  error: {latest_task['error']}")
        if latest_task.get("log_path"):
            print(f"  log: {latest_task['log_path']}")
    elif not active_tasks:
        print("ℹ️  无同步记录")


def _print_data_logs(
    root: Path,
    profile_name: str,
    *,
    task_id: str | None,
    lines: int,
    follow: bool,
    fmt: str,
) -> None:
    tasks = _list_data_task_summaries(root, profile_name)
    try:
        task = _resolve_data_task_summary(
            tasks,
            task_id=task_id,
            prefer_active=True,
        )
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        sys.exit(1)

    if task is None:
        print("❌ 未找到匹配的数据任务", file=sys.stderr)
        sys.exit(1)

    log_path = task.get("log_path")
    if not log_path:
        print("❌ 当前任务没有日志路径", file=sys.stderr)
        sys.exit(1)

    path = Path(str(log_path))
    if fmt == "json":
        if follow:
            print("❌ --follow 与 --format json 不能同时使用", file=sys.stderr)
            sys.exit(1)
        _print_result(
            {
                "task_id": task.get("task_id"),
                "run_id": task.get("run_id"),
                "log_path": str(path),
                "tail": _tail_text_file(path, max_lines=lines),
            },
            fmt,
        )
        return

    print(f"📄 task_id={task.get('task_id')} run_id={task.get('run_id')}")
    print(f"   log: {path}")
    tail = _tail_text_file(path, max_lines=lines)
    if tail:
        print(tail)
    else:
        print("ℹ️  日志文件暂无内容")

    if not follow:
        return

    print("---- follow mode (Ctrl+C 退出) ----")
    while not path.exists():
        time.sleep(0.5)

    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        handle.seek(0, os.SEEK_END)
        while True:
            line = handle.readline()
            if line:
                print(line, end="")
                continue
            time.sleep(0.5)


def _cancel_data_task(
    root: Path,
    profile_name: str,
    *,
    task_id: str | None,
    fmt: str,
) -> None:
    import dataclasses
    import signal as _signal

    from vortex.data.manifest import SyncManifest
    from vortex.runtime.database import Database
    from vortex.runtime.task_queue import TaskProgress, TaskQueue, TaskStatus
    from vortex.runtime.workspace import Workspace

    ws = Workspace(root)
    ws.ensure_initialized()
    manifest_path = ws.state_dir / "manifests" / profile_name / "sync_manifest.db"
    latest_run = None
    if manifest_path.exists():
        manifest = SyncManifest(manifest_path)
        try:
            latest_run = manifest.get_latest_run(profile_name)
        finally:
            manifest.close()

    tasks = _list_data_task_summaries(root, profile_name)
    active_candidates = _effective_active_data_tasks(tasks, latest_run)
    try:
        task = _resolve_data_task_summary(
            tasks,
            task_id=task_id,
            active_only=True,
            active_candidates=active_candidates,
        )
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        sys.exit(1)

    if task is None:
        print("❌ 当前没有可取消的活跃数据任务", file=sys.stderr)
        sys.exit(1)

    db = Database(ws.db_path)
    db.initialize_tables()
    task_queue = TaskQueue(db)
    row = task_queue.get_task(str(task["task_id"]))
    cancelled = row is not None and task_queue.cancel(str(task["task_id"]))
    if not cancelled:
        pid = task.get("pid")
        if row is not None and pid and _is_pid_alive(int(pid)):
            task_queue.update_status(
                str(task["task_id"]),
                TaskStatus.CANCELLED,
                error="cancelled by user",
            )
        else:
            db.close()
            print("❌ 任务已不在可取消状态", file=sys.stderr)
            sys.exit(1)

    if row is None:
        db.close()
        sys.exit(1)

    progress = _parse_task_progress(row.get("progress_json"))
    if progress is None:
        progress = TaskProgress(run_id=str(task.get("run_id") or ""))
    progress = dataclasses.replace(
        progress,
        current_stage="cancelled",
        message="cancel requested by user",
    )
    task_queue.update_progress(str(task["task_id"]), progress)
    db.close()

    if task.get("run_id"):
        manifest = SyncManifest(
            ws.state_dir / "manifests" / profile_name / "sync_manifest.db"
        )
        try:
            if manifest.get_run(str(task["run_id"])) is not None:
                manifest.update_status(
                    str(task["run_id"]),
                    "cancelled",
                    error_message="cancelled by user",
                )
        finally:
            manifest.close()

    signal_sent = False
    pid = task.get("pid")
    if pid:
        try:
            os.kill(int(pid), _signal.SIGTERM)
            signal_sent = True
        except ProcessLookupError:
            signal_sent = False

    time.sleep(0.2)
    payload = {
        "status": "cancel_requested",
        "task_id": task.get("task_id"),
        "run_id": task.get("run_id"),
        "pid": pid,
        "pid_alive": _is_pid_alive(int(pid)) if pid else False,
        "signal_sent": signal_sent,
        "log_path": task.get("log_path"),
    }
    if fmt == "json":
        _print_result(payload, fmt)
        return

    print("🛑 已请求取消后台任务")
    print(f"   task_id: {payload['task_id']}")
    print(f"   run_id: {payload['run_id']}")
    if pid:
        print(f"   PID: {pid}")
        print(f"   signal_sent: {signal_sent}")
        print(f"   pid_alive: {payload['pid_alive']}")
    if payload["log_path"]:
        print(f"   日志: {payload['log_path']}")


def _watch_data_status(root: Path, profile_name: str, fmt: str, interval: float) -> None:
    if fmt == "json":
        print("❌ --watch 与 --format json 不能同时使用", file=sys.stderr)
        sys.exit(1)

    refresh_interval = max(interval, 0.5)
    while True:
        status = _collect_data_status(root, profile_name)
        if sys.stdout.isatty():
            print("\033[2J\033[H", end="")
        _print_data_status(status, fmt)
        active_tasks = status.get("active_tasks", [])
        if not active_tasks:
            return
        time.sleep(refresh_interval)


def cmd_data(args: argparse.Namespace) -> None:
    """数据管理（Phase 1A: Data Ingestion & Storage）。

    对象构造遵循 06 §3.4：
      DataPipeline(provider, storage, quality_engine, manifest, calendar, derived)

    CLI 语义区分（01 §6、§13.3）：
      - backfill: 指定日期范围重跑（--start --end），不触发自动 publish
      - repair: 重跑某次失败运行的失败分区（当前阶段降级为日期范围重跑）
    """
    root = Path(getattr(args, "root", DEFAULT_WORKSPACE)).expanduser().resolve()
    _load_workspace_env(root)
    fmt = getattr(args, "format", "text")
    profile_name = _resolve_data_profile_name(getattr(args, "profile", None))
    dry_run = getattr(args, "dry_run", False)
    datasets_override = _parse_dataset_override(getattr(args, "datasets", None))
    try:
        update_frequencies = _parse_update_frequency_override(
            getattr(args, "frequencies", None)
        )
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        sys.exit(1)
    effective_update_frequencies = _resolve_update_frequency_scope(
        args.data_action,
        datasets_override,
        update_frequencies,
    )

    if args.data_action == "status":
        if getattr(args, "watch", False):
            _watch_data_status(
                root,
                profile_name,
                fmt,
                float(getattr(args, "interval", 1.0)),
            )
            return
        _print_data_status(_collect_data_status(root, profile_name), fmt)
        return

    if args.data_action == "logs":
        _print_data_logs(
            root,
            profile_name,
            task_id=getattr(args, "task_id", None),
            lines=int(getattr(args, "lines", 40)),
            follow=bool(getattr(args, "follow", False)),
            fmt=fmt,
        )
        return

    if args.data_action == "cancel":
        _cancel_data_task(
            root,
            profile_name,
            task_id=getattr(args, "task_id", None),
            fmt=fmt,
        )
        return

    if args.data_action == "inspect":
        try:
            payload = _collect_data_inspection(
                root,
                dataset=getattr(args, "dataset", None),
                columns=_parse_dataset_override(getattr(args, "columns", None)),
                raw_filters=list(getattr(args, "filters", []) or []),
                limit=int(getattr(args, "limit", 10)),
            )
        except (KeyError, ValueError) as exc:
            print(f"❌ {exc}", file=sys.stderr)
            sys.exit(1)
        _print_data_inspection(payload, fmt)
        return

    start_date = None
    end_date = None
    start_str = None
    end_str = None
    if args.data_action in {"backfill", "repair"}:
        start_date = _parse_cli_date(args.start, "--start")
        end_date = _parse_cli_date(args.end, "--end")
        if start_date > end_date:
            print(f"❌ --start ({args.start}) 不能晚于 --end ({args.end})", file=sys.stderr)
            sys.exit(1)
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

    as_of = None
    as_of_str = None
    if args.data_action == "publish":
        raw_as_of = getattr(args, "as_of", None)
        if raw_as_of:
            as_of = _parse_cli_date(raw_as_of, "--as-of")
            as_of_str = as_of.strftime("%Y%m%d")

    if (
        args.data_action in {"bootstrap", "update", "backfill", "repair", "publish"}
        and not getattr(args, "foreground", False)
        and not dry_run
    ):
        _submit_data_background_task(
            root=root,
            profile_name=profile_name,
            action=args.data_action,
            fmt=fmt,
            datasets=datasets_override,
            update_frequencies=effective_update_frequencies,
            start=start_str,
            end=end_str,
            as_of=as_of_str,
            verbose=getattr(args, "verbose", False),
        )
        return

    import dataclasses
    import signal
    from datetime import timedelta

    from vortex.data.recovery import (
        DEFAULT_DATA_AUTO_RECOVERY_DELAYS_SECONDS,
        build_run_notification_message,
        evaluate_run_report,
    )
    from vortex.shared.errors import DataError
    from vortex.shared.ids import generate_run_id
    from vortex.runtime.database import Database
    from vortex.runtime.task_queue import TaskProgress, TaskQueue, TaskStatus
    from vortex.runtime.workspace import Workspace

    ws = Workspace(root)
    ws.ensure_initialized()
    task_db = None
    task_queue = None
    notification_db = None
    notification_service = None
    task_id = getattr(args, "task_id", None)
    run_id = getattr(args, "run_id", None)
    cancel_requested = False
    profile = None

    def _update_task(status: TaskStatus | None, *, message: str, error: str | None = None) -> None:
        if task_queue is None or task_id is None:
            return
        current = task_queue.get_task(task_id)
        progress = _parse_task_progress(current.get("progress_json") if current else None)
        if progress is None:
            progress = TaskProgress(run_id=run_id or "")
        current_stage = progress.current_stage or "running"
        completed_stages = progress.completed_stages
        if status in {TaskStatus.SUCCESS, TaskStatus.PARTIAL_SUCCESS}:
            current_stage = "finished"
            completed_stages = progress.total_stages or 5
        elif status == TaskStatus.CANCELLED:
            current_stage = "cancelled"
        elif status == TaskStatus.FAILED:
            current_stage = "failed"
        task_queue.update_progress(
            task_id,
            dataclasses.replace(
                progress,
                run_id=progress.run_id or (run_id or ""),
                current_stage=current_stage,
                completed_stages=completed_stages,
                message=message,
                next_retry_at=None,
            ),
        )
        if status is not None:
            task_queue.update_status(task_id, status, error=error)

    task_progress_fields = {field.name for field in dataclasses.fields(TaskProgress)}

    def _emit_task_progress(
        progress: TaskProgress | None = None,
        *,
        force: bool = False,
        **kwargs: object,
    ) -> None:
        del force
        if task_queue is None or task_id is None:
            return
        current = task_queue.get_task(task_id)
        current_progress = _parse_task_progress(current.get("progress_json") if current else None)

        if progress is None:
            payload = {
                key: value
                for key, value in kwargs.items()
                if key in task_progress_fields
            }
            payload.setdefault("run_id", run_id or "")
            progress = TaskProgress(**payload)
        elif kwargs:
            payload = {
                key: value
                for key, value in kwargs.items()
                if key in task_progress_fields
            }
            if payload:
                progress = dataclasses.replace(progress, **payload)

        merged = progress
        if not merged.run_id:
            merged = dataclasses.replace(
                merged,
                run_id=(
                    (current_progress.run_id if current_progress else "")
                    or run_id
                    or ""
                ),
            )
        if current_progress is not None:
            if not merged.log_path and current_progress.log_path:
                merged = dataclasses.replace(merged, log_path=current_progress.log_path)
            if not merged.pid and current_progress.pid:
                merged = dataclasses.replace(merged, pid=current_progress.pid)
        task_queue.update_progress(task_id, merged)

    def _cancel_check() -> bool:
        if cancel_requested:
            return True
        if task_queue is None or task_id is None:
            return False
        return task_queue.is_cancelled(task_id)

    def _sleep_with_cancel(delay_seconds: float) -> None:
        remaining = max(delay_seconds, 0.0)
        while remaining > 0:
            if _cancel_check():
                raise DataError(
                    code="DATA_TASK_CANCELLED",
                    message="数据任务已取消",
                )
            tick = min(1.0, remaining)
            time.sleep(tick)
            remaining -= tick

    def _get_notification_service():
        nonlocal notification_db, notification_service
        if notification_service is not None:
            return notification_service
        from vortex.notification.service import NotificationService

        if task_db is not None:
            notification_service = NotificationService(task_db)
            return notification_service
        notification_db = Database(ws.db_path)
        notification_db.initialize_tables()
        notification_service = NotificationService(notification_db)
        return notification_service

    def _notify_data_result(result, plan) -> None:
        if profile is None or not plan.event_type or not plan.severity:
            return
        service = _get_notification_service()
        message = build_run_notification_message(
            report=result,
            plan=plan,
            action=args.data_action,
            root=root,
            task_id=task_id,
        )
        service.notify(message, getattr(profile, "notification", None))

    def _run_data_action_once(attempt_run_id: str):
        match args.data_action:
            case "bootstrap":
                return pipeline.bootstrap(profile, dry_run=dry_run, run_id=attempt_run_id)
            case "update":
                return pipeline.update(profile, dry_run=dry_run, run_id=attempt_run_id)
            case "backfill":
                assert start_date is not None and end_date is not None
                return pipeline.repair(
                    profile,
                    (start_date, end_date),
                    run_id=attempt_run_id,
                    action="backfill",
                )
            case "repair":
                assert start_date is not None and end_date is not None
                return pipeline.repair(
                    profile,
                    (start_date, end_date),
                    run_id=attempt_run_id,
                    action="repair",
                )
        raise AssertionError(f"unexpected data action: {args.data_action}")

    def _exit_cancelled(message: str, *, already_printed: bool = False) -> None:
        logger.warning("data %s 已取消: %s", args.data_action, message)
        if fmt == "json" and not already_printed:
            payload: dict[str, object] = {"status": "cancelled", "error": message}
            if run_id:
                payload["run_id"] = run_id
            _print_result(payload, fmt)
        elif fmt != "json":
            print(f"🛑 {message}", file=sys.stderr)
        sys.exit(130)

    previous_sigint = None
    previous_sigterm = None

    def _request_cancel(signum, _frame) -> None:
        nonlocal cancel_requested
        cancel_requested = True
        logger.warning("收到信号 %s，准备取消当前数据任务", signum)
        if task_queue is not None and task_id is not None:
            task_queue.cancel(task_id)
            _update_task(None, message="cancel requested, waiting current step to stop")

    manifest = None
    try:
        if task_id:
            task_db = Database(ws.db_path)
            task_db.initialize_tables()
            task_queue = TaskQueue(task_db)
            task_queue.update_status(task_id, TaskStatus.RUNNING)
            _update_task(None, message=f"running {args.data_action}")

        previous_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, _request_cancel)
        if hasattr(signal, "SIGTERM"):
            previous_sigterm = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, _request_cancel)

        _, manifest, pipeline, profile = _build_data_pipeline(
            root,
            profile_name,
            progress_callback=_emit_task_progress,
            cancel_check=_cancel_check,
        )
        frequency_resolver = getattr(
            pipeline,
            "_dataset_update_frequency",
            lambda _dataset: "other",
        )
        profile = _apply_dataset_override(
            profile,
            datasets_override,
            effective_update_frequencies,
            frequency_resolver=frequency_resolver,
        )
        if (
            args.data_action in {"bootstrap", "update", "backfill", "repair"}
            and not profile.effective_datasets
        ):
            print("❌ 当前数据集筛选条件没有匹配到任何 dataset", file=sys.stderr)
            sys.exit(1)

        result = None
        match args.data_action:
            case "publish":
                snapshot_id = pipeline.publish(profile, as_of, run_id=run_id)
                payload = {"snapshot_id": snapshot_id, "status": "published"}
                if run_id:
                    payload["run_id"] = run_id
                _print_result(payload, fmt)
                _update_task(TaskStatus.SUCCESS, message="published")
                return
            case "gc":
                print("⏳ data gc: 后续迭代实现")
                return
            case "bootstrap" | "update" | "backfill" | "repair":
                retry_delays = (
                    DEFAULT_DATA_AUTO_RECOVERY_DELAYS_SECONDS
                    if not dry_run
                    else ()
                )
                total_attempts = 1 + len(retry_delays)
                for attempt in range(1, total_attempts + 1):
                    attempt_run_id = (
                        run_id
                        if attempt == 1 and run_id
                        else str(generate_run_id("data"))
                    )
                    _emit_task_progress(
                        force=True,
                        run_id=attempt_run_id,
                        retry_attempt=attempt,
                        max_retry_attempts=total_attempts,
                        next_retry_at=None,
                        message=f"{args.data_action} attempt {attempt}/{total_attempts}",
                    )
                    result = _run_data_action_once(attempt_run_id)
                    plan = evaluate_run_report(
                        result,
                        attempt=attempt,
                        retry_delays=retry_delays,
                    )
                    if plan.should_retry:
                        next_retry_at = (
                            datetime.now()
                            + timedelta(seconds=plan.next_delay_seconds or 0.0)
                        ).isoformat(timespec="seconds")
                        retry_reason = "; ".join(
                            failure.reason
                            for failure in plan.retryable_failures[:3]
                        )
                        _emit_task_progress(
                            force=True,
                            run_id=attempt_run_id,
                            retry_attempt=attempt,
                            max_retry_attempts=plan.max_attempts,
                            next_retry_at=next_retry_at,
                            message=(
                                f"{args.data_action} 将自动恢复: "
                                f"{retry_reason or '存在可恢复失败'}"
                            ),
                        )
                        logger.warning(
                            "data %s 第 %d/%d 次执行未完全完成，%ss 后自动恢复: %s",
                            args.data_action,
                            attempt,
                            plan.max_attempts,
                            int(plan.next_delay_seconds or 0),
                            retry_reason or "存在可恢复失败",
                        )
                        _sleep_with_cancel(plan.next_delay_seconds or 0.0)
                        continue
                    break
            case _:
                print(f"❌ 未知操作: {args.data_action}", file=sys.stderr)
                sys.exit(1)

        if result is not None:
            plan = evaluate_run_report(result, attempt=1, retry_delays=())
            if plan.event_type:
                _notify_data_result(result, plan)
            _print_result(dataclasses.asdict(result), fmt)
            if result.status == "success":
                _update_task(TaskStatus.SUCCESS, message=result.status)
            elif result.status == "partial_success":
                _update_task(
                    TaskStatus.PARTIAL_SUCCESS,
                    message=result.error or result.status,
                    error=result.error,
                )
            elif result.status == "cancelled":
                _update_task(TaskStatus.CANCELLED, message=result.status, error=result.error)
                _exit_cancelled(result.error or "数据任务已取消", already_printed=True)
            else:
                _update_task(TaskStatus.FAILED, message=result.status, error=result.error)
                sys.exit(1)
    except DataError as e:
        if e.code == "DATA_TASK_CANCELLED":
            _update_task(TaskStatus.CANCELLED, message="cancelled", error=str(e))
            _exit_cancelled(str(e))
        _update_task(TaskStatus.FAILED, message=f"failed {args.data_action}", error=str(e))
        logger.error("data %s 失败: %s", args.data_action, e, exc_info=True)
        print(f"❌ {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        _update_task(TaskStatus.FAILED, message=f"failed {args.data_action}", error=str(e))
        logger.error("data %s 失败: %s", args.data_action, e, exc_info=True)
        print(f"❌ {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if previous_sigint is not None:
            signal.signal(signal.SIGINT, previous_sigint)
        if previous_sigterm is not None and hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, previous_sigterm)
        if manifest is not None:
            manifest.close()
        if notification_db is not None:
            notification_db.close()
        if task_db is not None:
            task_db.close()


def _parse_cli_date(s: str | None, flag: str) -> "date":
    """解析 CLI 日期参数（YYYYMMDD 或 YYYY-MM-DD）。"""
    from datetime import date as _date

    if not s:
        print(f"❌ 缺少必需参数 {flag}", file=sys.stderr)
        sys.exit(1)
    cleaned = s.strip().replace("-", "")
    if len(cleaned) != 8 or not cleaned.isdigit():
        print(f"❌ {flag} 日期格式错误（需 YYYYMMDD）: {s}", file=sys.stderr)
        sys.exit(1)
    return _date(int(cleaned[:4]), int(cleaned[4:6]), int(cleaned[6:8]))


def _print_result(result: dict, fmt: str) -> None:
    """格式化输出结果。"""
    if fmt == "json":
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        for key, value in result.items():
            if isinstance(value, dict):
                print(f"  {key}:")
                for k, v in value.items():
                    print(f"    {k}: {v}")
            elif isinstance(value, list):
                print(f"  {key}:")
                for item in value:
                    print(f"    - {item}")
            else:
                print(f"  {key}: {value}")


def cmd_strategy(args: argparse.Namespace) -> None:
    """策略研究与候选策略复核入口。"""

    if args.strategy_action != "earnings-forecast":
        raise SystemExit(f"未知 strategy 子命令: {args.strategy_action}")
    if args.earnings_action not in {
        "precise-review",
        "shadow-plan",
        "live-handoff",
        "opening-liquidity-review",
        "auction-execution-review",
    }:
        raise SystemExit(f"未知 earnings-forecast 子命令: {args.earnings_action}")

    from vortex.strategy.earnings_forecast_runner import (
        DEFAULT_AUCTION_EXECUTION_LABEL,
        DEFAULT_LIVE_HANDOFF_LABEL,
        DEFAULT_OPENING_LIQUIDITY_LABEL,
        DEFAULT_REVIEW_LABEL,
        DEFAULT_SHADOW_LABEL,
        run_opening_auction_execution_review,
        run_opening_liquidity_review,
        run_earnings_forecast_live_handoff,
        run_earnings_forecast_shadow_plan,
        run_precise_earnings_forecast_review,
    )

    if args.earnings_action == "opening-liquidity-review":
        artifacts = run_opening_liquidity_review(
            Path(args.root).expanduser(),
            opening_snapshot_path=Path(args.opening_snapshots).expanduser(),
            start=args.start,
            end=args.end,
            output_dir=Path(args.output_dir).expanduser() if args.output_dir else None,
            label=args.label or DEFAULT_OPENING_LIQUIDITY_LABEL,
            top_n_values=_parse_int_csv(args.top_n_values),
            position_modes=_parse_str_csv(args.position_modes),
            portfolio_notional=float(args.portfolio_notional),
            capped_max_weight=float(args.capped_max_weight),
            volume_unit=args.volume_unit,
            require_precise_data=not bool(getattr(args, "allow_missing_precise_data", False)),
        )
        if args.format == "json":
            print(json.dumps(artifacts.summary, ensure_ascii=False, indent=2, default=str))
            return
        print("业绩预告开盘卖一容量复核完成")
        print(f"  JSON: {artifacts.json_path}")
        print(f"  CSV: {artifacts.csv_path}")
        print(f"  Markdown: {artifacts.md_path}")
        best_variant = artifacts.summary.get("best_variant") or {}
        if isinstance(best_variant, dict) and best_variant:
            print(
                "  最优变体: "
                f"{best_variant.get('variant')}，"
                f"一手可成交率 {float(best_variant.get('one_lot_feasible_rate', 0.0)) * 100:.2f}%，"
                f"目标覆盖率 {float(best_variant.get('covered_shares_ratio', 0.0)) * 100:.2f}%"
            )
        return

    if args.earnings_action == "shadow-plan":
        artifacts = run_earnings_forecast_shadow_plan(
            Path(args.root).expanduser(),
            start=args.start,
            as_of=args.as_of,
            output_dir=Path(args.output_dir).expanduser() if args.output_dir else None,
            artifact_dir=Path(args.artifact_dir).expanduser() if args.artifact_dir else None,
            label=args.label or DEFAULT_SHADOW_LABEL,
            require_precise_data=not bool(getattr(args, "allow_missing_precise_data", False)),
        )
        if args.format == "json":
            print(json.dumps(artifacts.summary, ensure_ascii=False, indent=2, default=str))
            return
        print("业绩预告影子跟踪计划已生成")
        print(f"  JSON: {artifacts.json_path}")
        print(f"  HTML: {artifacts.html_path}")
        print(f"  目标持仓: {artifacts.target_path}")
        print(
            "  摘要: "
            f"目标仓位 {float(artifacts.summary['exposure']) * 100:.2f}%, "
            f"持仓 {artifacts.summary['holding_count']} 只, "
            f"调仓 {artifacts.summary['trade_count']} 只"
        )
        return

    if args.earnings_action == "live-handoff":
        artifacts = run_earnings_forecast_live_handoff(
            Path(args.root).expanduser(),
            start=args.start,
            as_of=args.as_of,
            qmt_bridge_url=args.qmt_bridge_url,
            qmt_bridge_token=args.qmt_bridge_token,
            qmt_account_id=args.qmt_account_id,
            output_dir=Path(args.output_dir).expanduser() if args.output_dir else None,
            artifact_dir=Path(args.artifact_dir).expanduser() if args.artifact_dir else None,
            label=args.label or DEFAULT_LIVE_HANDOFF_LABEL,
            require_precise_data=not bool(getattr(args, "allow_missing_precise_data", False)),
        )
        if args.format == "json":
            print(json.dumps(artifacts.summary, ensure_ascii=False, indent=2, default=str))
            return
        print("业绩预告实盘交接包已生成")
        print(f"  JSON: {artifacts.json_path}")
        print(f"  HTML: {artifacts.html_path}")
        print(f"  目标持仓: {artifacts.target_path}")
        print(f"  QMT 就绪: {'yes' if bool(artifacts.summary['qmt_ready']) else 'no'}")
        if artifacts.summary.get("blocking_reasons"):
            print("  阻断:")
            for item in artifacts.summary["blocking_reasons"]:
                print(f"    - {item}")
        return

    if args.earnings_action == "auction-execution-review":
        artifacts = run_opening_auction_execution_review(
            Path(args.root).expanduser(),
            opening_snapshot_path=Path(args.opening_snapshots).expanduser(),
            start=args.start,
            end=args.end,
            output_dir=Path(args.output_dir).expanduser() if args.output_dir else None,
            artifact_dir=Path(args.artifact_dir).expanduser() if args.artifact_dir else None,
            label=args.label or DEFAULT_AUCTION_EXECUTION_LABEL,
            top_n=int(args.top_n),
            position_mode=args.position_mode,
            portfolio_notional=float(args.portfolio_notional),
            capped_max_weight=float(args.capped_max_weight),
            volume_unit=args.volume_unit,
            require_precise_data=not bool(getattr(args, "allow_missing_precise_data", False)),
        )
        if args.format == "json":
            print(json.dumps(artifacts.summary, ensure_ascii=False, indent=2, default=str))
            return
        print("业绩预告开盘竞价可靠性回测完成")
        print(f"  JSON: {artifacts.json_path}")
        print(f"  HTML: {artifacts.html_path}")
        print(f"  持仓: {artifacts.holdings_path}")
        print(f"  交易: {artifacts.trades_path}")
        print(f"  买单意图: {artifacts.order_intents_path}")
        execution_summary = artifacts.summary.get("auction_execution_summary") or {}
        if isinstance(execution_summary, dict) and execution_summary:
            print(
                "  摘要: "
                f"买单完全成交率 {float(execution_summary.get('filled_order_rate', 0.0)) * 100:.2f}%, "
                f"股数执行率 {float(execution_summary.get('executed_shares_ratio', 0.0)) * 100:.2f}%"
            )
        return

    costs = _parse_float_csv(args.costs) if getattr(args, "costs", None) else None
    artifacts = run_precise_earnings_forecast_review(
        Path(args.root).expanduser(),
        start=args.start,
        end=args.end,
        output_dir=Path(args.output_dir).expanduser() if args.output_dir else None,
        artifact_dir=Path(args.artifact_dir).expanduser() if args.artifact_dir else None,
        label=args.label or DEFAULT_REVIEW_LABEL,
        cost_grid=costs if costs is not None else (0.0, 10.0, 20.0, 30.0, 50.0, 80.0, 100.0),
        portfolio_notional=float(args.portfolio_notional),
        require_precise_data=not bool(getattr(args, "allow_missing_precise_data", False)),
    )
    if args.format == "json":
        print(json.dumps(artifacts.summary, ensure_ascii=False, indent=2))
        return
    print("业绩预告精确可交易复核完成")
    print(f"  JSON: {artifacts.json_path}")
    print(f"  HTML: {artifacts.html_path}")
    print(f"  持仓: {artifacts.holdings_path}")
    metrics = artifacts.summary["metrics"]
    if isinstance(metrics, dict):
        print(
            "  指标: "
            f"年化 {float(metrics['annual_return']) * 100:.2f}%, "
            f"最大回撤 {float(metrics['max_drawdown']) * 100:.2f}%"
        )


def _trade_status_summary(
    root: Path,
    *,
    bridge_url: str | None = None,
    bridge_token: str | None = None,
    bridge_account_id: str | None = None,
) -> dict[str, object]:
    """Return a read-only summary of local Trade artifacts."""

    trade_root = root.expanduser() / "trade"
    executions_root = trade_root / "executions"
    execution_dirs = sorted([path for path in executions_root.glob("*") if path.is_dir()]) if executions_root.exists() else []
    latest = execution_dirs[-1].name if execution_dirs else None
    payload: dict[str, object] = {
        "workspace": str(root.expanduser()),
        "trade_root": str(trade_root),
        "execution_count": len(execution_dirs),
        "latest_exec_id": latest,
        "paper_ready": True,
        "qmt_ready": False,
        "qmt_blocking_reason": "Windows VM / QMT / bridge POC not connected",
    }
    if not bridge_url:
        return payload
    payload.update(
        {
            "qmt_bridge_url": bridge_url,
            "qmt_account_id": bridge_account_id or "",
        }
    )
    from vortex.trade import QmtBridgeAdapter, QmtBridgeConfig, is_known_connection_status_bug

    adapter = QmtBridgeAdapter(
        QmtBridgeConfig(
            base_url=bridge_url,
            token=bridge_token,
            account_id=bridge_account_id or None,
            allow_trading=False,
        )
    )
    health = adapter.health()
    payload["qmt_health"] = {"ok": health.ok, "message": health.message}
    if not health.ok:
        payload["qmt_blocking_reason"] = f"bridge health failed: {health.message}"
        return payload
    try:
        connection = adapter.connection_status()
        cash = adapter.get_cash()
        positions = adapter.get_positions()
        orders = adapter.get_orders()
        fills = adapter.get_fills()
    except Exception as exc:  # noqa: BLE001 - status command should surface failure in payload.
        payload["qmt_blocking_reason"] = f"bridge read failed: {exc}"
        return payload
    payload.update(
        {
            "qmt_connection_status": connection,
            "qmt_cash": cash.available_cash,
            "qmt_market_value": cash.market_value,
            "qmt_position_count": len(positions),
            "qmt_order_count": len(orders),
            "qmt_fill_count": len(fills),
            "qmt_ready": True,
            "qmt_blocking_reason": "-",
        }
    )
    if isinstance(connection, dict) and connection.get("connected") is False:
        if is_known_connection_status_bug(connection):
            payload["qmt_connection_status_warning"] = (
                "bridge connection_status endpoint uses incompatible xtdata API; "
                "treated as non-blocking because cash/positions/orders/fills are readable"
            )
        else:
            payload["qmt_ready"] = False
            payload["qmt_blocking_reason"] = f"bridge connected=false: {connection}"
    return payload


def _trade_quote_summary(
    root: Path,
    *,
    symbols: tuple[str, ...],
    bridge_url: str | None = None,
    bridge_token: str | None = None,
    bridge_account_id: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "root": str(root.expanduser()),
        "symbols": list(symbols),
        "qmt_ready": False,
        "qmt_blocking_reason": "-",
        "qmt_health": {"ok": False, "message": "未探测"},
        "quotes": {},
    }
    if not symbols:
        raise ValueError("symbols 不能为空")
    if not bridge_url:
        payload["qmt_blocking_reason"] = "缺少 --qmt-bridge-url"
        return payload

    from vortex.trade import QmtBridgeAdapter, QmtBridgeConfig

    adapter = QmtBridgeAdapter(
        QmtBridgeConfig(
            base_url=bridge_url,
            token=bridge_token,
            account_id=bridge_account_id or None,
            allow_trading=False,
        )
    )
    health = adapter.health()
    payload["qmt_health"] = {"ok": health.ok, "message": health.message}
    if not health.ok:
        payload["qmt_blocking_reason"] = f"bridge health failed: {health.message}"
        return payload
    try:
        quotes = adapter.get_quotes(list(symbols))
    except Exception as exc:  # noqa: BLE001 - quote command should surface failure in payload.
        payload["qmt_blocking_reason"] = f"bridge quote failed: {exc}"
        return payload
    payload["quotes"] = {
        symbol: {
            "open_price": quote.open_price,
            "last_price": quote.last_price,
            "volume": quote.volume,
            "amount": quote.amount,
            "is_suspended": quote.is_suspended,
            "is_limit_up": quote.is_limit_up,
            "is_limit_down": quote.is_limit_down,
        }
        for symbol, quote in quotes.items()
    }
    payload["qmt_ready"] = True
    return payload


def _trade_execution_report_path(root: Path, exec_id: str | None) -> Path:
    root = root.expanduser()
    if exec_id:
        return root / "trade" / "executions" / exec_id / "execution_report.json"
    latest = _trade_status_summary(root)["latest_exec_id"]
    if not latest:
        raise FileNotFoundError("没有可 inspect/reconcile 的 trade execution")
    return root / "trade" / "executions" / str(latest) / "execution_report.json"


def _read_trade_quotes(path: Path):
    from vortex.trade import Quote
    from vortex.trade.serialization import read_json

    raw = read_json(path.expanduser())
    rows = raw.get("quotes", raw) if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        raise ValueError("quotes JSON 必须是列表，或包含 quotes 列表字段")
    quotes = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("quotes JSON 每一项必须是对象")
        quotes.append(
            Quote(
                symbol=str(row["symbol"]),
                open_price=float(row["open_price"]),
                last_price=float(row["last_price"]) if row.get("last_price") is not None else None,
                volume=int(row["volume"]) if row.get("volume") is not None else None,
                amount=float(row["amount"]) if row.get("amount") is not None else None,
                is_suspended=bool(row.get("is_suspended", False)),
                is_limit_up=bool(row.get("is_limit_up", False)),
                is_limit_down=bool(row.get("is_limit_down", False)),
            )
        )
    return quotes


def _read_trade_st_flags(path: Path | None) -> dict[str, bool] | None:
    if path is None:
        return None
    from vortex.trade.serialization import read_json

    raw = read_json(path.expanduser())
    if isinstance(raw, dict) and "st_flags" in raw:
        raw = raw["st_flags"]
    if isinstance(raw, dict):
        return {str(symbol): bool(flag) for symbol, flag in raw.items()}
    if isinstance(raw, list):
        flags: dict[str, bool] = {}
        for row in raw:
            if not isinstance(row, dict):
                raise ValueError("st_flags JSON 列表每一项必须是对象")
            flags[str(row["symbol"])] = bool(row.get("is_st", row.get("st", False)))
        return flags
    raise ValueError("st_flags JSON 必须是 symbol->bool 对象、列表，或包含 st_flags 字段")


def _run_trade_paper_rebalance(args: argparse.Namespace) -> dict[str, object]:
    from vortex.trade import PaperBrokerAdapter, PaperBrokerConfig
    from vortex.trade.execution import run_paper_rebalance
    from vortex.trade.order_plan import OrderPlanConfig
    from vortex.trade.risk import PreTradeRiskConfig
    from vortex.trade.serialization import read_json, target_portfolio_from_dict

    portfolio = target_portfolio_from_dict(read_json(Path(args.target_portfolio).expanduser()))
    quotes = _read_trade_quotes(Path(args.quotes))
    st_flags = _read_trade_st_flags(Path(args.st_flags).expanduser() if args.st_flags else None)
    broker = PaperBrokerAdapter(
        PaperBrokerConfig(
            initial_cash=float(args.initial_cash),
            commission_bps=float(args.commission_bps),
            min_commission=float(args.min_commission),
            stamp_duty_sell_bps=float(args.stamp_duty_sell_bps),
            max_participation_rate=float(args.max_participation_rate),
            allow_trading=not bool(args.disable_trading),
        )
    )
    artifacts = run_paper_rebalance(
        portfolio,
        broker=broker,
        quotes=quotes,
        output_root=Path(args.root).expanduser(),
        st_flags=st_flags,
        order_config=OrderPlanConfig(
            buy_limit_bps=float(args.buy_limit_bps),
            sell_limit_bps=float(args.sell_limit_bps),
            min_order_value=float(args.min_order_value),
        ),
        risk_config=PreTradeRiskConfig(
            mode="paper",
            require_st_data=not bool(args.allow_missing_st_data),
            max_order_count=int(args.max_order_count),
            max_single_order_value=float(args.max_single_order_value),
            max_daily_order_value=float(args.max_daily_order_value),
        ),
    )
    report = artifacts.report
    return {
        "exec_id": artifacts.exec_id,
        "risk_passed": report.risk_result.passed,
        "blocking_reasons": report.risk_result.blocking_reasons,
        "order_count": len(report.orders),
        "fill_count": len(report.fills),
        "cash": report.cash.available_cash,
        "market_value": report.cash.market_value,
        "order_intent_path": str(artifacts.order_intent_path),
        "order_plan_path": str(artifacts.order_plan_path),
        "risk_result_path": str(artifacts.risk_result_path),
        "execution_report_path": str(artifacts.execution_report_path),
        "execution_report_md_path": str(artifacts.execution_report_md_path),
    }


def _trade_inspect_summary(root: Path, exec_id: str | None = None) -> dict[str, object]:
    from vortex.trade.serialization import execution_report_from_dict, read_json

    path = _trade_execution_report_path(root, exec_id)
    report = execution_report_from_dict(read_json(path))
    return {
        "exec_id": report.exec_id,
        "mode": report.mode,
        "trade_date": report.trade_date,
        "portfolio_id": report.portfolio_id,
        "risk_passed": report.risk_result.passed,
        "blocking_reasons": report.risk_result.blocking_reasons,
        "warnings": report.risk_result.warnings,
        "order_count": len(report.orders),
        "fill_count": len(report.fills),
        "position_count": len(report.positions),
        "cash": report.cash.available_cash,
        "market_value": report.cash.market_value,
        "unfilled_summary": report.unfilled_summary,
        "slippage_summary": report.slippage_summary,
        "execution_report_path": str(path),
    }


def _run_trade_reconcile(args: argparse.Namespace) -> dict[str, object]:
    from vortex.trade.reconcile import reconcile_execution_report, write_reconcile_report
    from vortex.trade.serialization import execution_report_from_dict, read_json

    path = _trade_execution_report_path(Path(args.root), args.exec_id)
    report = execution_report_from_dict(read_json(path))
    reconcile = reconcile_execution_report(
        report,
        cash_tolerance=float(args.cash_tolerance),
        share_tolerance=int(args.share_tolerance),
    )
    reconcile_path = path.parent / "reconcile_report.json"
    write_reconcile_report(reconcile_path, reconcile)
    return {
        "reconcile_id": reconcile.reconcile_id,
        "exec_id": reconcile.exec_id,
        "abnormal": reconcile.abnormal,
        "cash_diff": reconcile.cash_diff,
        "position_diff_count": len(reconcile.position_diffs),
        "order_diff_count": len(reconcile.order_diffs),
        "fill_diff_count": len(reconcile.fill_diffs),
        "blocking_reasons": reconcile.blocking_reasons,
        "reconcile_report_path": str(reconcile_path),
    }


def _print_trade_dict(title: str, payload: dict[str, object]) -> None:
    print(title)
    for key, value in payload.items():
        if isinstance(value, bool):
            value = "yes" if value else "no"
        elif isinstance(value, list):
            value = ", ".join(str(item) for item in value) if value else "-"
        elif isinstance(value, dict):
            value = json.dumps(value, ensure_ascii=False)
        elif value is None:
            value = "-"
        print(f"  {key}: {value}")


def cmd_trade(args: argparse.Namespace) -> None:
    """交易执行入口。"""

    if args.trade_action == "status":
        payload = _trade_status_summary(
            Path(args.root),
            bridge_url=getattr(args, "qmt_bridge_url", None),
            bridge_token=getattr(args, "qmt_bridge_token", None),
            bridge_account_id=getattr(args, "qmt_account_id", None),
        )
        title = "Trade 状态"
    elif args.trade_action == "quote":
        payload = _trade_quote_summary(
            Path(args.root),
            symbols=_parse_str_csv(args.symbols),
            bridge_url=getattr(args, "qmt_bridge_url", None),
            bridge_token=getattr(args, "qmt_bridge_token", None),
            bridge_account_id=getattr(args, "qmt_account_id", None),
        )
        title = "Trade 实时行情"
    elif args.trade_action == "inspect":
        payload = _trade_inspect_summary(Path(args.root), args.exec_id)
        title = "Trade 执行检查"
    elif args.trade_action == "reconcile":
        payload = _run_trade_reconcile(args)
        title = "Trade 对账完成"
    elif args.trade_action == "paper" and args.trade_paper_action == "rebalance":
        payload = _run_trade_paper_rebalance(args)
        title = "Paper rebalance 完成"
    else:
        raise SystemExit(f"未知 trade 子命令: {args.trade_action}")
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    _print_trade_dict(title, payload)


def _parse_float_csv(raw: str) -> tuple[float, ...]:
    values = []
    for item in raw.split(","):
        item = item.strip()
        if item:
            values.append(float(item))
    if not values:
        raise ValueError("浮点列表不能为空")
    return tuple(values)


def _parse_int_csv(raw: str) -> tuple[int, ...]:
    values = []
    for item in raw.split(","):
        item = item.strip()
        if item:
            values.append(int(item))
    if not values:
        raise ValueError("整数列表不能为空")
    return tuple(values)


def _parse_str_csv(raw: str) -> tuple[str, ...]:
    values = tuple(item.strip() for item in raw.split(",") if item.strip())
    if not values:
        raise ValueError("字符串列表不能为空")
    return values


# ------------------------------------------------------------------
# 主入口
# ------------------------------------------------------------------


def main() -> None:
    # 共享 --root 参数
    root_parser = argparse.ArgumentParser(add_help=False)
    root_parser.add_argument(
        "--root",
        default=DEFAULT_WORKSPACE,
        help=f"工作区根目录（默认 {DEFAULT_WORKSPACE}）",
    )

    parser = argparse.ArgumentParser(
        prog="vortex", description="Vortex 量化研究平台"
    )

    # 全局参数
    parser.add_argument("--verbose", action="store_true", help="详细日志输出")
    parser.add_argument(
        "--version", action="version", version=f"vortex {__version__}"
    )
    subparsers = parser.add_subparsers(dest="command")

    # --- vortex init ---
    init_parser = subparsers.add_parser("init", help="初始化工作区", parents=[root_parser])
    init_parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="非交互模式，使用默认配置（适合 CI/脚本）",
    )

    # --- vortex server {start|stop|status} ---
    server_parser = subparsers.add_parser("server", help="服务管理")
    server_sub = server_parser.add_subparsers(dest="server_action")
    start_parser = server_sub.add_parser("start", parents=[root_parser])
    start_parser.add_argument("--foreground", action="store_true", help=argparse.SUPPRESS)
    for action in ("stop", "status"):
        server_sub.add_parser(action, parents=[root_parser])

    # --- vortex profile {explain|resolve} ---
    profile_parser = subparsers.add_parser("profile", help="配置管理")
    profile_sub = profile_parser.add_subparsers(dest="profile_action")

    explain_p = profile_sub.add_parser("explain", parents=[root_parser])
    explain_p.add_argument("--name", required=True)
    explain_p.add_argument(
        "--type", required=True, choices=["data", "research", "strategy", "trade"]
    )

    resolve_p = profile_sub.add_parser("resolve", parents=[root_parser])
    resolve_p.add_argument("--name", required=True)
    resolve_p.add_argument(
        "--type", required=True, choices=["data", "research", "strategy", "trade"]
    )

    # --- vortex data ---
    data_parser = subparsers.add_parser("data", help="数据管理")
    data_sub = data_parser.add_subparsers(dest="data_action")

    # bootstrap / update / status 不需要额外参数
    for action in ["bootstrap", "update", "status"]:
        sub = data_sub.add_parser(action, parents=[root_parser])
        sub.add_argument("--profile", help=argparse.SUPPRESS)
        sub.add_argument("--datasets", help=argparse.SUPPRESS)
        if action != "status":
            sub.add_argument(
                "--frequencies",
                help=(
                    "仅运行指定更新频率的数据集，逗号分隔："
                    "daily,weekly,monthly,quarterly,other,intraday"
                ),
            )
            sub.add_argument(
                "--foreground",
                action="store_true",
                help="前台执行（默认提交后台任务）",
            )
            sub.add_argument("--task-id", help=argparse.SUPPRESS)
            sub.add_argument("--run-id", help=argparse.SUPPRESS)
            sub.add_argument("--dry-run", action="store_true")
        else:
            sub.add_argument("--watch", action="store_true", help="持续刷新直到没有活跃任务")
            sub.add_argument("--interval", type=float, default=1.0, help="watch 刷新间隔（秒）")
        sub.add_argument("--verbose", action="store_true")
        sub.add_argument("--format", choices=["text", "json"], default="text")

    # backfill 和 repair 当前阶段都使用 --start --end
    for action in ["backfill", "repair"]:
        sub = data_sub.add_parser(action, parents=[root_parser])
        sub.add_argument("--profile", help=argparse.SUPPRESS)
        sub.add_argument("--datasets", help=argparse.SUPPRESS)
        sub.add_argument(
            "--frequencies",
            help=(
                "仅运行指定更新频率的数据集，逗号分隔："
                "daily,weekly,monthly,quarterly,other,intraday"
            ),
        )
        sub.add_argument(
            "--foreground",
            action="store_true",
            help="前台执行（默认提交后台任务）",
        )
        sub.add_argument("--task-id", help=argparse.SUPPRESS)
        sub.add_argument("--run-id", help=argparse.SUPPRESS)
        sub.add_argument("--start", required=True, help="起始日期 YYYYMMDD")
        sub.add_argument("--end", required=True, help="结束日期 YYYYMMDD")
        sub.add_argument("--verbose", action="store_true")
        sub.add_argument("--format", choices=["text", "json"], default="text")

    # publish
    pub_sub = data_sub.add_parser("publish", parents=[root_parser])
    pub_sub.add_argument("--profile", help=argparse.SUPPRESS)
    pub_sub.add_argument(
        "--foreground",
        action="store_true",
        help="前台执行（默认提交后台任务）",
    )
    pub_sub.add_argument("--task-id", help=argparse.SUPPRESS)
    pub_sub.add_argument("--run-id", help=argparse.SUPPRESS)
    pub_sub.add_argument("--as-of", help="快照日期 YYYYMMDD（默认今天）")
    pub_sub.add_argument("--format", choices=["text", "json"], default="text")

    # gc
    gc_sub = data_sub.add_parser("gc", parents=[root_parser])
    gc_sub.add_argument("--profile", help=argparse.SUPPRESS)
    gc_sub.add_argument("--format", choices=["text", "json"], default="text")

    # logs
    logs_sub = data_sub.add_parser("logs", parents=[root_parser], help="查看后台任务日志")
    logs_sub.add_argument("--profile", help=argparse.SUPPRESS)
    logs_sub.add_argument("--task-id", help="任务 ID；默认取唯一活跃任务，否则取最近任务")
    logs_sub.add_argument("--lines", type=int, default=40, help="默认显示最后 N 行日志")
    logs_sub.add_argument("--follow", action="store_true", help="持续跟随日志输出")
    logs_sub.add_argument("--format", choices=["text", "json"], default="text")

    # cancel
    cancel_sub = data_sub.add_parser("cancel", parents=[root_parser], help="取消后台任务")
    cancel_sub.add_argument("--profile", help=argparse.SUPPRESS)
    cancel_sub.add_argument("--task-id", help="任务 ID；默认取消唯一活跃任务")
    cancel_sub.add_argument("--format", choices=["text", "json"], default="text")

    # inspect
    inspect_sub = data_sub.add_parser("inspect", parents=[root_parser], help="抽查某张表的字段与样例数据")
    inspect_sub.add_argument("--profile", help=argparse.SUPPRESS)
    inspect_sub.add_argument("--dataset", help="dataset 名；不传则列出当前已落盘表")
    inspect_sub.add_argument("--columns", help="只展示这些列，逗号分隔")
    inspect_sub.add_argument(
        "--filter",
        dest="filters",
        action="append",
        default=[],
        help="过滤条件，支持 col=value / col>=value / col<=value / col>value / col<value / col!=value",
    )
    inspect_sub.add_argument("--limit", type=int, default=10, help="样例行数（默认 10，0 表示只看元信息）")
    inspect_sub.add_argument("--format", choices=["text", "json"], default="text")

    # --- vortex strategy earnings-forecast precise-review ---
    strategy_parser = subparsers.add_parser("strategy", help="策略研究与复核")
    strategy_sub = strategy_parser.add_subparsers(dest="strategy_action")
    earnings_parser = strategy_sub.add_parser("earnings-forecast", help="业绩预告漂移策略")
    earnings_sub = earnings_parser.add_subparsers(dest="earnings_action")
    precise_sub = earnings_sub.add_parser(
        "precise-review",
        parents=[root_parser],
        help="运行业绩预告 v3 精确可交易复核",
    )
    precise_sub.add_argument("--start", required=True, help="起始日期 YYYYMMDD")
    precise_sub.add_argument("--end", required=True, help="结束日期 YYYYMMDD")
    precise_sub.add_argument("--output-dir", help="JSON/HTML 报告输出目录；默认 workspace/strategy")
    precise_sub.add_argument("--artifact-dir", help="持仓 CSV 输出目录；默认 workspace/strategy/artifacts")
    precise_sub.add_argument("--label", help="输出文件名前缀")
    precise_sub.add_argument(
        "--costs",
        help="成本压力网格，bps 逗号分隔；默认 0,10,20,30,50,80,100",
    )
    precise_sub.add_argument(
        "--portfolio-notional",
        type=float,
        default=100_000_000,
        help="容量测算本金，默认 1 亿元",
    )
    precise_sub.add_argument(
        "--allow-missing-precise-data",
        action="store_true",
        help="允许缺少 stk_limit/suspend_d 时降级运行（默认 fail-closed）",
    )
    precise_sub.add_argument("--format", choices=["text", "json"], default="text")

    shadow_sub = earnings_sub.add_parser(
        "shadow-plan",
        parents=[root_parser],
        help="生成某日影子跟踪目标持仓",
    )
    shadow_sub.add_argument("--start", required=True, help="影子跟踪回看起始日期 YYYYMMDD")
    shadow_sub.add_argument("--as-of", required=True, help="目标持仓日期 YYYYMMDD")
    shadow_sub.add_argument("--output-dir", help="JSON/HTML 报告输出目录；默认 workspace/strategy")
    shadow_sub.add_argument("--artifact-dir", help="目标持仓 CSV 输出目录；默认 workspace/strategy/artifacts")
    shadow_sub.add_argument("--label", help="输出文件名前缀")
    shadow_sub.add_argument(
        "--allow-missing-precise-data",
        action="store_true",
        help="允许缺少 stk_limit/suspend_d 时降级运行（默认 fail-closed）",
    )
    shadow_sub.add_argument("--format", choices=["text", "json"], default="text")

    handoff_sub = earnings_sub.add_parser(
        "live-handoff",
        parents=[root_parser],
        help="生成影子目标 + qmt-bridge 账户快照的实盘交接包",
    )
    handoff_sub.add_argument("--start", required=True, help="影子跟踪回看起始日期 YYYYMMDD")
    handoff_sub.add_argument("--as-of", required=True, help="目标持仓日期 YYYYMMDD")
    handoff_sub.add_argument("--qmt-bridge-url", required=True, help="qmt-bridge 地址，例如 http://10.0.0.2:8000")
    handoff_sub.add_argument("--qmt-bridge-token", help="qmt-bridge API Token（只读可选，按服务端配置）")
    handoff_sub.add_argument("--qmt-account-id", help="交易账户 ID（可选）")
    handoff_sub.add_argument("--output-dir", help="交接 JSON/HTML 输出目录；默认 workspace/strategy")
    handoff_sub.add_argument("--artifact-dir", help="目标持仓 CSV 输出目录；默认 workspace/strategy/artifacts")
    handoff_sub.add_argument("--label", help="输出文件名前缀")
    handoff_sub.add_argument(
        "--allow-missing-precise-data",
        action="store_true",
        help="允许缺少 stk_limit/suspend_d 时降级运行（默认 fail-closed）",
    )
    handoff_sub.add_argument("--format", choices=["text", "json"], default="text")

    opening_liquidity_sub = earnings_sub.add_parser(
        "opening-liquidity-review",
        parents=[root_parser],
        help="基于外部开盘快照复核开盘卖一容量",
    )
    opening_liquidity_sub.add_argument("--start", required=True, help="起始日期 YYYYMMDD")
    opening_liquidity_sub.add_argument("--end", required=True, help="结束日期 YYYYMMDD")
    opening_liquidity_sub.add_argument("--opening-snapshots", required=True, help="开盘快照 CSV/JSON/Parquet 路径")
    opening_liquidity_sub.add_argument("--output-dir", help="JSON/CSV/Markdown 输出目录；默认 workspace/strategy")
    opening_liquidity_sub.add_argument("--label", help="输出文件名前缀")
    opening_liquidity_sub.add_argument("--top-n-values", default="30,40,50", help="TopN 网格，默认 30,40,50")
    opening_liquidity_sub.add_argument(
        "--position-modes",
        default="full_equal_selected,capped_with_cash",
        help="仓位模式网格，默认 full_equal_selected,capped_with_cash",
    )
    opening_liquidity_sub.add_argument("--portfolio-notional", type=float, default=1_000_000.0)
    opening_liquidity_sub.add_argument("--capped-max-weight", type=float, default=0.05)
    opening_liquidity_sub.add_argument("--volume-unit", choices=["shares", "lots"], default="shares")
    opening_liquidity_sub.add_argument(
        "--allow-missing-precise-data",
        action="store_true",
        help="允许缺少 stk_limit/suspend_d 时降级运行（默认 fail-closed）",
    )
    opening_liquidity_sub.add_argument("--format", choices=["text", "json"], default="text")

    auction_execution_sub = earnings_sub.add_parser(
        "auction-execution-review",
        parents=[root_parser],
        help="把开盘竞价成交量约束真正施加到正式回测执行里",
    )
    auction_execution_sub.add_argument("--start", required=True, help="起始日期 YYYYMMDD")
    auction_execution_sub.add_argument("--end", required=True, help="结束日期 YYYYMMDD")
    auction_execution_sub.add_argument("--opening-snapshots", required=True, help="开盘竞价快照 CSV/JSON/Parquet 或 dataset 目录")
    auction_execution_sub.add_argument("--output-dir", help="JSON/HTML 报告输出目录；默认 workspace/strategy")
    auction_execution_sub.add_argument("--artifact-dir", help="持仓/成交/买单意图输出目录；默认 workspace/strategy/artifacts")
    auction_execution_sub.add_argument("--label", help="输出文件名前缀")
    auction_execution_sub.add_argument("--top-n", type=int, default=30, help="目标持仓上限，默认 30")
    auction_execution_sub.add_argument(
        "--position-mode",
        choices=["full_equal_selected", "capped_with_cash"],
        default="capped_with_cash",
        help="仓位模式，默认 capped_with_cash",
    )
    auction_execution_sub.add_argument("--portfolio-notional", type=float, default=1_000_000.0)
    auction_execution_sub.add_argument("--capped-max-weight", type=float, default=0.05)
    auction_execution_sub.add_argument("--volume-unit", choices=["shares", "lots"], default="shares")
    auction_execution_sub.add_argument(
        "--allow-missing-precise-data",
        action="store_true",
        help="允许缺少 stk_limit/suspend_d 时降级运行（默认 fail-closed）",
    )
    auction_execution_sub.add_argument("--format", choices=["text", "json"], default="text")

    # --- vortex trade ---
    trade_parser = subparsers.add_parser("trade", help="交易执行")
    trade_sub = trade_parser.add_subparsers(dest="trade_action")
    trade_status = trade_sub.add_parser("status", parents=[root_parser], help="查看交易线状态")
    trade_status.add_argument("--qmt-bridge-url", default=os.getenv("QMT_BRIDGE_BASE_URL"), help="qmt-bridge 地址")
    trade_status.add_argument("--qmt-bridge-token", default=os.getenv("QMT_BRIDGE_TOKEN"), help="qmt-bridge API Token")
    trade_status.add_argument("--qmt-account-id", default=os.getenv("QMT_ACCOUNT_ID"), help="交易账户 ID（可选）")
    trade_status.add_argument("--format", choices=["text", "json"], default="text")
    trade_quote = trade_sub.add_parser("quote", parents=[root_parser], help="通过 qmt-bridge 拉取实时行情")
    trade_quote.add_argument("--symbols", required=True, help="股票列表，逗号分隔，例如 000001.SZ,600000.SH")
    trade_quote.add_argument("--qmt-bridge-url", default=os.getenv("QMT_BRIDGE_BASE_URL"), help="qmt-bridge 地址")
    trade_quote.add_argument("--qmt-bridge-token", default=os.getenv("QMT_BRIDGE_TOKEN"), help="qmt-bridge API Token")
    trade_quote.add_argument("--qmt-account-id", default=os.getenv("QMT_ACCOUNT_ID"), help="交易账户 ID（可选）")
    trade_quote.add_argument("--format", choices=["text", "json"], default="text")
    trade_inspect = trade_sub.add_parser("inspect", parents=[root_parser], help="查看某次执行报告摘要")
    trade_inspect.add_argument("--exec-id", help="执行 ID；默认取最近一次")
    trade_inspect.add_argument("--format", choices=["text", "json"], default="text")
    trade_reconcile = trade_sub.add_parser("reconcile", parents=[root_parser], help="对账某次执行报告")
    trade_reconcile.add_argument("--exec-id", help="执行 ID；默认取最近一次")
    trade_reconcile.add_argument("--cash-tolerance", type=float, default=1.0, help="现金差异容忍度")
    trade_reconcile.add_argument("--share-tolerance", type=int, default=0, help="持仓股数差异容忍度")
    trade_reconcile.add_argument("--format", choices=["text", "json"], default="text")

    trade_paper = trade_sub.add_parser("paper", help="本地 paper broker")
    trade_paper_sub = trade_paper.add_subparsers(dest="trade_paper_action")
    paper_rebalance = trade_paper_sub.add_parser(
        "rebalance",
        parents=[root_parser],
        help="使用 target_portfolio 与 quotes JSON 运行本地 paper rebalance",
    )
    paper_rebalance.add_argument("--target-portfolio", required=True, help="target_portfolio.json 路径")
    paper_rebalance.add_argument("--quotes", required=True, help="quotes JSON 路径")
    paper_rebalance.add_argument("--st-flags", help="ST 标记 JSON 路径；默认缺失会触发 fail-closed 风控")
    paper_rebalance.add_argument("--allow-missing-st-data", action="store_true", help="允许缺少 ST 标记数据")
    paper_rebalance.add_argument("--initial-cash", type=float, default=1_000_000.0)
    paper_rebalance.add_argument("--max-participation-rate", type=float, default=0.05)
    paper_rebalance.add_argument("--commission-bps", type=float, default=2.5)
    paper_rebalance.add_argument("--min-commission", type=float, default=5.0)
    paper_rebalance.add_argument("--stamp-duty-sell-bps", type=float, default=5.0)
    paper_rebalance.add_argument("--buy-limit-bps", type=float, default=30.0)
    paper_rebalance.add_argument("--sell-limit-bps", type=float, default=30.0)
    paper_rebalance.add_argument("--min-order-value", type=float, default=3_000.0)
    paper_rebalance.add_argument("--max-order-count", type=int, default=80)
    paper_rebalance.add_argument("--max-single-order-value", type=float, default=100_000.0)
    paper_rebalance.add_argument("--max-daily-order-value", type=float, default=1_000_000.0)
    paper_rebalance.add_argument("--disable-trading", action="store_true", help="生成报告但禁用 broker 提交")
    paper_rebalance.add_argument("--format", choices=["text", "json"], default="text")

    args = parser.parse_args()

    # 日志
    setup_logging(verbose=getattr(args, "verbose", False))

    # 分发
    match args.command:
        case "init":
            cmd_init(args)
        case "server":
            cmd_server(args)
        case "profile":
            cmd_profile(args)
        case "data":
            cmd_data(args)
        case "strategy":
            cmd_strategy(args)
        case "trade":
            cmd_trade(args)
        case _:
            parser.print_help()
