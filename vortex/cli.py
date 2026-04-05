"""Vortex 命令行入口。

当前只实现两个最小命令：

1. `python -m vortex profile resolve`
2. `python -m vortex data sync`

这样可以先把运营者最关心的 profile 解析与数据下载跑通。
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any

from vortex.config.profile.exceptions import ProfileError
from vortex.config.profile.overrides import OverrideParser
from vortex.config.profile.service import build_profile_service
from vortex.data.exceptions import DataError
from vortex.data.registry import ProviderRegistry, StorageRegistry
from vortex.data.providers.tushare import TushareProvider
from vortex.data.services.sync_service import DataSyncService
from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vortex")
    subparsers = parser.add_subparsers(dest="command_group", required=True)

    profile_parser = subparsers.add_parser("profile", help="解析 profile")
    profile_subparsers = profile_parser.add_subparsers(dest="profile_command", required=True)
    resolve_parser = profile_subparsers.add_parser("resolve", help="查看 profile 的最终解析结果")
    resolve_parser.add_argument("--profile", required=True, help="profile 名称")
    resolve_parser.add_argument(
        "--type",
        required=True,
        choices=["data", "research", "strategy", "trade"],
        help="profile 类型",
    )
    resolve_parser.add_argument("--snapshot", help="可选的 snapshot 引用或别名")
    resolve_parser.add_argument(
        "--set",
        dest="override_values",
        action="append",
        default=[],
        help="运行时覆盖，例如 --set snapshot_policy.publish_time=21:30",
    )

    data_parser = subparsers.add_parser("data", help="数据域命令")
    data_subparsers = data_parser.add_subparsers(dest="data_command", required=True)
    sync_parser = data_subparsers.add_parser("sync", help="按 DataProfile 下载并落盘")
    sync_parser.add_argument("--profile", required=True, help="DataProfile 名称")
    sync_parser.add_argument(
        "--as-of",
        default=date.today().isoformat(),
        help="请求日期，格式为 YYYY-MM-DD；若为非交易日，系统会自动回退到最近一个开市日",
    )
    sync_parser.add_argument(
        "--set",
        dest="override_values",
        action="append",
        default=[],
        help="运行时覆盖，例如 --set datasets=[bar_1d,daily_basic]",
    )
    return parser


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _build_data_sync_service() -> DataSyncService:
    provider_registry = ProviderRegistry()
    provider_registry.register("tushare", TushareProvider)

    storage_registry = StorageRegistry()
    storage_registry.register("parquet_duckdb", ParquetDuckDBBackend)
    return DataSyncService(provider_registry=provider_registry, storage_registry=storage_registry)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    override = OverrideParser().parse(args.override_values)
    profile_service = build_profile_service()

    try:
        if args.command_group == "profile" and args.profile_command == "resolve":
            resolved = profile_service.prepare(
                name=args.profile,
                profile_type=args.type,
                command_scope=f"{args.type}.resolve",
                snapshot_ref=args.snapshot,
                override=override,
            )
            _print_json(resolved.to_public_dict())
            return 0

        if args.command_group == "data" and args.data_command == "sync":
            resolved = profile_service.prepare(
                name=args.profile,
                profile_type="data",
                command_scope="data.sync",
                override=override,
            )
            manifest = _build_data_sync_service().run(resolved_profile=resolved, as_of=args.as_of)
            _print_json(manifest.to_dict())
            return 0
    except (ProfileError, DataError) as exc:
        _print_json({"status": "failed", "reason": str(exc)})
        return 1

    parser.error("未知命令")
    return 2