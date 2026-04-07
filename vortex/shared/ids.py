"""ID 生成工具。"""
from __future__ import annotations

import hashlib
import secrets
from datetime import datetime

from vortex.shared.types import Domain, RunId


def generate_run_id(domain: Domain) -> RunId:
    """生成 RunId，格式: {domain}_{YYYYMMDD}_{HHMMSS}_{4位随机hex}。"""
    now = datetime.now()
    date_part = now.strftime("%Y%m%d")
    time_part = now.strftime("%H%M%S")
    rand_hex = secrets.token_hex(2)  # 4 位 hex
    return RunId(f"{domain}_{date_part}_{time_part}_{rand_hex}")


def generate_short_hash(content: str, length: int = 6) -> str:
    """对 content 做 SHA-256 后取前 length 位 hex。"""
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return digest[:length]
