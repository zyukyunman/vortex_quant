"""
serverchan.py
Server酱推送实现 (https://sct.ftqq.com/)

免费额度: 每天5条
"""
from __future__ import annotations

import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)

SERVERCHAN_URL = "https://sctapi.ftqq.com/{key}.send"


def send_serverchan(key: str, title: str, desp: str = "") -> bool:
    """
    通过 Server酱发送消息

    Parameters
    ----------
    key : str
        SendKey (SCTxxx...)
    title : str
        消息标题 (最大 32 字)
    desp : str
        消息正文 (Markdown 格式, 最大 64KB)

    Returns
    -------
    bool
        是否发送成功
    """
    if not key:
        logger.warning("Server酱 SendKey 未配置，跳过推送")
        return False

    url = SERVERCHAN_URL.format(key=key)
    data = {"title": title[:32], "desp": desp[:65000]}

    try:
        resp = requests.post(url, data=data, timeout=10)
        result = resp.json()
        if result.get("code") == 0:
            logger.info("Server酱推送成功: %s", title)
            return True
        else:
            logger.error("Server酱推送失败: %s", result.get("message", "未知错误"))
            return False
    except Exception as e:
        logger.error("Server酱推送异常: %s", e)
        return False
