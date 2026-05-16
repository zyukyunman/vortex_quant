from __future__ import annotations

from vortex.notification.channel.feishu import (
    FEISHU_API_BASE,
    LARK_API_BASE,
    FeishuChannel,
    FeishuConfig,
)


def test_lark_env_uses_larksuite_domain(monkeypatch):
    monkeypatch.setenv("LARK_APP_ID", "cli_lark")
    monkeypatch.setenv("LARK_APP_SECRET", "secret")
    monkeypatch.setenv("LARK_DEFAULT_RECEIVE_ID", "ou_xxx")

    config = FeishuConfig.from_env("lark")

    assert config.provider == "lark"
    assert config.api_base == LARK_API_BASE
    assert config.default_receive_id == "ou_xxx"
    assert FeishuChannel(config).name == "lark"


def test_feishu_env_remains_backward_compatible(monkeypatch):
    monkeypatch.setenv("FEISHU_APP_ID", "cli_feishu")
    monkeypatch.setenv("FEISHU_APP_SECRET", "secret")
    monkeypatch.setenv("FEISHU_DEFAULT_RECEIVE_ID", "ou_yyy")

    config = FeishuConfig.from_env("feishu")

    assert config.provider == "feishu"
    assert config.api_base == FEISHU_API_BASE
    assert config.default_receive_id == "ou_yyy"
    assert FeishuChannel(config).name == "feishu"


def test_lark_provider_switches_default_api_base_for_direct_config():
    config = FeishuConfig(
        app_id="cli_lark",
        app_secret="secret",
        default_receive_id="ou_xxx",
        provider="lark",
    )

    assert config.api_base == LARK_API_BASE
