"""测试全局守卫：默认禁用真 ffmpeg，防止用例意外转码真实在线源。

需要验证转码路径的用例自行 monkeypatch transcode.FFMPEG_BIN 指向假实现
（tests 内置 fake_ffmpeg 脚本），其余用例一律走无 ffmpeg 的回落分支。
"""
import pytest

from proxy import transcode as tc


@pytest.fixture(autouse=True)
def _no_real_ffmpeg(monkeypatch):
    monkeypatch.setattr(tc, "FFMPEG_BIN", None)
    monkeypatch.setattr(tc, "FFPROBE_BIN", None)
    # 隔离会话注册表与并发槽，避免跨用例串扰
    monkeypatch.setattr(tc, "_SESSIONS", {})
    monkeypatch.setattr(tc, "_ACTIVE", 0)
    monkeypatch.setattr(tc, "_SLOT_COND", None)


@pytest.fixture(autouse=True)
def _no_wan_direct(monkeypatch):
    # 测试环境统一关闭外网直连CDN：mock 直链若触发真实网络探测会拖慢/拖挂用例。
    # 直连行为由 test_wan_direct.py 单独覆盖。
    from proxy.app import CONF as _CONF

    monkeypatch.setitem(_CONF, "wan_direct_enabled", False)
