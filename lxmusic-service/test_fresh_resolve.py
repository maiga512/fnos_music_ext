"""fresh=1：proxy 判死链后强制跳过探活缓存重新取链（对齐安卓 failedUrls 重取）。

全部离线：用户源走 FakeRuntime，媒体探活走 MockTransport。
"""
from __future__ import annotations

import asyncio

import httpx

from conftest import FakeRuntime, lxapp, mock_client


def _media_handler():
    def handler(request: httpx.Request) -> httpx.Response:
        if "media.test" in str(request.url):
            return httpx.Response(
                206,
                headers={"Content-Type": "audio/flac", "Content-Range": "bytes 0-1/28936190"},
                content=b"fLaC",
            )
        return httpx.Response(404)

    return handler


_ITEM = {"id": "lx:kw:778899", "title": "测试曲", "artist": "某人", "duration_s": 200}


def test_fresh_kwarg_threads_through_public_api(isolated):
    """fresh=True 走完整公开链路不报错，且真实打了一次脚本。"""
    rt = FakeRuntime()
    isolated._runtime = rt

    async def run():
        http = mock_client(_media_handler())
        try:
            return await lxapp.resolve_and_probe(
                http, "kw", dict(_ITEM), "lossless", fresh=True
            )
        finally:
            await http.aclose()

    result = asyncio.run(run())
    assert result and str(result["url"]).startswith("https://media.test/")
    assert len(rt.calls) >= 1


def test_fresh_skips_probe_cache(isolated):
    """同一 item：fresh=0 命中探活缓存（不打脚本），fresh=1 跳过缓存重取。"""
    first = FakeRuntime()
    second = FakeRuntime()
    isolated._runtime = first
    item = dict(_ITEM)

    async def run():
        http = mock_client(_media_handler())
        try:
            r1 = await lxapp.resolve_and_probe(http, "kw", item, "lossless")
            assert item.get("_probe"), "首次解析应写入探活缓存"
            # 换成“只会给新签名链”的源，观察是否还打脚本
            isolated._runtime = second
            r_stale = await lxapp._resolve_with_runtime(
                http, "kw", item, "lossless", None, second
            )
            stale_calls = len(second.calls)
            r_fresh = await lxapp._resolve_with_runtime(
                http, "kw", item, "lossless", None, second, True
            )
            fresh_calls = len(second.calls)
            return r1, r_stale, r_fresh, stale_calls, fresh_calls
        finally:
            await http.aclose()

    r1, r_stale, r_fresh, stale_calls, fresh_calls = asyncio.run(run())
    assert r1["url"] == "https://media.test/a.flac"
    assert r_stale["url"] == "https://media.test/a.flac"  # 探活缓存命中
    assert stale_calls == 0                                # 常规解析没打脚本
    assert r_fresh and r_fresh["url"] == "https://media.test/a.flac"
    assert fresh_calls == 1                                # fresh=1 强制重打一枪
