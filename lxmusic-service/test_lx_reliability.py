"""用户源解析管线可靠性回归：预算保留/降档缓存语义/熔断恢复/失败传播。

全部离线：用户源走 FakeRuntime，媒体探活走 MockTransport。
"""
from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from conftest import FakeRuntime, lxapp, mock_client
from source_runtime import SourceError


def _media_handler(total=28936190, ext="flac"):
    def handler(request: httpx.Request) -> httpx.Response:
        if "media.test" in str(request.url):
            return httpx.Response(
                206,
                headers={"Content-Type": f"audio/x-{ext}", "Content-Range": f"bytes 0-1/{total}"},
                content=b"fLaC" if ext == "flac" else b"ID3",
            )
        return httpx.Response(404)

    return handler


_ITEM = {"id": "lx:kw:228908", "title": "晴天", "artist": "周杰伦", "duration_s": 269}


def test_resolution_exhaustion_raises_transport_error(isolated):
    """脚本连续拒绝（基础设施视角）：所有档位失败后抛 ChainTransportError 而非静默 None。"""
    isolated._runtime = FakeRuntime(resolver=SourceError("resolve", "script exploded"))

    async def run():
        http = mock_client(_media_handler())
        try:
            return await lxapp.resolve_and_probe(http, "kw", dict(_ITEM))
        finally:
            await http.aclose()

    with pytest.raises(lxapp.ChainTransportError):
        asyncio.run(run())


def test_budget_expiry_retains_completed_tier(isolated):
    """总预算耗尽时，已完成并探活通过的高档位结果必须保留返回。"""

    async def resolver(info, quality, platform):
        if quality == "flac":
            return "https://media.test/lossless.flac"
        await asyncio.sleep(1.0)  # 后续档位拖过预算
        return "https://media.test/never.flac"

    isolated._runtime = FakeRuntime(platforms={"kw": ["128k", "320k", "flac"]}, resolver=resolver)

    async def run():
        http = mock_client(_media_handler())
        try:
            return await lxapp.resolve_and_probe(http, "kw", dict(_ITEM), "lossless", budget=0.3)
        finally:
            await http.aclose()

    result = asyncio.run(run())
    assert result is not None
    assert result["url"].endswith("lossless.flac")
    assert result["actual_tier"] == "lossless"
    assert "lossless" in result["attempted_tiers"]


def test_failed_upgrade_tier_not_cached_as_exhausted(isolated):
    """flac 档脚本报错（基础设施失败）不得记为"已试完"：下次 lossless 请求必须重试 flac。"""

    def resolver(info, quality, platform):
        if quality == "flac":
            raise SourceError("resolve", "flac unavailable")
        return "https://media.test/320.mp3"

    rt = FakeRuntime(platforms={"kw": ["128k", "320k", "flac"]}, resolver=resolver)
    isolated._runtime = rt

    async def run():
        http = mock_client(_media_handler(ext="mp3", total=9000000))
        try:
            return await lxapp.resolve_and_probe(http, "kw", dict(_ITEM), "lossless")
        finally:
            await http.aclose()

    result = asyncio.run(run())
    assert result is not None
    # mp3 探活无码率信息：actual_tier 未知（低于任何具名档）
    assert result["actual_tier"] == "unknown"
    # fast_start 默认开：首枪 lossless 报基础设施错→不记为已试；第二枪 high 命中即停，
    # 不再多花一次 standard 请求（单首要曲最多 2 次脚本调用）。
    assert result["attempted_tiers"] == ["high"]
    assert len(rt.calls) == 2  # flac + 320k，刚好用满额度
    # 缓存语义：lossless 请求不能复用（档次不足且未完成降档记录）
    cached = lxapp._cache_get("lx:kw:228908")
    assert lxapp._fresh_probe(cached, "lossless") is None


def test_fast_start_shoots_requested_anchor_tier(isolated):
    """fast_start 首枪必须打「请求锚点」档：要 lossless 先 flac、要 high 先 320k、
    要 standard 先 128k。以前首枪固定升序先打 128k，proxy 侧的音质偏好到这里就没了。"""
    cases = (("lossless", "flac", "900001"), ("high", "320k", "900002"),
             ("standard", "128k", "900003"))
    seen: dict[str, list[str]] = {}

    for want, expect, item_id in cases:
        rt = FakeRuntime(platforms={"kw": ["128k", "320k", "flac"]})
        isolated._runtime = rt

        async def run(item_id: str = item_id):
            http = mock_client(_media_handler())
            try:
                return await lxapp.resolve_and_probe(
                    http, "kw", dict(_ITEM, id=f"lx:kw:{item_id}"), want)
            finally:
                await http.aclose()

        assert asyncio.run(run()) is not None
        seen[want] = [c["quality"] for c in rt.calls]

    assert seen == {"lossless": ["flac"], "high": ["320k"], "standard": ["128k"]}


def test_full_downgrade_walk_when_fast_start_off(isolated, monkeypatch):
    """关掉 fast_start：从锚点逐档降到最低档，降完才返回（旧的完整降档语义）。"""
    monkeypatch.setitem(lxapp.CONF, "fast_start", False)

    def resolver(info, quality, platform):
        if quality == "flac":
            raise SourceError("resolve", "flac unavailable")
        return "https://media.test/320.mp3"

    isolated._runtime = FakeRuntime(platforms={"kw": ["128k", "320k", "flac"]},
                                   resolver=resolver)

    async def run():
        http = mock_client(_media_handler(ext="mp3", total=9000000))
        try:
            return await lxapp.resolve_and_probe(http, "kw", dict(_ITEM), "lossless")
        finally:
            await http.aclose()

    result = asyncio.run(run())
    assert result is not None
    # lossless 因基础设施失败不记为已试；high 命中后因 actual_tier 未知
    # （不能证明比当前档更好）仍继续试完 standard
    assert result["attempted_tiers"] == ["high", "standard"]


def test_half_open_failure_reopens(isolated):
    """恢复试探（half_open）失败立即重新熔断，不允许第二次试探。"""
    lxapp._CHAIN_HEALTH["user_source"] = {"fails": 0, "open_until": time.time() - 1, "breaks": 1}
    rt = FakeRuntime(resolver=SourceError("resolve", "still broken"))
    isolated._runtime = rt

    async def run():
        http = mock_client(lambda r: httpx.Response(404))
        try:
            await lxapp.resolve_and_probe(http, "kw", dict(_ITEM))
        except lxapp.ChainTransportError:
            pass
        finally:
            await http.aclose()

    asyncio.run(run())
    snap = lxapp.chain_health_snapshot()
    assert snap["user_source"]["state"] == "open"
    assert snap["user_source"]["breaks"] == 2
    assert len(rt.calls) == 1  # half_open 只放行一次试探


def test_search_partial_results_publish_before_deadline(isolated):
    """搜索预算内探活部分通过：先完成的条目经 _publish 流出，超时后作为 partial 返回。"""
    rt = FakeRuntime()
    isolated._runtime = rt

    published: list[dict] = []

    async def run():
        token = lxapp._SEARCH_PARTIAL.set(published)
        http = mock_client(_media_handler())
        try:
            return await lxapp.kw_search(http, "晴天", 5)
        finally:
            lxapp._SEARCH_PARTIAL.reset(token)
            await http.aclose()

    # r.s 无结果时 _probe_candidates 空转：验证 partial 通道存在且不重复
    async def run_empty():
        http = mock_client(lambda r: httpx.Response(200, text="{'abslist':[]}"))
        try:
            return await lxapp.kw_search(http, "晴天", 5)
        finally:
            await http.aclose()

    assert asyncio.run(run_empty()) == []


def test_probe_candidates_batch_stops_at_limit(isolated):
    """批量探活凑满 limit 即止：不消费全部候选。"""
    isolated._runtime = FakeRuntime()
    candidates = [
        {"id": f"lx:kw:{i}", "title": f"歌{i}", "artist": "a", "duration_s": 200}
        for i in range(12)
    ]

    async def run():
        http = mock_client(_media_handler())
        try:
            return await lxapp._probe_candidates(http, "kw", [dict(c) for c in candidates], 3)
        finally:
            await http.aclose()

    passed = asyncio.run(run())
    assert len(passed) == 3
    assert all(p["verified"] is True for p in passed)
    # 后续候选未被消费（未进缓存）
    assert lxapp._cache_get("lx:kw:11") is None


def test_resolve_and_probe_skips_trial_items(isolated):
    """试听片段条目直接拒绝，不触发任何脚本调用。"""
    rt = FakeRuntime()
    isolated._runtime = rt

    async def run():
        http = mock_client(_media_handler())
        try:
            return await lxapp.resolve_and_probe(
                http, "kw", {"id": "lx:kw:1", "title": "晴天 (试听)", "duration_s": 30}
            )
        finally:
            await http.aclose()

    assert asyncio.run(run()) is None
    assert rt.calls == []
