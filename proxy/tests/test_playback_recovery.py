"""2.6.39 播放成功率对齐安卓：死链回收 / 降档 / fresh 重取 / 断流重连。

背景：安卓版取链带 failedUrls 排除 + 播放报错后重新取链（可降档）；此前
proxy 把只产 Request 的 lambda 当重连函数传给 stream_tee_response，await 必炸，
断流重连从未生效，CDN 一断就跳歌。本文件守住整条恢复链。
"""
import httpx
import pytest

from proxy.app import (
    CONF,
    _LX_DEAD_URLS,
    _LX_STREAM_FAILS,
    _LX_STREAM_FAIL_AT,
    _LX_URL_CACHE,
    _lx_dead_urls,
    _lx_mark_url_dead,
    resolve_lx_url,
    stream_tee_response,
)


@pytest.fixture(autouse=True)
def _lx_env(tmp_path, monkeypatch):
    _LX_URL_CACHE.clear()
    _LX_DEAD_URLS.clear()
    _LX_STREAM_FAILS.clear()
    _LX_STREAM_FAIL_AT.clear()
    monkeypatch.setitem(CONF, "quality_mode", "high")
    monkeypatch.setitem(CONF, "lx_quality", "lossless")
    monkeypatch.setitem(CONF, "cache_dir", str(tmp_path / "cache"))
    monkeypatch.setitem(CONF, "tee_save_enabled", False)
    yield
    _LX_URL_CACHE.clear()
    _LX_DEAD_URLS.clear()
    _LX_STREAM_FAILS.clear()
    _LX_STREAM_FAIL_AT.clear()


def _url_handler(state):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/api/v1/track/url":
            return httpx.Response(404)
        state["calls"].append(dict(request.url.params))
        # 第 1 次调用返 urls[0]，之后返 urls[1]（模拟重新签名后的新链）
        idx = min(len(state["calls"]) - 1, len(state["urls"]) - 1)
        return httpx.Response(
            200,
            json={"ok": True, "data": {"id": "x", "url": state["urls"][idx], "ext": "flac"}},
        )

    return handler


def _client(state):
    return httpx.AsyncClient(
        transport=httpx.MockTransport(_url_handler(state)), base_url="http://127.0.0.1:8772"
    )


@pytest.mark.anyio
async def test_dead_url_not_served_from_cache():
    """缓存里的链被判死 → 作废重取，拿到新链（对齐安卓 failedUrls 排除）。"""
    state = {"calls": [], "urls": ["http://cdn.test/a.flac", "http://cdn.test/b.flac"]}
    client = _client(state)
    song = "lx:kg:DEADBEEF00000001"
    r1 = await resolve_lx_url(client, song)
    assert r1["url"] == "http://cdn.test/a.flac"
    assert len(state["calls"]) == 1

    _lx_mark_url_dead(song, "http://cdn.test/a.flac")
    assert _lx_dead_urls(song) == {"http://cdn.test/a.flac"}

    r2 = await resolve_lx_url(client, song)
    assert r2["url"] == "http://cdn.test/b.flac"
    assert len(state["calls"]) == 2  # 死链没被直接吐给客户端


@pytest.mark.anyio
async def test_tier_downgrade_after_stream_fails():
    """两次流失败 → 下次取链从低两档起步（对齐安卓播放报错→降档回退）。"""
    state = {"calls": [], "urls": ["http://cdn.test/a.flac"]}
    client = _client(state)
    song = "lx:kg:DEADBEEF00000002"
    # quality_mode=high + lx_quality=lossless → 原档序 lossless,high,standard
    _lx_mark_url_dead(song, "http://cdn.test/x1")
    _lx_mark_url_dead(song, "http://cdn.test/x2")
    r = await resolve_lx_url(client, song)
    assert r is not None
    assert state["calls"][0]["quality"] == "standard"  # 降 2 档后第一枪


@pytest.mark.anyio
async def test_fresh_refetch_when_upstream_serves_dead_url():
    """服务端探活缓存仍下发死链 → fresh=1 强制重解析一趟拿到活链。"""
    song = "lx:kg:DEADBEEF00000003"
    dead, live = "http://cdn.test/a.flac", "http://cdn.test/b.flac"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/api/v1/track/url":
            return httpx.Response(404)
        state["calls"].append(dict(request.url.params))
        url = live if request.url.params.get("fresh") == "1" else dead
        return httpx.Response(200, json={"ok": True, "data": {"id": song, "url": url, "ext": "flac"}})

    state = {"calls": []}
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://127.0.0.1:8772")
    _lx_mark_url_dead(song, dead)
    r = await resolve_lx_url(client, song)
    assert r["url"] == live
    assert any(c.get("fresh") == "1" for c in state["calls"])


@pytest.mark.anyio
async def test_dead_url_not_recached_when_fresh_still_dead():
    """强制重解析仍拿不到活链 → 本轮判失败进冷却，绝不把死链写回缓存。"""
    song = "lx:kg:DEADBEEF00000004"
    dead = "http://cdn.test/a.flac"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/api/v1/track/url":
            return httpx.Response(404)
        return httpx.Response(200, json={"ok": True, "data": {"id": song, "url": dead, "ext": "flac"}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://127.0.0.1:8772")
    _lx_mark_url_dead(song, dead)
    r = await resolve_lx_url(client, song)
    assert r is None
    assert song not in _LX_URL_CACHE


class MidFailStream(httpx.AsyncByteStream):
    """交付第一块后断流。"""

    def __init__(self):
        self.closed = False

    async def __aiter__(self):
        yield b"x" * 1024
        raise httpx.ReadError("connection lost")

    async def aclose(self):
        self.closed = True


class ResumeStream(httpx.AsyncByteStream):
    def __init__(self, payload: bytes):
        self.payload = payload
        self.closed = False

    async def __aiter__(self):
        for i in range(0, len(self.payload), 1024):
            yield self.payload[i:i + 1024]

    async def aclose(self):
        self.closed = True


def _partial_resp(stream, offset, total):
    return httpx.Response(
        206, stream=stream,
        headers={
            "content-type": "audio/mpeg",
            "content-range": f"bytes {offset}-{offset + len(b'x') - 1}/{total}",
        },
    )


@pytest.mark.anyio
async def test_mid_stream_reconnect_resumes_and_keeps_playing():
    """断流后 Range 续传成功：同一响应继续出字节，on_death 不触发。"""
    total = 3072
    first = MidFailStream()
    calls = []
    deaths = []

    async def reconnect(offset: int):
        calls.append(offset)
        resumed = httpx.Response(
            206, stream=ResumeStream(b"y" * 1024 + b"z" * 1024),
            headers={"content-type": "audio/mpeg",
                     "content-range": f"bytes {offset}-{total - 1}/{total}"},
        )
        it = resumed.aiter_bytes()
        return resumed, it, await anext(it, b"")

    response = stream_tee_response(
        _partial_resp(first, 0, total), "online:lx:kg:rec-1", None, pre_info={},
        reconnect=reconnect, on_death=lambda: deaths.append(1),
    )
    data = b"".join([c async for c in response.body_iterator])
    assert data == b"x" * 1024 + b"y" * 1024 + b"z" * 1024
    assert calls == [1024]
    assert deaths == []  # 续传成功不算流失败


@pytest.mark.anyio
async def test_mid_stream_abort_marks_dead_exactly_once():
    """续传不可用（reconnect 返回 None）：on_death 恰好记一次，异常照常上抛。"""
    first = MidFailStream()
    deaths = []

    async def reconnect(offset: int):
        return None

    response = stream_tee_response(
        _partial_resp(first, 0, 3072), "online:lx:kg:rec-2", None, pre_info={},
        reconnect=reconnect, on_death=lambda: deaths.append(1),
    )
    with pytest.raises(httpx.ReadError):
        async for _ in response.body_iterator:
            pass
    assert deaths == [1]  # 内层记一次，外层不重复计数


@pytest.mark.anyio
async def test_stream_abort_without_reconnect_marks_dead():
    """没传 reconnect（旧行为兼容）：断流同样要记死链。"""
    first = MidFailStream()
    deaths = []
    response = stream_tee_response(
        _partial_resp(first, 0, 3072), "online:lx:kg:rec-3", None, pre_info={},
        on_death=lambda: deaths.append(1),
    )
    with pytest.raises(httpx.ReadError):
        async for _ in response.body_iterator:
            pass
    assert deaths == [1]
