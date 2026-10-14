"""服务端取链队列已满（503）时代理侧的退避行为。

503 是「稍后再试」而不是这首歌无货：必须立刻停止逐档连打（继续打只会再撞满
同一个队列），也不能把整个平台拖进冷却——冷却会让后面所有同平台歌曲直接 404 跳歌。
"""
import httpx
import pytest

import proxy.app as pa
from proxy.app import (
    CONF,
    _LX_FAIL_COOLDOWN,
    _LX_PLATFORM_COOLDOWN,
    _LX_URL_CACHE,
    resolve_lx_url,
)


@pytest.fixture(autouse=True)
def _clear_lx_state(monkeypatch):
    _LX_URL_CACHE.clear()
    _LX_FAIL_COOLDOWN.clear()
    _LX_PLATFORM_COOLDOWN.clear()
    monkeypatch.setattr(pa, "_LX_QUEUE_BACKOFF_UNTIL", 0.0)
    monkeypatch.setattr(pa, "_LX_QUEUE_RETRY_DELAY_S", 0.01)
    monkeypatch.setitem(CONF, "quality_mode", "high")
    monkeypatch.setitem(CONF, "lx_quality", "lossless")


@pytest.mark.anyio
async def test_queue_full_retries_once_then_backs_off_globally():
    """503 补一枪仍满 → 打开全局短时退避；不写单曲 15s 冷却、不开平台冷却。"""
    hits = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        hits["n"] += 1
        return httpx.Response(
            503, json={"ok": False, "error": "resolve failed: upstream queue full"}
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://127.0.0.1:8772"
    )
    try:
        assert await resolve_lx_url(client, "lx:kw:123456") is None
        # 退避窗口内后续歌曲一律先收手，不再撞队列（雪崩就是这么止住的）
        assert await resolve_lx_url(client, "lx:tx:999") is None
    finally:
        await client.aclose()

    assert hits["n"] == 2, f"应当只补一枪就收手，实际打了 {hits['n']} 次"
    assert pa._LX_QUEUE_BACKOFF_UNTIL > 0.0, "队列仍满时应打开全局退避"
    assert not _LX_PLATFORM_COOLDOWN, "队列已满不该打开平台冷却"
    assert not _LX_FAIL_COOLDOWN, "队列已满不是无货，不该写单曲 15s 冷却"


@pytest.mark.anyio
async def test_background_resolve_marks_header_and_prefetch_is_background():
    """预取走后台标记，服务端才知道该把它排到播放后面。"""
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("x-fnmusic-bg"))
        return httpx.Response(
            200, json={"ok": True, "data": {"id": "x", "url": "http://audio.test/a.mp3",
                                            "ext": "mp3"}}
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://127.0.0.1:8772"
    )
    try:
        assert await resolve_lx_url(client, "lx:kw:111", background=True)
        assert await resolve_lx_url(client, "lx:kw:222")
    finally:
        await client.aclose()

    assert seen == ["1", None], seen
