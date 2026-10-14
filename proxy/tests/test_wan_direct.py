"""方案B（外网直连CDN，2.6.26）：裸请求验证 + 307 直连 + 自动回落现有中转路径。"""
import os
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from proxy import app as app_module
from proxy.app import CONF, app
from proxy.app import _SEARCH_CACHE

AUDIO = b"WAN_DIRECT_CDN_FLAC_TEST_PAYLOAD_" * 80
GUID = "online:lx:kg:WANDIRECT123456"
LX_ID = "lx:kg:WANDIRECT123456"
CDN_URL = "http://cdn.wan-direct.test/kg_track.flac"


def _lx_service_handler(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/api/v1/track/url":
        assert request.url.params.get("id") == LX_ID
        return httpx.Response(
            200,
            json={
                "ok": True,
                "data": {
                    "id": LX_ID,
                    "url": CDN_URL,
                    "ext": "flac",
                    "br": 999000,
                    "file_size": len(AUDIO),
                    "headers": {},
                },
            },
        )
    if request.url.path == "/api/v1/track/info":
        return httpx.Response(
            200,
            json={
                "ok": True,
                "data": {
                    "id": LX_ID,
                    "lx_source": "kg",
                    "title": "测试曲",
                    "artist": "测试歌手",
                    "duration_s": 100.0,
                    "ext": "flac",
                    "file_size": len(AUDIO),
                },
            },
        )
    return httpx.Response(404)


def _install_cdn(monkeypatch, reject_without_ua: bool = False, status: int = 200):
    """给所有未显式指定 transport 的 AsyncClient 挂 mock CDN。

    覆盖方案B的裸请求探测客户端与现有中转路径的取流客户端。
    reject_without_ua=True 时模拟防盗链：无 User-Agent 的请求回 403 文本
    （裸请求带 httpx 默认的 python-httpx UA，会被拒）；中转请求带浏览器
    UA（_MEDIA_UA，含 Mozilla），正常回音频。status 非 200 时模拟死链。
    """
    def cdn_handler(request: httpx.Request) -> httpx.Response:
        assert "cdn.wan-direct.test" in str(request.url)
        ua = request.headers.get("user-agent") or ""
        if reject_without_ua and "Mozilla" not in ua:
            return httpx.Response(403, text="forbidden", headers={"Content-Type": "text/html"})
        if status != 200:
            return httpx.Response(status, text="gone", headers={"Content-Type": "text/html"})
        return httpx.Response(
            200,
            content=AUDIO,
            headers={
                "Content-Type": "audio/flac",
                "Content-Length": str(len(AUDIO)),
                "Accept-Ranges": "bytes",
            },
        )

    orig_init = httpx.AsyncClient.__init__

    def mock_init(self, *args, **kwargs):
        if "base_url" not in kwargs and not kwargs.get("transport"):
            kwargs["transport"] = httpx.MockTransport(cdn_handler)
        orig_init(self, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", mock_init)


@pytest.fixture()
def lx_env(tmp_path, monkeypatch):
    _SEARCH_CACHE.clear()
    app_module._LX_URL_CACHE.clear()
    app_module._LX_FAIL_COOLDOWN.clear()
    app_module._LX_URL_PROBE.clear()
    app_module._LX_DEAD_URLS.clear()
    app_module._LX_PREFETCH_RUNNING.clear()
    app_module._PREOPEN_STREAMS.clear()
    app_module._PREOPEN_INFLIGHT.clear()
    app_module.reset_probe_client()
    os.makedirs(str(tmp_path / "library"), exist_ok=True)
    os.makedirs(str(tmp_path / "fav"), exist_ok=True)
    monkeypatch.setitem(CONF, "cache_dir", str(tmp_path / "cache"))
    monkeypatch.setitem(CONF, "library_dir", str(tmp_path / "library"))
    monkeypatch.setitem(CONF, "fav_dir", str(tmp_path / "fav"))
    monkeypatch.setitem(CONF, "search_list_path", "data.list")
    monkeypatch.setitem(CONF, "online_limit", 30)
    monkeypatch.setitem(CONF, "netease_search_limit", 50)
    monkeypatch.setitem(CONF, "merge_suggest", False)
    monkeypatch.setitem(CONF, "lyric_field", "data.lyric")
    monkeypatch.setitem(CONF, "musicdl_enabled", False)
    monkeypatch.setitem(CONF, "netease_enabled", False)
    monkeypatch.setitem(CONF, "lx_enabled", True)
    monkeypatch.setitem(CONF, "lx_search_limit", 20)
    monkeypatch.setitem(CONF, "lx_quality", "lossless")
    monkeypatch.setitem(CONF, "search_cache_ttl", 300.0)
    monkeypatch.setitem(CONF, "late_page_wait_s", 5.0)
    monkeypatch.setitem(CONF, "search_debounce_s", 0.0)
    monkeypatch.setitem(CONF, "tee_save_enabled", False)
    monkeypatch.setitem(CONF, "wan_direct_enabled", True)
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(500)), base_url="http://unix"
    )
    app.state.lx_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_lx_service_handler), base_url="http://127.0.0.1:8772"
    )
    yield
    for attr in ("upstream_client", "musicbox_client", "lx_client", "musicdl_client"):
        setattr(app.state, attr, None)
    app_module.reset_probe_client()


def test_wan_direct_redirect_when_probe_ok(lx_env, monkeypatch):
    """直链裸请求验证通过：307 重定向到 CDN 直链，客户端直连取流。"""
    _install_cdn(monkeypatch)
    with TestClient(app) as client:
        resp = client.get(
            f"/music/api/v1/track/stream?guid={GUID}", follow_redirects=False
        )
    assert resp.status_code == 307
    assert resp.headers["location"] == CDN_URL
    assert resp.headers.get("cache-control") == "no-store"


def test_wan_direct_falls_back_when_probe_rejected(lx_env, monkeypatch):
    """CDN 拒绝裸请求（防盗链）：自动回落现有中转路径，照常返回音频。"""
    _install_cdn(monkeypatch, reject_without_ua=True)
    with TestClient(app) as client:
        resp = client.get(f"/music/api/v1/track/stream?guid={GUID}")
    assert resp.status_code == 200
    assert resp.content == AUDIO
    assert resp.headers.get("content-type") == "audio/flac"


def test_wan_direct_disabled_never_redirects(lx_env, monkeypatch):
    """开关关闭：完全走现有中转路径（旧行为）。"""
    _install_cdn(monkeypatch)
    monkeypatch.setitem(CONF, "wan_direct_enabled", False)
    with TestClient(app) as client:
        resp = client.get(f"/music/api/v1/track/stream?guid={GUID}")
    assert resp.status_code == 200
    assert resp.content == AUDIO


@pytest.mark.anyio
async def test_wan_direct_probe_accepts_audio(lx_env, monkeypatch):
    _install_cdn(monkeypatch)
    ok, detail = await app_module._wan_direct_probe(CDN_URL, None)
    assert ok, detail
    assert "status=200" in detail


@pytest.mark.anyio
async def test_wan_direct_probe_accepts_range_206(lx_env, monkeypatch):
    _install_cdn(monkeypatch)
    ok, detail = await app_module._wan_direct_probe(CDN_URL, "bytes=0-9")
    assert ok, detail


@pytest.mark.anyio
async def test_wan_direct_probe_rejects_hotlink_guard(lx_env, monkeypatch):
    _install_cdn(monkeypatch, reject_without_ua=True)
    ok, detail = await app_module._wan_direct_probe(CDN_URL, None)
    assert not ok
    assert "status=403" in detail


@pytest.mark.anyio
async def test_wan_direct_skips_unsupported_source(lx_env):
    """非 lx/netease 源（酷我等 musicdl 通道）不介入，走现有中转。"""
    assert await app_module._wan_direct_redirect(None, "online:kw:123", None) is None


@pytest.mark.anyio
async def test_wan_direct_skips_disabled_provider(lx_env, monkeypatch):
    """源未启用时不介入。"""
    monkeypatch.setitem(CONF, "lx_enabled", False)
    assert await app_module._wan_direct_redirect(None, GUID, None) is None
    monkeypatch.setitem(CONF, "lx_enabled", True)
    monkeypatch.setitem(CONF, "netease_enabled", False)
    assert await app_module._wan_direct_redirect(None, "online:netease:12345", None) is None


# === 2.6.53 预取探测验证（树夏方案移植）===

def _bg_request():
    from fastapi import Request
    return Request({
        "type": "http", "http_version": "1.1", "method": "GET", "path": "/internal",
        "raw_path": b"/internal", "query_string": b"", "root_path": "", "scheme": "http",
        "headers": [(b"host", b"127.0.0.1")], "client": ("127.0.0.1", 0),
        "server": ("127.0.0.1", 0), "app": app,
    })


@pytest.mark.anyio
async def test_wan_direct_skips_inline_probe_when_prefetch_verified(lx_env, monkeypatch):
    """预取已按手机口径验证过该链（同 URL、新鲜窗口内）：播放主路径跳过 inline 探测。"""
    calls = []

    async def _fake_probe(url, range_header, headers=None):
        calls.append(url)
        return False, "status=403 ctype=text/html"

    monkeypatch.setattr(app_module, "_wan_direct_probe", _fake_probe)
    app_module._LX_URL_PROBE[LX_ID] = (CDN_URL, time.time(), "direct")
    resp = await app_module._wan_direct_redirect(_bg_request(), GUID, None)
    assert resp is not None
    assert resp.status_code == 307
    assert resp.headers["location"] == CDN_URL
    assert calls == []  # 确实没有再打 inline 探测


@pytest.mark.anyio
async def test_prefetch_probe_records_direct(lx_env, monkeypatch):
    """裸探测 200 音频 → direct，播放快路径可用。"""
    _install_cdn(monkeypatch)
    await app_module._prefetch_probe_verify(LX_ID, CDN_URL, {})
    assert app_module._LX_URL_PROBE[LX_ID][2] == "direct"
    assert app_module._lx_url_probe_ok(LX_ID, CDN_URL)


@pytest.mark.anyio
async def test_prefetch_probe_404_marks_dead_without_tier_penalty(lx_env, monkeypatch):
    """裸探测 404 → dead：记死链、清 URL 缓存，但**不计流失败**（不触发音质降档）。"""
    _install_cdn(monkeypatch, status=404)
    app_module._LX_URL_CACHE[LX_ID] = ({"url": CDN_URL, "headers": {}}, time.time())
    await app_module._prefetch_probe_verify(LX_ID, CDN_URL, {})
    assert app_module._LX_URL_PROBE[LX_ID][2] == "dead"
    assert CDN_URL in app_module._lx_dead_urls(LX_ID)
    assert LX_ID not in app_module._LX_URL_CACHE
    assert app_module._LX_STREAM_FAILS.get(LX_ID, 0) == 0


@pytest.mark.anyio
async def test_prefetch_probe_hotlink_becomes_relay(lx_env, monkeypatch):
    """裸 403 + 带头 200 → relay（防盗链，链仍活着）：不算死链，也不给播放 307 快路径。"""
    _install_cdn(monkeypatch, reject_without_ua=True)
    await app_module._prefetch_probe_verify(
        LX_ID, CDN_URL, {"User-Agent": "Mozilla/5.0", "Referer": "https://www.kugou.com/"})
    assert app_module._LX_URL_PROBE[LX_ID][2] == "relay"
    assert not app_module._lx_url_probe_ok(LX_ID, CDN_URL)


@pytest.mark.anyio
async def test_prefetch_probe_wan_direct_disabled_is_noop(lx_env, monkeypatch):
    """方案B 关闭时不做探测（也不产生任何真实网络请求）。"""
    monkeypatch.setitem(CONF, "wan_direct_enabled", False)
    await app_module._prefetch_probe_verify(LX_ID, CDN_URL, {})
    assert LX_ID not in app_module._LX_URL_PROBE


# === 2.6.54 下一首预开流（preopen）===
import asyncio  # noqa: E402

from proxy.app import song_id_from_online_guid  # noqa: E402

NEXT_GUID = "online:lx:kg:PREOPENNEXT001"
NEXT_LX_ID = song_id_from_online_guid(NEXT_GUID)


def _fake_opened(payload: bytes = AUDIO) -> tuple:
    """模拟 _open_online_stream 返回的 8 元组（resp, owned, ext, info, chunks, first, reconnect, on_death）。"""
    resp = httpx.Response(
        200,
        content=payload,
        headers={
            "Content-Type": "audio/flac",
            "Content-Length": str(len(payload)),
            "Accept-Ranges": "bytes",
        },
    )
    first = payload[:32]

    async def _chunks():
        yield payload[32:]

    return (resp, None, "flac", {}, _chunks(), first, None, None)


def _seed_preopen(payload: bytes = AUDIO) -> tuple[str, tuple]:
    entry = {
        "opened": _fake_opened(payload),
        "expires_at": time.monotonic() + 60,
    }
    scope = app_module._credential_scope(_bg_request())
    app_module._PREOPEN_STREAMS[app_module._preopen_key(scope, NEXT_GUID)] = entry
    return scope, entry["opened"]


async def _wait_closed(resp, tries: int = 50) -> None:
    for _ in range(tries):
        if resp.is_closed:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("preopen stream was not closed")


@pytest.mark.anyio
async def test_preopen_take_hit_consumes_entry(lx_env):
    """切歌命中预开流：直接领取 8 元组，条目移除。"""
    _scope, opened = _seed_preopen()
    got = app_module._take_preopen(NEXT_GUID, None, _scope)
    assert got is opened
    assert not app_module._PREOPEN_STREAMS


@pytest.mark.anyio
async def test_preopen_take_rejects_seek_and_closes(lx_env):
    """seek（非从头 Range）不匹配预开流：丢弃并关闭，走正常路径。"""
    _scope, opened = _seed_preopen()
    got = app_module._take_preopen(NEXT_GUID, "bytes=1048576-", _scope)
    assert got is None
    assert not app_module._PREOPEN_STREAMS
    await _wait_closed(opened[0])


@pytest.mark.anyio
async def test_preopen_take_expired_closes(lx_env):
    """预开流过期（TTL 内没人切）：丢弃并关闭。"""
    opened = _fake_opened()
    entry = {"opened": opened, "expires_at": time.monotonic() - 1}
    scope = app_module._credential_scope(_bg_request())
    app_module._PREOPEN_STREAMS[app_module._preopen_key(scope, NEXT_GUID)] = entry
    assert app_module._take_preopen(NEXT_GUID, None, scope) is None
    await _wait_closed(opened[0])


@pytest.mark.anyio
async def test_preopen_next_track_opens_cdn_stream(lx_env, monkeypatch):
    """URL 已被预取缓存：预开只连 CDN 取首包，不碰音源取链。"""
    calls = []

    async def _fake_open(request, guid, range_header, force_mp3=False):
        calls.append((guid, range_header))
        return _fake_opened()

    monkeypatch.setattr(app_module, "_open_online_stream", _fake_open)
    app_module._LX_URL_CACHE[NEXT_LX_ID] = ({"url": CDN_URL, "headers": {}}, time.time())
    await app_module._preopen_next_track(_bg_request(), NEXT_GUID)
    assert calls == [(NEXT_GUID, "bytes=0-")]
    scope = app_module._credential_scope(_bg_request())
    assert app_module._PREOPEN_STREAMS[app_module._preopen_key(scope, NEXT_GUID)]["opened"]
    # 清理：取消 TTL 任务
    entry = app_module._PREOPEN_STREAMS.pop(app_module._preopen_key(scope, NEXT_GUID))
    entry["cleanup_task"].cancel()
    await app_module._close_stream_preview(entry)


@pytest.mark.anyio
async def test_preopen_next_track_skips_without_cached_url(lx_env, monkeypatch):
    """无缓存直链且预取未跑：预开绝不自调音源取链（不烧配额），直接放弃。"""
    calls = []

    async def _fake_open(request, guid, range_header, force_mp3=False):
        calls.append(guid)
        return _fake_opened()

    monkeypatch.setattr(app_module, "_open_online_stream", _fake_open)
    app_module._LX_URL_CACHE.pop(NEXT_LX_ID, None)
    await app_module._preopen_next_track(_bg_request(), NEXT_GUID)
    assert calls == []
    assert not app_module._PREOPEN_STREAMS


@pytest.mark.anyio
async def test_stream_track_consumes_preopen_without_redirect(lx_env, monkeypatch):
    """端到端：命中预开直接出音频（不走 307 重定向，手机不用自己连 CDN）。"""
    _scope, _opened = _seed_preopen()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/music/api/v1/track/stream?guid={NEXT_GUID}")
    assert resp.status_code == 200
    assert resp.content == AUDIO
    assert resp.headers.get("content-type") == "audio/flac"
    assert not app_module._PREOPEN_STREAMS
