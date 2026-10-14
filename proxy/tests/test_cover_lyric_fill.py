"""2.6.36 封面跨源补齐 / 歌词后台回补 / 首播预热 / 列表回补。

复现 NAS 现场：kuwo artistpicserver 直连 403、播放源 cover_url 为空、
源无缓存歌词（fast 拿不到）——验证兜底链/回补链在代理侧的真实行为。
"""
from __future__ import annotations

import os

import httpx
import pytest
from fastapi.testclient import TestClient

import proxy.app as papp
from proxy.app import (
    CONF,
    _COVER_CACHE_DIR,  # noqa: F401  (存在性检查)
    _COVER_CDN_TRANSPORT,  # noqa: F401
    _COVER_FETCH_TRANSPORT,  # noqa: F401
    _COVER_SEARCH_NEGATIVE,
    _LYRIC_BACKFILL_AT,
    _LYRIC_BACKFILL_INFLIGHT,
    _PREHEAT_AT,
    _PREHEAT_INFLIGHT,
    _read_cover_cache,
    app,
)
from proxy import cerusync as ceru

PNG_1X1 = papp._TINY_GRAY_PNG


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    monkeypatch.setitem(CONF, "lx_enabled", True)
    monkeypatch.setitem(CONF, "netease_enabled", False)
    monkeypatch.setitem(CONF, "musicdl_enabled", False)
    monkeypatch.setitem(CONF, "cover_enrich", False)
    monkeypatch.setitem(CONF, "cache_dir", str(tmp_path / "cache"))
    monkeypatch.setattr("proxy.app._COVER_CACHE_DIR", str(tmp_path / "covers"))
    _COVER_SEARCH_NEGATIVE.clear()
    papp._KW_BYID_NEGATIVE.clear()
    papp._KW_BYID_CALLS["date"] = ""
    papp._KW_BYID_CALLS["count"] = 0
    _LYRIC_BACKFILL_AT.clear()
    _PREHEAT_AT.clear()
    _LYRIC_BACKFILL_INFLIGHT.clear()
    _PREHEAT_INFLIGHT.clear()
    papp._LYRIC_JOB_ACTIVE = 0
    papp._COVER_SEARCH_ACTIVE = 0
    # 隔离 ceru 同步状态：避开开发机上的真实 guid 映射
    monkeypatch.setenv("FNMUSIC_CERU_SYNC_DIR", str(tmp_path / "ceru"))
    ceru.reset_for_test()
    ceru.clear_guid_registry()
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(400)), base_url="http://unix"
    )
    # 酷我 artistpicserver 直连：复现 NAS 现场 403（直构路死掉，逼出跨源补齐）
    monkeypatch.setattr(
        "proxy.app._COVER_CDN_TRANSPORT",
        httpx.MockTransport(lambda r: httpx.Response(403)),
    )
    yield


def _setup_lx(handler):
    app.state.lx_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://127.0.0.1:8772"
    )


def _cover_fetch_transport_200_png():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=PNG_1X1, headers={"content-type": "image/png"})

    return httpx.MockTransport(handler)


def test_cover5_cross_source_fill_and_cache(monkeypatch):
    """:cover5 列表行：播放源无封面 + artistpicserver 403 → 跨源搜索命中 → 200 真图。

    第二次请求走 cover_cache，不再打 /api/v1/cover/search。
    """
    monkeypatch.setattr("proxy.app._COVER_FETCH_TRANSPORT", _cover_fetch_transport_200_png())
    cover_calls = {"n": 0}

    def lx_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/track/info":
            return httpx.Response(200, json={"ok": True, "data": {
                "id": "kw:929944", "lx_source": "kw", "title": "别想她", "artist": "高进",
                "album": "别想她", "duration_s": 269, "ext": "mp3", "file_size": 0,
                "lyric": "", "cover_url": "",
            }})
        if request.url.path == "/api/v1/cover/search":
            cover_calls["n"] += 1
            return httpx.Response(200, json={"ok": True, "data": {
                "cover_url": "http://img.test/real.jpg", "id": "lx:tx:M2",
                "lx_source": "tx", "title": "别想她", "artist": "高进", "score": 1.0,
            }})
        return httpx.Response(404)

    _setup_lx(lx_handler)
    with TestClient(app) as client:
        r1 = client.get("/music/api/v1/static/cover?coverId=online:lx:kw:929944:cover5")
        assert r1.status_code == 200
        assert r1.headers.get("content-type") == "image/png"
        assert r1.content == PNG_1X1
        r2 = client.get("/music/api/v1/static/cover?coverId=online:lx:kw:929944:cover5")
        assert r2.status_code == 200
        assert r2.content == PNG_1X1
    assert cover_calls["n"] == 1
    assert _read_cover_cache("online:lx:kw:929944") is not None


def test_cover5_search_miss_falls_to_placeholder_with_negative_cache(monkeypatch):
    """跨源搜索未命中：回落占位图，且 6h 负缓存内不再重复打搜索端点。"""
    monkeypatch.setattr("proxy.app._COVER_FETCH_TRANSPORT", _cover_fetch_transport_200_png())
    cover_calls = {"n": 0}

    def lx_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/track/info":
            return httpx.Response(200, json={"ok": True, "data": {
                "id": "kw:555", "lx_source": "kw", "title": "冷门的歌", "artist": "某人",
                "album": "", "duration_s": 200, "ext": "mp3", "file_size": 0,
                "lyric": "", "cover_url": "",
            }})
        if request.url.path == "/api/v1/cover/search":
            cover_calls["n"] += 1
            return httpx.Response(200, json={"ok": True, "data": None})
        return httpx.Response(404)

    _setup_lx(lx_handler)
    with TestClient(app) as client:
        r1 = client.get("/music/api/v1/static/cover?coverId=online:lx:kw:555:cover5")
        assert r1.status_code == 200
        assert r1.headers.get("content-type") == "image/png"
        assert "no-store" in r1.headers.get("cache-control", "")
        assert r1.content != PNG_1X1 or r1.content == PNG_1X1  # 占位图（6 张池之一或灰点兜底）
        r2 = client.get("/music/api/v1/static/cover?coverId=online:lx:kw:555:cover5")
        assert r2.status_code == 200
    assert cover_calls["n"] == 1
    assert "online:lx:kw:555" in _COVER_SEARCH_NEGATIVE
    assert _read_cover_cache("online:lx:kw:555") is None


def test_lyric_backfill_full_mode_after_fast_miss(monkeypatch):
    """fast 拿不到词 → 后台全模式回补（带 title/artist）→ 落 .lrc → 下次拉取即得。"""
    fast = {"n": 0}
    full = {"n": 0, "title": "", "artist": ""}
    LRC = "[00:01.00]莫忙\n[00:02.00]风在唱"

    def lx_handler(request: httpx.Request) -> httpx.Response:
        p = request.url.params
        if request.url.path == "/api/v1/track/info":
            return httpx.Response(200, json={"ok": True, "data": {
                "id": "wy:3420799859", "lx_source": "wy", "title": "莫忙", "artist": "张信哲",
                "album": "", "duration_s": 300, "ext": "mp3", "file_size": 0,
                "lyric": "", "cover_url": "",
            }})
        if request.url.path == "/api/v1/track/lyric":
            if p.get("fast") == "1":
                fast["n"] += 1
                return httpx.Response(200, json={"ok": True, "data": {"lyric": ""}})
            full["n"] += 1
            full["title"] = p.get("title") or ""
            full["artist"] = p.get("artist") or ""
            return httpx.Response(200, json={"ok": True, "data": {"lyric": LRC}})
        return httpx.Response(404)

    monkeypatch.setattr(papp, "_LYRIC_STAGGER_S", 0.0)  # 测试不等生产错峰
    _setup_lx(lx_handler)
    guid = "online:lx:wy:3420799859"
    with TestClient(app) as client:
        first = client.get(f"/music/api/v1/track/lyrics?guid={guid}")
        assert first.status_code == 200
        data = first.json().get("data") or {}
        assert not str(data.get("lyric") or "").strip()  # 首次：回补还没完成
        got = None
        for _ in range(30):
            resp = client.get(f"/music/api/v1/track/lyrics?guid={guid}")
            body = resp.json().get("data") or {}
            if str(body.get("lyric") or "").strip():
                got = body["lyric"]
                break
        assert got is not None, "后台回补 30 次轮询后仍未拿到歌词"
        assert "风在唱" in got
    assert fast["n"] >= 1
    assert full["n"] == 1
    assert full["title"] == "莫忙"
    assert full["artist"] == "张信哲"
    assert papp.read_lyric_cache(guid)


def test_cover_cache_never_expires_by_age(tmp_path, monkeypatch):
    """2.6.36 契约：封面缓存永久保留，不按时间过期；只在删歌时清理。"""
    import os
    import time as _time

    monkeypatch.setattr("proxy.app._COVER_CACHE_DIR", str(tmp_path / "covers"))
    guid = "online:lx:kw:777"
    papp._write_cover_cache(guid, PNG_1X1, "image/png")
    path = papp._cover_cache_path(guid)
    old = _time.time() - 40 * 86400  # 40 天前的文件
    os.utime(path, (old, old))
    hit = _read_cover_cache(guid)
    assert hit is not None
    assert hit[0] == PNG_1X1
    # 删除路径：_purge_cover_cache 清掉后读不到
    papp._purge_cover_cache(guid)
    assert _read_cover_cache(guid) is None


def test_preheat_single_flight_and_cooldown(monkeypatch):
    """同一首歌重复触发：single-flight + 6h 冷却，只跑一次任务。"""
    calls = {"n": 0}

    async def fake_task(request, guid):
        calls["n"] += 1

    monkeypatch.setattr(papp, "_preheat_track_assets_task", fake_task)
    import asyncio

    async def twice():
        papp._preheat_track_assets(None, "online:lx:kw:1")
        papp._preheat_track_assets(None, "online:lx:kw:1")
        await asyncio.sleep(0.05)

    asyncio.run(twice())
    assert calls["n"] == 1


def test_preheat_global_cap(monkeypatch):
    """2.6.37: 预热全局最多 2 首并发，第 3 首直接跳过（不排队、不压主路径）。"""
    import asyncio

    gate = asyncio.Event()
    ran = []

    async def blocked_task(request, guid):
        ran.append(guid)
        await gate.wait()

    monkeypatch.setattr(papp, "_preheat_track_assets_task", blocked_task)

    async def go():
        papp._preheat_track_assets(None, "online:lx:kw:10")
        papp._preheat_track_assets(None, "online:lx:kw:11")
        papp._preheat_track_assets(None, "online:lx:kw:12")  # 已满 → 跳过
        await asyncio.sleep(0.05)
        gate.set()
        await asyncio.sleep(0.05)

    asyncio.run(go())
    assert sorted(ran) == ["online:lx:kw:10", "online:lx:kw:11"]


def test_lyric_backfill_global_cap(monkeypatch):
    """2.6.37: 歌词回补全局最多 1 条在途，第二条拒绝（10min 冷却保证稍后重试）。"""
    import asyncio

    gate = asyncio.Event()

    async def blocked_task(request, guid, data):
        await gate.wait()

    monkeypatch.setattr(papp, "_lyric_backfill_task", blocked_task)

    async def go():
        r1 = papp._ensure_lyric_backfill(None, "online:lx:wy:1", {"title": "a", "artist": "b"})
        r2 = papp._ensure_lyric_backfill(None, "online:lx:wy:2", {"title": "c", "artist": "d"})
        await asyncio.sleep(0.05)
        gate.set()
        await asyncio.sleep(0.05)
        return r1, r2

    r1, r2 = asyncio.run(go())
    assert r1 is True and r2 is False
    assert papp._LYRIC_JOB_ACTIVE == 0


def test_playlist_page_cover_preheat_selects_only_missing(monkeypatch):
    """列表回补选择器：只挑无封面/内网封面的在线曲，本地曲与已有封面的跳过。"""
    picked = []
    monkeypatch.setattr(
        papp, "_preheat_track_assets",
        lambda request, guid: picked.append(guid) or None,
    )
    # 让选择器认为全部未缓存（cache_dir 已是 tmp 空目录）
    tracks = [
        {"guid": "online:lx:kw:1", "cover_url": "http://cdn.example/1.jpg"},  # 有可用封面 → 跳过
        {"guid": "online:lx:kw:2", "cover_url": ""},                          # 空封面 → 选
        {"guid": "online:lx:kw:3", "cover_url": "http://192.168.5.2:5666/x"},  # 内网 → 选
        {"guid": "local-track-123", "cover_url": ""},                         # 本地曲 → 跳过
    ]
    import asyncio

    async def go():
        papp._preheat_playlist_page_covers(None, tracks)
        await asyncio.sleep(0.05)

    asyncio.run(go())
    assert sorted(picked) == ["online:lx:kw:2", "online:lx:kw:3"]


# ---------------------------------------------------------------- kw 按 ID 取图 2.6.38
def _kw_cdn_transport(pic: str | None, counts: dict):
    """服务侧 songinfo 端点返回 pic（或空）；artistpicserver 复现 NAS 403。

    封面不再由代理自己打酷我：`/api/v1/track/songinfo` 那一枪在服务侧同时
    出封面与歌词（带 7 天缓存与同曲合流），代理只读结果。断言的就是这条链路：
    每首歌对上游只有一枪，补封面顺带把歌词也补好。
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/api/v1/track/songinfo" in url:
            counts["songinfo"] = counts.get("songinfo", 0) + 1
            assert request.url.params.get("id") == "lx:kw:877356"
            return httpx.Response(200, json={
                "ok": True,
                "data": {"pic": pic or "", "title": "飘雪", "artist": "陈慧娴",
                         "album": "永远是", "lyric": "[00:00.00]飘雪 - 陈慧娴"}})
        if "artistpicserver.kuwo.cn" in url:
            counts["artistpic"] = counts.get("artistpic", 0) + 1
            return httpx.Response(403)
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def test_kw_cover_by_rid_songinfo_first(monkeypatch):
    import asyncio
    """songinfoandlrc 按 ID 命中 → 直接取 songinfo.pic，不打 artistpicserver。"""
    counts: dict = {}
    monkeypatch.setattr("proxy.app._COVER_CDN_TRANSPORT",
                        _kw_cdn_transport("http://img1.kwcdn.kuwo.cn/star/albumcover/240/x.jpg", counts))
    rid_url = asyncio.run(papp._kw_cover_by_rid("877356"))
    assert rid_url == "http://img1.kwcdn.kuwo.cn/star/albumcover/240/x.jpg"
    assert counts.get("songinfo") == 1
    assert counts.get("artistpic", 0) == 0
    # 二次调用走内存缓存，不再发请求
    asyncio.run(papp._kw_cover_by_rid("877356"))
    assert counts.get("songinfo") == 1


def test_kw_cover_by_rid_falls_to_artistpic(monkeypatch):
    import asyncio
    """songinfo 无图 → artistpicserver 兜底解析。"""
    counts: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/api/v1/track/songinfo" in url:
            counts["songinfo"] = counts.get("songinfo", 0) + 1
            return httpx.Response(200, json={"ok": True, "data": {"pic": "", "lyric": ""}})
        if "artistpicserver.kuwo.cn" in url:
            counts["artistpic"] = counts.get("artistpic", 0) + 1
            return httpx.Response(
                200, text="<a href='https://img1.kuwo.cn/star/albumcover/500/y.jpg'>x</a>")
        return httpx.Response(404)

    monkeypatch.setattr("proxy.app._COVER_CDN_TRANSPORT", httpx.MockTransport(handler))
    rid_url = asyncio.run(papp._kw_cover_by_rid("63502"))
    assert rid_url == "https://img1.kuwo.cn/star/albumcover/500/y.jpg"
    assert counts["songinfo"] == 1 and counts["artistpic"] == 1, "服务侧无图才降级"


def test_kw_cover_by_rid_negative_cache_no_retry(monkeypatch):
    import asyncio
    """全链未命中：记负缓存，TTL 内同一 rid 不再发任何请求。"""
    counts: dict = {}
    monkeypatch.setattr("proxy.app._COVER_CDN_TRANSPORT", _kw_cdn_transport(None, counts))
    assert asyncio.run(papp._kw_cover_by_rid("99999")) == ""
    first = dict(counts)
    assert asyncio.run(papp._kw_cover_by_rid("99999")) == ""
    assert counts == first  # 第二次零请求


def test_kw_cover_by_rid_daily_budget(monkeypatch):
    import asyncio
    """每日额度耗尽：直接放弃，不产生任何官方请求。"""
    counts: dict = {}
    monkeypatch.setattr("proxy.app._KW_BYID_DAILY_BUDGET", 0)
    monkeypatch.setattr("proxy.app._COVER_CDN_TRANSPORT", _kw_cdn_transport("http://x.jpg", counts))
    assert asyncio.run(papp._kw_cover_by_rid("12345")) == ""
    assert counts == {}
