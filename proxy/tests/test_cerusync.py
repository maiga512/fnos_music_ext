"""Ceru NAS 同步歌单/收藏集成测试。"""
import asyncio
import time
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from proxy import cerusync as ceru
from proxy.app import CONF, app


PLAYLISTS = [
    {"id": "pl-fav", "title": "我的喜欢", "semanticType": "favorites",
     "position": 0, "total": 2, "coverUrl": "http://img/fav.jpg",
     "createdAt": 1700000000, "updatedAt": 1700000100},
    {"id": "pl-1", "title": "周末歌单", "position": 1, "total": 1,
     "coverUrl": "", "createdAt": 1700000000, "updatedAt": 1700000200},
]

SONGS = {
    "pl-fav": [
        {"id": "101", "source": "wy", "title": "晴天", "artist": "周杰伦",
         "album": "叶惠美", "durationText": "04:29", "artworkUrl": "http://img/a.jpg"},
        {"id": "hashabc", "source": "kg", "title": "七里香", "artist": "周杰伦",
         "album": "七里香", "durationText": "04:56", "artworkUrl": "http://img/b.jpg",
         "hash": "b3a52a7a958bf0aed0ebfba2e9a818b7"},
    ],
    "pl-1": [
        {"id": "202", "source": "tx", "title": "稻香", "artist": "周杰伦",
         "album": "魔杰座", "durationText": "03:43", "artworkUrl": "http://img/c.jpg"},
    ],
}

FAVORITES = {
    "song": [
        {"entityType": "song", "entityId": "101", "title": "晴天",
         "source": "wy", "coverUrl": "http://img/a.jpg", "position": 0},
    ],
    "playlist": [],
}


def _ceru_handler():
    state = {"fav_create": 0, "fav_delete": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/playlists":
            return httpx.Response(200, json=PLAYLISTS)
        if path.startswith("/playlists/"):
            pid = path.split("/")[2]
            return httpx.Response(200, json={"songs": SONGS.get(pid, [])})
        if path == "/favorites":
            if request.method == "GET":
                entity_type = request.url.params.get("entityType") or "song"
                return httpx.Response(200, json={"items": FAVORITES.get(entity_type, [])})
            if request.method == "POST":
                state["fav_create"] += 1
                return httpx.Response(200, json={"id": "fav-1", "revision": 1})
            if request.method == "DELETE":
                state["fav_delete"] += 1
                return httpx.Response(200, json={"entityId": "", "revision": 1})
        return httpx.Response(404)

    handler.state = state
    return handler


def _auth_upstream():
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/user/me"):
            return httpx.Response(200, json={"code": 0, "data": {"guid": "user-ceru-1"}})
        if path.endswith("/playlist/list"):
            return httpx.Response(200, json={
                "code": 0,
                "data": {"list": [{"guid": "localpl", "name": "官方歌单", "coverId": "c1",
                                   "createdAt": 1, "updatedAt": 1, "trackCount": 3}],
                         "total": 1},
            })
        if path.endswith("/favorite-track/list"):
            return httpx.Response(200, json={"code": 0, "data": {"list": [], "total": 0}})
        return httpx.Response(200, json={"code": 0, "data": None})

    return handler


@pytest.fixture(autouse=True)
def setup_ceru(tmp_path, monkeypatch):
    ceru.reset_for_test()
    monkeypatch.setenv("FNMUSIC_CERU_SYNC_ENABLED", "true")
    monkeypatch.setenv("FNMUSIC_CERU_SYNC_HOST", "ceru")
    monkeypatch.setenv("FNMUSIC_CERU_SYNC_PORT", "31231")
    monkeypatch.setenv("FNMUSIC_CERU_SYNC_USE_HTTPS", "false")
    monkeypatch.setenv("FNMUSIC_CERU_SYNC_TOKEN", "test-token")
    monkeypatch.setenv("FNMUSIC_CERU_SYNC_DIR", str(tmp_path / "ceru_sync"))
    monkeypatch.setenv("FNMUSIC_FAV_DIR", str(tmp_path / "online_favorites"))
    monkeypatch.setenv("FNMUSIC_PLT_DIR", str(tmp_path / "playlist_tracks"))
    monkeypatch.setenv("FNMUSIC_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("FNMUSIC_MUSIC_DB", str(tmp_path / "missing.db"))
    monkeypatch.setenv("FNMUSIC_RECOMMEND_DIR", str(tmp_path / "recommend_cache"))
    monkeypatch.setenv("FNMUSIC_PLAY_HISTORY_DIR", str(tmp_path / "play_history"))
    monkeypatch.setenv("FNMUSIC_NM_PLAYLISTS_DIR", str(tmp_path / "nm_playlists_cache"))
    monkeypatch.setitem(CONF, "ceru_sync_enabled", True)
    monkeypatch.setitem(CONF, "ceru_sync_host", "ceru")
    monkeypatch.setitem(CONF, "ceru_sync_port", 31231)
    monkeypatch.setitem(CONF, "ceru_sync_use_https", False)
    monkeypatch.setitem(CONF, "lx_enabled", True)
    monkeypatch.setitem(CONF, "netease_enabled", False)
    monkeypatch.setitem(CONF, "musicdl_enabled", False)
    monkeypatch.setitem(CONF, "recommend_hot", False)
    monkeypatch.setitem(CONF, "recommend_daily", False)
    monkeypatch.setitem(CONF, "netease_my_playlists", False)
    monkeypatch.setitem(CONF, "fav_dir", str(tmp_path / "online_favorites"))
    monkeypatch.setitem(CONF, "plt_dir", str(tmp_path / "playlist_tracks"))
    monkeypatch.setitem(CONF, "cache_dir", str(tmp_path / "cache"))
    yield
    ceru.reset_for_test()


def _wire(ceru_handler):
    base_url = ceru.config()["url"]
    ceru._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(ceru_handler), base_url=base_url
    )
    ceru._http_base_url = base_url
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_auth_upstream()), base_url="http://unix"
    )
    return ceru_handler


@pytest.mark.anyio
async def test_sync_revision_without_event_refreshes_caches():
    """服务端 revision 前进但未显式标记变化时也必须清缓存。"""
    sync_seen = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/sync/register":
            return httpx.Response(200, json={"deviceId": "fnos-test"})
        if request.url.path == "/sync":
            sync_seen.set()
            return httpx.Response(200, json={"nextRevision": 7})
        return httpx.Response(404)

    _wire(handler)
    ceru._summary_cache = {"items": [{"total": 1393}], "saved_at": time.monotonic()}
    task = asyncio.create_task(ceru.sync_poll_loop())
    try:
        await asyncio.wait_for(sync_seen.wait(), timeout=1.0)
        await asyncio.sleep(0)
        assert ceru._summary_cache is None
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


def test_lx_item_mapping_for_sources():
    assert ceru.lx_item_for_song({
        "source": "kg", "id": "audio-id", "hash": "b3a52a7a958bf0aed0ebfba2e9a818b7",
        "title": "晴天", "artist": "周杰伦",
    })["id"] == "lx:kg:b3a52a7a958bf0aed0ebfba2e9a818b7"
    assert ceru.lx_item_for_song({"source": "wy", "id": "101", "title": "晴天"})["id"] == "lx:wy:101"
    assert ceru.lx_item_for_song({"source": "tx", "id": "002Kw", "title": "晴天"})["songmid"] == "002Kw"
    assert ceru.lx_item_for_song({"source": "kw", "id": "123", "title": "晴天"})["rid"] == "123"
    assert ceru.lx_item_for_song({"source": "mg", "id": "456", "title": "晴天"})["copyright_id"] == "456"
    assert ceru.lx_item_for_song({"source": "local", "id": "x", "title": "本地"}) is None


@pytest.mark.anyio
async def test_playlist_summary_disk_fallback_refreshes_on_next_request(tmp_path):
    """磁盘摘要作为过期兜底时，后台刷新完成后下一次请求必须看到新计数。"""
    ceru._atomic_write_json(ceru._disk_playlists_path(), {
        "savedAt": int(time.time()),
        "payload": [{"id": "pl-1", "title": "周末歌单", "total": 1393}],
    })

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/playlists":
            return httpx.Response(200, json=[{"id": "pl-1", "title": "周末歌单", "total": 1200}])
        return httpx.Response(404)

    _wire(handler)
    first = await ceru.list_playlists()
    assert first[0]["total"] == 1393
    await asyncio.sleep(0.05)
    second = await ceru.list_playlists()
    assert second[0]["total"] == 1200


@pytest.mark.anyio
async def test_sync_invalidation_fetches_fresh_summary_and_empty_tracks():
    """同步失效后，摘要不再吃磁盘旧值，服务器清空歌曲也不能回退旧列表。"""
    ceru._atomic_write_json(ceru._disk_playlists_path(), {
        "savedAt": int(time.time()),
        "payload": [{"id": "pl-1", "title": "周末歌单", "total": 1393}],
    })
    ceru._atomic_write_json(ceru._disk_songs_path("pl-1"), {
        "savedAt": int(time.time()),
        "payload": [{"id": "old", "source": "wy", "title": "旧歌"}],
    })

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/playlists":
            return httpx.Response(200, json=[{"id": "pl-1", "title": "周末歌单", "total": 0}])
        if request.url.path.startswith("/playlists/"):
            return httpx.Response(200, json={"songs": []})
        return httpx.Response(404)

    _wire(handler)
    ceru._summary_disk_stale = True
    ceru._tracks_disk_stale.add("pl-1")
    assert (await ceru.list_playlists())[0]["total"] == 0
    assert await ceru.list_playlist_songs("pl-1") == []


def test_playlist_list_injects_ceru_playlists():
    _wire(_ceru_handler())
    with TestClient(app) as client:
        resp = client.get("/music/api/v1/playlist/list")
    assert resp.status_code == 200
    guids = [it["guid"] for it in resp.json()["data"]["list"]]
    assert guids == ["online:playlist:ceru:pl-fav", "online:playlist:ceru:pl-1", "localpl"]
    assert resp.json()["data"]["list"][0]["name"] == "我的喜欢"


def test_ceru_playlist_track_list_keeps_order():
    _wire(_ceru_handler())
    with TestClient(app) as client:
        resp = client.get(
            "/music/api/v1/track/playlist-detail/list",
            params={"guid": "online:playlist:ceru:pl-fav"},
        )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["total"] == 2
    assert [t["title"] for t in data["list"]] == ["晴天", "七里香"]


def test_favorite_list_includes_ceru_songs():
    _wire(_ceru_handler())
    with TestClient(app) as client:
        resp = client.get("/music/api/v1/favorite-track/list")
    assert resp.status_code == 200
    titles = [it["title"] for it in resp.json()["data"]["list"]]
    assert "晴天" in titles


def test_unfavorite_writes_delete_to_ceru_server():
    handler = _wire(_ceru_handler())
    with TestClient(app) as client:
        # 先让收藏列表构建并登记 guid → song 反查
        client.get("/music/api/v1/favorite-track/list")
        # 取消收藏的 guid 是 wx 的“晴天”（wy 101）映射出的 online guid
        guid = ceru.lx_item_for_song({"source": "wy", "id": "101", "title": "晴天"})["id"]
        resp = client.post(
            "/music/api/v1/favorite-track/delete",
            json={"trackGUID": f"online:{guid}"},
        )
    assert resp.status_code == 200
    assert handler.state["fav_delete"] >= 1
