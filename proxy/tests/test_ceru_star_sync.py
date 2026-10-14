"""标星（收藏）多端同步：favorites 歌单 operation 回写协议测试。

覆盖：
* favorites_playlist_ids 语义筛选（semanticType=favorites）；
* playlist_song_operation 的 LWW 字段（operationId/deviceId/sequence/trackKey/occurredAtMs）；
* 同 trackKey 序号单调递增并持久化；
* 取消标星 → remove operation；重新标星 → add operation；
* _cover_url_unusable：内网/文本封面地址判定（封面闪烁兜底链前提）。
"""
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from proxy import cerusync as ceru
from proxy.app import (
    CONF,
    app,
    _ceru_favorite_match,
    _cover_url_unusable,
    _fav_identity,
    _forget_ceru_favorites_index,
    _guid_track_keys,
)

PLAYLISTS = [
    {"id": "pl-fav", "title": "我的喜欢", "semanticType": "favorites",
     "position": 0, "total": 1, "coverUrl": "", "createdAt": 1, "updatedAt": 1},
    {"id": "pl-2", "title": "本周精选", "position": 1, "total": 1,
     "coverUrl": "", "createdAt": 1, "updatedAt": 1},
    {"id": "pl-3", "title": "混合歌单", "position": 2, "total": 2,
     "coverUrl": "", "createdAt": 1, "updatedAt": 1},
]

FAV_SONG = {"id": "101", "source": "wy", "title": "晴天", "artist": "周杰伦",
            "album": "叶惠美", "durationText": "04:29", "artworkUrl": "http://img/a.jpg"}
OTHER_SONG = {"id": "202", "source": "tx", "title": "稻香", "artist": "周杰伦",
              "album": "魔杰座", "durationText": "03:43", "artworkUrl": "http://img/c.jpg"}

# 同一首歌在另一个平台的主键（阴天快乐：收藏里存 kw:88888，实际播的是 tx 版本）
CROSS_SONG = {"id": "88888", "source": "kw", "title": "阴天快乐", "artist": "陈奕迅",
              "album": "rice & shine", "durationText": "04:24",
              "artworkUrl": "http://img/d.jpg"}

PLAYLIST_SONGS = {"pl-fav": [FAV_SONG], "pl-2": [OTHER_SONG], "pl-3": [OTHER_SONG, FAV_SONG]}


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
    # 收藏索引有 30s 记忆，用例之间必须作废，否则上一个用例的歌单会泄漏到下一个
    _forget_ceru_favorites_index()
    yield
    _forget_ceru_favorites_index()
    ceru.reset_for_test()


def _wire(ops):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/playlists":
            return httpx.Response(200, json=PLAYLISTS)
        if path.startswith("/playlists/"):
            pid = path.split("/")[2]
            return httpx.Response(200, json={"songs": PLAYLIST_SONGS.get(pid, [])})
        if path == "/favorites":
            if request.method == "GET":
                return httpx.Response(200, json={"items": []})
            return httpx.Response(200, json={"id": "fav-x", "revision": 1})
        if path in ("/playlist-song-operations", "/playlist-song-ops"):
            ops.append(json.loads(request.content))
            return httpx.Response(200, json={"applied": True, "changed": True, "stale": False})
        if path.endswith("/user/me"):
            return httpx.Response(200, json={"code": 0, "data": {"guid": "user-ceru-1"}})
        if path.endswith("/favorite-track/list"):
            return httpx.Response(200, json={"code": 0, "data": {"list": [], "total": 0}})
        if path.endswith("/playlist/list"):
            return httpx.Response(200, json={"code": 0, "data": {"list": [], "total": 0}})
        return httpx.Response(200, json={"code": 0, "data": None})

    base_url = ceru.config()["url"]
    ceru._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=base_url)
    ceru._http_base_url = base_url
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://unix"
    )


@pytest.mark.anyio
async def test_favorites_playlist_ids_filters_semantic():
    _wire([])
    assert await ceru.favorites_playlist_ids() == ["pl-fav"]


@pytest.mark.anyio
async def test_operation_body_and_sequence_persistence():
    ops = []
    _wire(ops)
    r1 = await ceru.playlist_song_operation("pl-fav", "remove", "wy:101")
    r2 = await ceru.playlist_song_operation("pl-fav", "add", "wy:101", song=FAV_SONG)
    assert r1 is not None and r2 is not None
    op = ops[0]
    assert op["playlistId"] == "pl-fav"
    assert op["action"] == "remove"
    assert op["trackKey"] == "wy:101"
    assert op["deviceId"] == ceru.device_id()
    assert op["sequence"] == 1
    assert isinstance(op["occurredAtMs"], int) and op["occurredAtMs"] > 0
    assert op["operationId"]
    assert "song" not in op
    # 第二次同 trackKey：序号递增、add 携带 song
    assert ops[1]["sequence"] == 2
    assert ops[1]["action"] == "add"
    assert ops[1]["song"]["id"] == "101"
    state = ceru._load_state()
    assert state.get("operationSequences", {}).get("wy:101") == 2


def test_cover_url_unusable_lan_and_text():
    assert _cover_url_unusable("")
    assert _cover_url_unusable("http://192.168.5.2:5666/music/api/v1/static/cover?coverId=album_x")
    assert _cover_url_unusable("http://10.0.0.1:8080/a.jpg")
    assert _cover_url_unusable("http://172.20.1.9/a.jpg")
    assert _cover_url_unusable("http://127.0.0.1/a.jpg")
    assert _cover_url_unusable("http://artistpicserver.kuwo.cn/k.jpg")
    assert _cover_url_unusable("not-a-url")
    assert not _cover_url_unusable("http://y.gtimg.cn/music/photo_new/T002C100.jpg")
    assert not _cover_url_unusable("http://img1.kwcdn.kuwo.cn/star/albumcover/500.jpg")
    assert not _cover_url_unusable("http://172.50.1.9/a.jpg")  # 172.50 属公网段


def test_unfavorite_writes_remove_operation():
    ops = []
    _wire(ops)
    with TestClient(app) as client:
        # 先构建收藏列表，登记 guid → song 反查
        client.get("/music/api/v1/favorite-track/list")
        guid = ceru.lx_item_for_song(FAV_SONG)["id"]
        resp = client.post("/music/api/v1/favorite-track/delete",
                           json={"trackGUID": f"online:{guid}"})
    assert resp.status_code == 200
    removes = [op for op in ops if op.get("action") == "remove"]
    assert len(removes) == 1
    assert removes[0]["playlistId"] == "pl-fav"
    assert removes[0]["trackKey"] == "wy:101"


def test_refavorite_missing_song_writes_add_operation():
    ops = []
    _wire(ops)
    with TestClient(app) as client:
        # tx:202 只存在于非 favorites 歌单 pl-2；手工登记 guid 反查后标星，
        # 应触发对 pl-fav 的 add operation（歌曲带全量元数据）。
        song_guid = ceru.lx_item_for_song(OTHER_SONG)["id"]
        ceru.register_guid(f"online:{song_guid}", OTHER_SONG)
        resp = client.post("/music/api/v1/favorite-track/create",
                           json={"trackGUID": f"online:{song_guid}"})
    assert resp.status_code == 200
    adds = [op for op in ops if op.get("action") == "add"]
    assert len(adds) == 1
    assert adds[0]["playlistId"] == "pl-fav"
    assert adds[0]["trackKey"] == "tx:202"
    assert adds[0]["song"]["id"] == "202"


def test_playlist_tracks_stamped_is_favorite():
    """歌单曲目列表的 isFavorite：「我的喜欢」整单标星；混合歌单只标在里面的歌。"""
    _wire([])
    with TestClient(app) as client:
        resp_fav = client.get(
            "/music/api/v1/track/playlist-detail/list",
            params={"playlistGUID": "online:playlist:ceru:pl-fav", "page": 1, "size": 50})
        assert resp_fav.status_code == 200
        fav_tracks = resp_fav.json()["data"]["list"]
        assert len(fav_tracks) == 1
        assert fav_tracks[0]["isFavorite"] is True

        resp_mix = client.get(
            "/music/api/v1/track/playlist-detail/list",
            params={"playlistGUID": "online:playlist:ceru:pl-3", "page": 1, "size": 50})
        assert resp_mix.status_code == 200
        mix = {t["title"]: t for t in resp_mix.json()["data"]["list"]}
        assert set(mix) == {"晴天", "稻香"}
        assert mix["晴天"]["isFavorite"] is True
        assert mix["稻香"]["isFavorite"] is False


def test_dynamic_api_responses_no_store():
    """动态 API 响应带 no-store：防止远程访问网关/中间层缓存旧数据（内外网显示不一致）。"""
    _wire([])
    with TestClient(app) as client:
        resp = client.get(
            "/music/api/v1/track/playlist-detail/list",
            params={"playlistGUID": "online:playlist:ceru:pl-fav", "page": 1, "size": 50})
        assert resp.status_code == 200
        assert resp.headers.get("cache-control") == "no-store"

        resp2 = client.get("/music/api/v1/favorite-track/list")
        assert resp2.status_code == 200
        assert resp2.headers.get("cache-control") == "no-store"


def test_unstar_invalidates_tracks_cache():
    """取消标星成功后立即失效歌单曲目缓存：用户刷新即见最新，不等同步事件/TTL。"""
    _wire([])
    with TestClient(app) as client:
        client.get("/music/api/v1/favorite-track/list")  # 填充 pl-fav 曲目缓存
        assert "pl-fav" in ceru._tracks_cache
        guid = ceru.lx_item_for_song(FAV_SONG)["id"]
        resp = client.post("/music/api/v1/favorite-track/delete",
                           json={"trackGUID": f"online:{guid}"})
    assert resp.status_code == 200
    assert "pl-fav" not in ceru._tracks_cache
    assert "pl-fav" in ceru._tracks_disk_stale


def test_track_metadata_matches_playlist_star_and_cover():
    """播放页元数据与列表行一致：同 coverId、同原封面、标星口径一致。

    否则点下一首时列表数据先渲染（星/封面秒出），元数据一到就把星
    清掉、封面换新标识重拉，观感就是「亮一下红色再取消/封面闪一下消失」。"""
    _wire([])
    with TestClient(app) as client:
        resp_list = client.get(
            "/music/api/v1/track/playlist-detail/list",
            params={"playlistGUID": "online:playlist:ceru:pl-fav", "page": 1, "size": 50})
        row = resp_list.json()["data"]["list"][0]
        real_guid = next(
            g for g, s in ceru._guid_song_registry.items()
            if s.get("id") == "101" and s.get("source") == "wy")
        resp_meta = client.get("/music/api/v1/track/metadata", params={"guid": real_guid})
        assert resp_meta.status_code == 200
        meta = resp_meta.json()["data"]
        assert meta["track"]["isFavorite"] is True
        assert meta["track"]["coverId"] == row["coverId"]
        assert meta["album"]["coverId"] == row["coverId"]
        assert meta["track"]["coverUrl"] == "http://img/a.jpg"


def test_track_metadata_unstarred_song_stays_false():
    """不在收藏/「我的喜欢」里的在线曲：元数据 isFavorite 保持 False，不被误标。"""
    _wire([])
    with TestClient(app) as client:
        resp_meta = client.get("/music/api/v1/track/metadata",
                               params={"guid": "online:lx:mg:99999"})
        assert resp_meta.status_code == 200
        meta = resp_meta.json()["data"]
        assert meta["track"]["isFavorite"] is False


def test_guid_track_keys_derived_without_registry():
    """收藏存的是 {source}:{id}：guid 必须能直接推出主键，不靠内存注册表。"""
    assert _guid_track_keys("online:lx:tx:000Opyfj4bDwYX") == [
        "tx:000Opyfj4bDwYX", "lx:tx:000Opyfj4bDwYX"]
    assert _guid_track_keys("online:netease:186016") == ["wy:186016"]
    assert _guid_track_keys("online:kugou:xyz") == ["kg:xyz"]
    assert _guid_track_keys("online:migu:7:8") == ["mg:7:8"]
    assert _guid_track_keys("") == []
    assert _guid_track_keys("1a2b3c4d5e6f") == []


def test_fav_identity_normalization_rules():
    # 空格/标点差异不改变身份
    assert _fav_identity("阴天 快乐", "陈奕迅") == _fav_identity("阴天快乐", "陈奕迅")
    # 版本后缀是文字，必须区分开，不能并成同一首
    assert _fav_identity("阴天快乐", "陈奕迅") != _fav_identity("阴天快乐 (Live)", "陈奕迅")
    # 歌手顺序与 feat. 写法差异算同一组
    assert _fav_identity("安全距离", "珑熹音上/林夕石桂") == _fav_identity(
        "安全距离", "林夕石桂 feat. 珑熹音上")
    assert _fav_identity("", "陈奕迅") == ""


@pytest.mark.anyio
async def test_ceru_favorite_match_by_guid_key_when_registry_empty():
    """服务重启后注册表是空的：换源播放的 guid 也要能在「我的喜欢」里找回那条收藏。"""
    _wire([])
    song, key = await _ceru_favorite_match("online:lx:wy:101")
    assert key == "wy:101"
    assert song["title"] == "晴天"


@pytest.mark.anyio
async def test_ceru_favorite_match_across_platforms_by_identity(monkeypatch):
    """收藏记的是 kw 主键，实际播的是 tx 版本：靠歌名+歌手+时长认出同一首。"""
    _wire([])
    monkeypatch.setitem(PLAYLIST_SONGS, "pl-fav", [FAV_SONG, CROSS_SONG])
    song, key = await _ceru_favorite_match(
        "online:lx:tx:004ec8yZ02bRYx",
        {"title": "阴天快乐", "artist": "陈奕迅", "duration_s": 264})
    assert key == "kw:88888" and song["title"] == "阴天快乐"
    # 时长差得远 → 同名翻唱/现场版，不算同一首
    assert await _ceru_favorite_match(
        "online:lx:tx:zzz",
        {"title": "阴天快乐", "artist": "陈奕迅", "duration_s": 200}) == (None, "")
    # 另一首歌
    assert await _ceru_favorite_match(
        "online:lx:tx:zzz",
        {"title": "别的世界", "artist": "某人", "duration_s": 264}) == (None, "")


def test_metadata_marks_favorite_without_guid_registry():
    """播放页：注册表里没这条 guid（换源播放/刚重启）时，红心仍然亮着。"""
    _wire([])
    with TestClient(app) as client:
        resp = client.get("/music/api/v1/track/metadata",
                          params={"guid": "online:lx:wy:101"})
        assert resp.status_code == 200
        assert resp.json()["data"]["track"]["isFavorite"] is True


def test_unfavorite_unregistered_guid_still_removes_favorite():
    """注册表为空时取消标星：按 guid 主键找回收藏条目，remove 仍发给「我的喜欢」。"""
    ops = []
    _wire(ops)
    with TestClient(app) as client:
        resp = client.post("/music/api/v1/favorite-track/delete",
                           json={"trackGUID": "online:lx:wy:101"})
    assert resp.status_code == 200
    assert [op["trackKey"] for op in ops if op.get("action") == "remove"] == ["wy:101"]


def test_refavorite_other_platform_version_skips_duplicate_add(monkeypatch):
    """已收藏 kw 版本，再标星 tx 版本：不该往「我的喜欢」里塞第二条同一首歌。"""
    ops = []
    _wire(ops)
    monkeypatch.setitem(PLAYLIST_SONGS, "pl-fav", [FAV_SONG, CROSS_SONG])
    other = {"id": "777", "source": "tx", "title": "阴天快乐", "artist": "陈奕迅",
             "durationText": "04:24"}
    with TestClient(app) as client:
        guid = ceru.lx_item_for_song(other)["id"]
        ceru.register_guid(f"online:{guid}", other)
        resp = client.post("/music/api/v1/favorite-track/create",
                           json={"trackGUID": f"online:{guid}"})
    assert resp.status_code == 200
    assert [op for op in ops if op.get("action") == "add"] == []
