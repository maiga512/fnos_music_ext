"""添加到歌单 / 添加到我的喜欢的多端同步写回（2.6.52 行为修复）。

背景：iOS 端搜索洛雪歌曲后「添加到歌单」点了没反应（ceru 同步歌单与导入歌单
被旧逻辑静默吸收）；「添加到我的喜欢」只写本地（未注册歌曲推不到同步服务器）。

覆盖：
* ceru 同步歌单 add-track：注册表未命中时按 guid 主键合成歌曲，经 LWW add
  写回同步服务器（多端可见）；已在歌单（含同一首歌另一平台主键）跳过；
  operation 失败回 code:-1；favorites 语义歌单同时写 /favorites 独立存储。
* ceru 同步歌单 remove-track：服务器歌单确含该曲才发 remove（幂等）；
  favorites 歌单同时 DELETE /favorites。
* 导入歌单 add/remove-track：在线歌曲按平台主键追加/删除本地 JSON（查重/幂等），
  不触碰同步服务器。
* favorite-track/create：未注册 guid（搜索命中/换源/重启）合成后推服务器，
  登记注册表供取消收藏反查；无标题不推。
* favorite-track/delete：未注册且服务器「我的喜欢」查无此曲时按 guid 主键合成
  后删除 /favorites（no-op），不发明多余的 operation。
"""
import json

import httpx
import pytest
from fastapi.testclient import TestClient

import proxy.app as proxy_app
from proxy import cerusync as ceru
from proxy import playlist_import as plimp
from proxy.app import CONF, app, _forget_ceru_favorites_index

PLAYLISTS = [
    {"id": "pl-fav", "title": "我的喜欢", "semanticType": "favorites",
     "position": 0, "total": 1, "coverUrl": "", "createdAt": 1, "updatedAt": 1},
    {"id": "pl-2", "title": "本周精选", "position": 1, "total": 1,
     "coverUrl": "", "createdAt": 1, "updatedAt": 1},
]

FAV_SONG = {"id": "101", "source": "wy", "title": "晴天", "artist": "周杰伦",
            "album": "叶惠美", "durationText": "04:29", "artworkUrl": "http://img/a.jpg"}
PLAY2_SONG = {"id": "202", "source": "tx", "title": "稻香", "artist": "周杰伦",
              "album": "魔杰座", "durationText": "03:43", "artworkUrl": "http://img/c.jpg"}

PLAYLIST_SONGS = {"pl-fav": [FAV_SONG], "pl-2": [PLAY2_SONG]}

# 在线搜索到的歌曲元数据（_best_effort_online_info 的替身返回值）
NEW_SONG_INFO = {"tx:007NEW": {"title": "新歌", "artist": "某人", "album": "新专辑",
                               "duration_s": 200, "cover_url": "http://img/new.jpg"}}
MG_SONG_INFO = {"mg:555": {"title": "咪咕之歌", "artist": "某歌手", "album": "",
                           "duration_s": 180, "cover_url": ""}}
WY_SONG_INFO = {"wy:101": {"title": "晴天", "artist": "周杰伦", "album": "叶惠美",
                           "duration_s": 269, "cover_url": "http://img/a.jpg"}}
IMP_NEW_INFO = {"wy:9002": {"title": "导入新歌", "artist": "Beyond", "album": "命运派对",
                            "duration_s": 302, "cover_url": "http://img/wy9002.jpg"}}


@pytest.fixture(autouse=True)
def setup_env(tmp_path, monkeypatch):
    ceru.reset_for_test()
    plimp.set_dir(str(tmp_path / "imported_playlists"))
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
    monkeypatch.setitem(CONF, "import_dir", str(tmp_path / "imported_playlists"))
    monkeypatch.setitem(CONF, "cache_dir", str(tmp_path / "cache"))
    monkeypatch.setitem(CONF, "fav_auto_bind", False)
    # 收藏索引 30s 记忆 + 歌单摘要缓存，用例之间必须隔离
    _forget_ceru_favorites_index()
    yield
    _forget_ceru_favorites_index()
    ceru.reset_for_test()
    plimp.set_dir("")


def _wire(ops, fav_calls, op_status=200):
    """MockTransport：捕获 /playlist-song-operations 与 /favorites 写入。"""
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/playlists":
            return httpx.Response(200, json=PLAYLISTS)
        if path.startswith("/playlists/"):
            pid = path.split("/")[2]
            return httpx.Response(200, json={"songs": PLAYLIST_SONGS.get(pid, [])})
        if path == "/favorites":
            body = json.loads(request.content or b"{}")
            if request.method == "GET":
                return httpx.Response(200, json={"items": []})
            fav_calls.append(("create" if request.method == "POST" else request.method.lower(), body))
            return httpx.Response(200, json={"id": str(body.get("entityId") or ""), "revision": 1})
        if path in ("/playlist-song-operations", "/playlist-song-ops"):
            if op_status != 200:
                return httpx.Response(op_status, json={"code": 1})
            ops.append(json.loads(request.content))
            return httpx.Response(200, json={"applied": True, "changed": True, "stale": False})
        if path.endswith("/user/me"):
            return httpx.Response(200, json={"code": 0, "data": {"guid": "user-wb-1"}})
        if path.endswith("/favorite-track/list"):
            return httpx.Response(200, json={"code": 0, "data": {"list": [], "total": 0}})
        return httpx.Response(200, json={"code": 0, "data": None})

    base_url = ceru.config()["url"]
    ceru._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=base_url)
    ceru._http_base_url = base_url
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://unix")


def _fake_info(monkeypatch, by_key):
    """_best_effort_online_info 替身：按 guid 推出的平台主键查元数据。"""
    async def fake(request, guid):
        guid = str(guid or "")
        info = by_key.get(guid)
        if info is None:
            keys = proxy_app._guid_track_keys(guid)
            if keys:
                info = by_key.get(keys[0])
        return dict(info or {})
    monkeypatch.setattr(proxy_app, "_best_effort_online_info", fake)


def _imp_record(pid="impabc123"):
    return {"id": pid, "name": "导入的老歌", "cover": "http://img/cover.jpg",
            "origin": "link", "origin_id": "7788", "platform": "tx",
            "songs": [
                {"source": "tx", "id": "001A", "name": "晴天", "singer": "周杰伦",
                 "album": "叶惠美", "interval": 269, "cover": "http://img/tx1.jpg"},
                {"source": "wy", "id": "9001", "name": "光辉岁月", "singer": "Beyond",
                 "album": "命运派对", "interval": 302, "cover": "http://img/wy3.jpg"},
            ]}


# ---------------------------------------------------------------- ceru 歌单写入

def test_ceru_playlist_add_synthesizes_song_and_writes_operation(monkeypatch):
    """搜索到的歌（本地无注册）加进 ceru 同步歌单：合成歌曲 + LWW add 回服务器。"""
    ops, favs = [], []
    _wire(ops, favs)
    _fake_info(monkeypatch, dict(NEW_SONG_INFO))
    with TestClient(app) as client:
        resp = client.post("/music/api/v1/playlist/add-track", json={
            "guid": "online:playlist:ceru:pl-2", "trackGUIDs": ["online:lx:tx:007NEW"]})
    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    assert len(ops) == 1
    op = ops[0]
    assert op["playlistId"] == "pl-2"
    assert op["action"] == "add"
    assert op["trackKey"] == "tx:007NEW"
    assert op["song"]["title"] == "新歌"
    assert op["song"]["source"] == "tx"
    assert op["song"]["id"] == "007NEW"
    assert favs == []  # 非 favorites 歌单不触碰 /favorites


def test_ceru_playlist_add_dedup_skips_already_present(monkeypatch):
    """同一首歌（同主键/同身份时长）已在歌单：不再发 operation。"""
    ops, favs = [], []
    _wire(ops, favs)
    _fake_info(monkeypatch, dict(WY_SONG_INFO))
    with TestClient(app) as client:
        resp = client.post("/music/api/v1/playlist/add-track", json={
            "guid": "online:playlist:ceru:pl-fav", "trackGUIDs": ["online:lx:wy:101"]})
    assert resp.json()["code"] == 0
    assert ops == [] and favs == []


def test_ceru_playlist_add_to_favorites_writes_both_stores(monkeypatch):
    """加进「我的喜欢」：operation 进歌单 + /favorites 独立存储同步写入。"""
    ops, favs = [], []
    _wire(ops, favs)
    _fake_info(monkeypatch, dict(MG_SONG_INFO))
    with TestClient(app) as client:
        resp = client.post("/music/api/v1/playlist/add-track", json={
            "guid": "online:playlist:ceru:pl-fav", "trackGUIDs": ["online:lx:mg:555"]})
    assert resp.json()["code"] == 0
    assert [o["trackKey"] for o in ops if o["action"] == "add"] == ["mg:555"]
    assert any(k == "create" and b.get("entityId") == "555" for k, b in favs)


def test_ceru_playlist_add_operation_failure_returns_error(monkeypatch):
    """服务器拒收 operation：回 code:-1，客户端能看到失败而不是假成功。"""
    ops, favs = [], []
    _wire(ops, favs, op_status=500)
    _fake_info(monkeypatch, dict(NEW_SONG_INFO))
    with TestClient(app) as client:
        resp = client.post("/music/api/v1/playlist/add-track", json={
            "guid": "online:playlist:ceru:pl-2", "trackGUIDs": ["online:lx:tx:007NEW"]})
    assert resp.json()["code"] == -1
    assert ops == []


# ---------------------------------------------------------------- ceru 歌单移除

def test_ceru_playlist_remove_sends_operation_and_fav_delete():
    """从「我的喜欢」移除：LWW remove + /favorites DELETE 双写。"""
    ops, favs = [], []
    _wire(ops, favs)
    with TestClient(app) as client:
        resp = client.post("/music/api/v1/playlist/remove-track", json={
            "guid": "online:playlist:ceru:pl-fav", "trackGUIDs": ["online:lx:wy:101"]})
    assert resp.json()["code"] == 0
    removes = [o for o in ops if o["action"] == "remove"]
    assert len(removes) == 1
    assert removes[0]["playlistId"] == "pl-fav"
    assert removes[0]["trackKey"] == "wy:101"
    assert any(k == "delete" and b.get("entityId") == "101" for k, b in favs)


def test_ceru_playlist_remove_absent_is_idempotent():
    """服务器歌单里没有这首歌：不发 operation，静默成功（幂等）。"""
    ops, favs = [], []
    _wire(ops, favs)
    with TestClient(app) as client:
        resp = client.post("/music/api/v1/playlist/remove-track", json={
            "guid": "online:playlist:ceru:pl-fav", "trackGUIDs": ["online:lx:mg:555"]})
    assert resp.json()["code"] == 0
    assert ops == [] and favs == []


# ---------------------------------------------------------------- 导入歌单写入

def test_imp_playlist_add_appends_and_dedupes(monkeypatch):
    """导入歌单加歌：新曲追加本地 JSON，已有主键不重复追加，不触碰同步服务器。"""
    ops, favs = [], []
    _wire(ops, favs)
    plimp.write_record(_imp_record())
    _fake_info(monkeypatch, dict(IMP_NEW_INFO))
    with TestClient(app) as client:
        r1 = client.post("/music/api/v1/playlist/add-track", json={
            "guid": "online:playlist:imp:impabc123", "trackGUIDs": ["online:lx:wy:9002"]})
        r2 = client.post("/music/api/v1/playlist/add-track", json={
            "guid": "online:playlist:imp:impabc123", "trackGUIDs": ["online:lx:tx:001A"]})
        r3 = client.post("/music/api/v1/playlist/add-track", json={
            "guid": "online:playlist:imp:impabc123", "trackGUIDs": ["online:lx:wy:9002"]})
    assert all(r.json()["code"] == 0 for r in (r1, r2, r3))
    rec = plimp.read_record("impabc123")
    keys = [(s["source"], s["id"]) for s in rec["songs"]]
    assert keys.count(("wy", "9002")) == 1
    assert keys.count(("tx", "001A")) == 1  # 原本就有，没重复追加
    added = next(s for s in rec["songs"] if s["id"] == "9002")
    assert added["name"] == "导入新歌"
    assert added["singer"] == "Beyond"
    assert ops == [] and favs == []


def test_imp_playlist_remove_deletes_by_primary_key():
    """导入歌单删歌：按平台主键删除，重复删幂等。"""
    _wire([], [])
    plimp.write_record(_imp_record())
    with TestClient(app) as client:
        r1 = client.post("/music/api/v1/playlist/remove-track", json={
            "guid": "online:playlist:imp:impabc123",
            "trackGUIDs": ["online:lx:tx:001A", "online:lx:wy:9001"]})
        r2 = client.post("/music/api/v1/playlist/remove-track", json={
            "guid": "online:playlist:imp:impabc123",
            "trackGUIDs": ["online:lx:tx:001A"]})
    assert r1.json()["code"] == 0 and r2.json()["code"] == 0
    assert plimp.read_record("impabc123")["songs"] == []


# ---------------------------------------------------------------- 我的喜欢写回

def test_favorite_create_unregistered_song_synthesizes_and_pushes(monkeypatch):
    """搜索后直接标星（本地从未注册）：合成歌曲推服务器并登记注册表。"""
    ops, favs = [], []
    _wire(ops, favs)
    _fake_info(monkeypatch, dict(MG_SONG_INFO))
    with TestClient(app) as client:
        resp = client.post("/music/api/v1/favorite-track/create",
                           json={"trackGUID": "online:lx:mg:555"})
    assert resp.json()["code"] == 0
    assert [o["trackKey"] for o in ops if o["action"] == "add"
            and o["playlistId"] == "pl-fav"] == ["mg:555"]
    assert any(k == "create" and b.get("entityId") == "555" for k, b in favs)
    # 登记注册表：后续取消收藏可按主键反查
    song = ceru.song_for_guid("online:lx:mg:555")
    assert song is not None and song["title"] == "咪咕之歌"


def test_favorite_create_without_title_skips_server(monkeypatch):
    """拉不到歌名的 guid：不往服务器写无意义条目，本地收藏仍成功。"""
    ops, favs = [], []
    _wire(ops, favs)
    _fake_info(monkeypatch, {"mg:666": {"title": "", "artist": ""}})
    with TestClient(app) as client:
        resp = client.post("/music/api/v1/favorite-track/create",
                           json={"trackGUID": "online:lx:mg:666"})
    assert resp.json()["code"] == 0
    assert ops == [] and favs == []


def test_favorite_delete_unregistered_absent_song_no_invented_operation(monkeypatch):
    """服务器「我的喜欢」没有这首歌：合成后 DELETE /favorites（no-op），
    不发明 remove operation（服务器端会收到一个不存在的删除广播）。"""
    ops, favs = [], []
    _wire(ops, favs)
    _fake_info(monkeypatch, dict(MG_SONG_INFO))
    with TestClient(app) as client:
        resp = client.post("/music/api/v1/favorite-track/delete",
                           json={"trackGUID": "online:lx:mg:555"})
    assert resp.json()["code"] == 0
    assert ops == []
    assert any(k == "delete" and b.get("entityId") == "555" for k, b in favs)

