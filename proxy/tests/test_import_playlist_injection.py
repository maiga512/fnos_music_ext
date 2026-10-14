"""导入歌单注入飞牛音乐列表的行为用例（proxy/app.py + playlist_import）。

关注三件事：卡片出现在官方歌单之前、曲目分页能出可播 VO、写操作不透传官方。
"""
import httpx
import pytest
from fastapi.testclient import TestClient

import proxy.app as proxy_app
from proxy import playlist_import as plimp
from proxy.app import CONF, app, fake_official_guid, resolve_real_guid


SONGS = [
    {"source": "tx", "id": "001A", "name": "晴天", "singer": "周杰伦", "album": "叶惠美",
     "interval": 269, "cover": "http://img/tx1.jpg"},
    {"source": "kw", "id": "", "name": "小幸运", "singer": "田馥甄", "album": "My Love",
     "interval": 296, "cover": ""},
    {"source": "wy", "id": "9001", "name": "光辉岁月", "singer": "Beyond", "album": "命运派对",
     "interval": 302, "cover": "http://img/wy3.jpg"},
]


def _record(name="导入的老歌", pid="impabc123", count=None):
    return {"id": pid, "name": name, "cover": "http://img/cover.jpg",
            "origin": "link", "origin_id": "7788", "platform": "tx", "songs": list(SONGS)}


@pytest.fixture(autouse=True)
def env(tmp_path, monkeypatch):
    plimp.set_dir(str(tmp_path / "imported_playlists"))
    monkeypatch.setenv("FNMUSIC_RECOMMEND_DIR", str(tmp_path / "recommend_cache"))
    monkeypatch.setenv("FNMUSIC_PLAY_HISTORY_DIR", str(tmp_path / "play_history"))
    monkeypatch.setenv("FNMUSIC_MUSIC_DB", str(tmp_path / "missing.db"))
    monkeypatch.setitem(CONF, "fav_dir", str(tmp_path / "online_favorites"))
    monkeypatch.setitem(CONF, "plt_dir", str(tmp_path / "playlist_tracks"))
    monkeypatch.setitem(CONF, "import_dir", str(tmp_path / "imported_playlists"))
    monkeypatch.setitem(CONF, "cache_dir", str(tmp_path / "cache"))
    monkeypatch.setitem(CONF, "recommend_hot", False)
    monkeypatch.setitem(CONF, "recommend_daily", False)
    monkeypatch.setitem(CONF, "netease_my_playlists", False)
    monkeypatch.setitem(CONF, "netease_enabled", False)
    monkeypatch.setitem(CONF, "ceru_sync_enabled", False)
    yield
    plimp.set_dir("")


def _upstream(spy):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        spy.append(path)
        if path.endswith("/user/me"):
            return httpx.Response(200, json={"code": 0, "data": {"guid": "user-imp-1"}})
        if path.endswith("/playlist/list"):
            return httpx.Response(200, json={"code": 0, "data": {
                "list": [{"guid": "localpl", "name": "官方歌单", "coverId": "c1",
                          "createdAt": 1, "updatedAt": 1, "trackCount": 3}], "total": 1}})
        if path.endswith("/playlist/batch-detail"):
            spy.append("batch:" + httpx.URL(request.url).params.get("guids", ""))
            return httpx.Response(200, json={"code": 0, "data": {"list": [
                {"guid": "localpl", "trackCount": 3}]}})
        return httpx.Response(200, json={"code": 0, "msg": "ok", "data": None})
    return handler


@pytest.fixture
def spy():
    return []


@pytest.fixture
def client(spy):
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_upstream(spy)), base_url="http://unix")
    with TestClient(app) as test_client:
        yield test_client
    app.state.upstream_client = None


def test_list_injects_card_before_official(client):
    plimp.write_record(_record())
    resp = client.get("/music/api/v1/playlist/list")
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert [it["guid"] for it in data["list"]] == ["online:playlist:imp:impabc123", "localpl"]
    assert data["total"] == 2
    card = data["list"][0]
    assert card["name"] == "导入的老歌"
    assert card["trackCount"] == 3
    assert card["isDaily"] is True          # 客户端只对 isDaily 的在线歌单拉曲目
    assert card["coverId"].startswith("track_")
    assert card["createdAt"] >= 1_000_000_000  # epoch 秒，不是毫秒


def test_detail_works_for_real_and_fake_guid(client):
    plimp.write_record(_record())
    fake = fake_official_guid("online:playlist:imp:impabc123")
    for guid in ("online:playlist:imp:impabc123", "track_" + fake, fake):
        resp = client.get("/music/api/v1/playlist/detail", params={"guid": guid})
        body = resp.json()
        assert body["code"] == 0, guid
        assert body["data"]["guid"] == "online:playlist:imp:impabc123"
        assert body["data"]["trackCount"] == 3
    missing = client.get("/music/api/v1/playlist/detail",
                         params={"guid": "online:playlist:imp:nope"}).json()
    assert missing["code"] != 0


def test_track_list_returns_playable_vo(client):
    plimp.write_record(_record())
    resp = client.get("/music/api/v1/track/playlist-detail/list",
                      params={"playlistGUID": "online:playlist:imp:impabc123", "page": 1, "size": 2})
    data = resp.json()["data"]
    assert data["total"] == 3
    assert len(data["list"]) == 2
    first = data["list"][0]
    # guid/coverId 与推荐、Ceru 一样按官方惯例伪装成 32 位 id，反查回真实在线 id
    assert resolve_real_guid(first["guid"]) == "online:lx:tx:001A"
    assert resolve_real_guid(first["coverId"]) == "online:lx:tx:001A:cover5"
    assert first["title"] == "晴天"
    assert first["cover_url"] == "http://img/tx1.jpg"
    assert first["audioSpec"]["size"] > 0       # 手机会校验 size，为 0 直接跳歌
    # 没有平台主键的歌保留在列表里（占位 id），点播时靠标题+歌手懒搜索找回
    second = data["list"][1]
    assert resolve_real_guid(second["guid"]).startswith("online:lx:kw:ph")
    page2 = client.get("/music/api/v1/track/playlist-detail/list",
                       params={"playlistGUID": "online:playlist:imp:impabc123",
                               "page": 2, "size": 2}).json()["data"]
    assert page2["total"] == 3 and len(page2["list"]) == 1
    allat = client.get("/music/api/v1/track/playlist-detail/list",
                       params={"playlistGUID": "online:playlist:imp:impabc123", "size": -1})
    assert len(allat.json()["data"]["list"]) == 3


def test_track_list_works_with_disguised_guid(client):
    """客户端只会把伪装 id 带回来：曲目列表必须能按伪装 id 反查回导入歌单。"""
    plimp.write_record(_record())
    fake = fake_official_guid("online:playlist:imp:impabc123")
    for guid in ("track_" + fake, fake):
        resp = client.get("/music/api/v1/track/playlist-detail/list",
                          params={"playlistGUID": guid, "size": 50})
        data = resp.json()["data"]
        assert data["total"] == 3, guid
        assert resolve_real_guid(data["list"][0]["guid"]) == "online:lx:tx:001A", guid


def test_track_list_only_prefetches_first_track(client, monkeypatch):
    calls = []
    monkeypatch.setattr(proxy_app, "schedule_lx_prefetch", lambda ids: calls.append(list(ids)))
    plimp.write_record(_record())
    client.get("/music/api/v1/track/playlist-detail/list",
               params={"playlistGUID": "online:playlist:imp:impabc123", "size": 50})
    assert calls == [["lx:tx:001A"]]      # 一开歌单只吃一次上游额度


def test_batch_detail_does_not_forward_imp_guid(client, spy):
    plimp.write_record(_record())
    spy.clear()
    resp = client.get("/music/api/v1/playlist/batch-detail",
                      params={"guids": "online:playlist:imp:impabc123,localpl"})
    guids = [it["guid"] for it in resp.json()["data"]["list"]]
    assert guids == ["online:playlist:imp:impabc123", "localpl"]
    forwarded = [call for call in spy if call.startswith("batch:")]
    assert forwarded == ["batch:localpl"]      # 假 id 绝不能透传给官方


def test_add_and_remove_tracks_are_absorbed(client, spy):
    plimp.write_record(_record())
    spy.clear()
    for path in ("/music/api/v1/playlist/add-track", "/music/api/v1/playlist/remove-track"):
        resp = client.post(path, json={"guid": "online:playlist:imp:impabc123",
                                       "trackGuids": ["online:lx:tx:001A"]})
        assert resp.status_code == 200
        assert resp.json()["code"] == 0, path
    assert [call for call in spy if "add-track" in call or "remove-track" in call] == []
    assert len(plimp.read_record("impabc123")["songs"]) == 3


def test_delete_removes_record(client):
    plimp.write_record(_record())
    resp = client.post("/music/api/v1/playlist/delete",
                       json={"guid": "online:playlist:imp:impabc123"})
    assert resp.status_code == 200 and resp.json()["code"] == 0
    assert plimp.read_record("impabc123") is None
    assert plimp.list_records() == []


def test_playlist_cover_uses_stored_direct_link(client, monkeypatch):
    seen = {}

    async def fake_response(guid, url):
        seen["guid"] = guid
        seen["url"] = url
        return httpx.Response(200, content=b"img")

    monkeypatch.setattr(proxy_app, "_cached_cover_response", fake_response)
    plimp.write_record(_record())
    resp = client.get("/music/api/v1/static/cover",
                      params={"guid": "online:playlist:imp:impabc123"})
    assert resp.status_code == 200
    assert seen["url"] == "http://img/cover.jpg"


def test_song_cover_uses_registered_direct_link(client, monkeypatch):
    seen = {}

    async def fake_response(guid, url):
        seen["cache_key"] = guid
        seen["url"] = url
        return httpx.Response(200, content=b"img")

    monkeypatch.setattr(proxy_app, "_cached_cover_response", fake_response)
    plimp.write_record(_record())
    client.get("/music/api/v1/track/playlist-detail/list",
               params={"playlistGUID": "online:playlist:imp:impabc123", "size": 50})
    resp = client.get("/music/api/v1/static/cover", params={"guid": "online:lx:tx:001A:cover5"})
    assert resp.status_code == 200
    assert seen["url"] == "http://img/tx1.jpg"
    assert seen["cache_key"] == "online:lx:tx:001A:imp"


def test_empty_imports_do_not_intercept_playlist_list(client, spy):
    spy.clear()
    resp = client.get("/music/api/v1/playlist/list")
    assert [it["guid"] for it in resp.json()["data"]["list"]] == ["localpl"]
