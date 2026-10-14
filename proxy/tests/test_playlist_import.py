"""导入歌单：链接识别、洛雪 JSON 解码、平台响应解析与落盘（全离线）。

上游接口形状取自安卓版树夏music 的 LinkParser / fetch*PlaylistDirect，
这里用 MockTransport 回放同样的响应，避免用例真的去打平台接口。
"""
import json

import httpx
import pytest

from proxy import playlist_import as pim


@pytest.fixture(autouse=True)
def _tmp_store(tmp_path, monkeypatch):
    pim.set_dir(str(tmp_path / "imported_playlists"))
    yield
    pim.set_dir("")


# ---------------------------------------------------------------- 链接识别 ---

@pytest.mark.parametrize("text,platform,pid", [
    ("https://y.qq.com/n/ryqq/playlist/7927905745", "tx", "7927905745"),
    ("复制全文打开:https://y.qq.com/n/ryqq/playlist/7927905745?ADTAG=", "tx", "7927905745"),
    ("https://music.163.com/playlist?id=88888&ct=6", "wy", "88888"),
    ("https://music.163.com/#/m/playlist?id=1234567", "wy", "1234567"),
    ("https://music.163.com/playlist/3778678/", "wy", "3778678"),
    ("打开网易云音乐查看https://music.163.com/playlist/2884035/?ct=18&authorId=", "wy", "2884035"),
    ("http://www.kuwo.cn/playlist_detail/2819693901", "kw", "2819693901"),
    ("https://www.kugou.com/songlist/gcid_2118a1x01x3x21/", "kg", "2118a1x01x3x21"),
    ("https://m.kugou.com/plist/list/3118740412/", "kg", "3118740412"),
    ("https://music.migu.cn/v3/music/playlist/62797173", "mg", "62797173"),
    ("看这个 https://music.migu.cn/v5/#/playlist?playlistId=666000 挺好", "mg", "666000"),
    ("随便一句话，没有链接", None, None),
])
def test_parse_link(text, platform, pid):
    got = pim.parse_link(text)
    if platform is None:
        assert got is None
    else:
        assert got == (platform, pid)


# ---------------------------------------------------------------- 时长与映射 ---

@pytest.mark.parametrize("value,seconds", [
    (253, 253.0), ("253", 253.0), ("3:25", 205.0), ("1:02:03", 3723.0),
    (245000, 245.0), ("", 0.0), (None, 0.0), (-5, 0.0),
])
def test_parse_interval(value, seconds):
    assert pim.parse_interval(value) == seconds


def test_song_item_uses_lx_primary_key_and_placeholder():
    item = pim.song_item({"source": "tx", "id": "003uqv3H0ZIitc", "name": "千年泪",
                          "singer": "Tank", "album": "三国", "interval": 265})
    assert item["id"] == "lx:tx:003uqv3H0ZIitc"
    assert item["duration_s"] == 265.0

    missing = pim.song_item({"source": "kg", "id": "", "name": "无名", "singer": "某人"})
    assert missing["id"].startswith("lx:kg:ph")
    # 占位键必须稳定：同一首歌两次映射结果一致，翻页不会变 guid
    assert missing["id"] == pim.song_item({"source": "kg", "id": "", "name": "无名",
                                           "singer": "某人"})["id"]


def test_make_song_rejects_unknown_platform_and_blank_title():
    assert pim.make_song("xx", "1", "歌") is None
    assert pim.make_song("tx", "1", "  ") is None


# ---------------------------------------------------------------- 洛雪 JSON ---

def _song(mid="003uqv3H0ZIitc", name="千年泪", singer="Tank", source="tx"):
    return {"id": f"{source}_{mid}", "name": name, "singer": singer, "source": source,
            "interval": "4:25", "meta": {"songId": mid, "albumName": "三国",
                                         "picUrl": "//img.test/a.jpg"}}


def test_decode_lx_all_wrappers():
    playlist = {"id": "pl1", "name": "我的歌单", "source": "tx",
                "sourceListId": "7927905745", "list": [_song()]}
    cases = {
        "playListPart_v2": {"type": "playListPart_v2", "data": playlist},
        "playList_v2": {"type": "playList_v2", "data": [playlist]},
        "allData_v2": {"type": "allData_v2", "playList": [playlist]},
    }
    for label, payload in cases.items():
        docs = pim.decode_lx_json(json.dumps(payload))
        assert len(docs) == 1, label
        assert docs[0]["name"] == "我的歌单"
        assert docs[0]["source_list_id"] == "7927905745"
        assert docs[0]["songs"][0]["id"] == "003uqv3H0ZIitc"
        assert docs[0]["songs"][0]["interval"] == 265.0
        assert docs[0]["songs"][0]["cover"] == "https://img.test/a.jpg"


def test_decode_lx_bare_and_container_forms():
    docs = pim.decode_lx_json(json.dumps([_song(), _song(mid="other", name="另一首")]))
    assert len(docs) == 1 and len(docs[0]["songs"]) == 2

    container = {"defaultList": {"name": "试听列表", "list": [_song()]},
                 "loveList": {"list": [_song(source="kw", mid="111")]},
                 "userList": [{"id": "u1", "name": "自建", "list": [_song(source="wy", mid="222")]}]}
    docs = pim.decode_lx_json(json.dumps(container))
    names = [doc["name"] for doc in docs]
    assert names == ["试听列表", "我的收藏", "自建"]
    assert docs[2]["songs"][0]["source"] == "wy"

    assert pim.decode_lx_json("不是 JSON") == []
    assert pim.decode_lx_json(json.dumps({"name": "空歌单", "list": []})) == []


# ---------------------------------------------------------------- 平台抓取 ---

def _client(routes):
    def handler(request: httpx.Request) -> httpx.Response:
        for marker, payload in routes.items():
            if marker in str(request.url):
                if isinstance(payload, tuple):
                    return httpx.Response(payload[0], text=payload[1])
                return httpx.Response(200, json=payload)
        return httpx.Response(404, text="{}")

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.anyio
async def test_fetch_tx_playlist():
    payload = {"code": 0, "cdlist": [{
        "dissname": "华语精选", "logo": "https://img.test/cover.jpg",
        "songlist": [{"data": {"songmid": "003uqv3H0ZIitc", "songname": "千年泪",
                               "singer": [{"name": "Tank"}], "albumname": "三国",
                               "albummid": "00432LtM0WjmpF", "interval": 265}}],
    }]}
    client = _client({"fcg_v8_playlist_cp": payload})
    try:
        data = await pim.fetch_platform(client, "tx", "7927905745")
    finally:
        await client.aclose()
    assert data["name"] == "华语精选"
    song = data["songs"][0]
    assert song["source"] == "tx" and song["id"] == "003uqv3H0ZIitc"
    assert song["singer"] == "Tank" and song["interval"] == 265.0
    assert "T002R300x300M00000432LtM0WjmpF" in song["cover"]


@pytest.mark.anyio
async def test_fetch_kw_playlist():
    payload = {"title": "酷我热歌", "pic": "https://img.test/kw.jpg", "musiclist": [
        {"id": "123456", "name": "单曲", "artist": "歌手", "album": "专辑",
         "duration": 210, "pic": "https://img.test/s.jpg"}]}
    client = _client({"nplserver.kuwo.cn": payload})
    try:
        data = await pim.fetch_platform(client, "kw", "2819693901")
    finally:
        await client.aclose()
    assert data["songs"][0]["id"] == "123456" and data["songs"][0]["source"] == "kw"


@pytest.mark.anyio
async def test_fetch_kg_playlist_parses_global_data():
    html = ('<script>var info = {name: "酷狗榜单", pic: "https://img.test/kg.jpg"};'
            'global.data = [{"hash":"ABCD","songname":"某歌","singername":"某人",'
            '"album_name":"某专","duration":200,"trans_param":{"union_cover":"https://c.test/{size}.jpg"}}];'
            '</script>')
    client = _client({"www2.kugou.kugou.com": (200, html)})
    try:
        data = await pim.fetch_platform(client, "kg", "3118740412")
    finally:
        await client.aclose()
    assert data["name"] == "酷狗榜单"
    song = data["songs"][0]
    assert song["id"] == "ABCD" and song["interval"] == 200.0
    assert "{size}" not in song["cover"]


@pytest.mark.anyio
async def test_fetch_mg_playlist_pages():
    info = {"code": "000000", "data": {"title": "咪咕推荐", "imgItem": {"img": "https://img.test/mg.jpg"}}}
    page1 = {"code": "000000", "data": {"totalCount": 51, "songList": [
        {"copyrightId": f"c{i}", "songName": f"歌{i}", "singerList": [{"name": "歌手"}],
         "album": "专", "duration": 180, "img3": "/pic/i.jpg"} for i in range(50)]}}
    page2 = {"code": "000000", "data": {"totalCount": 51, "songList": [
        {"copyrightId": "c50", "songName": "歌50", "duration": 180}]}}

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "resource/playlist/v2.0" in url:
            return httpx.Response(200, json=info)
        if "pageNo=1" in url:
            return httpx.Response(200, json=page1)
        if "pageNo=2" in url:
            return httpx.Response(200, json=page2)
        return httpx.Response(404, text="{}")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        data = await pim.fetch_platform(client, "mg", "62797173")
    finally:
        await client.aclose()
    assert len(data["songs"]) == 51
    assert data["songs"][0]["cover"] == "https://d.musicapp.migu.cn/pic/i.jpg"


@pytest.mark.anyio
async def test_fetch_wy_fills_missing_details():
    detail = {"code": 200, "result": {"name": "网易精选", "coverImgUrl": "https://img.test/wy.jpg",
                                      "trackIds": [{"id": 1}, {"id": 2}],
                                      "tracks": [{"id": 1, "name": "第一首",
                                                  "artists": [{"name": "甲"}],
                                                  "album": {"name": "A", "picUrl": "https://p/1.jpg"},
                                                  "duration": 200000}]}}
    batch = {"songs": [{"id": 2, "name": "第二首", "artists": [{"name": "乙"}],
                        "album": {"name": "B", "picUrl": "https://p/2.jpg"},
                        "duration": 190000}]}
    client = _client({"api/song/detail": batch, "playlist/detail": detail})
    try:
        data = await pim.fetch_platform(client, "wy", "88888")
    finally:
        await client.aclose()
    assert [s["id"] for s in data["songs"]] == ["1", "2"]
    assert data["songs"][1]["source"] == "wy"
    assert data["songs"][0]["interval"] == 200.0


@pytest.mark.anyio
async def test_fetch_failures_return_none_without_raising():
    client = _client({})
    try:
        assert await pim.fetch_platform(client, "tx", "1") is None
        assert await pim.fetch_platform(client, "nope", "1") is None
    finally:
        await client.aclose()


# ---------------------------------------------------------------- 导入与存储 ---

@pytest.mark.anyio
async def test_import_from_link_persists_and_pages():
    payload = {"title": "酷我热歌", "musiclist": [
        {"id": str(i), "name": f"歌{i}", "artist": "歌手", "duration": 200} for i in range(3)]}
    client = _client({"nplserver.kuwo.cn": payload})
    try:
        records, error = await pim.import_from_text(
            client, "看看这个 http://www.kuwo.cn/playlist_detail/2819693901")
    finally:
        await client.aclose()
    assert error == "" and len(records) == 1
    record = records[0]
    assert record["name"] == "酷我热歌" and record["count"] == 3
    assert record["origin"] == "link" and record["origin_id"] == "2819693901"
    assert pim.is_imp_playlist_guid(record["guid"])

    assert [item["name"] for item in pim.list_records()] == ["酷我热歌"]
    first, total = pim.tracks_page(record["id"], 0, 2)
    assert total == 3 and [song["name"] for song in first] == ["歌0", "歌1"]
    page, _ = pim.tracks_page(record["id"], 2, 2)
    assert [song["name"] for song in page] == ["歌2"]
    assert pim.song_item(page[0])["id"] == "lx:kw:2"


@pytest.mark.anyio
async def test_import_from_lx_json_text_and_bad_input():
    payload = {"type": "playList_v2", "data": [
        {"id": "p1", "name": "洛雪歌单", "source": "tx", "list": [_song()]}]}
    client = _client({})
    try:
        records, error = await pim.import_from_text(client, json.dumps(payload))
        assert error == "" and records[0]["name"] == "洛雪歌单"
        assert records[0]["refreshable"] is False

        bad, error = await pim.import_from_text(client, "不是链接也不是 JSON")
        assert bad == [] and "无法识别" in error

        empty, error = await pim.import_from_text(client, "")
        assert empty == [] and error
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_refresh_overwrites_tracks_and_keeps_on_failure():
    payload = {"title": "酷我热歌", "musiclist": [{"id": "1", "name": "老歌", "duration": 200}]}
    client = _client({"nplserver.kuwo.cn": payload})
    try:
        records, _ = await pim.import_from_text(client, "http://www.kuwo.cn/playlist_detail/999")
        pid = records[0]["id"]

        updated = {"title": "酷我热歌", "musiclist": [{"id": "1", "name": "新歌", "duration": 200},
                                                      {"id": "2", "name": "加一首", "duration": 200}]}
        client2 = _client({"nplserver.kuwo.cn": updated})
        record, error = await pim.refresh(client2, pid)
        await client2.aclose()
        assert error == "" and record["count"] == 2

        broken = _client({})
        record, error = await pim.refresh(broken, pid)
        await broken.aclose()
        assert record is None and "已保留原有曲目" in error
        assert pim.read_record(pid)["count"] == 2

        assert await pim.refresh(client, "nope") == (None, "导入歌单不存在")
    finally:
        await client.aclose()


def test_delete_and_corrupt_file_tolerated(tmp_path):
    record = pim.write_record({"name": "A", "origin": "lxjson", "songs": [_song()]})
    assert pim.read_record(record["id"])
    assert pim.delete(record["id"]) is True
    assert pim.read_record(record["id"]) is None
    (tmp_path / "imported_playlists" / "broken.json").write_text("{不是JSON", encoding="utf-8")
    assert pim.list_records() == []


def test_decode_lx_json_key_wrapped_variants():
    """没有 type 字段的导出/二次保存文件：歌单直接放在键里也要认。"""
    variants = [
        '{"playList_v2":[{"name":"深夜电台","list":[{"songmid":"S1","name":"夜","source":"tx"}]}]}',
        '{"playList":[{"name":"深夜电台","list":[{"songmid":"S1","name":"夜","source":"tx"}]}]}',
        '{"allData_v2":{"playList":[{"name":"深夜电台","list":[{"songmid":"S1","name":"夜","source":"tx"}]}]}}',
        '{"playListPart_v2":{"name":"深夜电台","list":[{"songmid":"S1","name":"夜","source":"tx"}]}}',
    ]
    for raw in variants:
        docs = pim.decode_lx_json(raw)
        assert len(docs) == 1, raw
        assert docs[0]["name"] == "深夜电台", raw
        assert docs[0]["songs"][0]["id"] == "S1", raw


def test_decode_lx_json_ignores_junk():
    assert pim.decode_lx_json("") == []
    assert pim.decode_lx_json("not json") == []
    assert pim.decode_lx_json("[1,2,3]") == []
    assert pim.decode_lx_json('{"type":"other"}') == []
