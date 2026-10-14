"""酷我 songinfo（封面+歌词同一枪）与网易歌词参数回归。

现场依据：1387 首收藏里 169 首无封面，全部是酷我；约 45% 无歌词。酷我
m.kuwo.cn/newh5/singles/songinfoandlrc 一次返回 songinfo.pic 与 lrclist，
但实测首次请求约一半会回 {"msg":"音乐查询失败"} 的空载荷，重试一次即恢复；
网易 side 歌词必须带 lv=-1，lv=1 直接回 lyric:""。
全部离线：上游走 MockTransport。
"""
from __future__ import annotations

import asyncio

import httpx

from conftest import lxapp, mock_client

_SONGINFO_BODY = {
    "msg": "ok",
    "data": {
        "songinfo": {"songName": "小幸运", "artist": "田馥甄", "album": "我的少女时代",
                     "pic": "http://img.test/cover.jpg", "id": "877356"},
        "lrclist": [
            {"lineLyric": "小幸运 - 田馥甄", "time": "0"},
            {"lineLyric": "我听见雨滴落在青青草地", "time": "18.42"},
            {"lineLyric": "", "time": "20"},
        ],
    },
}


def _flaky_handler(state: dict):
    """第一枪回空载荷（接口抖动），第二枪才给数据——线上约一半是这个形态。"""
    def handler(request: httpx.Request) -> httpx.Response:
        assert "songinfoandlrc" in str(request.url)
        state["calls"] += 1
        if state["calls"] == 1:
            return httpx.Response(200, json={"msg": "音乐查询失败", "data": {}})
        return httpx.Response(200, json=_SONGINFO_BODY)
    return handler


def test_kw_songinfo_retries_after_empty_payload(isolated):
    state = {"calls": 0}
    client = mock_client(_flaky_handler(state))
    try:
        got = asyncio.run(lxapp._kw_songinfo_fetch(client, "877356"))
    finally:
        asyncio.run(client.aclose())
    assert state["calls"] == 2, "空载荷没有触发重试"
    assert got["pic"] == "http://img.test/cover.jpg"
    assert got["title"] == "小幸运"
    assert lxapp._KW_SONGINFO_STATS["retries"] == 1


def test_kw_songinfo_cache_avoids_second_upstream_call(isolated):
    state = {"calls": 0}
    client = mock_client(_flaky_handler(state))
    try:
        first = asyncio.run(lxapp._kw_songinfo_fetch(client, "877356"))
        calls_after_first = state["calls"]
        second = asyncio.run(lxapp._kw_songinfo_fetch(client, "877356"))
    finally:
        asyncio.run(client.aclose())
    assert first == second
    assert state["calls"] == calls_after_first, "命中缓存还重复打上游"
    assert lxapp._KW_SONGINFO_STATS["hits"] == 1


def test_kw_songinfo_miss_is_negatively_cached(isolated):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"msg": "音乐查询失败", "data": {}})

    state = {"calls": 0}

    def counting(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        return handler(request)

    client = mock_client(counting)
    try:
        asyncio.run(lxapp._kw_songinfo_fetch(client, "999999"))
        after_first = state["calls"]
        asyncio.run(lxapp._kw_songinfo_fetch(client, "999999"))
    finally:
        asyncio.run(client.aclose())
    assert after_first == lxapp._KW_SONGINFO_TRIES, "未命中应打满重试次数"
    assert state["calls"] == after_first, "未命中结果没有进负缓存"
    assert lxapp._KW_SONGINFO_STATS["miss"] == 1


def test_kw_resolve_lyric_builds_standard_lrc(isolated):
    client = mock_client(lambda request: httpx.Response(200, json=_SONGINFO_BODY))
    try:
        text = asyncio.run(lxapp.kw_resolve_lyric(client, {"id": "lx:kw:877356"}))
    finally:
        asyncio.run(client.aclose())
    assert text.startswith("[ti:小幸运]\n[ar:田馥甄]\n[al:我的少女时代]\n[by:]\n[offset:0]\n")
    assert "[00:00.00]小幸运 - 田馥甄" in text
    assert "[00:18.42]我听见雨滴落在青青草地" in text
    assert "[] " not in text, "空行不该产出时间戳行"


def test_kw_resolve_lyric_without_numeric_id_does_not_call_upstream(isolated):
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("非法 rid 不应该打上游")

    client = mock_client(handler)
    try:
        assert asyncio.run(lxapp.kw_resolve_lyric(client, {"id": "lx:kw:abc"})) == ""
    finally:
        asyncio.run(client.aclose())


def test_wy_lyric_requests_full_lrc(isolated):
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"lrc": {"lyric": "[00:01.00]测试行"}})

    client = mock_client(handler)
    try:
        text = asyncio.run(lxapp.wy_resolve_lyric(client, "528283"))
    finally:
        asyncio.run(client.aclose())
    assert text == "[00:01.00]测试行"
    assert "lv=-1" in seen["url"], "lv 必须为 -1，否则网易返回空歌词"


def test_track_songinfo_endpoint_returns_cover_and_lyric(isolated):
    def handler(request: httpx.Request) -> httpx.Response:
        if "songinfoandlrc" in str(request.url):
            return httpx.Response(200, json=_SONGINFO_BODY)
        return httpx.Response(404)

    lxapp.app.state.http = mock_client(handler)
    from fastapi.testclient import TestClient

    with TestClient(lxapp.app) as client:
        resp = client.get("/api/v1/track/songinfo", params={"id": "lx:kw:877356"})
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["pic"] == "http://img.test/cover.jpg"
    assert data["title"] == "小幸运"
    assert "[00:18.42]" in data["lyric"]


def test_track_songinfo_endpoint_rejects_other_platforms(isolated):
    from fastapi.testclient import TestClient

    with TestClient(lxapp.app) as client:
        resp = client.get("/api/v1/track/songinfo", params={"id": "lx:tx:0039MnYb0qxYhV"})
    assert resp.status_code == 200
    assert resp.json()["data"]["pic"] == ""
