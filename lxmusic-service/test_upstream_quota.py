"""上游音源按天配额（现场原文：该 API Key 今日成功请求已达 1000 次上限）。

配额打光必须当成"这家今天没了"，不是"这首歌没货"：优先用还有额度的家，
打光的那家停到手但保留在队尾兜底，一家不剩时行为与旧版完全一致。
"""
import asyncio

import httpx
import pytest

from conftest import FakeRuntime, lxapp, mock_client


def _media_handler():
    def handler(request: httpx.Request) -> httpx.Response:
        if "media.test" in str(request.url):
            return httpx.Response(
                206,
                headers={"Content-Type": "audio/flac", "Content-Range": "bytes 0-1/28936190"},
                content=b"fLaC",
            )
        return httpx.Response(404)

    return handler


@pytest.fixture(autouse=True)
def _clean_quota(monkeypatch):
    lxapp._QUOTA_PARKED.clear()
    monkeypatch.setitem(lxapp.CONF, "upstream_spacing_ms", 0)
    monkeypatch.setitem(lxapp.CONF, "fast_start", True)
    yield
    lxapp._QUOTA_PARKED.clear()


_QUOTA_TEXT = "该 API Key 今日成功请求已达 1000 次上限"


def _dead_quota(order, tag):
    async def resolver(music_info, quality, platform):
        order.append(tag)
        raise RuntimeError(_QUOTA_TEXT)

    rt = FakeRuntime(resolver=resolver)
    rt.lane_key = f"host:{tag}.test"
    return rt


def _healthy(order, tag, url="https://media.test/a.flac"):
    async def resolver(music_info, quality, platform):
        order.append(tag)
        return url

    rt = FakeRuntime(resolver=resolver)
    rt.lane_key = f"host:{tag}.test"
    return rt


def _probe(rt):
    async def run():
        http = mock_client(_media_handler())
        try:
            return await lxapp.resolve_and_probe(http, "kw", {
                "id": "lx:kw:778899", "title": "测试曲", "artist": "某人", "duration_s": 200})
        finally:
            await http.aclose()

    return asyncio.run(run())


def test_quota_error_parks_operator_and_next_song_skips_it(isolated, monkeypatch):
    order = []
    dead = _dead_quota(order, "A")
    good = _healthy(order, "B")
    isolated._runtime = dead
    monkeypatch.setattr(lxapp.SOURCE_POOL, "runtimes_for", lambda src: [("src_b", good)])

    assert _probe(dead) is not None, "另一家还有配额时必须出链"
    assert lxapp._quota_parked("host:A.test"), "报配额上限的那家必须当天停用"
    assert not lxapp._quota_parked("host:B.test"), "别家不受影响"

    order.clear()
    assert _probe(dead) is not None
    assert order and all(t == "B" for t in order), f"第二首歌不该再打已停用的一家：{order}"


def test_parked_operator_stays_last_resort(isolated, monkeypatch):
    """全都停用时必须照旧尝试，绝不能因为停用判定而少试一家。"""
    order = []
    good = _healthy(order, "B")
    isolated._runtime = good
    monkeypatch.setattr(lxapp.SOURCE_POOL, "runtimes_for", lambda src: [])
    lxapp._quota_park("host:B.test")

    assert _probe(good) is not None
    assert order, "全部停用时仍要照常解析"


def test_quota_park_until_resets_within_a_day():
    now = 1_760_000_000.0
    until = lxapp._quota_park_until(now)
    assert now < until <= now + 86400.0
    assert lxapp._is_quota_error(RuntimeError(_QUOTA_TEXT))
    assert not lxapp._is_quota_error(RuntimeError("歌曲不存在"))
