"""跨源封面搜索端点契约测试：匹配阈值 / 打分 / 单平台预算 / single-flight / fast 歌词。"""
from __future__ import annotations

import asyncio

import httpx

from conftest import lxapp, mock_client


def run(coro):
    return asyncio.run(coro)


def _tx_item(mid: str, title: str, artist: str, album: str = "", interval: int = 0) -> dict:
    return {
        "mid": mid,
        "title": title,
        "singer": [{"name": artist}],
        "album": {"name": album, "mid": f"alb{mid}"},
        "interval": interval,
        "pay": {"pay_play": 0},
    }


def _tx_body(items: list[dict]) -> dict:
    return {"req_1": {"data": {"body": {"song": {"list": items}}}}}


def _search_handler(items: list[dict], kw_delay_s: float = 0.0):
    """tx 返回候选；其余四平台 404（可选：kw 加延迟用于单平台预算用例）。"""

    async def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "musicu.fcg" in url:
            return httpx.Response(200, json=_tx_body(items))
        if "search.kuwo.cn" in url and kw_delay_s:
            await asyncio.sleep(kw_delay_s)
        return httpx.Response(404, json={})

    return handler


def test_cover_search_picks_exact_title_over_dj_version(isolated):
    items = [
        _tx_item("M1", "别想她 (DJ版)", "高进", "别想她", 250),
        _tx_item("M2", "别想她", "高进", "别想她", 269),
        _tx_item("M3", "别想她", "周杰伦", "叶赫那拉", 260),
    ]
    lxapp.app.state.http = mock_client(_search_handler(items))
    result = run(lxapp.cover_search(title="别想她", artist="高进", album="", duration=0.0))
    assert result["ok"] is True
    data = result["data"]
    assert data is not None
    assert data["id"] == "lx:tx:M2"
    assert data["lx_source"] == "tx"
    assert data["cover_url"] == "https://y.gtimg.cn/music/photo_new/T002R300x300M000albM2.jpg"


def test_cover_search_rejects_dj_only(isolated):
    """只有 DJ 版：标题相似度 < 0.86，宁可 None 也不拿错封面。"""
    items = [_tx_item("M1", "别想她 (DJ版)", "高进", "别想她", 250)]
    lxapp.app.state.http = mock_client(_search_handler(items))
    result = run(lxapp.cover_search(title="别想她", artist="高进", album="", duration=0.0))
    assert result["ok"] is True
    assert result["data"] is None


def test_cover_search_rejects_artist_mismatch(isolated):
    items = [_tx_item("M3", "别想她", "周杰伦", "叶赫那拉", 260)]
    lxapp.app.state.http = mock_client(_search_handler(items))
    result = run(lxapp.cover_search(title="别想她", artist="高进", album="", duration=0.0))
    assert result["data"] is None


def test_cover_search_artistless_uses_looser_title_threshold(isolated):
    """无歌手参数：标题阈值放宽到 0.90；时长加成决定平手局。"""
    items = [
        _tx_item("M2", "别想她", "高进", "别想她", 269),
        _tx_item("M4", "别想她", "张三", "杂锦", 400),
    ]
    lxapp.app.state.http = mock_client(_search_handler(items))
    result = run(lxapp.cover_search(title="别想她", artist="", album="", duration=269.0))
    assert result["data"] is not None
    assert result["data"]["id"] == "lx:tx:M2"


def test_cover_search_no_match_returns_none(isolated):
    items = [_tx_item("M5", "晴天", "周杰伦", "叶惠美", 269)]
    lxapp.app.state.http = mock_client(_search_handler(items))
    result = run(lxapp.cover_search(title="稻香", artist="周杰伦", album="", duration=0.0))
    assert result["data"] is None


def test_cover_search_duration_bonus_tiebreak(isolated):
    items = [
        _tx_item("M1", "稻香", "周杰伦", "魔杰座", 200),
        _tx_item("M2", "稻香", "周杰伦", "魔杰座", 269),
    ]
    lxapp.app.state.http = mock_client(_search_handler(items))
    result = run(lxapp.cover_search(title="稻香", artist="周杰伦", album="", duration=269.0))
    assert result["data"] is not None
    assert result["data"]["id"] == "lx:tx:M2"
    assert result["data"]["score"] >= 1.0


def test_cover_search_requires_title(isolated):
    lxapp.app.state.http = mock_client(_search_handler([]))
    resp = run(lxapp.cover_search(title="  ", artist="高进", album="", duration=0.0))
    assert resp.status_code == 400


def test_cover_search_per_source_budget(monkeypatch, isolated):
    """单平台 5s 预算内超时的源被放弃，不影响其他平台的结果。"""
    monkeypatch.setattr(lxapp, "_COVER_SEARCH_PER_SRC_S", 0.2)
    items = [_tx_item("M2", "别想她", "高进", "别想她", 269)]
    lxapp.app.state.http = mock_client(_search_handler(items, kw_delay_s=0.5))
    result = run(lxapp.cover_search(title="别想她", artist="高进", album="", duration=0.0))
    assert result["ok"] is True
    assert result["data"] is not None
    assert result["data"]["id"] == "lx:tx:M2"


def test_cover_search_single_flight(isolated):
    """同一首歌并发两次：只跑一轮搜索（任务被复用）。"""
    started = {"n": 0}

    async def slow_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "musicu.fcg" in url:
            started["n"] += 1
            await asyncio.sleep(0.2)
            return httpx.Response(200, json=_tx_body(
                [_tx_item("M2", "别想她", "高进", "别想她", 269)]))
        return httpx.Response(404, json={})

    lxapp.app.state.http = mock_client(slow_handler)

    async def both():
        return await asyncio.gather(
            lxapp.cover_search(title="别想她", artist="高进", album="", duration=0.0),
            lxapp.cover_search(title="别想她", artist="高进", album="", duration=0.0),
        )

    r1, r2 = run(both())
    assert r1["ok"] and r2["ok"]
    assert r1["data"] is not None and r2["data"] is not None
    assert started["n"] == 1


SEARCH_HOSTS = ("mobilecdn.kugou.com", "music.163.com", "c.music.migu.cn",
                "u.y.qq.com", "search.kuwo.cn")


def _lyric_handler(counts: dict):
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if any(h in url for h in SEARCH_HOSTS):
            counts["search"] += 1
        return httpx.Response(404, json={})

    return handler


def test_track_lyric_fast_skips_cross_platform_search(isolated):
    counts = {"search": 0}
    lxapp.app.state.http = mock_client(_lyric_handler(counts))
    r1 = run(lxapp.track_lyric(id="kw:123", guid="", title="测试歌", artist="测试歌手", fast=1))
    assert r1["ok"] is True
    assert r1["data"]["lyric"] == ""
    assert counts["search"] == 0

    r2 = run(lxapp.track_lyric(id="kw:123", guid="", title="测试歌", artist="测试歌手", fast=0))
    assert r2["ok"] is True
    assert counts["search"] >= 1


# ---------------------------------------------------------------- 阶梯搜索 2.6.38
def _kw_abs_item(rid: str, title: str, artist: str, duration: int = 200, pic: str = "") -> dict:
    return {
        "MUSICRID": f"MUSIC_{rid}",
        "SONGNAME": title,
        "ARTIST": artist,
        "ALBUM": "",
        "DURATION": str(duration),
        "PAY": "0",
        "payInfo": {"cannotOnlinePlay": "0"},
        "web_albumpic_short": pic or f"star/albumcover/240/x/{rid}.jpg",
    }


def _staged_handler(counts: dict, tx_items: list[dict], kw_items: list[dict]):
    """tx / kw 返回候选，其余平台 404；按主机计数请求。"""

    async def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "musicu.fcg" in url:
            counts["tx"] += 1
            return httpx.Response(200, json=_tx_body(tx_items))
        if "search.kuwo.cn" in url:
            counts["kw"] += 1
            return httpx.Response(200, text="{'abslist': " + repr(kw_items) + "}")
        counts.setdefault("other", 0)
        counts["other"] += 1
        return httpx.Response(404, json={})

    return handler


def test_cover_search_staged_stops_on_first_hit(isolated):
    """命中即停：kw 命中后不再打 tx/kg/wy/mg。"""
    counts = {"tx": 0, "kw": 0, "other": 0}
    lxapp.app.state.http = mock_client(_staged_handler(
        counts, [], [_kw_abs_item("877356", "飘雪", "陈慧娴", 236)]))
    result = run(lxapp.cover_search(title="飘雪", artist="陈慧娴", album="", duration=0.0, source="kw"))
    assert result["data"] is not None
    assert result["data"]["lx_source"] == "kw"
    assert counts["tx"] == 0
    assert counts["other"] == 0


def test_cover_search_staged_falls_through_to_next(isolated):
    """自身平台未命中 → 阶梯进入下一平台（tx 命中）。"""
    counts = {"tx": 0, "kw": 0, "other": 0}
    lxapp.app.state.http = mock_client(_staged_handler(
        counts, [_tx_item("M9", "别想她", "高进", "别想她", 269)],
        [_kw_abs_item("999", "别想她的DJ嗨曲", "DJ小帅", 269)]))
    result = run(lxapp.cover_search(title="别想她", artist="高进", album="", duration=0.0, source="kw"))
    assert result["data"] is not None
    assert result["data"]["lx_source"] == "tx"
    assert result["data"]["id"] == "lx:tx:M9"
    assert counts["kw"] == 1 and counts["tx"] == 1
    assert counts["other"] == 0


def test_cover_search_multi_artist_singer_match(isolated):
    """合唱曲：查询单歌手 vs 候选双歌手（张智霖 / 许秋怡）不再被整串相似度误杀。"""
    items = [_tx_item("M6", "片片枫叶情", "张智霖 / 许秋怡", "片片枫叶情", 250)]
    lxapp.app.state.http = mock_client(_search_handler(items))
    result = run(lxapp.cover_search(title="片片枫叶情", artist="张智霖", album="", duration=0.0))
    assert result["data"] is not None
    assert result["data"]["id"] == "lx:tx:M6"


def test_cover_search_daily_budget_stops_calls(monkeypatch, isolated):
    """每日预算耗尽：不再打官方搜索接口，返回 None。"""
    monkeypatch.setattr(lxapp, "_COVER_DAILY_BUDGET", 1)
    counts = {"tx": 0, "kw": 0, "other": 0}
    handler = _staged_handler(
        counts,
        [_tx_item("M1", "别想她", "高进", "别想她", 269),
         _tx_item("M2", "稻香", "周杰伦", "魔杰座", 269)],
        [])
    lxapp.app.state.http = mock_client(handler)
    r1 = run(lxapp.cover_search(title="别想她", artist="高进", album="", duration=0.0, source="tx"))
    assert r1["data"] is not None
    # 第二首：预算已用完（第一首消耗 1 次），直接 None 且不产生请求
    r2 = run(lxapp.cover_search(title="稻香", artist="周杰伦", album="", duration=0.0, source="tx"))
    assert r2["ok"] is True
    assert r2["data"] is None
    assert counts["tx"] == 1
