"""上游取链串行闸门：并发爆发会把公益中转源打到限流。

现场实测（NAS「我的喜欢」随机 100 首、同一 seed）：8 并发起播 34 成 66 败，
同一批歌逐首排队取链 94 成。闸门守住四件事：全局在途=1、两次调用最小间隔、
播放请求插到后台预热之前、后台任务队列积压时先甩单且不写熔断器。
全部离线：用户源走 FakeRuntime，媒体探活走 MockTransport。
"""
from __future__ import annotations

import asyncio
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from conftest import FakeRuntime, lxapp, mock_client

_ITEM = {"id": "lx:kw:778899", "title": "测试曲", "artist": "某人", "duration_s": 200}


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


def _tight(monkeypatch, **over) -> None:
    """把间隔/等待预算调小，让用例跑得快且与真实数值解耦。"""
    conf = {
        "upstream_spacing_ms": 20,      # 毫秒
        "upstream_wait_ms": 5000.0,     # 毫秒
        "upstream_bg_wait_ms": 2000.0,  # 毫秒
        "upstream_queue_max": 16,
    }
    conf.update(over)
    for key, val in conf.items():
        monkeypatch.setitem(lxapp.CONF, key, val)


def _probe_many(n: int):
    async def run():
        http = mock_client(_media_handler())
        try:
            tasks = [
                lxapp.resolve_and_probe(
                    http, "kw", dict(_ITEM, id=f"lx:kw:{1000 + i}"), "lossless"
                )
                for i in range(n)
            ]
            return await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await http.aclose()

    return asyncio.run(run())


def test_music_url_calls_are_serialized(isolated, monkeypatch):
    """8 首同发起播：真正打上游的脚本调用必须一次只有一个。"""
    _tight(monkeypatch, upstream_wait_ms=60000.0)
    state = {"now": 0, "peak": 0}

    async def resolver(music_info, quality, platform):
        state["now"] += 1
        state["peak"] = max(state["peak"], state["now"])
        await asyncio.sleep(0.03)
        state["now"] -= 1
        return "https://media.test/a.flac"

    isolated._runtime = FakeRuntime(resolver=resolver)
    results = _probe_many(8)
    ok = [r for r in results if isinstance(r, dict) and r.get("url")]
    assert len(ok) == 8, results
    assert state["peak"] == 1, "上游调用出现并发，闸门失效"
    assert lxapp.LX_UPSTREAM_GATE.snapshot()["waited"] >= 7


def test_minimum_spacing_between_calls(isolated, monkeypatch):
    """相邻两次脚本调用之间必须留出强制间隔（给上游的喘息）。"""
    _tight(monkeypatch, upstream_spacing_ms=120, upstream_wait_ms=60000.0)
    stamps: list[float] = []

    async def resolver(music_info, quality, platform):
        stamps.append(asyncio.get_running_loop().time())
        return "https://media.test/a.flac"

    isolated._runtime = FakeRuntime(resolver=resolver)
    _probe_many(3)
    assert len(stamps) == 3
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    assert min(gaps) >= 0.1, gaps


def test_playback_jumps_ahead_of_background(isolated, monkeypatch):
    """列表页探活（后台）排在前面时，播放解析必须插队先走。"""
    _tight(monkeypatch)
    gate = lxapp.LX_UPSTREAM_GATE
    order: list[str] = []

    async def hold():
        order.append("hold")
        await asyncio.sleep(0.1)

    async def bg():
        order.append("bg")

    async def play():
        order.append("play")

    async def run():
        first = asyncio.create_task(gate.run(hold, background=True))
        await asyncio.sleep(0.02)
        behind = asyncio.create_task(gate.run(bg, background=True))
        await asyncio.sleep(0.01)
        front = asyncio.create_task(gate.run(play, background=False))
        await asyncio.gather(first, behind, front)
        return order

    assert asyncio.run(run()) == ["hold", "play", "bg"]


def test_background_sheds_instead_of_squeezing_playback(isolated, monkeypatch):
    """队列一积压就先甩后台；同批播放请求照常排队成功。"""
    _tight(monkeypatch, upstream_queue_max=1)
    gate = lxapp.LX_UPSTREAM_GATE
    order: list[str] = []

    async def hold():
        await asyncio.sleep(0.05)

    async def bg1():
        order.append("bg1")

    async def bg2():
        order.append("bg2")

    async def play():
        order.append("play")

    async def run():
        tasks = [asyncio.create_task(gate.run(hold, background=True))]
        await asyncio.sleep(0.02)
        tasks.append(asyncio.create_task(gate.run(bg1, background=True)))
        await asyncio.sleep(0.01)
        tasks.append(asyncio.create_task(gate.run(bg2, background=True)))
        tasks.append(asyncio.create_task(gate.run(play, background=False)))
        return await asyncio.gather(*tasks, return_exceptions=True)

    done = asyncio.run(run())
    assert isinstance(done[2], lxapp.UpstreamBusy), done
    assert "play" in order and "bg1" in order and "bg2" not in order
    assert gate.snapshot()["shed"] >= 1


def test_busy_shed_does_not_trip_circuit(isolated, monkeypatch):
    """自己甩掉的请求不算源故障：不写熔断器、不记解析失败。"""
    _tight(monkeypatch, upstream_bg_wait_ms=0.05)
    isolated._runtime = FakeRuntime()
    lxapp.app.state.http = mock_client(_media_handler())
    lxapp.LX_UPSTREAM_GATE._est_s = 900.0  # 前面排到明年：后台应立刻甩单
    lxapp.LX_UPSTREAM_GATE._running = 1    # 模拟已有一次调用在途

    async def run():
        lxapp._UPSTREAM_BACKGROUND.set(True)
        http = mock_client(_media_handler())
        try:
            return await lxapp.resolve_and_probe(http, "kw", dict(_ITEM), "lossless")
        finally:
            await http.aclose()

    with pytest.raises(lxapp.UpstreamBusy):
        asyncio.run(run())
    assert lxapp._UPSTREAM_BACKGROUND.get() is False, "后台标记泄漏到调用方上下文"
    assert lxapp.chain_health_snapshot()["user_source"]["fails"] == 0
    assert isolated._runtime.calls == [], "被甩单的请求不该已经把脚本打出去"


def test_track_url_returns_503_when_queue_saturated(isolated, monkeypatch):
    """队列排不上要回 503：代理据此立即收手，不再逐档连撞同一个队列。"""
    _tight(monkeypatch, upstream_wait_ms=0.05)
    isolated._runtime = FakeRuntime()
    lxapp.app.state.http = mock_client(_media_handler())
    lxapp.LX_UPSTREAM_GATE._est_s = 900.0
    lxapp.LX_UPSTREAM_GATE._running = 1

    with TestClient(lxapp.app) as client:
        resp = client.get("/api/v1/track/url", params={"id": "lx:kw:228908"})
    assert resp.status_code == 503
    # 播放绝不按预估甩单，只可能等满预算：shed=0 / timed_out=1
    snap = lxapp.LX_UPSTREAM_GATE.snapshot()
    assert snap["shed"] == 0 and snap["timed_out"] == 1, snap


def test_proxy_background_flag_downgrades_priority(isolated, monkeypatch):
    """代理标了 X-Fnmusic-Bg 的取链（预取/预热）必须按后台处理：预估过深就直接甩。"""
    _tight(monkeypatch, upstream_bg_wait_ms=50.0, upstream_wait_ms=60000.0)
    isolated._runtime = FakeRuntime()
    lxapp.app.state.http = mock_client(_media_handler())
    lxapp.LX_UPSTREAM_GATE._est_s = 900.0
    lxapp.LX_UPSTREAM_GATE._running = 1  # 已有一枪在途

    with TestClient(lxapp.app) as client:
        resp = client.get("/api/v1/track/url", params={"id": "lx:kw:228908"},
                          headers={"X-Fnmusic-Bg": "1"})
    assert resp.status_code == 503
    snap = lxapp.LX_UPSTREAM_GATE.snapshot()
    assert snap["shed"] == 1 and snap["timed_out"] == 0, snap


def _two_operators(isolated, monkeypatch, resolver, *, lane_a="host:a.test", lane_b="host:b.test"):
    """主源 + 池内另一家运营商（不同 lane_key），返回 (primary, other)。"""
    primary = FakeRuntime(resolver=resolver)
    primary.lane_key = lane_a
    other = FakeRuntime(resolver=resolver)
    other.lane_key = lane_b
    isolated._runtime = primary
    monkeypatch.setattr(lxapp.SOURCE_POOL, "runtimes_for", lambda src: [("src_other", other)])
    return primary, other


def test_lanes_are_independent_objects(isolated):
    gate = lxapp.LX_UPSTREAM_GATE
    assert gate.lane("host:a") is gate.lane("host:a")
    assert gate.lane("host:a") is not gate.lane("host:b")
    assert gate.lane("") is gate.lane(lxapp.DEFAULT_UPSTREAM_LANE)
    snap = gate.snapshot()
    assert set(snap["lanes"]) == {"host:a", "host:b", lxapp.DEFAULT_UPSTREAM_LANE}
    gate.reset()
    assert gate.snapshot()["lanes"] == {}


def test_same_lane_serializes_different_lanes_do_not(isolated, monkeypatch):
    """同一家运营商必须串行；不同运营商各走各的道，互不排队。"""
    _tight(monkeypatch, upstream_spacing_ms=0, upstream_wait_ms=60000.0)
    state = {"now": 0, "peak_same": 0, "peak_diff": 0}

    async def slow():
        key = "peak_same" if state["tag"] == "same" else "peak_diff"
        state["now"] += 1
        state[key] = max(state[key], state["now"])
        await asyncio.sleep(0.03)
        state["now"] -= 1

    async def run(tag: str, lanes: list):
        state["tag"] = tag
        state["now"] = 0
        gate = lxapp.LX_UPSTREAM_GATE
        await asyncio.gather(*[gate.run(slow, lane=lane) for lane in lanes])

    asyncio.run(run("same", ["host:a", "host:a", "host:a"]))
    assert state["peak_same"] == 1, "同一家运营商出现了并发"
    asyncio.run(run("diff", ["host:a", "host:b", "host:c"]))
    assert state["peak_diff"] == 3, "不同运营商被强行挤进同一条队列"


def test_burst_spreads_across_operators(isolated, monkeypatch):
    """连续起播时后面的歌要改打空闲的那家，而不是在主源队尾干等。"""
    _tight(monkeypatch, upstream_spacing_ms=0, upstream_wait_ms=60000.0)
    state = {"now": 0, "peak": 0}

    async def resolver(music_info, quality, platform):
        state["now"] += 1
        state["peak"] = max(state["peak"], state["now"])
        await asyncio.sleep(0.05)
        state["now"] -= 1
        return "https://media.test/a.flac"

    primary, other = _two_operators(isolated, monkeypatch, resolver)
    isolated._runtime = primary
    lxapp.app.state.http = mock_client(_media_handler())

    async def run():
        http = mock_client(_media_handler())
        try:
            first = asyncio.create_task(lxapp.resolve_and_probe(
                http, "kw", dict(_ITEM, id="lx:kw:1"), "lossless"))
            await asyncio.sleep(0.01)  # 第一首已经占上主源那条道
            second = asyncio.create_task(lxapp.resolve_and_probe(
                http, "kw", dict(_ITEM, id="lx:kw:2"), "lossless"))
            return await asyncio.gather(first, second)
        finally:
            await http.aclose()

    results = asyncio.run(run())
    assert all(isinstance(r, dict) and r.get("url") for r in results), results
    assert state["peak"] == 2, "两首歌没有分流到两家运营商"
    assert len(primary.calls) == 1 and len(other.calls) == 1, (primary.calls, other.calls)


def test_idle_still_prefers_configured_order(isolated, monkeypatch):
    """各道都空闲时必须仍是用户配置的第一优先：主源先打，中用即停。"""
    _tight(monkeypatch)

    async def resolver(music_info, quality, platform):
        return "https://media.test/a.flac"

    primary, other = _two_operators(isolated, monkeypatch, resolver)
    isolated._runtime = primary
    lxapp.app.state.http = mock_client(_media_handler())
    _probe_many(1)
    assert len(primary.calls) == 1
    assert other.calls == [], "空闲时也分流了，用户配置的主源优先级被忽略"


def test_busy_primary_falls_through_to_other_operator(isolated, monkeypatch):
    """主源那条道排满了不能判死这首歌：换一家运营商的队列继续试。"""
    _tight(monkeypatch, upstream_wait_ms=0.05, upstream_spacing_ms=0)

    async def slow(music_info, quality, platform):
        await asyncio.sleep(0.5)
        return "https://media.test/slow.flac"

    async def fast(music_info, quality, platform):
        return "https://media.test/other.flac"

    primary = FakeRuntime(resolver=slow)
    primary.lane_key = "host:a.test"
    other = FakeRuntime(resolver=fast)
    other.lane_key = "host:b.test"
    isolated._runtime = primary
    monkeypatch.setattr(lxapp.SOURCE_POOL, "runtimes_for", lambda src: [("src_other", other)])
    lxapp.app.state.http = mock_client(_media_handler())

    async def hold_factory():
        await asyncio.sleep(0.5)

    async def run():
        http = mock_client(_media_handler())
        # 主源那条道先被一个后台调用占满，播放请求 50ms 内排不上
        hold = asyncio.create_task(
            lxapp.LX_UPSTREAM_GATE.run(hold_factory, background=True, lane="host:a.test")
        )
        await asyncio.sleep(0.01)
        try:
            return await lxapp.resolve_and_probe(http, "kw", dict(_ITEM), "lossless")
        finally:
            await http.aclose()

    result = asyncio.run(run())
    assert isinstance(result, dict) and result.get("url") == "https://media.test/other.flac", result
    assert len(other.calls) == 1, "没有落到另一家运营商"


def test_spread_prefers_lane_that_frees_up_soonest(isolated, monkeypatch):
    """分流按「预计还要等多久」而不是排队条数：慢源在途时要给快源让路。

    现场依据：100 首并发里聆澜道 268 枪、单枪 0.53s、18 次播放等待超时，
    独家道单枪 0.09s 却几乎空闲——只按条数排会把歌继续送进慢道。
    """
    _tight(monkeypatch, upstream_spacing_ms=0, upstream_wait_ms=60000.0)
    slow_lane = lxapp.LX_UPSTREAM_GATE.lane("host:slow.test")
    slow_lane._est_s = 0.6  # 该道实测单枪耗时（EWMA），快道保持默认更小值
    lxapp.LX_UPSTREAM_GATE.lane("host:fast.test")._est_s = 0.05

    calls = {"slow": 0, "fast": 0}

    async def slow_resolver(music_info, quality, platform):
        calls["slow"] += 1
        raise RuntimeError("慢源这轮不出链")

    async def fast_resolver(music_info, quality, platform):
        calls["fast"] += 1
        return "https://media.test/fast.flac"

    slow_rt = FakeRuntime(resolver=slow_resolver)
    slow_rt.lane_key = "host:slow.test"
    fast_rt = FakeRuntime(resolver=fast_resolver)
    fast_rt.lane_key = "host:fast.test"
    isolated._runtime = slow_rt
    monkeypatch.setattr(lxapp.SOURCE_POOL, "runtimes_for", lambda src: [("src_fast", fast_rt)])
    lxapp.app.state.http = mock_client(_media_handler())

    async def hold():
        await asyncio.sleep(0.4)  # 真实占住慢道一个名额

    async def run():
        http = mock_client(_media_handler())
        # 慢道上真的有一枪在途：它预计还要 0.6s，快道空闲 0s
        busy = asyncio.create_task(
            lxapp.LX_UPSTREAM_GATE.run(hold, background=True, lane="host:slow.test")
        )
        await asyncio.sleep(0.02)
        try:
            return await lxapp.resolve_and_probe(http, "kw", dict(_ITEM), "lossless")
        finally:
            await http.aclose()
            busy.cancel()
            await asyncio.gather(busy, return_exceptions=True)

    result = asyncio.run(run())
    assert isinstance(result, dict) and result.get("url") == "https://media.test/fast.flac", result
    assert calls["fast"] == 1 and calls["slow"] == 0, calls


def test_wait_cap_rotates_to_next_operator(isolated, monkeypatch):
    """一家道排长队时不能死等：到点必须换下一家（不同域名不同队列）。

    现场依据：排队失败的歌单独跑 0.9-2.7s 就出链，说明它们在聆澜队尾空等，
    而独家/ikun 当时是空的。多家时单道等待上限 upstream_alt_wait_ms 生效，
    只有一家时仍按 upstream_wait_ms 长等（没得换）。
    """
    _tight(monkeypatch, upstream_spacing_ms=0, upstream_wait_ms=60000.0, upstream_alt_wait_ms=60.0)
    calls: dict[str, int] = {"slow": 0, "other": 0}

    async def slow(music_info, quality, platform):
        calls["slow"] += 1
        await asyncio.sleep(0.5)
        return "https://media.test/slow.flac"

    async def other(music_info, quality, platform):
        calls["other"] += 1
        return "https://media.test/other.flac"

    slow_rt = FakeRuntime(resolver=slow)
    slow_rt.lane_key = "host:slow.test"
    other_rt = FakeRuntime(resolver=other)
    other_rt.lane_key = "host:other.test"
    isolated._runtime = slow_rt
    monkeypatch.setattr(lxapp.SOURCE_POOL, "runtimes_for", lambda src: [("src_other", other_rt)])
    lxapp.app.state.http = mock_client(_media_handler())

    async def hold():
        await asyncio.sleep(0.5)  # 真实占住慢道名额

    async def run():
        http = mock_client(_media_handler())
        # 慢道上先压两枪（一枪在途 + 一枪排队），本首要在这条道上排到超时才换家
        holds = [
            asyncio.create_task(
                lxapp.LX_UPSTREAM_GATE.run(hold, background=True, lane="host:slow.test")
            )
            for _ in range(2)
        ]
        await asyncio.sleep(0.02)
        try:
            return await lxapp.resolve_and_probe(http, "kw", dict(_ITEM), "lossless")
        finally:
            await http.aclose()
            for h in holds:
                h.cancel()
            await asyncio.gather(*holds, return_exceptions=True)

    started = time.monotonic()
    result = asyncio.run(run())
    elapsed = time.monotonic() - started
    assert isinstance(result, dict) and result.get("url") == "https://media.test/other.flac", result
    assert elapsed < 0.5, f"没有按 alt 上限换家，等了 {elapsed:.2f}s"
    assert calls["other"] == 1, calls


def test_single_operator_keeps_long_wait(isolated, monkeypatch):
    """只有一个源时不能提前判死：没有别的道可换，仍按 12s 预算排队等。"""
    _tight(monkeypatch, upstream_spacing_ms=0, upstream_wait_ms=60000.0, upstream_alt_wait_ms=60.0)

    async def slow(music_info, quality, platform):
        await asyncio.sleep(0.15)
        return "https://media.test/only.flac"

    rt = FakeRuntime(resolver=slow)
    rt.lane_key = "host:only.test"
    isolated._runtime = rt
    monkeypatch.setattr(lxapp.SOURCE_POOL, "runtimes_for", lambda src: [])
    lxapp.app.state.http = mock_client(_media_handler())

    async def hold():
        await asyncio.sleep(0.15)

    async def run():
        http = mock_client(_media_handler())
        holds = [
            asyncio.create_task(
                lxapp.LX_UPSTREAM_GATE.run(hold, background=True, lane="host:only.test")
            )
            for _ in range(2)
        ]
        await asyncio.sleep(0.02)
        try:
            return await lxapp.resolve_and_probe(http, "kw", dict(_ITEM), "lossless")
        finally:
            await http.aclose()
            for h in holds:
                h.cancel()
            await asyncio.gather(*holds, return_exceptions=True)

    result = asyncio.run(run())
    assert isinstance(result, dict) and result.get("url") == "https://media.test/only.flac", result
