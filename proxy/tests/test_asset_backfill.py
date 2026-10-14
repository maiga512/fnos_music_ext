"""起播预取窗口与后台封面/歌词补齐扫描。

现场实测：外网起播的尾部超时来自"按下下一首才开始排队取链"，而后台补齐扫描
一旦不设限会把播放挤死（2.6.36 事故：每播一首同时跑 5 平台封面搜索 + 20s
歌词搜索，单进程代理被后台任务打满，主流请求跟着 502 跳歌）。这里锁住两件
事：预取窗口按配置铺开且带冷却，补齐扫描只处理真缺资产的歌、并且可断点续扫。
"""
import json
import time

import pytest

import proxy.app as pa


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    pa._CERU_PREFETCH_AT.clear()
    pa._LX_URL_CACHE.clear()
    pa._LX_PREFETCH_RUNNING.clear()
    monkeypatch.setattr(pa, "_CERU_LAST_PAGE_GUIDS", [])
    monkeypatch.setattr(pa, "_CERU_PAGE_INDEX", {})


def _page(n: int) -> list[str]:
    return [f"online:lx:kw:{1000 + i}" for i in range(n)]


def test_prefetch_window_covers_three_next_tracks(monkeypatch):
    seen: list[list[str]] = []
    monkeypatch.setattr(pa, "schedule_lx_prefetch", lambda ids: seen.append(list(ids)))
    monkeypatch.setattr(pa, "_CERU_LAST_PAGE_GUIDS", _page(6))
    monkeypatch.setattr(pa, "_CERU_PAGE_INDEX", {g: i for i, g in enumerate(_page(6))})

    pa._schedule_next_track_prefetch(_page(6)[0])

    assert seen == [["lx:kw:1001", "lx:kw:1002", "lx:kw:1003"]], seen


def test_prefetch_cooldown_prevents_repeated_work(monkeypatch):
    seen: list[list[str]] = []
    monkeypatch.setattr(pa, "schedule_lx_prefetch", lambda ids: seen.append(list(ids)))
    monkeypatch.setattr(pa, "_CERU_LAST_PAGE_GUIDS", _page(6))
    monkeypatch.setattr(pa, "_CERU_PAGE_INDEX", {g: i for i, g in enumerate(_page(6))})

    pa._schedule_next_track_prefetch(_page(6)[0])
    pa._schedule_next_track_prefetch(_page(6)[0])  # 冷却期内不得重复排队

    assert len(seen) == 1, seen


def test_prefetch_window_is_configurable(monkeypatch):
    seen: list[list[str]] = []
    monkeypatch.setattr(pa, "schedule_lx_prefetch", lambda ids: seen.append(list(ids)))
    monkeypatch.setattr(pa, "_CERU_PREFETCH_WINDOW", 1)
    monkeypatch.setattr(pa, "_CERU_LAST_PAGE_GUIDS", _page(6))
    monkeypatch.setattr(pa, "_CERU_PAGE_INDEX", {g: i for i, g in enumerate(_page(6))})

    pa._schedule_next_track_prefetch(_page(6)[1])

    assert seen == [["lx:kw:1002"]], seen


@pytest.mark.anyio
async def test_sweeper_only_backfills_missing_assets(monkeypatch, tmp_path):
    """已有封面+歌词的歌必须跳过：官方接口有预算，重复扫等于白烧配额。"""
    guids = [f"online:lx:kw:{i}" for i in range(1, 6)]
    songs = [{"id": g} for g in guids]

    async def fake_index():
        return {"by_key": {g: {"id": g} for g in guids}}

    preheated: list[str] = []
    monkeypatch.setattr(pa, "_ceru_favorites_index", fake_index)
    monkeypatch.setattr(pa, "_ceru_light_track", lambda song: {"guid": song["id"]})
    monkeypatch.setattr(pa, "_background_request", lambda: None)
    monkeypatch.setattr(pa, "_preheat_track_assets", lambda request, guid: preheated.append(guid))
    # 1、2 首资产齐全；3 缺封面；4 缺歌词；5 两者都缺
    monkeypatch.setattr(pa, "_read_cover_cache",
                        lambda guid: (b"x", "image/jpeg") if guid in guids[:2] else None)
    monkeypatch.setattr(pa, "read_lyric_cache", lambda guid: "[00:00.00]x" if guid not in (guids[3], guids[4]) else "")
    monkeypatch.setitem(pa.CONF, "cache_dir", str(tmp_path))
    monkeypatch.setattr(pa, "_ASSET_SWEEP_START_DELAY_S", 0.0)
    monkeypatch.setattr(pa, "_ASSET_SWEEP_GAP_S", 0.0)
    monkeypatch.setattr(pa, "_ASSET_SWEEP_PLAY_AVOID_S", 0.0)

    task = pa.asyncio.create_task(pa._asset_sweeper())
    for _ in range(200):
        if len(preheated) >= 3:
            break
        await pa.asyncio.sleep(0.01)
    task.cancel()
    await pa.asyncio.gather(task, return_exceptions=True)

    assert preheated == [guids[2], guids[3], guids[4]], preheated
    state = json.loads((tmp_path / "asset_sweep.json").read_text())
    assert state["cursor"] == len(songs), state
    assert state["finishedAt"], "整轮跑完要落时间戳，复扫间隔靠它判定"


@pytest.mark.anyio
async def test_sweeper_resumes_from_saved_cursor(monkeypatch, tmp_path):
    """重启后从断点继续：一轮没扫完不该从头再打一遍官方接口。"""
    guids = [f"online:lx:kw:{i}" for i in range(1, 6)]
    (tmp_path / "asset_sweep.json").write_text(json.dumps({"cursor": 4, "finishedAt": 0}))

    async def fake_index():
        return {"by_key": {g: {"id": g} for g in guids}}

    preheated: list[str] = []
    monkeypatch.setattr(pa, "_ceru_favorites_index", fake_index)
    monkeypatch.setattr(pa, "_ceru_light_track", lambda song: {"guid": song["id"]})
    monkeypatch.setattr(pa, "_background_request", lambda: None)
    monkeypatch.setattr(pa, "_preheat_track_assets", lambda request, guid: preheated.append(guid))
    monkeypatch.setattr(pa, "_read_cover_cache", lambda guid: None)
    monkeypatch.setattr(pa, "read_lyric_cache", lambda guid: "")
    monkeypatch.setitem(pa.CONF, "cache_dir", str(tmp_path))
    monkeypatch.setattr(pa, "_ASSET_SWEEP_START_DELAY_S", 0.0)
    monkeypatch.setattr(pa, "_ASSET_SWEEP_GAP_S", 0.0)
    monkeypatch.setattr(pa, "_ASSET_SWEEP_PLAY_AVOID_S", 0.0)

    task = pa.asyncio.create_task(pa._asset_sweeper())
    for _ in range(200):
        if preheated:
            break
        await pa.asyncio.sleep(0.01)
    task.cancel()
    await pa.asyncio.gather(task, return_exceptions=True)

    assert preheated == [guids[4]], preheated


def test_last_play_at_set_when_stream_opens():
    """补齐扫描靠这个时间戳让路，它必须在取流入口最前面就更新。"""
    import inspect

    src = inspect.getsource(pa._open_online_stream)
    assert "_LAST_PLAY_AT" in src.split("\n\n")[0] or "_LAST_PLAY_AT" in src[:800], src[:600]
