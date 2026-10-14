# TOP[top.lxsvc] 洛雪 HTTP 服务：ID 契约 lx:<source>:<id>，搜索/歌词/封面内置、播放走源运行时
"""lxmusic HTTP 服务：洛雪音乐 (LX Music) 风格音源 API.

统一曲目 ID 契约: "lx:<source>:<identifier>"，例如：
  - "lx:kg:<filehash>"    酷狗
  - "lx:wy:<song_id>"     网易云
  - "lx:mg:<copyrightId>" 咪咕

分工与洛雪桌面版一致：搜索/歌词/封面/热门榜单走内置平台免登录接口；
播放直链解析只由用户提供的洛雪自定义源脚本完成（source_runtime.py，
Node 沙箱执行，规范见 https://lxmusic.toside.cn/desktop/custom-source）。
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any

import httpx
from fastapi import FastAPI, Header, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from source_runtime import (
    SourceError,
    SourceManager,
    MultiSourceManager,
    UserSource,
    build_music_info,
    is_source_url,
    memory_guard_enabled,
    parse_script_meta,
    run_memory_guard,
    save_upload,
    script_quality_for_tier,
)

logger = logging.getLogger("lxmusic_service")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

SERVICE_VERSION = "2.0.0"

# 支持的音源别名归一化
_SOURCE_ALIASES = {
    "kg": "kg",
    "kugou": "kg",
    "wy": "wy",
    "netease": "wy",
    "163": "wy",
    "mg": "mg",
    "migu": "mg",
    "tx": "tx",
    "qq": "tx",
    "tencent": "tx",
    "kw": "kw",
    "kuwo": "kw",
}


def normalize_source(raw: str) -> str:
    return _SOURCE_ALIASES.get((raw or "").strip().lower(), "")


def _normalize_sources(raw_list: list) -> list:
    """LX_SOURCES → 规范平台代码，去重去非法；全部非法时回退默认。

    别名（kugou/kuwo 等）若不归一，会被 /api/v1/search 的 `_SEARCHERS[src]`
    查找静默跳过，表现为"配置了平台却搜不到结果"。"""
    out: list = []
    for item in raw_list:
        code = normalize_source(item)
        if code and code not in out:
            out.append(code)
    return out or ["kg", "wy", "mg", "kw"]


CONF = {
    "sources": _normalize_sources(
        [s.strip() for s in os.environ.get("LX_SOURCES", "kg,wy,mg,kw").split(",") if s.strip()]
    ),
    "search_timeout": float(os.environ.get("LX_SEARCH_TIMEOUT", "12")),
    "limit_per_source": int(os.environ.get("LX_LIMIT_PER_SOURCE", "20")),
    # 播放端 track/url 端点总预算：容纳单档解析（12s）+ 探活（5s）后再降档，
    # 需略低于 proxy 侧 resolve_lx_url 的单档 HTTP 超时（22s）
    "url_timeout": float(os.environ.get("LX_URL_TIMEOUT", "20")),
    "cache_max": int(os.environ.get("LX_CACHE_MAX", "2000")),
    "cache_ttl": int(os.environ.get("LX_CACHE_TTL", "1800")),
    # 用户自定义源脚本地址（state.json 持久化优先，env 仅作首次种子）
    "source_url": (os.environ.get("LX_SOURCE_URL") or "").strip(),
    # 用户源单次 musicUrl 解析预算：野生源多为二级转发（脚本→中转服务→平台），
    # 实证水位在 3-8s（verify_source 用 12s），4s 会把慢源全部掐死
    "resolver_timeout": float(os.environ.get("LX_RESOLVER_TIMEOUT", "12.0")),
    "probe_timeout": float(os.environ.get("LX_PROBE_TIMEOUT", "5.0")),
    # 搜索期 VIP/第三方直链曲目的探活结果有效期（秒）：过期后 track/url 重新解析
    "probe_fresh_s": int(os.environ.get("LX_PROBE_FRESH_S", "900")),
    # 逐曲探活开关（beta）：默认关。开启时在搜索阶段逐曲调用用户源解析+Range探活；
    # 慢速用户源或多平台并发容易引发搜索超时，建议保持关闭（播放时实时解析直链）
    "search_probe": os.environ.get("FNMUSIC_SEARCH_PROBE", "false").lower() in ("true", "1", "yes"),
    # 低频快起：首播只打请求锚点档、一中即停，失败才逐降兜底。
    # 不在首包前逐档扫描，请求数从最多 3 降为 1-2。
    "fast_start": os.environ.get("LX_FAST_START", "true").lower() in ("true", "1", "yes"),
    # 错峰抢跑延迟：首枪800ms无结果才补第二枪，避免三档齐发触发限流。
    "hedge_delay": float(os.environ.get("LX_HEDGE_DELAY", "0.8")),
    # 上游取链串行闸门（见 LxUpstreamGate）：在途并发、两次调用最小间隔、等待预算。
    "upstream_concurrency": int(os.environ.get("LX_UPSTREAM_CONCURRENCY", "1")),
    "upstream_spacing_ms": float(os.environ.get("LX_UPSTREAM_SPACING_MS", "250")),
    # 播放解析最多愿意排队等多久。必须明显小于 url_timeout(20s)：宁可在服务端
    # 排队等一轮，也不要回 404 让飞牛客户端直接跳歌（实测 8 首同发时后几首要
    # 排 10s 左右，等待预算太短会把还能播的歌误判成无货）。
    "upstream_wait_ms": float(os.environ.get("LX_UPSTREAM_WAIT_MS", "12000")),
    # 后台探活/预热只允许占用零碎余量：等不起就直接甩掉，不挤占播放
    "upstream_bg_wait_ms": float(os.environ.get("LX_UPSTREAM_BG_WAIT_MS", "2000")),
    # 池里有不止一家运营商时，单条道的等待上限。排不上就换下一家（不同
    # 域名、不同队列），比在一家队尾干等更省客户端时间：12s 是只有一个源
    # 时的 ride-out 时长，四个道各等 5s 已经覆盖 url_timeout 的 20s 总预算。
    "upstream_alt_wait_ms": float(os.environ.get("LX_UPSTREAM_ALT_WAIT_MS", "5000")),
    "upstream_queue_max": int(os.environ.get("LX_UPSTREAM_QUEUE_MAX", "24")),
}

# 当前激活的用户自定义源（启动时从 state.json / LX_SOURCE_URL 恢复）
SOURCE_MANAGER = SourceManager(seed_url=CONF["source_url"])
SOURCE_POOL = MultiSourceManager(SOURCE_MANAGER.state_dir)

UA_PC = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
UA_MOBILE = "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36"

# 探活通过的媒体直链所需请求头，随 track/url 一起回传给代理层。
# 没有这组头时，调用方若用默认客户端 UA 取流，酷我等 CDN 会 403。
MEDIA_REFERERS = {
    "kg": "https://www.kugou.com/",
    "kw": "http://www.kuwo.cn/",
    "tx": "https://y.qq.com/",
    "wy": "https://music.163.com/",
    "mg": "https://music.migu.cn/",
}

# id -> {"item": {...}, "ts": float}
_SONG_CACHE: dict[str, dict] = {}
_SONG_CACHE_PATH = SOURCE_MANAGER.state_dir / "song_cache.json"
_CACHE_PERSIST_LAST = 0.0
_CACHE_DISK_RELOAD_LAST = 0.0
_CACHE_PERSIST_INTERVAL_S = 5.0
_CACHE_DISK_RELOAD_INTERVAL_S = 30.0
_RESOLUTION_FAILURES: ContextVar[list | None] = ContextVar("lx_resolution_failures", default=None)
# SingleFlight：手机 HEAD + GET 0-1 + GET 0- 三连击合并为一次解析，避免限流。
# key -> asyncio.Future，进程内有效。
_URL_INFLIGHT: dict[str, Any] = {}
# 当前协程的上游取链是否属于后台任务（列表页逐曲探活/预热）。后台调用排在播放
# 请求之后，并且队列一积压就先甩掉它。ContextVar 随 create_task 复制，互不串台。
_UPSTREAM_BACKGROUND: ContextVar[bool] = ContextVar("lx_upstream_background", default=False)


def _record_failure(exc: Exception) -> None:
    failures = _RESOLUTION_FAILURES.get()
    if failures is not None:
        failures.append(str(exc) or type(exc).__name__)


_SEARCH_PARTIAL: ContextVar[list | None] = ContextVar("lx_search_partial", default=None)
_SEARCH_PROBE_ENABLED: ContextVar[bool | None] = ContextVar("lx_search_probe_enabled", default=None)


def is_probe_enabled() -> bool:
    v = _SEARCH_PROBE_ENABLED.get()
    if v is not None:
        return v
    return bool(CONF.get("search_probe", False))


def _publish(item: dict) -> None:
    partial = _SEARCH_PARTIAL.get()
    if partial is not None:
        partial.append(item)


_STATS = {"searches": 0, "url_resolutions": 0, "errors": 0}

# 试运行（verify_source）只覆盖当前 asyncio 任务的解析运行时，
# 不替换进程级 SOURCE_MANAGER，避免安装/WebUI 校验打挂正在播的源。
_RUNTIME_OVERRIDE: ContextVar[Any] = ContextVar("lx_runtime_override", default=None)


def current_runtime():
    override = _RUNTIME_OVERRIDE.get()
    if override is not None:
        return override
    return SOURCE_MANAGER.get()


def _lenient_json(resp: httpx.Response, tag: str = "") -> dict | list | None:
    """宽容解析 JSON：第三方接口可能返回 Content-Type text/html 但内容为合法 JSON。"""
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        pass
    text = (resp.text or "").strip()
    if not text or (not text.startswith("{") and not text.startswith("[")):
        logger.warning(
            "%s: upstream returned non-JSON body (HTTP %s, CT %s): %.80s",
            tag,
            resp.status_code,
            resp.headers.get("content-type"),
            text,
        )
        return None
    import json as _json
    try:
        return _json.loads(text)
    except Exception as e:  # noqa: BLE001
        logger.warning("%s: json parse failed: %s (%.80s)", tag, e, text)
        return None



def parse_track_id(track_id: str) -> "tuple[str, str]":
    """解析 "lx:<source>:<identifier>" -> ("kg", "<identifier>")；异常时返回 ("", "")."""
    parts = (track_id or "").strip().split(":", 2)
    if len(parts) == 3 and parts[0] == "lx":
        src = normalize_source(parts[1])
        if src and parts[2]:
            return src, parts[2]
    # 兼容 "kg:xxx" / "wy:xxx" 形式
    if len(parts) == 2:
        src = normalize_source(parts[0])
        if src and parts[1]:
            return src, parts[1]
    return "", ""


def _cache_metadata(item: dict) -> dict:
    """只持久化元数据（封面/歌词/平台主键），丢弃会过期的探活直链与校验态。"""
    return {
        k: v for k, v in item.items()
        if k not in ("_probe", "verified", "validation_status", "completeness", "attempted_tiers")
    }


def _persist_song_cache(force: bool = False) -> None:
    global _CACHE_PERSIST_LAST
    now = time.monotonic()
    if not force and now - _CACHE_PERSIST_LAST < _CACHE_PERSIST_INTERVAL_S:
        return
    _CACHE_PERSIST_LAST = now
    items = {str(k): _cache_metadata(v["item"]) for k, v in _SONG_CACHE.items()}
    if not items:
        return
    part = str(_SONG_CACHE_PATH) + ".part"
    try:
        with open(part, "w", encoding="utf-8") as f:
            json.dump({"savedAt": int(time.time()), "items": items}, f, ensure_ascii=False)
        os.replace(part, _SONG_CACHE_PATH)
    except Exception as e:  # noqa: BLE001
        logger.debug("lx song cache persist failed: %s", e)
        try:
            os.remove(part)
        except OSError:
            pass


def _load_song_cache() -> None:
    global _CACHE_DISK_RELOAD_LAST
    now = time.monotonic()
    if now - _CACHE_DISK_RELOAD_LAST < _CACHE_DISK_RELOAD_INTERVAL_S:
        return
    _CACHE_DISK_RELOAD_LAST = now
    try:
        with open(_SONG_CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, dict):
            return
        for track_id, item in items.items():
            if not isinstance(item, dict) or track_id in _SONG_CACHE:
                continue
            if len(_SONG_CACHE) >= CONF["cache_max"]:
                break
            _SONG_CACHE[track_id] = {"item": item, "ts": time.time()}
    except Exception:
        return


def _cache_put(item: dict) -> None:
    if not item.get("id"):
        return
    if len(_SONG_CACHE) >= CONF["cache_max"]:
        for k in sorted(_SONG_CACHE, key=lambda k: _SONG_CACHE[k]["ts"])[: len(_SONG_CACHE) // 2]:
            _SONG_CACHE.pop(k, None)
    _SONG_CACHE[item["id"]] = {"item": item, "ts": time.time()}
    _persist_song_cache()


def _cache_get(track_id: str) -> dict | None:
    entry = _SONG_CACHE.get(track_id)
    if not entry:
        _load_song_cache()
        entry = _SONG_CACHE.get(track_id)
        if not entry:
            return None
    if time.time() - entry["ts"] > CONF["cache_ttl"]:
        _SONG_CACHE.pop(track_id, None)
        return None
    return entry["item"]


def _quality_tiers(quality: str) -> list[str]:
    q = (quality or "").strip().lower()
    if q in ("lossless", "flac", "sq", "hires", "hr"):
        return ["lossless", "high", "standard"]
    if q in ("high", "320", "exhigh", "hq"):
        return ["high", "standard"]
    return ["standard"]


# ------------------------------------------------------------------ 酷狗 kg ---

async def kg_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict]:
    # 检索更多条目以便剔除收费/VIP曲目后仍能满足 limit 数量
    fetch_size = max(limit * 3, 20)
    r = await client.get(
        "http://mobilecdn.kugou.com/api/v3/search/song",
        params={
            "keyword": keyword,
            "format": "json",
            "page": 1,
            "pagesize": fetch_size,
            "showtype": 1,
        },
        headers={"User-Agent": UA_MOBILE},
    )
    r.raise_for_status()
    data = r.json()
    raw = ((data or {}).get("data") or {}).get("info") or []
    items = []
    vip_candidates = []
    for it in raw:
        if not isinstance(it, dict) or _explicit_trial(it):
            continue
        fhash = str(it.get("hash") or "")
        if not fhash:
            continue

        # 可播放性过滤：
        # 1. 收费/VIP 曲目不直接丢弃，转入探活队列（直链解析+Range探活通过才返回）
        pay_type = int(it.get("pay_type") or 0)
        is_vip = pay_type != 0 or int(it.get("pkg_price") or 0) != 0 or int(it.get("price") or 0) != 0
        # 2. 排除仅免费试听片段标记 (is_free_part=1) 及 VIP 拦截 (fail_process=4)，真不可播
        if int(it.get("is_free_part") or 0) != 0 or int(it.get("fail_process") or 0) == 4:
            continue

        singer = str(it.get("singername") or "")
        title = str(it.get("songname") or it.get("filename") or "").replace(f"{singer} - ", "")
        # 3. 标题带有试听片段标记的坚决不返回
        if any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue
        sq = str(it.get("sqhash") or "")
        hq = str(it.get("hqhash") or "")
        # v3 搜索接口 duration 单位为秒（2026-09 实测：晴天=269，4:29）；
        # 按毫秒除以 1000 会把时长变成 0:00（下游按秒渲染）
        duration_s = int(it.get("duration") or 0)
        trans = it.get("trans_param") if isinstance(it.get("trans_param"), dict) else {}
        cover = str(
            it.get("origin_cover")
            or it.get("img")
            or trans.get("union_cover")
            or it.get("sizable_cover")
            or ""
        ).replace("{size}", "480")
        item = {
            "id": f"lx:kg:{fhash}",
            "lx_source": "kg",
            "title": title,
            "artist": singer,
            "album": str(it.get("album_name") or ""),
            "duration_s": duration_s,
            "ext": "flac" if sq else "mp3",
            "cover_url": cover,
            "file_size": int(sq and it.get("sq_size") or it.get("filesize") or 0) or 0,
            "lyric": "",
            "hash": fhash,
            "hash_hq": hq,
            "hash_sq": sq,
            "album_id": str(it.get("album_id") or ""),
            "mixsongid": str(it.get("mixsongid") or ""),
            # 酷狗数字 ID（audio_id/album_audio_id）：Ceru 侧同步过来的酷狗歌曲
            # 只有数字 ID，没有 hash；带上这两个字段才能在换源时精确对齐同一首。
            "audio_id": str(it.get("audio_id") or ""),
            "album_audio_id": str(it.get("album_audio_id") or ""),
            "pay_type": pay_type,
        }
        if is_vip:
            vip_candidates.append(item)
        else:
            item.update(verified=False, validation_status="unverified", completeness="unknown")
            _cache_put(item)
            items.append(item)
            _publish(item)
    # VIP 候选：开启逐曲探活时探活后补进；未开启时直接作为未验证候选补齐结果
    if vip_candidates and len(items) < limit:
        want = min(len(vip_candidates), limit - len(items) + limit // 2)
        if is_probe_enabled():
            items.extend(await _probe_candidates(client, "kg", vip_candidates[:want], limit - len(items)))
        else:
            for it in vip_candidates[:want]:
                it.update(verified=False, validation_status="unverified", completeness="unknown")
                _cache_put(it)
                items.append(it)
                _publish(it)
    return items[: limit + limit // 2]


async def kg_resolve_lyric(client: httpx.AsyncClient, item: dict) -> str:
    duration_ms = int(float(item.get("duration_s") or 0) * 1000)
    r = await client.get(
        "https://krcs.kugou.com/search",
        params={
            "ver": 1,
            "man": "yes",
            "client": "mobi",
            "keyword": f"{item.get('title','')} {item.get('artist','')}".strip(),
            "duration": duration_ms,
            "hash": item.get("hash") or "",
        },
        headers={"User-Agent": UA_MOBILE},
    )
    candidates = ((r.json() or {}).get("candidates") or [])
    if not candidates:
        return ""
    cand = candidates[0]
    r2 = await client.get(
        "http://lyrics.kugou.com/download",
        params={
            "ver": 1,
            "client": "pc",
            "id": cand.get("id"),
            "accesskey": cand.get("accesskey"),
            "fmt": "lrc",
            "charset": "utf8",
        },
        headers={"User-Agent": UA_PC},
    )
    content = (r2.json() or {}).get("content") or ""
    if not content:
        return ""
    try:
        return base64.b64decode(content).decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        return ""


# ------------------------------------------------------------------ 网易 wy ---

async def wy_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict]:
    fetch_limit = max(limit * 2, 20)
    r = await client.post(
        "https://music.163.com/api/search/get/web",
        data={"s": keyword, "type": 1, "offset": 0, "limit": fetch_limit, "total": "true"},
        headers={
            "User-Agent": UA_PC,
            "Referer": "https://music.163.com/",
            "Cookie": "os=pc; appver=9.1.15",
        },
    )
    r.raise_for_status()
    songs = ((r.json() or {}).get("result") or {}).get("songs") or []
    items = []
    vip_candidates = []
    for it in songs:
        if not isinstance(it, dict) or _explicit_trial(it):
            continue
        sid = str(it.get("id") or "")
        if not sid:
            continue

        # 可播放性过滤：fee∉(0,8) 的 VIP/付费曲不直接丢弃，转探活队列验证
        fee = int(it.get("fee") or 0)
        is_vip = fee not in (0, 8)

        # 排除无版权（真不可播）
        if it.get("noCopyrightRcmd") is not None and it.get("noCopyrightRcmd") != 0:
            continue

        # 检查 privilege 状态
        priv = it.get("privilege")
        if isinstance(priv, dict):
            priv_fee = int(priv.get("fee", fee))
            if priv_fee not in (0, 8):
                is_vip = True
            if int(priv.get("pl") or 0) <= 0 and int(priv.get("st") or 0) < 0:
                continue
            if priv.get("freeTrialPrivilege") and priv.get("freeTrialPrivilege", {}).get("cannotListenReason"):
                continue

        title = str(it.get("name") or "")
        # 排除标题含试听片段标记
        if any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue

        artists = it.get("artists") or []
        artist = " / ".join(
            str(a.get("name") or "") for a in artists if isinstance(a, dict)
        )
        album = it.get("album") or {}
        cover = str(album.get("picUrl") or "") if isinstance(album, dict) else ""
        if not cover:
            artists_cover = [a.get("img1v1Url") for a in (it.get("artists") or []) if isinstance(a, dict) and a.get("img1v1Url")]
            cover = str(artists_cover[0]) if artists_cover else ""
        item = {
            "id": f"lx:wy:{sid}",
            "lx_source": "wy",
            "title": str(it.get("name") or ""),
            "artist": artist,
            "album": str(album.get("name") or "") if isinstance(album, dict) else "",
            "duration_s": int(it.get("duration") or 0) / 1000.0,
            "ext": "mp3",
            "cover_url": cover,
            "file_size": 0,
            "lyric": "",
            "song_id": sid,
            "fee": fee,
        }
        if is_vip:
            vip_candidates.append(item)
        else:
            item.update(verified=False, validation_status="unverified", completeness="unknown")
            _cache_put(item)
            items.append(item)
            _publish(item)
    # VIP 候选：开启逐曲探活时探活后补进；未开启时直接作为未验证候选补齐结果
    if vip_candidates and len(items) < limit:
        want = min(len(vip_candidates), limit - len(items) + limit // 2)
        if is_probe_enabled():
            items.extend(await _probe_candidates(client, "wy", vip_candidates[:want], limit - len(items)))
        else:
            for it in vip_candidates[:want]:
                it.update(verified=False, validation_status="unverified", completeness="unknown")
                _cache_put(it)
                items.append(it)
                _publish(it)
    return items[: limit + limit // 2]


async def wy_resolve_lyric(client: httpx.AsyncClient, identifier: str) -> str:
    # lv 必须是 -1：实测 lv=1 时网易直接返回 lyric:""（本插件网易歌曲长期无词
    # 就是这个参数造成的），lv=-1 才回完整 LRC。
    r = await client.get(
        "https://music.163.com/api/song/lyric",
        params={"id": identifier, "lv": -1, "kv": -1, "tv": -1},
        headers={"User-Agent": UA_PC, "Referer": "https://music.163.com/", "Cookie": "os=pc"},
    )
    lrc = (r.json() or {}).get("lrc") or {}
    return str(lrc.get("lyric") or "")


# ------------------------------------------------------------------ 咪咕 mg ---

async def mg_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict]:
    fetch_size = max(limit * 2, 10)
    r = await client.get(
        "https://c.music.migu.cn/MIGUM2.0/v1.0/content/search_all.do",
        params={"text": keyword, "pageNo": 1, "pageSize": fetch_size, "resource": 1},
        headers={"User-Agent": UA_MOBILE, "Referer": "https://m.music.migu.cn/"},
    )
    r.raise_for_status()
    data = r.json() or {}
    raw = data.get("songs") or (data.get("songResultData") or {}).get("result") or []

    def _map_one(it: dict) -> dict | None:
        if not isinstance(it, dict):
            return None
        cid = str(it.get("copyrightId") or it.get("id") or "")
        if not cid:
            return None
        title = str(it.get("songName") or "")
        if _explicit_trial(it) or any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            return None
        singers = it.get("singers") or []
        artist = " / ".join(str(s.get("name") or "") for s in singers if isinstance(s, dict))
        album = it.get("albums") or []
        album_name = str(album[0].get("albumName") or album[0].get("name") or "") if album and isinstance(album[0], dict) else ""
        covers = it.get("albumMaterialList") or []
        cover = str((covers[0] or {}).get("coverUrl") or "") if covers else ""
        tones = {str(t.get("toneType") or "").upper() for t in (it.get("toneFlags") or []) if isinstance(t, dict)}
        length_ms = int(it.get("length") or 0)
        item = {
            "id": f"lx:mg:{cid}",
            "lx_source": "mg",
            "title": title,
            "artist": artist,
            "album": album_name,
            "duration_s": length_ms / 1000.0,
            "ext": "flac" if tones & {"SQ", "ZQ", "ZQ24"} else "mp3",
            "cover_url": cover,
            "file_size": 0,
            "lyric": "",
            "lrc_url": str(it.get("lrcUrl") or ""),
            "copyright_id": cid,
        }
        return item

    candidates = [item for it in raw[:fetch_size] if (item := _map_one(it))]
    if is_probe_enabled():
        return await _probe_candidates(client, "mg", candidates, limit)
    items = []
    for it in candidates[:limit]:
        it.update(verified=False, validation_status="unverified", completeness="unknown")
        _cache_put(it)
        items.append(it)
        _publish(it)
    return items


async def mg_resolve_lyric(client: httpx.AsyncClient, item: dict) -> str:
    lrc_url = str(item.get("lrc_url") or "")
    if not lrc_url:
        return ""
    r = await client.get(lrc_url, headers={"User-Agent": UA_MOBILE})
    return r.text or ""


# ------------------------------------------------------------ 用户源解析与熔断 ---
#
# 播放直链解析的唯一通道是用户提供的洛雪自定义源脚本（source_runtime.py）。
# 熔断器沿用原第三方链路机制：脚本连续解析失败达阈值后暂停调用一段时间，
# 避免每次搜索白等超时；媒体探活（probe_url）仍由本模块统一执行。


def _content_total_size(r: httpx.Response) -> int:
    cr = r.headers.get("content-range") or ""
    if "/" in cr:
        tail = cr.rsplit("/", 1)[-1].strip()
        if tail.isdigit():
            return int(tail)
    # A partial response's Content-Length is NOT the whole file size.
    cl = r.headers.get("content-length") or ""
    return int(cl) if r.status_code == 200 and cl.isdigit() else 0


_PROBE_BYTES = 4096


class ChainTransportError(Exception):
    """Infrastructure failure, unlike a healthy resolver's song miss."""


def _media_signature(body: bytes) -> str:
    """Positive signatures only; MIME and byte counts do not prove a full song."""
    if body.startswith(b"fLaC"):
        return "flac"
    if body.startswith(b"ID3"):
        return "mp3"
    if body.startswith(b"OggS"):
        return "ogg"
    if len(body) >= 12 and body[:4] == b"RIFF" and body[8:12] == b"WAVE":
        return "wav"
    if len(body) >= 12 and body[4:8] == b"ftyp":
        return "m4a"
    if len(body) >= 4 and body[0] == 0xff:
        if body[1] & 0xf6 == 0xf0:  # ADTS AAC
            return "aac"
        if (body[1] & 0xe0 == 0xe0 and body[1] & 6
                and body[2] & 0xf0 not in (0, 0xf0) and body[2] & 12 != 12):
            return "mp3"
    return ""


async def probe_url(
    client: httpx.AsyncClient, url: str, headers: "dict | None" = None,
    *, report_transport: bool = False,
) -> "tuple[bool, str, str, int]":
    """Bounded streaming prefix inspection, even when a CDN ignores Range.

    The tuple remains API-compatible. Success means media prefix verified, not
    complete-song verification. Streams close on success, failure and cancellation.
    """
    h = {k: v for k, v in (headers or {}).items()
         if k.lower() not in ("range", "accept-encoding")}
    h.setdefault("User-Agent", UA_PC)
    h.update({"Range": f"bytes=0-{_PROBE_BYTES - 1}", "Accept-Encoding": "identity"})
    try:
        async with asyncio.timeout(CONF["probe_timeout"]):
            for _ in range(6):  # at most five redirects, one shared deadline
                async with client.stream("GET", url, headers=h, follow_redirects=False,
                                         timeout=CONF["probe_timeout"]) as r:
                    if r.status_code in (301, 302, 303, 307, 308):
                        location = r.headers.get("location")
                        if not location:
                            return False, str(r.url), "", 0
                        target = r.url.join(location)
                        if target.scheme not in ("http", "https"):
                            return False, str(r.url), "", 0
                        if target.host != r.url.host:
                            h = {k: v for k, v in h.items()
                                 if k.lower() not in ("authorization", "cookie", "host")}
                        url = str(target)
                        # Leaving this context closes the intermediate response
                        # WITHOUT HTTPX's automatic redirect body draining.
                        continue
                    ct = (r.headers.get("content-type") or "").lower()
                    if r.status_code >= 500 or r.status_code in (408, 429):
                        raise ChainTransportError(f"media HTTP {r.status_code}")
                    if r.status_code not in (200, 206):
                        return False, str(r.url), ct, 0
                    if (ct.startswith("text/") or "json" in ct or "xml" in ct
                            or r.headers.get("content-encoding", "identity") != "identity"):
                        return False, str(r.url), ct, 0
                    prefix = bytearray()
                    # aiter_raw avoids decompression/buffering the full response. Slice
                    # each transport chunk and stop as soon as a signature is known.
                    if r.is_stream_consumed:  # in-memory transports (e.g. MockTransport)
                        prefix.extend(r.content[:_PROBE_BYTES])
                    else:
                        async for chunk in r.aiter_raw():
                            prefix.extend(chunk[:_PROBE_BYTES - len(prefix)])
                            ext = _media_signature(prefix)
                            if ext or len(prefix) >= _PROBE_BYTES:
                                break
                    ext = _media_signature(prefix)
                    return bool(ext), str(r.url), (f"audio/{ext}" if ext else ct), _content_total_size(r) if ext else 0
            raise ChainTransportError("media redirect limit exceeded")
    except (httpx.HTTPError, TimeoutError, ChainTransportError) as exc:
        if report_transport:
            raise ChainTransportError(str(exc)) from exc
        return False, url, "", 0


# 链路熔断器：连续失败达阈值后暂停该链路一段时间，避免每次搜索白等超时
_CHAIN_FAIL_THRESHOLD = 3
# 原来熔断 600s、每 30s 才试探一次，且试探失败会重新计时，等于无限熔断：
# 用户看到的现象就是“腾讯突然全不能播，必须重启插件才恢复”。
# 压到 45s / 8s，源恢复后最多几十秒自动放行，不需要人工重启。
_CHAIN_OPEN_SECONDS = 45
_CHAIN_RETRY_SECONDS = 8
_CHAIN_HEALTH: dict[str, dict] = {}


def _chain_available(name: str) -> bool:
    h = _CHAIN_HEALTH.get(name)
    if not h:
        return True
    if h.get("half_open"):
        return False
    open_until = h.get("open_until", 0)
    if not open_until:
        return True
    # 冷却期内保留按需试探窗口：打开 RETRY 秒后即可 half-open 一次，
    # 成功立即闭合、失败仅顺延下一个试探窗口（每 30s 至多一个请求打到故障源）
    return time.time() >= open_until - (_CHAIN_OPEN_SECONDS - _CHAIN_RETRY_SECONDS)


def _chain_acquire(name: str) -> bool:
    if not _chain_available(name):
        return False
    h = _CHAIN_HEALTH.get(name)
    if h and h.get("open_until"):
        h["half_open"] = True  # synchronous claim: only one recovery request
    return True


def _chain_report(name: str, ok: bool) -> None:
    h = _CHAIN_HEALTH.setdefault(name, {"fails": 0, "open_until": 0.0, "breaks": 0})
    was_half_open = h.pop("half_open", False)
    if ok:
        h["fails"] = 0
        h["open_until"] = 0.0
        return
    h["fails"] = int(h.get("fails") or 0) + 1
    if was_half_open or h["fails"] >= _CHAIN_FAIL_THRESHOLD:
        h["open_until"] = time.time() + _CHAIN_OPEN_SECONDS
        h["fails"] = 0
        h["breaks"] = int(h.get("breaks") or 0) + 1


def chain_health_snapshot() -> dict:
    now = time.time()
    h = _CHAIN_HEALTH.get("user_source", {})
    state = ("half_open" if h.get("half_open") else
             "open" if h.get("open_until", 0) > now else
             "recovery_ready" if h.get("open_until") else "closed")
    return {"user_source": {"fails": h.get("fails", 0), "open": state == "open",
                            "breaks": h.get("breaks", 0), "state": state}}


class UpstreamBusy(Exception):
    """取链队列已满或等待超预算。

    这是我们自己甩掉的请求，不代表这首歌无货、更不代表音源故障，
    因此既不写熔断器也不算解析失败。
    """


_UPSTREAM_WAIT_CAP: ContextVar["float | None"] = ContextVar("fnmusic_upstream_wait_cap", default=None)
"""本次解析在单条道上最多等多久（秒）。多家运营商时由 _resolve_and_probe 收紧，
让排不上号的歌尽快换下一家，而不是在一家队尾把客户端等到跳歌。"""


class LxUpstreamGate:
    """上游取链全局队列：在途并发上限 + 两次调用最小间隔 + 播放优先插队。

    实测依据（NAS「我的喜欢」随机 100 首、同一 seed）：8 并发起播 34 成 66 败，
    同一批歌逐首排队取链 94 成（剩下 6 首里 5 首是上游真无货 404）。失败全部
    停在「取不到链」——聆澜/ikun 这类公益中转源对并发爆发限流。一次播放请求在
    代理侧最多放大成 4 候选 x 2 次尝试 x 3 音质档 x 2 次脚本调用，再叠加预取下
    一首和列表页逐曲探活 6 并发，正好撞上它的限流窗口；安卓版稳就是因为取链串行。
    """

    def __init__(self) -> None:
        self._queue: list[dict] = []
        self._running = 0
        self._last_start = 0.0
        self._calls = 0
        self._waits = 0
        self._shed = 0
        self._timed_out = 0
        self._waited_ms = 0.0
        self._est_s = 0.8  # 单次上游调用耗时 EWMA，用来估算还要排多久

    def reset(self) -> None:
        for slot in self._queue:
            if not slot["fut"].done():
                slot["fut"].cancel()
        self._queue.clear()
        self._running = 0
        self._last_start = 0.0
        self._calls = 0
        self._waits = 0
        self._shed = 0
        self._timed_out = 0
        self._waited_ms = 0.0
        self._est_s = 0.8

    def snapshot(self) -> dict:
        return {
            "running": self._running,
            "queued": len(self._queue),
            "calls": self._calls,
            "waited": self._waits,
            "shed": self._shed,
            "timed_out": self._timed_out,
            "avg_wait_ms": round(self._waited_ms / self._waits, 1) if self._waits else 0.0,
            "est_call_s": round(self._est_s, 2),
        }

    def _ahead_seconds(self, index: int) -> float:
        """第 index 个槽位还要排多久（含正在途的那一次）。"""
        spacing = float(CONF["upstream_spacing_ms"]) / 1000.0
        return (index + 1) * (self._est_s + spacing)

    def _pump(self) -> None:
        concurrency = max(1, int(CONF["upstream_concurrency"]))
        while self._running < concurrency and self._queue:
            slot = self._queue.pop(0)
            fut = slot["fut"]
            if fut.done():  # 等待方已超时/取消，槽位作废
                continue
            slot["granted"] = True
            self._running += 1
            fut.set_result(True)

    def _leave(self) -> None:
        self._running = max(0, self._running - 1)
        self._pump()

    async def _enter(self, background: bool) -> dict:
        spacing = float(CONF["upstream_spacing_ms"]) / 1000.0
        budget = float(
            CONF["upstream_bg_wait_ms"] if background else CONF["upstream_wait_ms"]
        ) / 1000.0
        cap = _UPSTREAM_WAIT_CAP.get()
        if cap is not None:
            budget = min(budget, cap)
        if self._running < max(1, int(CONF["upstream_concurrency"])) and not self._queue:
            self._running += 1  # 空队列直接占用，不产生等待
            return {"granted": True, "bg": background, "fut": None}
        if background and len(self._queue) >= int(CONF["upstream_queue_max"]):
            self._shed += 1
            raise UpstreamBusy("upstream queue full")
        slot = {"granted": False, "bg": background, "fut": asyncio.get_running_loop().create_future()}
        if background:
            index = len(self._queue)
            self._queue.append(slot)
        else:
            # 播放请求插到所有后台槽位之前（前面的播放任务仍保持先来后到）
            index = next((i for i, s in enumerate(self._queue) if s["bg"]), len(self._queue))
            self._queue.insert(index, slot)
        if background and self._ahead_seconds(index) > budget:
            # 只有后台任务才按预估提前甩单。播放请求绝不预估拒绝：实测
            # 8 首同发时阶梯会瞬间堆出 20+ 槽位，预估拒绝会把还能播的歌
            # 在 100ms 内判死（客户端直接跳歌）。播放只受下面的硬等待上限约束。
            self._queue.remove(slot)
            self._shed += 1
            raise UpstreamBusy("upstream queue full")
        self._waits += 1
        started = time.monotonic()
        try:
            await asyncio.wait_for(slot["fut"], max(budget, 0.05))
        except asyncio.TimeoutError:
            # 排到超时仍未放行：与「预估太深提前甩单」分开计数，现场看数字时
            # 这两件事的处置方式完全不同（前者该加时长，后者该减量）。
            self._timed_out += 1
            if slot["granted"]:  # 与超时同刻被放行：把拿到的名额交还
                self._leave()
            else:
                if slot in self._queue:
                    self._queue.remove(slot)
                self._pump()
            raise UpstreamBusy("upstream wait timeout") from None
        except BaseException:
            if slot in self._queue:
                self._queue.remove(slot)
            elif slot["granted"]:
                self._leave()
            self._pump()
            raise
        self._waited_ms += (time.monotonic() - started) * 1000.0
        return slot

    async def run(self, factory, *, background: bool = False):
        """在闸门内执行一次上游调用。factory 必须只包含真正打上游的动作。"""
        await self._enter(background)
        try:
            spacing = float(CONF["upstream_spacing_ms"]) / 1000.0
            if self._last_start:
                gap = spacing - (time.monotonic() - self._last_start)
                if gap > 0:
                    await asyncio.sleep(gap)  # 仍占着名额：间隔是给上游的休息
            started = time.monotonic()
            self._last_start = started
            self._calls += 1
            try:
                return await factory()
            finally:
                cost = max(time.monotonic() - started, 0.05)
                self._est_s = round(0.7 * self._est_s + 0.3 * cost, 3)
        finally:
            self._leave()


DEFAULT_UPSTREAM_LANE = "primary"


class LxUpstreamLanes:
    """按「音源运营商」分道的上游取链闸门集合。

    为什么按源分道而不是按平台分道：现场查过当前生效的脚本，聆澜
    （source.shiqianjiang.cn）这类中转源把 kw/tx/wy/kg/mg 五个平台全部转发到
    同一个域名，按平台分道等于对同一台主机加并发，正好撞上我们实测到的限流；
    而聆澜 / 独家音源 / ikun 是三个互不相干的运营商，各自独立限流。
    分道后每台上游主机的在途并发仍是 1、间隔仍是 250ms（对上游的压力与分道前
    完全一致），只是不再让一首排队中的歌堵住另一家本来空闲的源。
    """

    def __init__(self) -> None:
        self._lanes: "dict[str, LxUpstreamGate]" = {}

    def lane(self, key: str = "") -> "LxUpstreamGate":
        name = str(key or "").strip() or DEFAULT_UPSTREAM_LANE
        gate = self._lanes.get(name)
        if gate is None:
            gate = self._lanes[name] = LxUpstreamGate()
        return gate

    # 兼容旧的单闸门写法：未指定道次的调用与用例都落在主源道上
    @property
    def _est_s(self) -> float:
        return self.lane()._est_s

    @_est_s.setter
    def _est_s(self, value: float) -> None:
        self.lane()._est_s = value

    @property
    def _running(self) -> int:
        return self.lane()._running

    @_running.setter
    def _running(self, value: int) -> None:
        self.lane()._running = value

    def reset(self) -> None:
        for gate in self._lanes.values():
            gate.reset()
        self._lanes.clear()

    async def run(self, factory, *, background: bool = False, lane: str = ""):
        return await self.lane(lane).run(factory, background=background)

    def snapshot(self) -> dict:
        lanes = {name: gate.snapshot() for name, gate in sorted(self._lanes.items())}
        waited = sum(item["waited"] for item in lanes.values())
        return {
            "running": sum(item["running"] for item in lanes.values()),
            "queued": sum(item["queued"] for item in lanes.values()),
            "calls": sum(item["calls"] for item in lanes.values()),
            "waited": waited,
            "shed": sum(item["shed"] for item in lanes.values()),
            "timed_out": sum(item["timed_out"] for item in lanes.values()),
            "avg_wait_ms": round(
                sum(item["avg_wait_ms"] * item["waited"] for item in lanes.values()) / waited, 1
            ) if waited else 0.0,
            "est_call_s": max((item["est_call_s"] for item in lanes.values()), default=0.0),
            "lanes": lanes,
        }


LX_UPSTREAM_GATE = LxUpstreamLanes()


def _upstream_lane(runtime: "UserSource | None") -> str:
    """同一家运营商共用一条道（见 script_lane_key）；替身/测试源落主源道。"""
    return str(getattr(runtime, "lane_key", "") or "") or DEFAULT_UPSTREAM_LANE


def _lane_pressure(lane: str) -> float:
    """这条道预计还要排多久 = (在途 + 排队) x 该道实测单次耗时。

    只按「排队条数」排是不够的：现场跑 100 首时聆澜那条道 268 枪、实测单枪
    0.53s 并出现 18 次播放等待超时，而同机的独家道单枪 0.09s、ikun 0.2s——
    同样排 3 条，等待差 6 倍。按秒排才真的把歌送到「最快能腾出手」的那家。
    各道都空闲时估计都是 0，排序稳定，用户配置的主源仍然第一优先。
    """
    gate = LX_UPSTREAM_GATE.lane(lane)
    return (gate._running + len(gate._queue)) * max(gate._est_s, 0.05)


# ---- 上游按天配额（现场实测出来的硬限制，2.6.47）----
# NAS 现场原文：「该 API Key 今日成功请求已达 1000 次上限」。公益/付费中转音源的
# key 都是按天计数的，一家一天就 1000 次成功请求。以前配额打光后表现成「这首歌
# 没货」：同一首歌继续把这家试到 2 档、下一首歌还从这家开始，一路 404 到客户端
# 跳歌，既白花请求又让整批歌看起来"播不出"。现在把返回该错误的运营商（lane）
# 当天停用手，其余家优先；**停用手永远排在末尾兜底，一家都不剩时行为与以前
# 完全一致**，所以不可能因为误判而少试一家。
_QUOTA_MARKERS = ("今日成功请求已达", "成功请求已达", "请求次数已达上限", "quota exceeded")
_QUOTA_PARKED: dict[str, float] = {}


def _is_quota_error(exc: BaseException) -> bool:
    text = str(exc or "").lower()
    return any(marker.lower() in text for marker in _QUOTA_MARKERS)


def _quota_park_until(now: float | None = None) -> float:
    """停用到下一个本地零点 + 2 分钟（配额按天重置），最长不超过 24 小时。"""
    now = time.time() if now is None else now
    local = time.localtime(now)
    seconds_today = local.tm_hour * 3600 + local.tm_min * 60 + local.tm_sec
    until = now + (86400 - seconds_today) + 120.0
    return min(until, now + 86400.0)


def _quota_park(lane: str) -> float:
    until = _quota_park_until()
    if _QUOTA_PARKED.get(lane, 0.0) != until:
        _QUOTA_PARKED[lane] = until
        logger.warning("上游 %s 当天取链配额已打光，暂停使用至 %s",
                       lane, time.strftime("%m-%d %H:%M", time.localtime(until)))
    return until


def _quota_parked(lane: str, now: float | None = None) -> bool:
    now = time.time() if now is None else now
    until = _QUOTA_PARKED.get(lane, 0.0)
    if until and until <= now:
        _QUOTA_PARKED.pop(lane, None)
        return False
    return bool(until)


def _quota_snapshot() -> dict:
    now = time.time()
    return {lane: time.strftime("%m-%d %H:%M", time.localtime(until))
            for lane, until in list(_QUOTA_PARKED.items()) if until > now}


def source_capabilities() -> dict:
    runtimes: list[UserSource] = []
    primary = SOURCE_MANAGER.get()
    if primary is not None:
        runtimes.append(primary)
    runtimes.extend(rt for _sid, rt in SOURCE_POOL.all_runtimes())
    ready = bool(runtimes)
    configured = bool(SOURCE_MANAGER.active_url or SOURCE_MANAGER.seed_url or SOURCE_POOL.configured())
    result = {}
    for src in _SEARCHERS:
        matching = [rt for rt in runtimes if src in rt.platforms]
        supported = bool(matching)
        available = supported
        if available:
            reason = ""
        elif not ready:
            reason = "source_init_failed" if configured else "no_source_configured"
        elif not supported:
            reason = "platform_not_supported"
        else:
            reason = "source_circuit_open"
        qualitys: list[str] = []
        for runtime in matching:
            for q in runtime.qualitys(src):
                if q not in qualitys:
                    qualitys.append(q)
        result[src] = {"search_available": bool(available), "playback_available": bool(available),
                       "user_source": bool(supported),
                       "qualitys": qualitys,
                       "reason": reason,
                       "validation_status": "unverified", "completeness": "unknown"}
    return result


# ------------------------------------------------------------ 可播性验证 ---

_TRIAL_TITLE_MARKERS = ("(试听)", "（试听）", "试听片段", "片段试听", "试听版")


_TIER_RANK = {"standard": 0, "high": 1, "lossless": 2}


def _explicit_trial(data: dict) -> bool:
    """Only explicit clip metadata; fee/VIP and size are not trial proof."""
    for key in ("trial", "is_trial", "isTrial", "is_free_part", "isFreePart"):
        if str(data.get(key, "")).lower() in ("1", "true", "yes"):
            return True
    return bool(data.get("freeTrialInfo") or data.get("trialInfo")
                or data.get("trial_url") or data.get("trialUrl"))


def _actual_tier(result: dict) -> str:
    if result.get("ext") in ("flac", "wav", "ape"):
        return "lossless"
    br = int(result.get("br") or 0)
    if br >= 256000:
        return "high"
    return "standard" if br > 0 else "unknown"


async def _verify_result(client: httpx.AsyncClient, result: dict | None,
                         *, report_transport: bool = False) -> dict | None:
    if not result or not result.get("url") or _explicit_trial(result):
        return None
    result = dict(result)
    if not result.get("probed"):
        ok, final, ct, size = await probe_url(client, result["url"], result.get("headers"),
                                            report_transport=report_transport)
        if not ok:
            return None
        result.update(url=final, file_size=size or result.get("file_size") or 0,
                      ext=ct.split("/")[-1], probed=True)
    result.update(validation_status="media_verified", completeness="unknown")
    result["actual_tier"] = _actual_tier(result)
    return result


def _fresh_probe(item: "dict | None", want_tier: str = "standard") -> "dict | None":
    """Reuse actual quality or a completed downgrade for this requested tier."""
    if not isinstance(item, dict) or _explicit_trial(item):
        return None
    p = item.get("_probe")
    if not (isinstance(p, dict) and p.get("url") and p.get("probed")
            and p.get("validation_status") == "media_verified"):
        return None
    if _explicit_trial(p):
        return None
    if time.time() - p.get("ts", 0) >= CONF["probe_fresh_s"]:
        return None
    want_tier = _quality_tiers(want_tier)[0]
    rank = _TIER_RANK.get(p.get("actual_tier"), -1)
    if rank < _TIER_RANK[want_tier] and want_tier not in p.get("attempted_tiers", []):
        return None
    return {k: v for k, v in p.items() if k not in ("ts", "tier")}


async def _fill_script_meta(client: httpx.AsyncClient, src: str, item: dict, identifier: str) -> None:
    """缓存过期后的播放只剩平台主键。QQ 的 media mid、酷狗的 albumId 要补回来再交给脚本。"""
    if not identifier:
        return
    try:
        if src == "tx" and (not item.get("str_media_mid") or not item.get("album_id")):
            r = await client.get(
                "https://c.y.qq.com/v8/fcg-bin/fcg_play_single_song.fcg",
                params={"songmid": identifier, "format": "json"},
                headers={"User-Agent": UA_PC, "Referer": "https://y.qq.com/"},
            )
            song = ((r.json() or {}).get("data") or [None])[0] or {}
            if isinstance(song, dict):
                file_obj = song.get("file") if isinstance(song.get("file"), dict) else {}
                album = song.get("album") if isinstance(song.get("album"), dict) else {}
                media = str(file_obj.get("media_mid") or "").strip()
                if media:
                    item["str_media_mid"] = media
                album_id = str(album.get("id") or "").strip()
                if album_id and album_id not in ("0", "None"):
                    item["album_id"] = album_id
                if not item.get("title"):
                    item["title"] = str(song.get("name") or song.get("title") or "")
                if not item.get("songmid"):
                    item["songmid"] = str(song.get("mid") or identifier)
        elif src == "kg" and not str(item.get("album_id") or "").strip():
            r = await client.get(
                "http://m.kugou.com/app/i/getSongInfo.php",
                params={"cmd": "playInfo", "hash": identifier},
                headers={"User-Agent": UA_MOBILE},
            )
            data = r.json() or {}
            if isinstance(data, dict):
                album_id = str(data.get("albumid") or data.get("req_albumid") or "").strip()
                if album_id and album_id not in ("0", "None"):
                    item["album_id"] = album_id
                if not item.get("hash"):
                    item["hash"] = identifier
    except Exception as exc:  # noqa: BLE001
        logger.warning("lx meta fill %s %s failed: %s", src, identifier, exc)


async def _resolve_and_probe(client: httpx.AsyncClient, src: str, item: dict,
                             tier: str = "standard", retained: dict | None = None,
                             fresh: bool = False) -> "dict | None":
    """按启用顺序尝试主源与附加源，任一成功即返回。"""
    runtimes: list[UserSource] = []
    primary = current_runtime()
    if primary is not None and src in primary.platforms:
        runtimes.append(primary)
    runtimes.extend(rt for _sid, rt in SOURCE_POOL.runtimes_for(src))
    if not runtimes:
        return None
    # 分流而不是全部挤主源：用户配置的顺序仍然是第一优先级（各道都空闲时
    # 排序稳定、主源照旧先行），但一旦主源那条道在排队，同一批歌里后面的
    # 就自动改打另一家运营商。聆澜/独家/ikun/全豆要是四个互不相干的上游，
    # 每家同时仍然只有 1 路在途、间隔 250ms，对上游的压力一点没加，只是不
    # 再让一首排队中的歌把另外三家本来空闲的源一起堵死（实测 8 首同发的
    # 尾部等待就是这么来的）。
    runtimes.sort(key=lambda rt: _lane_pressure(_upstream_lane(rt)))
    lanes = {_upstream_lane(rt) for rt in runtimes}
    # 当天配额打光的运营商排到最后：优先用还有配额的家，但保留它们兜底
    # （万一上游谎报配额，全打完还能像以前一样试到）。
    parked = [rt for rt in runtimes if _quota_parked(_upstream_lane(rt))]
    if parked:
        runtimes = [rt for rt in runtimes if not _quota_parked(_upstream_lane(rt))] + parked
    # 只有一家时保持 12s 长等（没有别的道可换）；多家时每条道最多等
    # upstream_alt_wait_ms，排不上就换下一家，四道各 5s 已覆盖 20s 总预算。
    cap_token = None
    if len(lanes) > 1:
        cap_token = _UPSTREAM_WAIT_CAP.set(float(CONF["upstream_alt_wait_ms"]) / 1000.0)
    last_error: Exception | None = None
    all_busy = False
    try:
        for runtime in runtimes:
            try:
                result = await _resolve_with_runtime(client, src, item, tier, retained, runtime, fresh)
            except UpstreamBusy:
                # 队列满只代表这一家此刻排不开。池里其它源是不同运营商、
                # 不同队列，必须继续试；全部都在甩单时才把 UpstreamBusy 抛给
                # 上层（代理据此回 503 稍后重试，而不是当成无货跳歌）。
                all_busy = True
                continue
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue
            if result:
                return result
    finally:
        if cap_token is not None:
            _UPSTREAM_WAIT_CAP.reset(cap_token)
    if all_busy:
        raise UpstreamBusy("all lx sources busy")
    if last_error is not None:
        logger.debug("all lx sources failed for %s: %s", src, last_error)
    return None


async def _resolve_one_tier(client: httpx.AsyncClient, src: str, item: dict,
                          music_info: dict, tier: str, runtime: UserSource,
                          retained: dict | None, attempted: list,
                          best: "dict | None", failures: "list | None") -> "dict | None":
    """解析单个档位：musicUrl + 探活。失败返回 None，不抛给上层重试风暴。"""
    before = len(failures or [])
    quality = script_quality_for_tier(tier, runtime.qualitys(src))
    if quality is None:
        attempted.append(tier)
        return best
    # 播放成功率优先：不再用全局熔断拦截后续歌曲。用户源对个别歌曲失败
    # 时只影响当前解析，不能让后面整张歌单都被锁死 10 分钟。
    _chain_acquire("user_source")
    recovering = bool(_CHAIN_HEALTH.get("user_source", {}).get("half_open"))
    result = None

    async def _fetch_url() -> str:
        return await runtime.music_url(
            music_info, quality, platform=src, timeout=CONF["resolver_timeout"]
        )

    try:
        # 脚本调用过全局串行闸门；媒体探活打的是 CDN，不进闸门、不占名额
        url = await LX_UPSTREAM_GATE.run(
            _fetch_url, background=_UPSTREAM_BACKGROUND.get(), lane=_upstream_lane(runtime)
        )
        result = await _verify_result(client, {"url": url}, report_transport=True)
    except UpstreamBusy:
        # 自己甩掉的排队请求不说明源健康状况：不进熔断、不记解析失败
        raise
    except asyncio.CancelledError:
        # Deadline/client cancellation says nothing about source health.
        _CHAIN_HEALTH.get("user_source", {}).pop("half_open", None)
        raise
    except Exception as exc:  # transport, timeout, or script rejection
        if _is_quota_error(exc):
            # 配额打光不是"这首歌没货"：整家当天别再打了，把额度留给别家
            _quota_park(_upstream_lane(runtime))
        _record_failure(exc)
        _chain_report("user_source", False)
    else:
        # A clean resolve without usable media is not a source outage.
        # A late pre-open request must not close a circuit opened by siblings.
        if recovering or not _CHAIN_HEALTH.get("user_source", {}).get("open_until"):
            _chain_report("user_source", True)
    if result:
        result["resolver"] = "user_source"
        if not best or _TIER_RANK.get(result["actual_tier"], -1) > _TIER_RANK.get(best["actual_tier"], -1):
            best = result
            if retained is not None:
                retained.update(best=best, attempted=attempted)
    # Infrastructure-interrupted tiers must not be cached as exhausted.
    if len(failures or []) == before:
        attempted.append(tier)
    return best


async def _resolve_with_runtime(client: httpx.AsyncClient, src: str, item: dict,
                                tier: str, retained: dict | None,
                                runtime: UserSource, fresh: bool = False) -> "dict | None":
    """Single user-source pipeline: musicUrl -> verify -> downgrade.

    fast_start 开启时：首枪只打请求锚点档（要 lossless 先 flac，要 high 先 320k），
    一中即停；首枪无结果才补下一档降档兜底。
    请求数从最多 3 降为 1-2，避免三档齐发触发上游限流。
    """
    if _explicit_trial(item) or any(m in str(item.get("title") or "") for m in _TRIAL_TITLE_MARKERS):
        return None
    if runtime is None or not runtime.running or src not in runtime.platforms:
        return None
    tiers = _quality_tiers(tier)
    # fresh=1：proxy 已确认探活缓存里的直链失效，强制跳过缓存重新取链
    # （对齐安卓排除 failedUrls 后重取）；常规解析行为不变。
    cached = None if fresh else _fresh_probe(item, tiers[0])
    if cached:
        return cached
    item.pop("_probe", None)
    item.update(verified=False, validation_status="unverified", completeness="unknown")
    identifier = parse_track_id(str(item.get("id") or ""))[1]
    item.setdefault("_identifier", identifier)
    await _fill_script_meta(client, src, item, identifier)
    music_info = build_music_info(item, src)
    attempted = []
    best = None
    failures = _RESOLUTION_FAILURES.get()
    if CONF.get("fast_start") and len(tiers) > 1:
        # 按请求锚点降序逐档试（要 lossless 先 flac，要 high 先 320k），一中即停。
        # 限流靠「最多 2 次真实脚本调用」卡住，而不是靠写死最低档：
        # 源未声明的档位 _resolve_one_tier 会直接跳过（不花额度、不占名额），
        # 所以只声明 128k 的源仍能从 lossless 一路降到 standard 拿到链接。
        # 以前这里无条件升序先打 128k，把 proxy 侧的音质偏好整个抵消掉了。
        declared = runtime.qualitys(src)
        spent = 0
        for t in tiers:
            if best or spent >= 2:
                break
            if script_quality_for_tier(t, declared) is not None:
                spent += 1
            best = await _resolve_one_tier(client, src, item, music_info, t,
                                           runtime, retained, attempted, best, failures)
    else:
        for t in tiers:
            # Once a better known tier is retained, lower tiers cannot improve it.
            if best and _TIER_RANK.get(best["actual_tier"], -1) >= _TIER_RANK[t]:
                break
            best = await _resolve_one_tier(client, src, item, music_info, t,
                                           runtime, retained, attempted, best, failures)
    if best:
        best["attempted_tiers"] = attempted
        item["_probe"] = dict(best, ts=time.time(), tier=best["actual_tier"])
        item.update(verified=True, validation_status="media_verified", completeness="unknown")
        _cache_put(item)
    return best


async def resolve_and_probe(client: httpx.AsyncClient, src: str, item: dict,
                            tier: str = "standard", *, budget: float | None = None,
                            fresh: bool = False) -> dict | None:
    failures: list[str] = []
    retained: dict = {}
    token = _RESOLUTION_FAILURES.set(failures)
    try:
        deadline = asyncio.timeout(budget)
        try:
            async with deadline:
                result = await _resolve_and_probe(client, src, item, tier, retained, fresh)
        except TimeoutError:
            # Only our resolution budget may return retained media. External
            # caller cancellation remains CancelledError and propagates after
            # the awaited resolver/upgrade has completed cancellation cleanup.
            if not deadline.expired() or not retained.get("best"):
                raise
            result = dict(retained["best"], attempted_tiers=list(retained["attempted"]))
            item["_probe"] = dict(result, ts=time.time(), tier=result["actual_tier"])
            item.update(verified=True, validation_status="media_verified", completeness="unknown")
            _cache_put(item)
        if result is None and failures:
            raise ChainTransportError("resolution infrastructure exhausted: " + failures[-1])
        return result
    finally:
        _RESOLUTION_FAILURES.reset(token)


def _chunks(seq: list, n: int) -> list:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


async def _probe_candidates(
    client: httpx.AsyncClient, src: str, candidates: list[dict], limit: int
) -> list[dict]:
    """批量并发探活候选曲目，返回通过的条目（附带 verified/_probe 标记）。"""
    if limit <= 0:
        return []
    passed: list[dict] = []
    async def one(it):
        # 探活属后台批量任务：排到播放请求之后，队列积压时先甩它
        _UPSTREAM_BACKGROUND.set(True)
        res = await resolve_and_probe(client, src, it)
        if res:
            it.update(verified=True, validation_status="media_verified", completeness="unknown",
                      ext=res.get("ext") or it.get("ext") or "mp3",
                      file_size=res.get("file_size") or 0)
            _cache_put(it)
            _publish(it)
            return it
        return None

    for batch in _chunks(candidates, 6):
        tasks = [asyncio.create_task(one(it)) for it in batch]
        try:
            for done in asyncio.as_completed(tasks):
                try:
                    it = await done
                except Exception:
                    continue
                if it:
                    passed.append(it)
                    if len(passed) >= limit:
                        return passed
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    return passed


# ------------------------------------------------------------------ QQ tx ---

TX_SEARCH_BODY = {
    "req_1": {
        "method": "DoSearchForQQMusicDesktop",
        "module": "music.search.SearchCgiService",
        "param": {"search_type": 0, "query": "", "page_num": 1, "num_per_page": 20},
    }
}


async def tx_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict]:
    """QQ 音乐官方免登录搜索（musicu.fcg）。直链由用户自定义源解析后探活。"""
    fetch_size = max(limit * 2, 20)
    body = {"req_1": {**TX_SEARCH_BODY["req_1"], "param": {**TX_SEARCH_BODY["req_1"]["param"], "query": keyword, "num_per_page": fetch_size}}}
    try:
        r = await client.post(
            "https://u.y.qq.com/cgi-bin/musicu.fcg",
            json=body,
            headers={"User-Agent": UA_PC, "Referer": "https://y.qq.com/"},
        )
        data = r.json() or {}
    except Exception:  # noqa: BLE001
        raise
    songs = ((((data.get("req_1") or {}).get("data") or {}).get("body") or {}).get("song") or {}).get("list") or []
    candidates = []
    for it in songs:
        if not isinstance(it, dict) or _explicit_trial(it):
            continue
        mid = str(it.get("mid") or it.get("songmid") or "")
        title = str(it.get("title") or "")
        if not mid or not title:
            continue
        if any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue
        singers = it.get("singer") or []
        album = it.get("album") if isinstance(it.get("album"), dict) else {}
        file_obj = it.get("file") if isinstance(it.get("file"), dict) else {}
        pay = it.get("pay") or {}
        candidates.append(
            {
                "id": f"lx:tx:{mid}",
                "lx_source": "tx",
                "title": title,
                "artist": " / ".join(str(s.get("name") or "") for s in singers if isinstance(s, dict)),
                "album": str(album.get("name") or ""),
                "duration_s": int(it.get("interval") or 0),
                "ext": "mp3",
                "cover_url": (
                    f"https://y.gtimg.cn/music/photo_new/T002R300x300M000{album.get('mid')}.jpg"
                    if album.get("mid")
                    else ""
                ),
                "file_size": 0,
                "lyric": "",
                "songmid": mid,
                "str_media_mid": str(file_obj.get("media_mid") or ""),
                "album_id": str(album.get("id") or ""),
                "pay_type": int(pay.get("pay_play") or 0),
            }
        )
    # 直链由用户自定义源解析+探活；未开启逐曲探活时直接作为未验证条目返回
    if is_probe_enabled():
        return await _probe_candidates(client, "tx", candidates, limit)
    items = []
    for it in candidates[:limit]:
        it.update(verified=False, validation_status="unverified", completeness="unknown")
        _cache_put(it)
        items.append(it)
        _publish(it)
    return items


async def tx_resolve_lyric(client: httpx.AsyncClient, identifier: str) -> str:
    import html as _html

    try:
        r = await client.get(
            "https://c.y.qq.com/lyric/fcgi-bin/fcg_query_lyric_new.fcg",
            params={
                "songmid": identifier,
                "g_tk": "5381",
                "loginUin": "0",
                "hostUin": "0",
                "format": "json",
                "inCharset": "utf8",
                "outCharset": "utf-8",
                "notice": "0",
                "platform": "yqq",
                "needNewCode": "0",
            },
            headers={"User-Agent": UA_PC, "Referer": "https://y.qq.com/portal/player.html"},
        )
        data = _lenient_json(r, f"tx lyric {identifier}")
        content = str((data or {}).get("lyric") or "")
        if not content:
            return ""
        return _html.unescape(base64.b64decode(content).decode("utf-8", errors="replace"))
    except Exception:  # noqa: BLE001
        return ""


# ------------------------------------------------------------------ 酷我 kw ---

def _lenient_pydict(resp: httpx.Response, tag: str = "") -> "dict | None":
    """酷我 r.s 老接口返回 Python 字面量风格（单引号），宽容解析。"""
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        pass
    import ast as _ast

    text = (resp.text or "").strip()
    if text.startswith("{"):
        try:
            return _ast.literal_eval(text)
        except Exception as e:  # noqa: BLE001
            logger.warning("%s: py-dict parse failed: %s", tag, e)
    return None


def _title_relevance(title: str, keyword: str) -> int:
    """搜索候选排序：标题与关键词越接近越靠前（r.s 相关度常把原版排在伴奏/翻唱之后）。"""
    import re as _re

    def norm(s: str) -> str:
        s = _re.sub(r"\([^)]*\)|（[^）]*）|\[[^\]]*\]", "", s or "")
        return _re.sub(r"[\s\-—·・&]", "", s).lower()

    t, k = norm(title), norm(keyword)
    if not k:
        return 3
    if t == k:
        return 0
    if k.startswith(t) or t.startswith(k):
        return 1  # 原版：title 即关键词主体（"晴天" ⊂ "晴天 周杰伦"）
    if k in t or t in k:
        return 2
    return 3


async def kw_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict]:
    """酷我官方免登录搜索（r.s 老接口）。直链由用户自定义源解析后探活。"""
    import html as _html

    # r.s 相关度常把原版排在伴奏/翻唱之后，抓取窗口放大再按标题相关度重排
    fetch_size = max(limit * 4, 60)
    r = await client.get(
        "http://search.kuwo.cn/r.s",
        params={
            "all": keyword,
            "ft": "music",
            "itemset": "web_2013",
            "client": "kt",
            "pn": 0,
            "rn": fetch_size,
            "rformat": "json",
            "encoding": "utf8",
        },
        headers={"User-Agent": UA_PC, "Referer": "http://www.kuwo.cn/"},
    )
    r.raise_for_status()
    raw = _lenient_pydict(r, "kw r.s") or {}
    candidates = []
    for it in raw.get("abslist") or []:
        if not isinstance(it, dict) or _explicit_trial(it):
            continue
        rid = str(it.get("MUSICRID") or "").replace("MUSIC_", "").strip()
        if not rid.isdigit():
            continue
        payinfo = it.get("payInfo") or {}
        # cannotOnlinePlay=1 表示无在线播放版权，真不可播，直接剔除
        if str(payinfo.get("cannotOnlinePlay") or "0") == "1":
            continue
        title = _html.unescape(str(it.get("SONGNAME") or "")).replace("\xa0", " ").strip()
        if not title or any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue
        cover_short = str(it.get("web_albumpic_short") or "").strip()
        cover_url = (
            f"https://img1.kuwo.cn/star/albumcover/{cover_short}"
            if cover_short
            else f"https://img4.kuwo.cn/star/albumcover/{rid}.jpg"
        )
        candidates.append(
            {
                "id": f"lx:kw:{rid}",
                "lx_source": "kw",
                "title": title,
                "artist": _html.unescape(str(it.get("ARTIST") or "")).replace("\xa0", " ").strip(),
                "album": _html.unescape(str(it.get("ALBUM") or "")).replace("\xa0", " ").strip(),
                "duration_s": int(it.get("DURATION") or 0),
                "ext": "mp3",
                "cover_url": cover_url,
                "file_size": 0,
                "lyric": "",
                "rid": rid,
                "pay_type": int(it.get("PAY") or 0),
            }
        )
    # 标题相关度排序：原版（title≈keyword）优先于伴奏/DJ/翻唱版本
    candidates.sort(key=lambda c: _title_relevance(c["title"], keyword))
    if is_probe_enabled():
        return await _probe_candidates(client, "kw", candidates, limit)
    items = []
    for it in candidates[:limit]:
        it.update(verified=False, validation_status="unverified", completeness="unknown")
        _cache_put(it)
        items.append(it)
        _publish(it)
    return items


# ------------------------------------------------------------ 酷我 H5 songinfo ---
# 一次调用同时拿到专辑封面与整页歌词（安卓版 musicSdk/kw/lyric.js 用的就是这条）。
# 此前 kw_resolve_lyric 是直接 return "" 的桩，注释写"接口已失效"：实测 2026-10
# 仍可用，但该接口**首打约一半概率返回 msg=音乐查询失败 的空响应**，重试一次即
# 成功——"失效"的结论是被这个抖动骗了。影响面：收藏 1387 首里 428 首酷我歌全无歌词。
# 封面与歌词共用这一条结果（含 7 天命中缓存 / 6h 未命中负缓存 + 同 rid 合流）：
# 补封面顺带把词也补上，官方接口每首歌只打一次。
_KW_SONGINFO_TTL_S = float(os.environ.get("LX_KW_SONGINFO_TTL", "604800"))
_KW_SONGINFO_MISS_TTL_S = 6 * 3600.0
_KW_SONGINFO_TRIES = 3
_KW_SONGINFO_RETRY_DELAY_S = 0.15
_KW_SONGINFO_CACHE: dict[str, tuple[dict, float]] = {}
_KW_SONGINFO_INFLIGHT: dict[str, Any] = {}
_KW_SONGINFO_STATS = {"calls": 0, "hits": 0, "retries": 0, "miss": 0}


def _kw_rid_of(item: dict, identifier: str = "") -> str:
    rid = str(identifier or "").strip()
    if rid.isdigit():
        return rid
    raw_id = str((item or {}).get("_identifier") or (item or {}).get("id") or "")
    rid = raw_id.split(":")[-1].strip()
    return rid if rid.isdigit() else ""


def _lrc_stamp(seconds: Any) -> str:
    try:
        total = max(0.0, float(seconds or 0))
    except (TypeError, ValueError):
        return ""
    return f"{int(total // 60):02d}:{total % 60:05.2f}"


def _kw_build_lrc(songinfo: dict, rows: list) -> str:
    """按安卓 musicSdk 同款格式拼标准 LRC（本插件歌词解析器直接可用）。"""
    lines = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        text = str(row.get("lineLyric") or "").strip()
        stamp = _lrc_stamp(row.get("time"))
        if text and stamp:
            lines.append(f"[{stamp}]{text}")
    if not lines:
        return ""
    head = (
        f"[ti:{songinfo.get('songName') or ''}]\n"
        f"[ar:{songinfo.get('artist') or ''}]\n"
        f"[al:{songinfo.get('album') or ''}]\n"
        "[by:]\n[offset:0]\n"
    )
    return head + "\n".join(lines) + "\n"


async def _kw_songinfo_fetch(client: httpx.AsyncClient, rid: str) -> dict:
    """打官方接口拿 {pic, lyric, title, artist, album}；带重试与负缓存。"""
    cached = _KW_SONGINFO_CACHE.get(rid)
    if cached and time.time() - cached[1] < _KW_SONGINFO_TTL_S:
        _KW_SONGINFO_STATS["hits"] += 1
        return cached[0]
    if cached:  # 负缓存也走同一入口：未命中记录在 TTL 内不再重试
        _KW_SONGINFO_CACHE.pop(rid, None)
    fut = _KW_SONGINFO_INFLIGHT.get(rid)
    if fut is not None:
        try:
            return await asyncio.shield(fut)
        except Exception:  # noqa: BLE001
            return {}
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    _KW_SONGINFO_INFLIGHT[rid] = fut
    result: dict = {}
    try:
        for attempt in range(_KW_SONGINFO_TRIES):
            if attempt:
                _KW_SONGINFO_STATS["retries"] += 1
                await asyncio.sleep(_KW_SONGINFO_RETRY_DELAY_S)
            _KW_SONGINFO_STATS["calls"] += 1
            try:
                r = await client.get(
                    "http://m.kuwo.cn/newh5/singles/songinfoandlrc",
                    params={"musicId": rid},
                    headers={"User-Agent": UA_MOBILE, "Referer": "https://m.kuwo.cn/"},
                    timeout=8.0,
                )
                payload = r.json() if r.status_code == 200 else {}
            except (httpx.HTTPError, ValueError):
                payload = {}
            data = (payload or {}).get("data") or {}
            songinfo = data.get("songinfo") or {}
            rows = data.get("lrclist") or []
            if not isinstance(songinfo, dict) or not (songinfo or rows):
                continue  # 空响应：接口抖动，重试
            pic = str(songinfo.get("pic") or songinfo.get("pic500") or "").strip()
            result = {
                "pic": pic if pic.startswith("http") else "",
                "lyric": _kw_build_lrc(songinfo, rows if isinstance(rows, list) else []),
                "title": str(songinfo.get("songName") or ""),
                "artist": str(songinfo.get("artist") or ""),
                "album": str(songinfo.get("album") or ""),
            }
            break
        _KW_SONGINFO_CACHE[rid] = (result, time.time())
        if not result:
            # 未命中：缩短缓存窗口，避免一次抖动把这首歌永久判死
            _KW_SONGINFO_CACHE[rid] = (result, time.time() - _KW_SONGINFO_TTL_S + _KW_SONGINFO_MISS_TTL_S)
            _KW_SONGINFO_STATS["miss"] += 1
        if len(_KW_SONGINFO_CACHE) > 4000:
            for stale in sorted(_KW_SONGINFO_CACHE, key=lambda k: _KW_SONGINFO_CACHE[k][1])[:1000]:
                _KW_SONGINFO_CACHE.pop(stale, None)
        if not fut.done():
            fut.set_result(result)
        return result
    except BaseException as exc:
        if not fut.done():
            fut.set_exception(exc)
        raise
    finally:
        if _KW_SONGINFO_INFLIGHT.get(rid) is fut:
            _KW_SONGINFO_INFLIGHT.pop(rid, None)


async def kw_resolve_lyric(client: httpx.AsyncClient, item: dict, identifier: str = "") -> str:
    rid = _kw_rid_of(item or {}, identifier)
    if not rid:
        return ""
    return str((await _kw_songinfo_fetch(client, rid)).get("lyric") or "")


# ------------------------------------------------------------ 免登录榜单推荐 ---

# 实测存活的免登录榜单（2026-09）：kg 移动端 TOP500 / kw kbang 飙升榜 / wy 新歌速递
_KG_RANK_ID = 8888  # m.kugou.com/rank/info 必须带 page 参数，否则 songs.list 为空
_KW_BANG_ID = 93    # kbangserver.kuwo.cn 免签老接口


async def kg_chart_songs(client: httpx.AsyncClient, limit: int) -> list[dict]:
    """酷狗移动端 TOP500 榜单（免登录）。免费曲直接收录，付费曲探活通过才返回。"""
    try:
        r = await client.get(
            "http://m.kugou.com/rank/info/",
            params={"rankid": _KG_RANK_ID, "page": 1, "json": "true"},
            headers={"User-Agent": UA_MOBILE},
            timeout=10.0,
        )
        r.raise_for_status()
        rows = ((r.json() or {}).get("songs") or {}).get("list") or []
    except Exception as e:  # noqa: BLE001
        logger.warning("kg chart rank %s failed: %s", _KG_RANK_ID, e)
        return []
    items: list[dict] = []
    vip_candidates: list[dict] = []
    for it in rows:
        if not isinstance(it, dict) or _explicit_trial(it):
            continue
        fhash = str(it.get("hash") or "")
        if not fhash:
            continue
        # 榜单曲目几乎全部带 pay_type/fail_process VIP 标记，但实测匿名 playInfo
        # 仍能解析出完整时长直链（2026-09 验证 3.6MB/225s），故不按标记硬过滤，
        # 一律以探活结果为准（与搜索侧"可播性验证取代收费过滤"同一原则）
        if int(it.get("is_free_part") or 0) != 0:
            continue
        singer = " / ".join(
            str(a.get("author_name") or "")
            for a in (it.get("authors") or [])
            if isinstance(a, dict) and a.get("author_name")
        )
        title = str(it.get("songname") or it.get("filename") or "").replace(f"{singer} - ", "")
        if not title or any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue
        pay_type = int(it.get("pay_type") or 0)
        is_vip = pay_type != 0 or int(it.get("pkg_price") or 0) != 0 or int(it.get("price") or 0) != 0
        sq = str(it.get("sqhash") or "")
        item = {
            "id": f"lx:kg:{fhash}",
            "lx_source": "kg",
            "title": title,
            "artist": singer,
            "album": "",  # rank 接口不返回专辑名
            "duration_s": int(it.get("duration") or 0),  # rank 接口 duration 单位为秒
            "ext": "flac" if sq else "mp3",
            "cover_url": str(it.get("album_sizable_cover") or "").replace("{size}", "480"),
            "file_size": int(it.get("sqfilesize") or it.get("320filesize") or 0) or 0,
            "lyric": "",
            "hash": fhash,
            "hash_hq": str(it.get("320hash") or ""),
            "hash_sq": sq,
            "mixsongid": str(it.get("album_audio_id") or ""),
            "pay_type": pay_type,
        }
        if is_vip:
            vip_candidates.append(item)
        else:
            item.update(verified=False, validation_status="unverified", completeness="unknown")
            _cache_put(item)
            items.append(item)
            _publish(item)
    if vip_candidates and len(items) < limit:
        if is_probe_enabled():
            items.extend(await _probe_candidates(client, "kg", vip_candidates, limit - len(items)))
        else:
            for it in vip_candidates[:limit - len(items)]:
                it.update(verified=False, validation_status="unverified", completeness="unknown")
                _cache_put(it)
                items.append(it)
                _publish(it)
    return items[:limit]


async def kw_chart_songs(client: httpx.AsyncClient, limit: int) -> list[dict]:
    """酷我 kbang 免签飙升榜（老接口字段较旧）。直链由用户自定义源解析，全部探活。"""
    import html as _html

    try:
        r = await client.get(
            "http://kbangserver.kuwo.cn/ksong.s",
            params={
                "from": "pc",
                "fmt": "json",
                "pn": 0,
                "rn": max(limit * 2, 40),
                "id": _KW_BANG_ID,
                "nc": 1,
            },
            headers={"User-Agent": UA_PC},
            timeout=10.0,
        )
        r.raise_for_status()
        rows = (r.json() or {}).get("musiclist") or []
    except Exception as e:  # noqa: BLE001
        logger.warning("kw bang %s failed: %s", _KW_BANG_ID, e)
        return []
    candidates: list[dict] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        rid = str(it.get("id") or "")
        if not rid.isdigit():
            continue
        payinfo = it.get("payInfo") or {}
        # cannotOnlinePlay=1 无在线版权；listen_fragment=1 仅有试听片段，均真不可播
        if str(payinfo.get("cannotOnlinePlay") or "0") == "1":
            continue
        if str(payinfo.get("listen_fragment") or "0") == "1":
            continue
        title = _html.unescape(str(it.get("name") or "")).replace("\xa0", " ").strip()
        if not title or any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue
        formats = str(it.get("formats") or "")
        fee_type = payinfo.get("feeType") or {}
        candidates.append(
            {
                "id": f"lx:kw:{rid}",
                "lx_source": "kw",
                "title": title,
                "artist": _html.unescape(str(it.get("artist") or "")).replace("\xa0", " ").strip(),
                "album": _html.unescape(str(it.get("album") or "")).replace("\xa0", " ").strip(),
                "duration_s": int(float(it.get("song_duration") or 0)),
                "ext": "flac" if "ALFLAC" in formats else "mp3",
                "cover_url": "",
                "file_size": 0,
                "lyric": "",
                "rid": rid,
                "pay_type": 1 if str(fee_type.get("song") or "0") != "0" else 0,
            }
        )
    if is_probe_enabled():
        return await _probe_candidates(client, "kw", candidates, limit)
    items = []
    for it in candidates[:limit]:
        it.update(verified=False, validation_status="unverified", completeness="unknown")
        _cache_put(it)
        items.append(it)
    return items


async def wy_chart_songs(client: httpx.AsyncClient, limit: int) -> list[dict]:
    """网易新歌速递（免登录）。fee∉(0,8) 的付费曲转探活验证。"""
    try:
        r = await client.get(
            "https://music.163.com/api/personalized/newsong",
            params={"limit": max(limit * 2, 30)},
            headers={
                "User-Agent": UA_PC,
                "Referer": "https://music.163.com/",
                "Cookie": "os=pc; appver=9.1.15",
            },
            timeout=10.0,
        )
        r.raise_for_status()
        rows = (r.json() or {}).get("result") or []
    except Exception as e:  # noqa: BLE001
        logger.warning("wy newsong failed: %s", e)
        return []
    items: list[dict] = []
    vip_candidates: list[dict] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        song = it.get("song") if isinstance(it.get("song"), dict) else {}
        sid = str(song.get("id") or it.get("id") or "")
        if not sid:
            continue
        name = str(song.get("name") or it.get("name") or "")
        if not name or any(marker in name for marker in _TRIAL_TITLE_MARKERS):
            continue
        artists = song.get("artists") or []
        artist = " / ".join(str(a.get("name") or "") for a in artists if isinstance(a, dict))
        album = song.get("album") or {}
        fee = int(song.get("fee") or 0)
        item = {
            "id": f"lx:wy:{sid}",
            "lx_source": "wy",
            "title": name,
            "artist": artist,
            "album": str(album.get("name") or "") if isinstance(album, dict) else "",
            "duration_s": int(song.get("duration") or 0) / 1000.0,
            "ext": "mp3",
            "cover_url": str(album.get("picUrl") or "") if isinstance(album, dict) else "",
            "file_size": 0,
            "lyric": "",
            "song_id": sid,
            "fee": fee,
        }
        if fee not in (0, 8):
            vip_candidates.append(item)
        else:
            item.update(verified=False, validation_status="unverified", completeness="unknown")
            _cache_put(item)
            items.append(item)
            _publish(item)
    if vip_candidates and len(items) < limit:
        if is_probe_enabled():
            items.extend(await _probe_candidates(client, "wy", vip_candidates, limit - len(items)))
        else:
            for it in vip_candidates[:limit - len(items)]:
                it.update(verified=False, validation_status="unverified", completeness="unknown")
                _cache_put(it)
                items.append(it)
                _publish(it)
    return items[:limit]


_CHARTERS: dict[str, Any] = {"kg": kg_chart_songs, "kw": kw_chart_songs, "wy": wy_chart_songs}


# --------------------------------------------------------------------- app ---

_SEARCHERS = {"kg": kg_search, "wy": wy_search, "mg": mg_search, "tx": tx_search, "kw": kw_search}


def _all_source_runtimes() -> list:
    """旧版单源 + 附加源池的全部运行时（内存守护枚举入口）。"""
    out: list = []
    primary = SOURCE_MANAGER.get()
    if primary is not None:
        out.append(primary)
    out.extend(rt for _sid, rt in SOURCE_POOL.all_runtimes())
    return out


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    _load_song_cache()
    created = False
    if getattr(fastapi_app.state, "http", None) is None:
        fastapi_app.state.http = httpx.AsyncClient(
            timeout=httpx.Timeout(CONF["search_timeout"], connect=5.0),
            follow_redirects=True,
        )
        created = True
    loader = asyncio.create_task(SOURCE_MANAGER.load())
    pool_loader = asyncio.create_task(SOURCE_POOL.load())
    guard = (
        asyncio.create_task(run_memory_guard(_all_source_runtimes))
        if memory_guard_enabled()
        else None
    )
    try:
        yield
    finally:
        if guard is not None:
            guard.cancel()
        loader.cancel()
        pool_loader.cancel()
        await asyncio.gather(
            loader, pool_loader, *([guard] if guard is not None else []), return_exceptions=True
        )
        await SOURCE_MANAGER.shutdown()
        await SOURCE_POOL.shutdown()
        if created:
            await fastapi_app.state.http.aclose()


app = FastAPI(title="fnmusic-lxmusic", version=SERVICE_VERSION, lifespan=lifespan)


def get_http(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "http", None)
    if client is None:
        client = httpx.AsyncClient(timeout=CONF["search_timeout"], follow_redirects=True)
        fastapi_app.state.http = client
    return client


@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "service": "fnmusic-lxmusic",
        "version": SERVICE_VERSION,
        "sources": CONF["sources"],
        "user_source": SOURCE_MANAGER.describe(),
        "circuit": chain_health_snapshot(),
        "upstream_gate": LX_UPSTREAM_GATE.snapshot(),
        "upstream_quota": _quota_snapshot(),
        "capabilities": source_capabilities(),
        "charts": sorted(_CHARTERS),
    }


def _err(msg: str, code: int = 404) -> JSONResponse:
    return JSONResponse(content={"ok": False, "error": msg}, status_code=code)


class _LxSuperseded:
    pass


_LX_SUPERSEDED = _LxSuperseded()


class LxSearchGate:
    """同时只搜一个关键词。同一范围的新词取消正在跑的平台任务；其他范围排队。"""

    def __init__(self):
        self._epoch = 0
        self._running: dict | None = None
        self._queue: list[dict] = []

    def reset(self) -> None:
        self._epoch += 1
        job = self._running
        self._running = None
        queued = self._queue
        self._queue = []
        if job and job.get("task") and not job["task"].done():
            job["task"].cancel()
        for slot in queued:
            _lx_resolve(slot, _LX_SUPERSEDED)
        if job:
            _lx_resolve(job, _LX_SUPERSEDED)

    async def run(self, scope: str, keyword: str, factory):
        fut = asyncio.get_running_loop().create_future()
        running = self._running
        if running and running["scope"] == scope and running["keyword"] == keyword and not running.get("superseded"):
            running["waiters"].append(fut)
        elif running and running["scope"] == scope and running["keyword"] != keyword:
            running["superseded"] = True
            _lx_resolve(running, _LX_SUPERSEDED)
            task = running.get("task")
            if task and not task.done():
                task.cancel()
            self._upsert(scope, keyword, factory, fut, front=True)
        else:
            self._upsert(scope, keyword, factory, fut, front=False)
        self._pump()
        return await asyncio.shield(fut)

    def _upsert(self, scope, keyword, factory, fut, front: bool) -> None:
        for slot in self._queue:
            if slot["scope"] != scope:
                continue
            if slot["keyword"] != keyword:
                _lx_resolve(slot, _LX_SUPERSEDED)
                slot["keyword"] = keyword
                slot["factory"] = factory
                slot["waiters"] = [fut]
            else:
                slot["waiters"].append(fut)
            if front:
                self._queue.remove(slot)
                self._queue.insert(0, slot)
            return
        slot = {"scope": scope, "keyword": keyword, "factory": factory, "waiters": [fut]}
        self._queue.insert(0, slot) if front else self._queue.append(slot)

    def _pump(self) -> None:
        if self._running is not None or not self._queue:
            return
        slot = self._queue.pop(0)
        job = {
            "scope": slot["scope"], "keyword": slot["keyword"], "factory": slot["factory"],
            "waiters": slot["waiters"], "superseded": False, "epoch": self._epoch, "task": None,
        }
        self._running = job
        job["task"] = asyncio.get_running_loop().create_task(self._execute(job))

    async def _execute(self, job: dict) -> None:
        result = _LX_SUPERSEDED
        try:
            result = await job["factory"]()
        except asyncio.CancelledError:
            result = _LX_SUPERSEDED
        except Exception as exc:
            result = exc
        if job.get("epoch") != self._epoch:
            _lx_resolve(job, _LX_SUPERSEDED)
            return
        if self._running is job:
            self._running = None
        if job.get("superseded") or result is _LX_SUPERSEDED:
            _lx_resolve(job, _LX_SUPERSEDED)
        elif isinstance(result, Exception):
            _lx_resolve(job, result, error=True)
        else:
            _lx_resolve(job, result)
        self._pump()


def _lx_resolve(job: dict, result, error: bool = False) -> None:
    for fut in job.get("waiters", []):
        if not fut.done():
            if error:
                fut.set_exception(result)
            else:
                fut.set_result(result)


LX_SEARCH_GATE = LxSearchGate()


@app.get("/api/v1/search")
async def search(
    keyword: str = Query("", alias="keyword"),
    q: str = Query("", alias="q"),
    limit: int = Query(0),
    sources: str = Query(""),
    probe: int | None = Query(None, alias="probe"),
    x_fnmusic_scope: str = Header(default=""),
):
    kw = (keyword or q or "").strip()
    if not kw:
        return _err("keyword required", 400)
    _STATS["searches"] += 1
    if limit <= 0:
        limit = CONF["limit_per_source"]
    wanted_raw = [s.strip() for s in (sources or "").split(",") if s.strip()]
    wanted = [normalize_source(s) for s in wanted_raw]
    wanted = [s for s in wanted if s] or CONF["sources"]
    scope = (x_fnmusic_scope or "").strip()

    async def _run():
        return await _search_platforms(kw, limit, wanted, probe=probe)

    result = await LX_SEARCH_GATE.run(scope, kw, _run)
    if result is _LX_SUPERSEDED:
        return {"ok": True, "items": [], "superseded": True, "errors": {}}
    return result


async def _search_platforms(kw: str, limit: int, wanted: list[str], *, probe: int | None = None) -> dict:
    probe_val = bool(probe) if probe is not None else None
    probe_token = _SEARCH_PROBE_ENABLED.set(probe_val)
    try:
        client = get_http(app)
        tasks = {}
        partials = {}
        errors: dict[str, str] = {}
        capabilities = source_capabilities()

        async def run_source(src):
            token = _SEARCH_PARTIAL.set(partials[src])
            try:
                return await _SEARCHERS[src](client, kw, limit)
            finally:
                _SEARCH_PARTIAL.reset(token)

        for src in dict.fromkeys(wanted):
            if src not in _SEARCHERS:
                continue
            if not capabilities[src]["playback_available"]:
                errors[src] = capabilities[src]["reason"]
                continue
            partials[src] = []
            tasks[src] = asyncio.create_task(run_source(src))
        items: list[dict] = []
        try:
            if tasks:
                # One shared budget, below the proxy's default 15s timeout. Never
                # multiply the timeout by source count or wait in insertion order.
                await asyncio.wait(tasks.values(), timeout=max(0.001, min(CONF["search_timeout"], 13.0)))
        finally:
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
        for src, task in tasks.items():
            if task.cancelled():
                errors[src] = "search deadline exceeded"
                results = partials[src]
            elif task.exception() is not None:
                errors[src] = str(task.exception()) or type(task.exception()).__name__
                results = partials[src]
            else:
                results = task.result()
            seen = set()
            for item in results:
                if item.get("id") not in seen:
                    _cache_put(item)
                    items.append(item)
                    seen.add(item.get("id"))
                    if len(seen) >= limit:
                        break
        _STATS["errors"] += len(errors)
        return {"ok": True, "items": items, "errors": errors, "stats": dict(_STATS),
                "capabilities": capabilities}
    finally:
        _SEARCH_PROBE_ENABLED.reset(probe_token)


# ------------------------------------------------------------------ 跨源封面搜索 ---
# 独立封面取图渠道（供代理封面兜底链调用）：阶梯搜索——曲目自身平台优先，
# 命中即停，未命中才走下一平台（移植桌面端 coverResolver 匹配/打分算法：
# bigram Dice 相似度 + 时长加成）。与 /search 的区别：不受播放可用性门禁
# ——封面只要一张图，不要求可播（VIP 曲目的封面也能找到）。
# 官方搜索接口有访问限制：单平台 4.5s 超时 + 每日总调用预算
# （FNMUSIC_COVER_SEARCH_DAILY_BUDGET，默认 600 次/天，0 点重置），
# 预算耗尽当天不再打官方搜索；single-flight 去重。

_COVER_SEARCH_BUDGET_S = 12.0
_COVER_SEARCH_PER_SRC_S = 4.5
_COVER_SEARCH_GLOBAL_SEM = asyncio.Semaphore(8)
_COVER_SEARCH_INFLIGHT: dict[str, "asyncio.Task[dict | None]"] = {}
_COVER_NAME_MIN = 0.86
_COVER_ARTIST_MIN = 0.72
_COVER_ARTISTLESS_NAME_MIN = 0.90
_COVER_STAGE_ORDER = ("tx", "kw", "kg", "wy", "mg")
_COVER_DAILY_BUDGET = int(os.environ.get("FNMUSIC_COVER_SEARCH_DAILY_BUDGET", "600"))
_COVER_CALLS: dict[str, Any] = {"date": "", "count": 0}


def _cover_daily_budget_left() -> int:
    """官方搜索接口每日剩余调用额度（按本地日期 0 点重置）。"""
    import datetime as _dt

    today = _dt.datetime.now().strftime("%Y-%m-%d")
    if _COVER_CALLS["date"] != today:
        _COVER_CALLS["date"] = today
        _COVER_CALLS["count"] = 0
    return max(0, _COVER_DAILY_BUDGET - _COVER_CALLS["count"])


def _cover_charge() -> bool:
    """记账一次官方搜索调用；额度耗尽返回 False（调用方应停止搜索）。"""
    if _cover_daily_budget_left() <= 0:
        return False
    _COVER_CALLS["count"] += 1
    return True


def _cover_search_order(source: str) -> list:
    """阶梯搜索顺序：曲目自身平台最优先，其余按 tx→kw→kg→wy→mg 依次兜底。"""
    s = str(source or "").strip().lower()
    order = [s] if s in _SEARCHERS else []
    order.extend(x for x in _COVER_STAGE_ORDER if x != s)
    return order


def reset_cover_search_state() -> None:
    """测试用：清空 single-flight 任务与每日计数。"""
    _COVER_SEARCH_INFLIGHT.clear()
    _COVER_CALLS["date"] = ""
    _COVER_CALLS["count"] = 0


def _cover_str_sim(s1, s2) -> float:
    """bigram Dice 相似度（与桌面端 strSim 同算法），用于标题/歌手匹配。"""
    t1 = str(s1 or "").lower().strip()
    t2 = str(s2 or "").lower().strip()
    if t1 == t2:
        return 1.0
    b1 = {t1[i:i + 2] for i in range(len(t1) - 1)}
    b2 = {t2[i:i + 2] for i in range(len(t2) - 1)}
    if not b1 or not b2:
        return 0.0
    return (2.0 * len(b1 & b2)) / (len(b1) + len(b2))


def _cover_duration_bonus(orig_s: float, cand_s: float) -> float:
    """时长相近加分、差异过大减分（与桌面端一致：<=8s +0.2, >45s -0.35）。"""
    if orig_s > 0 and cand_s > 0:
        d = abs(orig_s - cand_s)
        if d <= 8:
            return 0.2
        if d > 45:
            return -0.35
    return 0.0


def _cover_artist_sim(query_artist: str, cand_artist: str) -> float:
    """歌手相似度：两侧歌手逐个拆分后取两两 bigram Dice 的最大值。
    防止合唱曲整串对比被拉低（如"张智霖"对"张智霖 / 许秋怡"旧算法 <0.72 误杀）。"""
    q = [p for p in re.split(r"[/、,，&|｜\s]+", str(query_artist or "")) if p]
    c = [p for p in re.split(r"[/、,，&|｜\s]+", str(cand_artist or "")) if p]
    if not q:
        return 1.0
    if not c:
        return 0.0
    return max(_cover_str_sim(a, b) for a in q for b in c)


def _cover_artist_ok(query_artist: str, cand_artist: str) -> bool:
    if not query_artist:
        return True
    if _cover_artist_sim(query_artist, cand_artist) >= _COVER_ARTIST_MIN:
        return True
    ql = str(query_artist).casefold().strip()
    cl = str(cand_artist).casefold().strip()
    return bool(ql and cl) and (ql in cl or cl in ql)


def _cover_score_candidate(
    title: str, artist: str, album: str, duration_s: float, it: dict
) -> "dict | None":
    """单个候选按桌面端同款阈值/打分；不过关返回 None。"""
    cover = str(it.get("cover_url") or "").strip()
    if not cover.startswith(("http://", "https://")):
        return None
    name_sim = _cover_str_sim(title, str(it.get("title") or ""))
    if name_sim < (_COVER_NAME_MIN if artist else _COVER_ARTISTLESS_NAME_MIN):
        return None
    cand_artist = str(it.get("artist") or "")
    if artist and not _cover_artist_ok(artist, cand_artist):
        return None
    score = name_sim * 0.65 + _cover_artist_sim(artist, cand_artist) * 0.35
    if album:
        score += _cover_str_sim(album, str(it.get("album") or "")) * 0.05
    try:
        cand_dur = float(it.get("duration_s") or 0)
    except (TypeError, ValueError):
        cand_dur = 0.0
    score += _cover_duration_bonus(duration_s, cand_dur)
    return {
        "cover_url": cover,
        "id": str(it.get("id") or ""),
        "lx_source": str(it.get("lx_source") or ""),
        "title": str(it.get("title") or ""),
        "artist": cand_artist,
        "score": round(score, 4),
    }


async def _cover_search_run(
    title: str, artist: str, album: str, duration_s: float, source: str = ""
) -> "dict | None":
    """阶梯搜索：自身平台优先，命中即停，未命中才进下一平台。
    每平台一次官方调用（旧版 5 平台并行 = 每歌 5 次），并受每日预算约束。"""
    client = get_http(app)
    keyword = f"{title} {artist}".strip()
    probe_token = _SEARCH_PROBE_ENABLED.set(False)  # 封面只要图，不探活
    partial_token = _SEARCH_PARTIAL.set([])
    try:
        for src in _cover_search_order(source):
            if not _cover_charge():
                logger.info("cover search daily budget exhausted before stage %s", src)
                return None
            try:
                async with _COVER_SEARCH_GLOBAL_SEM:
                    items = await asyncio.wait_for(
                        _SEARCHERS[src](client, keyword, 20), timeout=_COVER_SEARCH_PER_SRC_S
                    )
            except Exception:  # noqa: BLE001
                continue
            best = None
            for it in items or []:
                if not isinstance(it, dict):
                    continue
                cand = _cover_score_candidate(title, artist, album, duration_s, it)
                if cand and (best is None or cand["score"] > best["score"]):
                    best = cand
            if best is not None:
                best["lx_source"] = src
                return best
    finally:
        _SEARCH_PROBE_ENABLED.reset(probe_token)
        _SEARCH_PARTIAL.reset(partial_token)
    return None


def _cover_search_drop(key: str, task: "asyncio.Task") -> None:
    if _COVER_SEARCH_INFLIGHT.get(key) is task:
        _COVER_SEARCH_INFLIGHT.pop(key, None)
    if not task.cancelled():
        try:
            task.exception()
        except Exception:  # noqa: BLE001
            pass


@app.get("/api/v1/cover/search")
async def cover_search(
    title: str = Query("", alias="title"),
    artist: str = Query("", alias="artist"),
    album: str = Query("", alias="album"),
    duration: float = Query(0.0, alias="duration"),
    source: str = Query("", alias="source"),
):
    title = (title or "").strip()
    artist = (artist or "").strip()
    album = (album or "").strip()
    if not title:
        return _err("title required", 400)
    try:
        duration_s = max(0.0, float(duration or 0))
    except (TypeError, ValueError):
        duration_s = 0.0
    key = f"{title.casefold()}|{artist.casefold()}|{str(source or '').strip().lower()}"
    task = _COVER_SEARCH_INFLIGHT.get(key)
    if task is None or task.done():
        task = asyncio.create_task(_cover_search_run(title, artist, album, duration_s, source))
        _COVER_SEARCH_INFLIGHT[key] = task
        task.add_done_callback(lambda _t, k=key: _cover_search_drop(k, _t))
    try:
        return {"ok": True, "data": await asyncio.wait_for(
            asyncio.shield(task), timeout=_COVER_SEARCH_BUDGET_S + 1.0
        )}
    except asyncio.TimeoutError:
        return {"ok": True, "data": None}


@app.get("/api/v1/recommend")
async def recommend_charts(
    limit: int = Query(0),
    sources: str = Query(""),
):
    """免登录榜单推荐：kg TOP500 / kw 飙升榜 / wy 新歌速递，按源顺序聚合凑满 limit。"""
    if limit <= 0:
        limit = CONF["limit_per_source"]
    wanted_raw = [s.strip() for s in (sources or "").split(",") if s.strip()]
    wanted = [normalize_source(s) for s in wanted_raw]
    wanted = [s for s in wanted if s in _CHARTERS] or [s for s in CONF["sources"] if s in _CHARTERS]

    client = get_http(app)
    capabilities = source_capabilities()
    items: list[dict] = []
    errors: dict[str, str] = {}
    for src in dict.fromkeys(wanted):
        if not capabilities.get(src, {}).get("playback_available"):
            errors[src] = capabilities.get(src, {}).get("reason") or "playback unavailable"
            continue
        try:
            chunk = await _CHARTERS[src](client, limit)
        except Exception as e:  # noqa: BLE001
            errors[src] = str(e) or type(e).__name__
            continue
        items.extend(chunk)
        if len(items) >= limit:
            break
    _STATS["errors"] += len(errors)
    return {"ok": True, "items": items[:limit], "errors": errors, "charts": sorted(_CHARTERS)}


@app.get("/api/v1/track/url")
async def track_url(
    id: str = Query("", alias="id"),
    guid: str = Query("", alias="guid"),
    quality: str = Query("lossless"),
    fresh: int = Query(0),
    x_fnmusic_bg: str = Header(default=""),
):
    track_id = (id or guid or "").strip()
    src, identifier = parse_track_id(track_id)
    if not src or not identifier:
        return _err(f"invalid track id: {track_id}", 400)
    _STATS["url_resolutions"] += 1
    # 代理会区分「用户正在等这一首」与「预取/预热」：后者排到播放之后、
    # 队列积压时先甩它。不分开的话预取能把播放挤成队列满。
    background = (x_fnmusic_bg or "").strip().lower() in ("1", "true", "yes")
    _UPSTREAM_BACKGROUND.set(background)
    canonical_id = f"lx:{src}:{identifier}"
    cached = _cache_get(canonical_id) or {"id": canonical_id, "lx_source": src}
    client = get_http(app)
    # SingleFlight：同首歌并发解析合并为一次，手机三连击不再 triple 解析。
    # fresh=1 与常规解析分开合流：proxy 判死链后的强制重取不能被正在进行的
    # 常规解析 future 顶掉（两者要的东西不同）。
    sf_key = f"{canonical_id}:{quality}:{int(bool(fresh))}:{int(background)}"
    fut = _URL_INFLIGHT.get(sf_key)
    if fut is None:
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        _URL_INFLIGHT[sf_key] = fut
        try:
            result = await resolve_and_probe(
                client, src, cached, quality, budget=CONF["url_timeout"], fresh=bool(fresh)
            )
            if not fut.done():
                fut.set_result(result)
        except Exception as e:  # noqa: BLE001
            if not fut.done():
                fut.set_exception(e)
            _STATS["errors"] += 1
            if isinstance(e, UpstreamBusy):
                # 排队没排上：回 503 让代理立刻收手，不再逐档连打撞同一个队列
                logger.info("lx url resolve %s queued out: %s", track_id, e)
                return _err(f"resolve failed: {e}", 503)
            logger.warning("lx url resolve %s failed: %s", track_id, e)
            return _err(f"resolve failed: {e}", 502)
        finally:
            _URL_INFLIGHT.pop(sf_key, None)
    else:
        try:
            result = await asyncio.shield(fut)
        except Exception as e:  # noqa: BLE001
            _STATS["errors"] += 1
            if isinstance(e, UpstreamBusy):
                return _err(f"resolve failed: {e}", 503)
            logger.warning("lx url resolve %s failed: %s", track_id, e)
            return _err(f"resolve failed: {e}", 502)

    if not result:
        return _err(source_capabilities()[src]["reason"] or "no playable url", 404)
    data = {"id": track_id, "quality": quality, **{k: v for k, v in result.items() if k != "probed"}}
    # 回传与探活一致的请求头，调用方不必再猜平台防盗链规则。
    media_headers = {"User-Agent": UA_PC}
    referer = MEDIA_REFERERS.get(src)
    if referer:
        media_headers["Referer"] = referer
    data["headers"] = media_headers
    return {"ok": True, "data": data}


@app.get("/api/v1/track/info")
async def track_info(id: str = Query("", alias="id"), guid: str = Query("", alias="guid")):
    track_id = (id or guid or "").strip()
    src, identifier = parse_track_id(track_id)
    if not src:
        return _err(f"invalid track id: {track_id}", 400)
    cached = _cache_get(f"lx:{src}:{identifier}")
    if cached:
        return {"ok": True, "data": cached}
    return {"ok": True, "data": {"id": track_id, "source": "lx", "lx_source": src, "title": "", "artist": "", "album": "", "duration_s": 0, "ext": "mp3", "file_size": 0, "cover_url": "", "lyric": ""}}


async def _resolve_lyric_for(client: httpx.AsyncClient, src: str, identifier: str, item: dict) -> str:
    if src == "kg":
        return await kg_resolve_lyric(client, item or {"hash": identifier, "title": "", "artist": "", "duration_s": 0})
    if src == "wy":
        return await wy_resolve_lyric(client, identifier)
    if src == "mg":
        return await mg_resolve_lyric(client, item)
    if src == "tx":
        return await tx_resolve_lyric(client, identifier)
    if src == "kw":
        return await kw_resolve_lyric(client, item or {}, identifier)
    return ""


@app.get("/api/v1/track/songinfo")
async def track_songinfo(id: str = Query("", alias="id"), guid: str = Query("", alias="guid")):
    """按平台 ID 直查封面+歌词。

    目前只有酷我有这种一次性接口；代理封面兜底链改走这里之后，同一首歌的
    封面与歌词共用一次官方调用（此前两边各打一次，等于白烧一倍额度）。
    """
    track_id = (id or guid or "").strip()
    src, identifier = parse_track_id(track_id)
    if not src or not identifier:
        return _err(f"invalid track id: {track_id}", 400)
    data = {"id": track_id, "source": src, "pic": "", "lyric": "",
            "title": "", "artist": "", "album": ""}
    if src == "kw":
        info = await _kw_songinfo_fetch(get_http(app), identifier)
        data.update(info)
    return {"ok": True, "data": data}


@app.get("/api/v1/track/lyric")
async def track_lyric(
    id: str = Query("", alias="id"),
    guid: str = Query("", alias="guid"),
    title: str = Query("", alias="title"),
    artist: str = Query("", alias="artist"),
    fast: int = Query(0),
):
    track_id = (id or guid or "").strip()
    src, identifier = parse_track_id(track_id)
    if not src:
        return _err(f"invalid track id: {track_id}", 400)
    cached = _cache_get(f"lx:{src}:{identifier}") or {}
    client = get_http(app)
    text = ""
    try:
        text = await _resolve_lyric_for(client, src, identifier, cached)
    except Exception as e:  # noqa: BLE001
        logger.warning("lx lyric %s failed: %s", track_id, e)
    # fast=1：只做播放平台直连解析（单次请求，亚秒级），不触发跨平台搜索。
    # 代理层同步路径（4s 预算）用它；跨平台回退由代理后台回补任务以全模式调用。
    if not text and (title or artist) and not fast:
        keyword = f"{title} {artist}".strip()
        candidates: list[dict] = []
        tasks = [
            asyncio.create_task(_SEARCHERS[name](client, keyword, 5))
            for name in ("kg", "wy", "tx", "kw", "mg")
            if name in _SEARCHERS
        ]
        try:
            results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=8.0)
        except Exception:
            results = []
        for result in results:
            if isinstance(result, list):
                candidates.extend([x for x in result if isinstance(x, dict)])
        for candidate in candidates[:8]:
            c_src, c_identifier = parse_track_id(str(candidate.get("id") or ""))
            if not c_src or not c_identifier:
                continue
            if c_src == src and c_identifier == identifier:
                continue
            try:
                text = await _resolve_lyric_for(client, c_src, c_identifier, candidate)
            except Exception:
                text = ""
            if text:
                break
    return {"ok": True, "data": {"id": track_id, "lyric": text or ""}}


# ------------------------------------------------------------ 用户源管理端点 ---

class SourceBody(BaseModel):
    url: str = ""
    script: str = ""  # 上传场景：脚本文本（与 url 二选一；url 可为 file:// 上传地址）


@app.get("/api/v1/source")
async def source_info():
    await SOURCE_POOL.wait_loaded()
    return {
        "ok": True,
        "data": SOURCE_MANAGER.describe(),
        "pool": SOURCE_POOL.describe_all(),
        "circuit": chain_health_snapshot(),
        "upstream_gate": LX_UPSTREAM_GATE.snapshot(),
        "upstream_quota": _quota_snapshot(),
    }


class PoolSourceBody(BaseModel):
    url: str = ""
    script: str = ""
    name: str = ""


class PoolToggleBody(BaseModel):
    enabled: bool = True


class PoolReorderBody(BaseModel):
    ids: list[str] = []


class PoolAutoUpdateBody(BaseModel):
    auto_update: bool = True


PRIMARY_SOURCE_ID = "__primary__"


def _primary_source_row() -> dict:
    """主源（洛雪自定义源）→ 与源池同形状的条目，供管理页合并成一个列表。

    主源不可删除/不可停用（安装时即激活），更新走 /api/v1/source/update，
    所以行内动作与附加源不同，用 primary 标记区分。
    """
    desc = SOURCE_MANAGER.describe() or {}
    meta = desc.get("meta") or {}
    source = desc.get("source") or {}
    return {
        "id": PRIMARY_SOURCE_ID,
        "primary": True,
        "name": str(source.get("name") or meta.get("name") or "") or "洛雪自定义源",
        "url": str(desc.get("url") or ""),
        "enabled": True,
        "auto_update": False,
        "position": -1,
        "script_hash": str(desc.get("script_hash") or ""),
        "running": bool(source.get("running")),
        "last_error": str(desc.get("last_error") or ""),
        "update_alert": None,
        "configured": bool(desc.get("configured")),
        "meta": meta,
        "source": source,
    }


def _merged_source_rows() -> list[dict]:
    """主源 + 附加源合并视图（每源一行）。同一脚本正文只留一行。"""
    rows: list[dict] = []
    primary = _primary_source_row()
    rows.append(primary)
    primary_hash = primary.get("script_hash") or ""
    for item in SOURCE_POOL.describe_all():
        if primary_hash and str(item.get("script_hash") or "") == primary_hash:
            continue
        rows.append(item)
    return rows


@app.get("/api/v1/sources")
async def pool_sources():
    await SOURCE_POOL.wait_loaded()
    return {"ok": True, "data": {"items": _merged_source_rows()}}


@app.post("/api/v1/sources")
async def pool_add(body: PoolSourceBody):
    await SOURCE_POOL.wait_loaded()
    url = (body.url or "").strip()
    if body.script:
        url = url or f"file:///uploaded/{int(time.time())}.js"
        script = body.script
    else:
        if not is_source_url(url):
            return _err("url 必须以 http:// 、https:// 或 file:// 开头", 400)
        script = None
    try:
        entry = await SOURCE_POOL.add(url, script=script, name=body.name)
    except Exception as exc:  # noqa: BLE001
        return _err(f"添加源失败: {exc}", 400)
    return {"ok": True, "data": entry}


@app.delete("/api/v1/sources/{source_id}")
async def pool_remove(source_id: str):
    ok = await SOURCE_POOL.remove(source_id)
    if not ok:
        return _err("源不存在", 404)
    return {"ok": True, "data": {"id": source_id}}


@app.post("/api/v1/sources/{source_id}/enabled")
async def pool_toggle(source_id: str, body: PoolToggleBody):
    entry = await SOURCE_POOL.set_enabled(source_id, body.enabled)
    if entry is None:
        return _err("源不存在", 404)
    return {"ok": True, "data": entry}


@app.post("/api/v1/sources/{source_id}/check-update")
async def pool_check_update(source_id: str):
    result = await SOURCE_POOL.check_update(source_id)
    if result is None:
        return _err("源不存在", 404)
    return {"ok": True, "data": result}


@app.post("/api/v1/sources/{source_id}/update")
async def pool_update(source_id: str):
    result = await SOURCE_POOL.update(source_id)
    if result is None:
        return _err("源不存在", 404)
    return {"ok": True, "data": result}


@app.post("/api/v1/sources/{source_id}/auto-update")
async def pool_auto_update(source_id: str, body: PoolAutoUpdateBody):
    entry = await SOURCE_POOL.set_auto_update(source_id, body.auto_update)
    if entry is None:
        return _err("源不存在", 404)
    return {"ok": True, "data": entry}


@app.post("/api/v1/sources/reorder")
async def pool_reorder(body: PoolReorderBody):
    await SOURCE_POOL.reorder([str(x) for x in body.ids if x])
    return {"ok": True, "data": {"items": SOURCE_POOL.describe_all()}}


@app.post("/api/v1/source/check-update")
async def main_source_check_update():
    return {"ok": True, "data": await SOURCE_MANAGER.check_update()}


@app.post("/api/v1/source/update")
async def main_source_update():
    return {"ok": True, "data": await SOURCE_MANAGER.update()}


@app.post("/api/v1/source/verify")
async def source_verify(body: SourceBody):
    """端到端试运行一个源 URL（不改变当前激活源）。"""
    from verify_source import verify_url  # 延迟导入：verify_source 反向 import 本模块

    url = (body.url or "").strip()
    if not is_source_url(url):
        return _err("url 必须以 http:// 、https:// 或 file:// 开头", 400)
    try:
        report = await asyncio.wait_for(verify_url(url), timeout=120.0)
    except asyncio.TimeoutError:
        return _err("校验超时（120s）", 504)
    except SourceError as exc:
        return JSONResponse(content={"ok": False, "data": {"category": exc.category, "message": str(exc)}})
    except Exception as exc:  # noqa: BLE001  校验失败必须以结构化报告返回，绝不让 WebUI 收到裸 500 文本
        return JSONResponse(content={"ok": False, "data": {"category": "internal", "message": f"校验过程出现内部错误: {exc}"}})
    return {"ok": bool(report.get("ok")), "data": report}


class UploadBody(BaseModel):
    filename: str
    script: str


@app.post("/api/v1/source/upload")
async def source_upload(body: UploadBody):
    """落盘一段上传的脚本文本（不激活）：校验头部元数据后写入 uploads/，返回 file:// URL。"""
    try:
        meta = parse_script_meta(body.script)
    except SourceError as exc:
        return JSONResponse(
            content={"ok": False, "error": str(exc), "category": exc.category}, status_code=400
        )
    try:
        path, url = save_upload(SOURCE_MANAGER.state_dir, body.filename, body.script)
    except SourceError as exc:
        return JSONResponse(
            content={"ok": False, "error": str(exc), "category": exc.category}, status_code=400
        )
    except OSError as exc:
        return JSONResponse(
            content={"ok": False, "error": f"写入上传文件失败: {exc}", "category": "download"}, status_code=500
        )
    return {"ok": True, "data": {"path": path, "url": url, "meta": meta}}


@app.post("/api/v1/source")
async def source_set(body: SourceBody):
    """校验并切换当前激活源（state.json 持久化，热生效）。"""
    url = (body.url or "").strip()
    if not url and body.script:
        # 直接以脚本文本激活（先落盘为上传文件，再走统一 file:// 链路）
        try:
            meta = parse_script_meta(body.script)
        except SourceError as exc:
            return JSONResponse(
                content={"ok": False, "error": str(exc), "category": exc.category}, status_code=400
            )
        try:
            _path, url = save_upload(SOURCE_MANAGER.state_dir, "source.js", body.script)
        except (SourceError, OSError) as exc:
            return JSONResponse(
                content={"ok": False, "error": f"保存上传脚本失败: {exc}", "category": "download"}, status_code=500
            )
    if not is_source_url(url):
        return _err("url 必须以 http:// 、https:// 或 file:// 开头", 400)
    try:
        await SOURCE_MANAGER.activate(url)
    except SourceError as exc:
        return JSONResponse(
            content={"ok": False, "error": str(exc), "category": exc.category}, status_code=400
        )
    # 旧源攒下的熔断状态（连续解析失败/open）不应连带拦截新源：换源即清零健康度
    _CHAIN_HEALTH.pop("user_source", None)
    return {"ok": True, "data": SOURCE_MANAGER.describe()}


@app.delete("/api/v1/source")
async def source_clear():
    """停用当前源并清除持久化状态。"""
    await SOURCE_MANAGER.shutdown()
    SOURCE_MANAGER.active_url = ""
    SOURCE_MANAGER.last_error = ""
    for path in (SOURCE_MANAGER.state_path, SOURCE_MANAGER.script_cache):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
    return {"ok": True, "data": SOURCE_MANAGER.describe()}
