"""fnmusic-ext 拦截代理 (FastAPI + httpx).

功能：
1. 通用透传：所有非拦截路径原样转发到 trim-music unix socket
2. 搜索合并：GET /music/api/v1/search/track* （兼容 q/keyword，并行 musicdl）
3. 在线播放：stream + HLS 兜底 + transcode 空操作 + tee 缓存回放（音频与歌词 sidecar）
4. 在线元数据/歌词/封面
5. GET /_ext/healthz
"""
from __future__ import annotations

import asyncio
import hashlib
import math
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
import unicodedata
from contextlib import asynccontextmanager
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Callable, Coroutine
from urllib.parse import quote, urlparse
from uuid import uuid4

import httpx
import anyio
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

try:
    from . import recommend as dailyrec
    from . import nmplaylists as nmpl
    from . import cerusync as ceru
    from . import playlist_import as plimp
    from . import transcode as tc
    from .cache_gc import purge_rolling, sweep_orphan_lyrics
    from .env_merge import parse_env_file
    from .version import get_version
except ImportError:  # uvicorn --app-dir proxy
    import recommend as dailyrec  # type: ignore
    import nmplaylists as nmpl  # type: ignore
    import cerusync as ceru  # type: ignore
    import playlist_import as plimp  # type: ignore
    import transcode as tc  # type: ignore
    from cache_gc import purge_rolling, sweep_orphan_lyrics  # type: ignore
    from env_merge import parse_env_file  # type: ignore
    from version import get_version  # type: ignore

logger = logging.getLogger("fnmusic_proxy")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

_HOME = dailyrec.home_dir()

# lx 平台别名 → 规范代码（与 lxmusic-service/app.py 的 _SOURCE_ALIASES 保持一致）
_LX_ALIASES = {
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


def _normalize_lx_sources(raw: str) -> list[str]:
    """LX_SOURCES 环境变量 → 规范平台代码列表；空 = 不限制（跟随 lx 服务配置）。"""
    out: list[str] = []
    for part in (raw or "").split(","):
        code = _LX_ALIASES.get(part.strip().lower(), "")
        if code and code not in out:
            out.append(code)
    return out


CONF = {
    "musicdl_url": os.environ.get("FNMUSIC_MUSICDL_URL", "http://127.0.0.1:8768"),
    "musicbox_url": os.environ.get("FNMUSIC_MUSICBOX_URL", "http://127.0.0.1:8770"),
    "lx_url": os.environ.get("FNMUSIC_LX_URL", "http://127.0.0.1:8772"),
    "musicdl_enabled": os.environ.get("FNMUSIC_MUSICDL_ENABLED", "false").lower() in ("true", "1", "yes"),
    "netease_enabled": os.environ.get("FNMUSIC_NETEASE_ENABLED", "false").lower() in ("true", "1", "yes"),
    "lx_enabled": os.environ.get("FNMUSIC_LX_ENABLED", "false").lower() in ("true", "1", "yes"),
    # 外网直连CDN（2.6.26 方案B）：在线歌解析出 CDN 直链后，先用“裸请求”（模拟手机、
    # 不带 Referer/UA）验证 CDN 是否接受直连；通过则 307 重定向让手机直连 CDN 取流
    # （绕过 NAS 中转，接近洛雪客户端直连体验）；验证失败或源不支持（仅 lx/netease）
    # 时自动回落现有中转路径，起播率不劣于旧路径。默认开；管理页「播放与推荐」可关，立即生效。
    "wan_direct_enabled": os.environ.get("FNMUSIC_WAN_DIRECT", "true").lower() in ("true", "1", "yes"),
    "lx_search_limit": int(os.environ.get("FNMUSIC_LX_SEARCH_LIMIT", "20")),
    "lx_quality": os.environ.get("FNMUSIC_LX_QUALITY", "lossless"),
    "netease_wait_s": float(os.environ.get("FNMUSIC_NETEASE_WAIT_S", "3.0")),
    # 输入停满这么久才向音源发起搜索。窗口内的新词会替换旧词并重新计时。
    "search_debounce_s": float(os.environ.get("FNMUSIC_SEARCH_DEBOUNCE_S", "0.3")),
    "netease_quality": os.environ.get("FNMUSIC_NETEASE_QUALITY", "lossless"),
    "netease_search_limit": int(os.environ.get("FNMUSIC_NETEASE_SEARCH_LIMIT", "50")),
    "upstream_sock": os.environ.get("FNMUSIC_UPSTREAM_SOCK", "/var/run/trim_music_upstream.socket"),
    "online_limit": int(os.environ.get("FNMUSIC_ONLINE_LIMIT", "30")),
    "search_list_path": os.environ.get("FNMUSIC_SEARCH_LIST_PATH", "data.list"),
    "cache_dir": os.environ.get("FNMUSIC_CACHE_DIR", os.path.join(_HOME, "cache")),
    # 空=从飞牛 shared_library.path 自动探测；测试可覆盖到临时目录
    "library_dir": os.environ.get("FNMUSIC_LIBRARY_DIR", ""),
    "music_db": os.environ.get(
        "FNMUSIC_MUSIC_DB", "/usr/local/apps/@appdata/trim.music/db/music.db"
    ),
    # 边听边存：默认开；保存路径空=自动探测飞牛共享曲库，不可用自动回退；
    # tee_cache_max 仅在关闭边听边存时生效（滚动保留最新 N 首试听缓存）
    "tee_save_enabled": os.environ.get("FNMUSIC_TEE_SAVE_ENABLED", "true").lower() in ("true", "1", "yes"),
    "tee_save_dir": os.environ.get("FNMUSIC_TEE_SAVE_DIR", ""),
    "tee_cache_max": int(os.environ.get("FNMUSIC_TEE_CACHE_MAX", "2")),
    # 收藏/加入歌单自动绑定本地：默认关；在线歌曲收藏后后台自动整轨下载并绑定本地文件，
    # 官方曲库收录后再把收藏/歌单写进官方（成功后删本地映射，避免双列表重复显示）
    "fav_auto_bind": os.environ.get("FNMUSIC_FAV_AUTO_BIND", "false").lower() in ("true", "1", "yes"),
    # 官方绑定轮询窗口（秒）：下载落库后等待官方扫入库并完成官方写入的时间上限
    "official_bind_timeout_s": float(os.environ.get("FNMUSIC_OFFICIAL_BIND_TIMEOUT_S", "120")),
    # 边听边存切歌续传：允许并行完成的续传任务上限（防快速跳歌时的下载洪泛；0=关闭续传）
    "tee_handoff_max": int(os.environ.get("FNMUSIC_TEE_HANDOFF_MAX", "3")),
    # 自动下载封面：自动下载的音乐完整落库后，把源站封面内嵌进音频文件。
    # 飞牛扫描器只认内嵌图（dhowden/tag，无外置 cover.jpg 能力），这是官方
    # App 显示下载歌曲封面的唯一途径；封面在音乐落库成功后才处理，不存在
    # 音乐失败、封面先落污染目录的情况
    "auto_cover": os.environ.get("FNMUSIC_AUTO_COVER", "true").lower() in ("true", "1", "yes"),
    # 自动下载歌词（2.6.0）：默认关；仅边听边存开启时生效——音乐完整落库
    # 成功后才下载同名 .lrc 到歌曲所在目录（飞牛扫描入库即带歌词），音乐
    # 下载失败不产生任何歌词文件，不存在歌词先落污染目录的情况
    "lyric_auto_dl": os.environ.get("FNMUSIC_LYRIC_AUTO_DL", "false").lower() in ("true", "1", "yes"),
    # 在线取流 Range 探针：记录每条在线 /track/stream 的 Range 形态与落盘资格，
    # 用于真机确认手机播放器是否按定长窗口取流（那样边听边存永不触发）
    "stream_probe": os.environ.get("FNMUSIC_STREAM_PROBE", "true").lower() in ("true", "1", "yes"),
    # 标准音质转码（App 音质偏好=标准时走 HLS 转码而非直拉原文件）：
    # ffmpeg 输出 AAC fMP4 分片；宿主机无 ffmpeg 自动回落单分片桩
    "transcode_enabled": os.environ.get("FNMUSIC_TRANSCODE_ENABLED", "true").lower() in ("true", "1", "yes"),
    "transcode_bitrate": os.environ.get("FNMUSIC_TRANSCODE_BITRATE", "128k"),
    # 原版实测值：标准音质模式下手机端用 10s/片的 fMP4 能播（CHANGELOG）。
    "transcode_hls_time": float(os.environ.get("FNMUSIC_TRANSCODE_HLS_TIME", "10")),
    "transcode_max_sessions": int(os.environ.get("FNMUSIC_TRANSCODE_MAX_SESSIONS", "2")),
    "transcode_ttl_s": float(os.environ.get("FNMUSIC_TRANSCODE_TTL_S", "90")),
    "transcode_cache_max_mb": int(os.environ.get("FNMUSIC_TRANSCODE_CACHE_MAX_MB", "512")),
    # 下载"标准"档目标码率：官方实测 MP3 320k（prepare quality=standard, FLAC 源 → 320067bps）
    "transcode_dl_bitrate": os.environ.get("FNMUSIC_TRANSCODE_DL_BITRATE", "320k"),
    # 落盘进曲库后通知官方重扫的接口路径（POST）；空=禁用。官方无公开文档，
    # 真机在官方 App 手动点一次扫描、从代理请求日志捕获真实路径后填入启用
    "library_scan_path": (os.environ.get("FNMUSIC_LIBRARY_SCAN_PATH", "") or "").strip(),
    "merge_suggest": os.environ.get("FNMUSIC_MERGE_SUGGEST", "false").lower() in ("true", "1", "yes"),
    "online_sources": os.environ.get("FNMUSIC_ONLINE_SOURCES", "KuwoMusicClient,MiguMusicClient"),
    # lx 平台白名单（同 .env 的 LX_SOURCES；install.sh --sources lx-<平台> 写入）；
    # 空 = 不限制。GUID 第 3 段携带平台（online:lx:kg:xxx），据此过滤与透传 ?sources=
    "lx_sources": _normalize_lx_sources(os.environ.get("LX_SOURCES", "")),
    "lyric_field": os.environ.get("FNMUSIC_LYRIC_FIELD", "data.lyric"),
    "search_timeout": float(os.environ.get("FNMUSIC_SEARCH_TIMEOUT", "15")),
    "search_probe": os.environ.get("FNMUSIC_SEARCH_PROBE", "false").lower() in ("true", "1", "yes"),
    "search_cache_ttl": float(os.environ.get("FNMUSIC_SEARCH_CACHE_TTL", "604800")),
    "late_page_wait_s": float(os.environ.get("FNMUSIC_LATE_PAGE_WAIT_S", "5.0")),
    "fav_dir": os.environ.get(
        "FNMUSIC_FAV_DIR", os.path.join(_HOME, "online_favorites")
    ),
    # 官方歌单内在线附加条目的存储目录（同 fav_dir 按用户分文件）
    "plt_dir": os.environ.get(
        "FNMUSIC_PLT_DIR", os.path.join(_HOME, "playlist_tracks")
    ),
    # 导入歌单（洛雪 JSON / 五平台分享链接）的落盘目录，一单一文件
    "import_dir": os.environ.get(
        "FNMUSIC_IMPORT_PLAYLISTS_DIR", os.path.join(_HOME, "imported_playlists")
    ),
    "llm_base_url": (os.environ.get("FNMUSIC_LLM_BASE_URL") or "").strip().rstrip("/"),
    "llm_model": (os.environ.get("FNMUSIC_LLM_MODEL") or "gpt-4o-mini").strip() or "gpt-4o-mini",
    # v2.0.0：音质模式 high|balanced|smooth（档序见 quality_order）；
    # 推荐双开关 / 封面补全 / .env 热重载默认开，均可被 .env 覆盖
    "quality_mode": (os.environ.get("FNMUSIC_QUALITY_MODE") or "high").strip().lower(),
    "recommend_hot": os.environ.get("FNMUSIC_RECOMMEND_HOT", "true").lower() in ("true", "1", "yes"),
    "recommend_daily": os.environ.get("FNMUSIC_RECOMMEND_DAILY", "true").lower() in ("true", "1", "yes"),
    # 网易账号歌单注入（2.6.0）：默认关；需网易盒子启用并扫码登录，
    # 音乐页在热门推荐与官方歌单之间展示账号自建歌单（只读，不回写网易）
    "netease_my_playlists": os.environ.get("FNMUSIC_NETEASE_MY_PLAYLISTS", "false").lower() in ("true", "1", "yes"),
    "cover_enrich": os.environ.get("FNMUSIC_COVER_ENRICH", "true").lower() in ("true", "1", "yes"),
    # Ceru 自部署同步服务：飞牛音乐作为一台设备接入，拉取歌单/收藏并双向回写。
    # 默认关；URL + 配对码/账号密码齐备后由代理在音乐页注入 Ceru 歌单。
    "ceru_sync_enabled": os.environ.get("FNMUSIC_CERU_SYNC_ENABLED", "false").lower() in ("true", "1", "yes"),
    "ceru_sync_host": (os.environ.get("FNMUSIC_CERU_SYNC_HOST") or "").strip(),
    "ceru_sync_port": int(os.environ.get("FNMUSIC_CERU_SYNC_PORT") or "31231"),
    "ceru_sync_use_https": os.environ.get("FNMUSIC_CERU_SYNC_USE_HTTPS", "false").lower() in ("true", "1", "yes"),
    "env_watch": os.environ.get("FNMUSIC_ENV_WATCH", "true").lower() in ("true", "1", "yes"),
    # 官方端点取证：未拦截的 /music/api 请求首见 INFO、之后每 50 次采样一条；
    # =detail 时逐条记录。官方 App 更新引入新端点时（如 2.5.0 前的 download/*），
    # 日志可直接看到客户端在调什么，避免"Unknown 元数据"类问题无迹可循
    "trace_forward": os.environ.get("FNMUSIC_TRACE_FORWARD", "").strip().lower() in ("detail", "1", "true", "yes"),
}

# 导入歌单存储目录由代理统一持有（webui 侧通过 HTTP 接口读写，不共享文件系统）
plimp.set_dir(CONF["import_dir"])

_REDACT_KEY_PARTS = ("api_key", "apikey", "token", "secret", "password")

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

CACHE_EXTS = ("mp3", "flac", "wav", "ogg", "opus", "m4a", "aac", "ape", "wv", "dsf", "dff", "tta")

# 飞牛 Kl() 归一化：mpeg/mp3→mp3，wav/pcm→wav，m4a/aac/mp4→m4a，其余小写原样（flac/ogg/ape/wv…）
_FORMAT_ALIASES = {
    "mp3": "mp3",
    "mpeg": "mp3",
    "mpga": "mp3",
    "flac": "flac",
    "wav": "wav",
    "wave": "wav",
    "pcm": "wav",
    "lpcm": "wav",
    "ogg": "ogg",
    "vorbis": "ogg",
    "opus": "opus",
    "m4a": "m4a",
    "mp4": "m4a",
    "mp4a": "m4a",
    "aac": "m4a",
    "alac": "m4a",
    "ape": "ape",
    "wv": "wv",
    "wavpack": "wv",
    "dsf": "dsf",
    "dff": "dff",
    "dsd": "dsd",
    "tta": "tta",
    "tak": "tak",
    "wma": "wma",
    "aiff": "aiff",
    "aif": "aiff",
}


# 模块级搜索缓存
_SEARCH_CACHE: dict[str, dict] = {}
# 每个登录用户当前要搜的词。换词时作废这个用户其余关键词的缓存，不刷新 ts。
_USER_SEARCH_GEN: dict[str, int] = {}
_USER_SEARCH_WORD: dict[str, str] = {}
# 每个用户正在等待的输入窗口。新词到达时 set，让上一词立刻结束等待。
_SEARCH_DEBOUNCE: dict[str, asyncio.Event] = {}
# fetch_* 读这个范围：搜索框用凭证哈希，补链加 ":play"，联想加 ":suggest"。
_FETCH_SCOPE: ContextVar[str] = ContextVar("fnmusic_fetch_scope", default="")
_SCOPE_HEADER = "X-Fnmusic-Scope"


class _SupersededSearch:
    """同一用户的更新关键词替换了这次搜索。"""


_SUPERSEDED = _SupersededSearch()


class SourceSearchGate:
    """一个音源同时只跑一个关键词。每个用户在队列里只留最新词。

    cancellable 时，同一用户的新词取消正在跑的搜索并立刻开始。
    否则等当前请求自然结束，丢掉响应，再搜新词。其他用户按到达顺序排在后面。
    """

    def __init__(self, cancellable: bool):
        self.cancellable = cancellable
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
            _resolve_waiters(slot, _SUPERSEDED)
        if job:
            _resolve_waiters(job, _SUPERSEDED)

    async def run(self, scope: str, keyword: str, factory):
        fut = asyncio.get_running_loop().create_future()
        self._admit(scope, keyword, factory, fut)
        self._pump()
        try:
            return await asyncio.shield(fut)
        except asyncio.CancelledError:
            self._detach(fut)
            raise

    def _admit(self, scope: str, keyword: str, factory, fut) -> None:
        running = self._running
        if running and running["scope"] == scope and running["keyword"] == keyword and not running.get("superseded"):
            running["waiters"].append(fut)
            return
        if running and running["scope"] == scope and running["keyword"] != keyword:
            running["superseded"] = True
            _resolve_waiters(running, _SUPERSEDED)
            if self.cancellable:
                task = running.get("task")
                if task and not task.done():
                    task.cancel()
            self._upsert(scope, keyword, factory, fut, front=True)
            return
        self._upsert(scope, keyword, factory, fut, front=False)

    def _upsert(self, scope: str, keyword: str, factory, fut, front: bool) -> None:
        for slot in self._queue:
            if slot["scope"] != scope:
                continue
            if slot["keyword"] != keyword:
                _resolve_waiters(slot, _SUPERSEDED)
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
        if front:
            self._queue.insert(0, slot)
        else:
            self._queue.append(slot)

    def _detach(self, fut) -> None:
        running = self._running
        if running and fut in running["waiters"]:
            running["waiters"].remove(fut)
            if self.cancellable and not running["waiters"]:
                running["superseded"] = True
                task = running.get("task")
                if task and not task.done():
                    task.cancel()
            return
        for slot in list(self._queue):
            if fut in slot["waiters"]:
                slot["waiters"].remove(fut)
                if not slot["waiters"]:
                    self._queue.remove(slot)
                return

    def _pump(self) -> None:
        if self._running is not None or not self._queue:
            return
        slot = self._queue.pop(0)
        job = {
            "scope": slot["scope"],
            "keyword": slot["keyword"],
            "factory": slot["factory"],
            "waiters": slot["waiters"],
            "superseded": False,
            "task": None,
        }
        job["epoch"] = self._epoch
        self._running = job
        job["task"] = asyncio.get_running_loop().create_task(self._execute(job))

    async def _execute(self, job: dict) -> None:
        result = _SUPERSEDED
        try:
            result = await job["factory"]()
        except asyncio.CancelledError:
            result = _SUPERSEDED
        except Exception as exc:
            result = exc
        # 取消之后不能再 await，否则事件循环会把清理和下一词一起丢掉。
        self._finish(job, result)

    def _finish(self, job: dict, result) -> None:
        if job.get("epoch") != self._epoch:
            _resolve_waiters(job, _SUPERSEDED)
            return
        if self._running is job:
            self._running = None
        if job.get("superseded") or result is _SUPERSEDED:
            _resolve_waiters(job, _SUPERSEDED)
        elif isinstance(result, Exception):
            _resolve_waiters(job, result, error=True)
        else:
            _resolve_waiters(job, result)
        self._pump()


def _resolve_waiters(job: dict, result, error: bool = False) -> None:
    for fut in job.get("waiters", []):
        if fut.done():
            continue
        if error:
            fut.set_exception(result)
        else:
            fut.set_result(result)


def _note_user_keyword(scope: str, keyword: str) -> int:
    """记下这个用户正在搜的词。换词时清掉该用户其他词的缓存，ts 归零而不是续期。"""
    if _USER_SEARCH_WORD.get(scope) != keyword:
        _USER_SEARCH_GEN[scope] = _USER_SEARCH_GEN.get(scope, 0) + 1
        _USER_SEARCH_WORD[scope] = keyword
        for entry in _SEARCH_CACHE.values():
            if entry.get("credentials") == scope and entry.get("keyword") != keyword:
                entry["superseded"] = True
                entry["items"] = []
                entry["ts"] = 0
        pending = _SEARCH_DEBOUNCE.get(scope)
        if pending is not None:
            pending.set()
    return _USER_SEARCH_GEN.get(scope, 1)


async def _wait_search_debounce(scope: str) -> asyncio.Event | None:
    """停手满 search_debounce_s 才返回当前窗口。0 表示不延迟。"""
    delay = float(CONF.get("search_debounce_s") or 0)
    if delay <= 0:
        return None
    event = asyncio.Event()
    _SEARCH_DEBOUNCE[scope] = event
    try:
        await asyncio.wait_for(event.wait(), timeout=delay)
    except asyncio.TimeoutError:
        pass
    return event


def _user_search_stale(entry: dict, scope: str) -> bool:
    return bool(entry.get("superseded")) or entry.get("gen") != _USER_SEARCH_GEN.get(scope)


def reset_source_search_gates() -> None:
    for gate in (_LX_SEARCH_GATE, _MUSICDL_SEARCH_GATE, _MUSICBOX_SEARCH_GATE):
        gate.reset()
    _USER_SEARCH_GEN.clear()
    _USER_SEARCH_WORD.clear()
    _SEARCH_DEBOUNCE.clear()


_LX_SEARCH_GATE = SourceSearchGate(cancellable=True)
_MUSICDL_SEARCH_GATE = SourceSearchGate(cancellable=False)
_MUSICBOX_SEARCH_GATE = SourceSearchGate(cancellable=False)


def _search_ttl(entry: dict) -> float:
    # Backend IDs/URLs are memory scoped; positive results revalidate in 5m.
    # 超时交回的空列表也是 partial。先看有没有歌，避免把 0 条缓存半分钟，
    # 用户紧接着再搜同词还是空的。
    if not entry.get("items"):
        return 10.0
    if entry.get("partial"):
        return 30.0
    return min(float(CONF.get("search_cache_ttl", 604800)), 300.0)


def _clean_search_cache() -> None:
    now = time.time()
    expired = [k for k, v in _SEARCH_CACHE.items() if now - v.get("accessed", v.get("ts", 0)) >= 900]
    if len(_SEARCH_CACHE) > 2000:
        expired += sorted(_SEARCH_CACHE, key=lambda k: _SEARCH_CACHE[k].get("accessed", 0))[:1000]
    for key in expired:
        entry = _SEARCH_CACHE.pop(key, {})
        task = entry.get("task")
        if task and not task.done():
            task.cancel()


# TOP[top.search.cache] 搜索防抖与结果缓存（按用户×关键词 TTL，本地优先口径）
def _set_search_cache(keyword: str, entry: dict) -> None:
    _clean_search_cache()
    _SEARCH_CACHE[keyword] = entry


# === .env 热重载（FNMUSIC_ENV_WATCH=1 默认开） ===
# WebUI 切源 / 手工编辑 .env 后无需重启 proxy：白名单键同步进 CONF 与
# os.environ（recommend 的 LLM 配置直接读环境变量），并清空搜索缓存让
# 新选音源立即生效。路径/端口类配置不在白名单，仍需重启。
_ENV_WATCH_KEYS: dict[str, tuple[str, str]] = {
    "FNMUSIC_MUSICDL_ENABLED": ("musicdl_enabled", "bool"),
    "FNMUSIC_NETEASE_ENABLED": ("netease_enabled", "bool"),
    "FNMUSIC_WAN_DIRECT": ("wan_direct_enabled", "bool"),
    "FNMUSIC_LX_ENABLED": ("lx_enabled", "bool"),
    "FNMUSIC_ONLINE_SOURCES": ("online_sources", "str"),
    "LX_SOURCES": ("lx_sources", "lx_sources"),
    "FNMUSIC_SEARCH_PROBE": ("search_probe", "bool"),
    "FNMUSIC_QUALITY_MODE": ("quality_mode", "quality_mode"),
    "FNMUSIC_TEE_SAVE_ENABLED": ("tee_save_enabled", "bool"),
    "FNMUSIC_TEE_SAVE_DIR": ("tee_save_dir", "str"),
    "FNMUSIC_TEE_CACHE_MAX": ("tee_cache_max", "tee_cache_max"),
    "FNMUSIC_FAV_AUTO_BIND": ("fav_auto_bind", "bool"),
    "FNMUSIC_OFFICIAL_BIND_TIMEOUT_S": ("official_bind_timeout_s", "bind_timeout"),
    "FNMUSIC_TEE_HANDOFF_MAX": ("tee_handoff_max", "tee_handoff_max"),
    "FNMUSIC_AUTO_COVER": ("auto_cover", "bool"),
    "FNMUSIC_LYRIC_AUTO_DL": ("lyric_auto_dl", "bool"),
    "FNMUSIC_LIBRARY_SCAN_PATH": ("library_scan_path", "str"),
    "FNMUSIC_RECOMMEND_HOT": ("recommend_hot", "bool"),
    "FNMUSIC_RECOMMEND_DAILY": ("recommend_daily", "bool"),
    "FNMUSIC_NETEASE_MY_PLAYLISTS": ("netease_my_playlists", "bool"),
    "FNMUSIC_COVER_ENRICH": ("cover_enrich", "bool"),
    "FNMUSIC_CERU_SYNC_ENABLED": ("ceru_sync_enabled", "bool"),
    "FNMUSIC_CERU_SYNC_HOST": ("ceru_sync_host", "str"),
    "FNMUSIC_CERU_SYNC_PORT": ("ceru_sync_port", "port"),
    "FNMUSIC_CERU_SYNC_USE_HTTPS": ("ceru_sync_use_https", "bool"),
    "FNMUSIC_CERU_SYNC_PAIR_CODE": ("", "str"),
    "FNMUSIC_CERU_SYNC_TOKEN": ("", "str"),
    "FNMUSIC_TRACE_FORWARD": ("trace_forward", "bool"),
    "FNMUSIC_TRANSCODE_ENABLED": ("transcode_enabled", "bool"),
    "FNMUSIC_TRANSCODE_BITRATE": ("transcode_bitrate", "str"),
    "FNMUSIC_TRANSCODE_HLS_TIME": ("transcode_hls_time", "seconds"),
    "FNMUSIC_TRANSCODE_MAX_SESSIONS": ("transcode_max_sessions", "tee_cache_max"),
    "FNMUSIC_TRANSCODE_TTL_S": ("transcode_ttl_s", "ttl_seconds"),
    "FNMUSIC_TRANSCODE_CACHE_MAX_MB": ("transcode_cache_max_mb", "mb_int"),
    "FNMUSIC_TRANSCODE_DL_BITRATE": ("transcode_dl_bitrate", "str"),
    "FNMUSIC_LLM_BASE_URL": ("llm_base_url", "llm_url"),
    "FNMUSIC_LLM_API_KEY": ("", "str"),
    "FNMUSIC_LLM_MODEL": ("llm_model", "str"),
    "FNMUSIC_SEARCH_TIMEOUT": ("search_timeout", "seconds"),
    # issue #29：推荐构建预算/候选数/逐首校验运行期可调（recommend.py 动态读 os.environ）
    "FNMUSIC_RECOMMEND_BUDGET_S": ("", "str"),
    "FNMUSIC_RECOMMEND_CANDIDATES": ("", "str"),
    "FNMUSIC_LLM_TIMEOUT_S": ("", "str"),
    "FNMUSIC_RECOMMEND_VERIFY_PLAYABLE": ("", "str"),
    "FNMUSIC_RECOMMEND_VERIFY_TIMEOUT_S": ("", "str"),
    "FNMUSIC_REC_SEARCH_CONCURRENCY": ("", "str"),
    "FNMUSIC_REC_SEARCH_INTERVAL": ("", "str"),
}
_ENV_WATCH_INTERVAL_S = 2.0
_ENV_WATCH_DEBOUNCE_S = 0.5


def _env_watch_path() -> str:
    return os.environ.get("FNMUSIC_ENV_FILE") or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", ".env"
    )


def _env_watch_parse(raw: str, kind: str):
    raw = str(raw or "").strip()
    if kind == "bool":
        return raw.lower() in ("true", "1", "yes")
    if kind == "tee_cache_max":
        try:
            return max(1, min(100, int(raw)))
        except (TypeError, ValueError):
            return None
    if kind == "quality_mode":
        return raw.lower() if raw.lower() in ("high", "balanced", "smooth") else None
    if kind == "lx_sources":
        return _normalize_lx_sources(raw)
    if kind == "llm_url":
        return raw.rstrip("/")
    if kind == "seconds":
        try:
            return max(1.0, min(60.0, float(raw)))
        except (TypeError, ValueError):
            return None
    if kind == "bind_timeout":
        try:
            return max(10.0, min(3600.0, float(raw)))
        except (TypeError, ValueError):
            return None
    if kind == "ttl_seconds":
        try:
            return max(10.0, min(3600.0, float(raw)))
        except (TypeError, ValueError):
            return None
    if kind == "mb_int":
        try:
            return max(1, min(102400, int(raw)))
        except (TypeError, ValueError):
            return None
    if kind == "tee_handoff_max":
        try:
            return max(0, min(20, int(raw)))
        except (TypeError, ValueError):
            return None
    if kind == "port":
        try:
            return max(1, min(65535, int(raw)))
        except (TypeError, ValueError):
            return None
    return raw


# TOP[top.env] .env 热重载：2s 轮询，白名单键免重启生效
def apply_env_hot_reload(env_path: "str | None" = None) -> list[str]:
    """解析 .env 应用白名单键；返回发生变化的 CONF 键名（空 = 无变化）。

    环境变量同步写原始字符串（recommend 等模块直接读 os.environ）。
    """
    path = env_path or _env_watch_path()
    kv = dict(parse_env_file(path)[0])
    changed: list[str] = []
    for env_key, (conf_key, kind) in _ENV_WATCH_KEYS.items():
        if env_key not in kv:
            continue
        os.environ[env_key] = str(kv[env_key])
        value = _env_watch_parse(kv[env_key], kind)
        if value is None:
            continue
        if conf_key and CONF.get(conf_key) != value:
            CONF[conf_key] = value
            changed.append(conf_key)
    return changed


def _reset_search_cache() -> None:
    tasks = [
        entry.get("task") for entry in _SEARCH_CACHE.values()
        if entry.get("task") and not entry["task"].done()
    ]
    for task in tasks:
        task.cancel()
    _SEARCH_CACHE.clear()


def _env_watch_stat(path: "str | None" = None):
    try:
        st = os.stat(path or _env_watch_path())
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None


# 音源集合相关的 CONF 键：任一变化意味着推荐缓存里的旧源曲目可能不可播（issue #22）
_SOURCE_CONF_KEYS = {"musicdl_enabled", "netease_enabled", "lx_enabled", "online_sources", "lx_sources"}


def _cancel_daily_tasks() -> None:
    for key in list(_DAILY_TASKS):
        old = _DAILY_TASKS.pop(key, None)
        if old is not None and not old.done():
            old.cancel()


async def _env_watch_loop() -> None:
    last = _env_watch_stat()
    while True:
        try:
            await asyncio.sleep(_ENV_WATCH_INTERVAL_S)
            cur = _env_watch_stat()
            if cur is None or cur == last:
                last = cur
                continue
            await asyncio.sleep(_ENV_WATCH_DEBOUNCE_S)
            settled = _env_watch_stat()
            if settled != cur:
                continue  # 仍在写入，下一轮再看
            last = settled
            changed = apply_env_hot_reload()
            if changed:
                _reset_search_cache()
                if _SOURCE_CONF_KEYS & set(changed):
                    # issue #22：切换音源后当日推荐立即失效，切回歌单按新音源重建，
                    # 旧源曲目不再残留到当日结束
                    removed = dailyrec.invalidate_today_cache_all_users()
                    _cancel_daily_tasks()
                    logger.info(
                        "音源配置变化(%s)：已失效当日推荐缓存 %d 个，推荐歌单将按新音源重建",
                        ",".join(sorted(_SOURCE_CONF_KEYS & set(changed))), removed,
                    )
                logger.info(".env 热重载生效: %s", ",".join(sorted(changed)))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug("env watch loop error: %s", e)


def _source_config() -> dict:
    return {k: v for k, v in CONF.items() if k.endswith(("_enabled", "_url", "_limit", "_quality", "_sources")) or k == "online_sources"}


def _search_scope(request: Request) -> str:
    auth = [request.headers.get(k, "") for k in ("cookie", "authorization", "x-trim-music-temp-token")]
    config = _source_config()
    filters = sorted((k, v) for k, v in request.query_params.multi_items() if k not in ("page", "q", "query", "keyword"))
    return hashlib.sha256(json.dumps([auth, config, filters, request.url.path], sort_keys=True).encode()).hexdigest()


def _source_enabled(guid: str) -> bool:
    source = source_from_online_guid(guid)
    if not CONF.get({"netease": "netease_enabled", "lx": "lx_enabled"}.get(source, "musicdl_enabled")):
        return False
    if source == "lx":
        # lx 平台白名单（online:lx:<platform>:<id>）；未配置 = 跟随 lx 服务全部已启用平台
        selected = CONF.get("lx_sources") or []
        if selected:
            parts = guid.split(":")
            return len(parts) >= 4 and parts[2] in selected
        return True
    if source != "netease" and CONF.get("online_sources"):
        selected = {name.strip().lower().removesuffix("musicclient") for name in str(CONF["online_sources"]).split(",")}
        return source.lower() in selected
    return True


ONLINE_TRIAL_MARKERS = (
    "(试听)",
    "（试听）",
    "试听片段",
    "片段试听",
    "试听版",
    "[试听]",
    "【试听】",
    "- 试听",
    " - 试听",
)


def is_playable_online_track(item: dict, require_id: bool = False, allow_paywall: bool = False) -> bool:
    """最终防线校验：过滤无音频流或试听标记的不可播曲目。"""
    if not isinstance(item, dict):
        return False

    title = str(item.get("title") or item.get("name") or item.get("song_name") or "").strip()
    if not title:
        return False

    if require_id:
        sid = str(item.get("id") or item.get("song_id") or item.get("guid") or "").strip()
        if not sid:
            return False

    # 1. 标题含试听标记
    if any(marker in title for marker in ONLINE_TRIAL_MARKERS):
        return False

    # 2. 字段试听标记
    if item.get("is_trial") is True or item.get("freeTrialInfo") or item.get("freeTrialPrivilege"):
        return False
    if int(item.get("is_free_part") or 0) != 0 or int(item.get("fail_process") or 0) == 4:
        return False

    # 3. 收费/VIP 拦截（verified 条目已由服务端完成"直链解析+Range探活"验证，可播性有实证，跳过收费元数据拦截；
    #    当 allow_paywall=True 时同样跳过收费拦截，用于逐曲探活关闭时放行第三方源候选歌曲）
    if not allow_paywall and item.get("verified") is not True:
        if int(item.get("pay_type") or 0) != 0:
            return False
        if int(item.get("pkg_price") or 0) != 0 or int(item.get("price") or 0) != 0:
            return False
        fee = item.get("fee")
        if fee is not None:
            try:
                if int(fee) not in (0, 8):
                    return False
            except (ValueError, TypeError):
                pass

    # 4. 显式不可播/无流标记
    if item.get("unplayable") is True or item.get("playable") is False:
        return False
    if item.get("has_stream") is False:
        return False

    # 5. 音频流直链校验：若带有 download_url 或 url 键，则必须合法可用，绝不能是空串或 404
    if "download_url" in item:
        d_url = str(item.get("download_url") or "").strip()
        if not d_url or not d_url.startswith(("http://", "https://")) or "404/error.html" in d_url or "error.html" in d_url:
            return False
    if "url" in item:
        u = str(item.get("url") or "").strip()
        if not u or "404/error.html" in u or "error.html" in u:
            return False

    # 6. 片段时长校验（<=35s 且带有试听迹象）
    duration = item.get("duration_s") or (item.get("duration") or 0)
    try:
        duration_s = float(duration)
        if 0 < duration_s <= 35 and ("试听" in title or item.get("is_trial")):
            return False
    except (ValueError, TypeError):
        pass

    return True


def _same_recording(left: dict, right: dict) -> bool:
    """Conservative identity: never strip live/remix/version markers."""
    for key in ("title", "artist", "version"):
        a, b = (str(x.get(key) or "").strip().casefold() for x in (left, right))
        if a != b or (key != "version" and not a):
            return False
    try:
        a, b = float(left.get("duration_s") or 0), float(right.get("duration_s") or 0)
        return math.isfinite(a) and math.isfinite(b) and a > 0 and b > 0 and abs(a - b) <= 2.0
    except (TypeError, ValueError):
        return False


def deduplicate_online_items(items: list[dict]) -> list[dict]:
    """Keep the published representative and strict recording alternatives."""
    result: list[dict] = []
    seen = set()
    for item in items:
        if not is_playable_online_track(item):
            continue
        guid = online_guid_from_item(item)
        if guid in seen:
            continue
        seen.add(guid)
        representative = next((x for x in result if _same_recording(x, item)), None)
        if representative is None:
            representative = dict(item)
            representative["_alternatives"] = list(item.get("_alternatives", []))
            result.append(representative)
        else:
            alternatives = representative.setdefault("_alternatives", [])
            if guid not in {online_guid_from_item(x) for x in alternatives}:
                alternatives.append({k: v for k, v in item.items() if k != "_alternatives"})
    return result


def play_format_from_ext(ext: str | None) -> str:
    raw = (ext or "mp3").strip().lower().lstrip(".")
    if raw.startswith("audio/"):
        raw = raw.split("/", 1)[-1]
    return _FORMAT_ALIASES.get(raw, raw or "mp3")


def _sanitize_content_disposition(value: str) -> str:
    """把 Content-Disposition 里的非 latin-1 文件名规范化为 RFC 6266/5987 格式。

    上游（官方 trim-music）在交付转码/下载文件时会在 filename="..." 填入原始中文歌名，
    同时在 filename*=UTF-8''... 填入 URL 编码的歌名。Starlette 要求所有响应头
    必须能被 latin-1 编码，否则抛出 UnicodeEncodeError 导致 500 报错。
    本函数将 filename="..." 中的非 ASCII 字符替换为安全字符，并确保 filename*= 存在。
    """
    m_star = re.search(r"filename\*=([^;]+)", value, re.IGNORECASE)
    m_plain = re.search(r'filename="([^"]+)"', value, re.IGNORECASE)
    if not m_plain:
        m_plain = re.search(r'filename=([^; ]+)', value, re.IGNORECASE)

    raw_fn = m_plain.group(1) if m_plain else "track"
    ext = "." + raw_fn.rsplit(".", 1)[-1] if "." in raw_fn else ""
    ascii_fn = re.sub(r'[^\x20-\x7e]', '_', raw_fn)
    if not ascii_fn.strip() or ascii_fn == ext:
        ascii_fn = f"track{ext}"

    if m_star:
        star_part = f"filename*={m_star.group(1).strip()}"
    else:
        quoted = quote(raw_fn, encoding="utf-8")
        star_part = f"filename*=UTF-8''{quoted}"

    disposition = value.split(";")[0].strip() or "attachment"
    return f'{disposition}; filename="{ascii_fn}"; {star_part}'


def filter_headers(headers: Any, exclude_keys: set | None = None) -> dict:
    exclude = HOP_BY_HOP | {k.lower() for k in (exclude_keys or set())}
    out = {}
    for k, v in headers.items():
        if k.lower() in exclude:
            continue
        try:
            str(v).encode("latin-1")
            out[k] = v
        except UnicodeEncodeError:
            if k.lower() == "content-disposition":
                out[k] = _sanitize_content_disposition(str(v))
            else:
                out[k] = str(v).encode("latin-1", errors="replace").decode("latin-1")
    return out


def copy_incoming_headers(request: Request) -> dict:
    """透传鉴权 Cookie / Token。Starlette 头名为小写，需显式回填以免丢失 music-token。

    authx 为新版官方前端登录后的逐请求签名头（含时间戳与随机数），
    原样转发给上游即可通过校验；切勿缓存或复用其值。"""
    headers = filter_headers(request.headers, exclude_keys={"host", "content-length"})
    headers["accept-encoding"] = "identity"
    for key in ("cookie", "authorization", "x-trim-music-temp-token", "authx"):
        val = request.headers.get(key)
        if val:
            headers[key] = val
    return headers


def get_by_path(d: Any, path: str) -> Any:
    curr = d
    for p in path.split("."):
        if isinstance(curr, dict) and p in curr:
            curr = curr[p]
        else:
            return None
    return curr


def set_by_path(d: dict, path: str, val: Any):
    parts = path.split(".")
    curr = d
    for p in parts[:-1]:
        if p not in curr or not isinstance(curr[p], dict):
            curr[p] = {}
        curr = curr[p]
    curr[parts[-1]] = val


def extract_keyword(request: Request) -> str:
    """前端打包用 q，部分调用/验收用 keyword。"""
    params = request.query_params
    return (params.get("keyword") or params.get("q") or params.get("query") or "").strip()


def online_guid_from_item(item: dict) -> str:
    raw_id = str(item.get("id") or "")
    src = str(item.get("source") or "")
    if raw_id.startswith("online:"):
        return raw_id
    if ":" in raw_id:
        return f"online:{raw_id}"
    return f"online:{src}:{raw_id}"


def song_id_from_online_guid(guid: str) -> str:
    if guid.startswith("online:"):
        return guid[len("online:") :]
    return guid


def is_online_guid(guid: str) -> bool:
    return bool(guid) and guid.startswith("online:")


def source_from_online_guid(guid: str) -> str:
    parts = (guid or "").split(":")
    return parts[1] if len(parts) >= 3 else ""


def build_online_track(item: dict) -> dict:
    """对齐飞牛前端 ZQ 解构 / _h() 期望：artists、album 对象、genres 数组、audioSpec、duration 毫秒。"""
    guid = online_guid_from_item(item)
    src = str(item.get("source") or source_from_online_guid(guid) or "")
    title = str(item.get("title") or item.get("name") or "")
    artist = str(item.get("artist") or "")
    album = str(item.get("album") or "")
    duration_s = item.get("duration_s") or 0
    try:
        duration_s = float(duration_s)
    except (TypeError, ValueError):
        duration_s = 0
    duration_ms = int(duration_s * 1000)
    ext = str(item.get("ext") or "mp3") or "mp3"
    play_format = play_format_from_ext(ext)
    file_size = item.get("file_size") or 0
    try:
        file_size = int(file_size or 0)
    except (TypeError, ValueError):
        file_size = 0
    # 飞牛移动端会校验 audioSpec.size：为 0 时判定为无效音频，直接跳过、
    # 连 stream 都不请求（网页端不校验，所以只有手机高失败率）。在线源在
    # 解析直链之前拿不到真实字节数，用码率×时长估算一个合理值。
    if file_size <= 0 and duration_s > 0:
        est_bitrate = 1411000 if play_format in ("flac", "wav", "ape", "wv") else 320000
        file_size = int(duration_s * est_bitrate / 8)
    cover = str(item.get("cover_url") or "")
    # 路径带真实后缀，飞牛 ll() 用 path 解析 extension；封面走 guid 以便 /static/cover 拦截
    spec_path = f"online/{src}/{guid}.{play_format}"

    artists_list = [{"name": artist, "guid": f"{guid}:artist"}] if artist else []
    album_obj = {
        "name": album,
        "guid": f"{guid}:album",
        "artists": artists_list,
        "coverId": guid,
    }
    # 专辑伪装 guid 同步登记（issue #22）：客户端点击专辑时按 fake 反解分源适配详情
    register_fake_album(guid, album, item)
    audio_spec = {
        "path": spec_path,
        "format": play_format,
        "codec": play_format,
        "container": play_format,
        "duration": duration_ms,
        "size": file_size,
        "channel": 2,
        "sampleRate": 44100,
        "bitDepth": 16 if play_format in ("wav", "flac", "aiff") else None,
        "bitrate": 1411000 if play_format in ("flac", "wav", "ape", "wv") else 320000,
    }
    audio_spec = {k: v for k, v in audio_spec.items() if v is not None}

    return {
        "guid": guid,
        "id": guid,
        "title": title,
        "name": title,
        "artist": artist,
        "artists": artists_list,
        "album": album_obj,
        "albumName": album,
        "audioSpec": audio_spec,
        "duration": duration_ms,
        "duration_ms": duration_ms,
        "durationMs": duration_ms,
        "duration_s": duration_s,
        "codec": play_format,
        "codecName": play_format,
        "format": play_format,
        "ext": ext,
        "size": file_size,
        "file_size": file_size,
        "coverId": guid,
        "cover_url": cover,
        "coverUrl": cover,
        "coverURL": cover,
        "source": src,
        "is_online": True,
        "isFavorite": False,
        "isCue": False,
        "hasLyric": bool(item.get("lyric")),
        "genres": [],
        "accessStatus": 0,
    }


def artist_from_track(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    a = item.get("artist") or item.get("singer") or item.get("singers") or ""
    if isinstance(a, list):
        names = []
        for x in a:
            if isinstance(x, dict):
                names.append(str(x.get("name") or ""))
            else:
                names.append(str(x))
        return " ".join(n for n in names if n).strip().lower()
    if isinstance(a, dict):
        return str(a.get("name") or "").strip().lower()
    return str(a).strip().lower()


def title_from_track(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    return str(item.get("title") or item.get("name") or "").strip().lower()


def should_cache(range_header: str | None) -> bool:
    """完整拉取才落盘：无 Range，或 bytes=0-（开区间）。Safari bytes=0-1 探测不落盘。"""
    if not range_header:
        return True
    r = range_header.strip().lower()
    return bool(re.match(r"^bytes=0-$", r))


def is_range_from_zero_or_none(range_header: str | None) -> bool:
    return should_cache(range_header)


def log_stream_probe(method: str, guid: str, range_header: str | None, cached: bool) -> None:
    """在线取流探针：一行记录 Range 形态与落盘资格，供真机确认播放器取流行为。"""
    if CONF.get("stream_probe"):
        logger.info(
            "stream probe: %s %s range=%r cached=%s tee_eligible=%s",
            method, guid, range_header, cached, should_cache(range_header),
        )


def cache_safe_guid(guid: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]", "_", guid)


def online_file_id(guid: str) -> str:
    """online:migu:600929… → 600929…，仅用于查找旧文件，不再写进文件名。"""
    return song_id_from_online_guid(guid).rsplit(":", 1)[-1]


def safe_basename_title(title: str) -> str:
    t = re.sub(r'[/\\:\0]', "_", (title or "").strip()) or "unknown"
    t = re.sub(r"\s+", " ", t).strip(" .")
    return t[:120]


def library_basename(title: str, artist: str = "") -> str:
    """曲库文件名：歌手 - 歌名（无源站 id）。飞牛无标签时会用文件名当标题。"""
    title_s = safe_basename_title(title)
    artist_s = safe_basename_title(artist) if (artist or "").strip() else ""
    if artist_s and artist_s.lower() != title_s.lower() and artist_s != "unknown":
        return f"{artist_s} - {title_s}"
    return title_s


def media_ref_path(guid: str) -> str:
    return os.path.join(CONF["cache_dir"], f"{cache_safe_guid(guid)}.ref")


def _path_stem(path: str) -> str:
    root, ext = os.path.splitext(path)
    known = set(CACHE_EXTS) | {"lrc", "part"}
    if ext.lstrip(".").lower() in known:
        return root
    return path


def remember_media_path(guid: str, media_path: str) -> None:
    """记住曲库里的文件词干（不含扩展名），音频和 .lrc 共用。"""
    stem = _path_stem(media_path)
    try:
        os.makedirs(CONF["cache_dir"], exist_ok=True)
        with open(media_ref_path(guid), "w", encoding="utf-8") as f:
            f.write(stem)
        # .ref 静默写丢会让已落库的歌重新出网、曲库出现重复文件，写后读回校验
        with open(media_ref_path(guid), encoding="utf-8") as f:
            if f.read().strip() != stem:
                raise OSError("read-back mismatch")
    except Exception as e:
        logger.warning("Failed to remember media path for %s: %s", guid, e)


def recalled_media_stem(guid: str) -> str | None:
    ref = media_ref_path(guid)
    if not os.path.exists(ref):
        return None
    try:
        with open(ref, encoding="utf-8") as f:
            stem = _path_stem(f.read().strip())
        if stem:
            return stem
    except Exception:
        return None
    return None


def recalled_media_path(guid: str) -> str | None:
    stem = recalled_media_stem(guid)
    if not stem:
        return None
    for ext in CACHE_EXTS:
        path = f"{stem}.{ext}"
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    return None


def unique_library_path(directory: str, basename: str, ext: str) -> str:
    dest = os.path.join(directory, f"{basename}.{ext}")
    if not os.path.exists(dest):
        return dest
    n = 2
    while os.path.exists(os.path.join(directory, f"{basename} ({n}).{ext}")):
        n += 1
    return os.path.join(directory, f"{basename} ({n}).{ext}")


def write_audio_tags(path: str, title: str, artist: str = "", album: Any = "") -> None:
    """写入 title/artist/album，飞牛扫描后用标签而不是文件名显示。

    album 可能是前端曲目对象 {name, guid, ...}；先归一成纯字符串再写入，
    避免把整个对象的文本写进专辑标签。
    """
    title, artist, album = _tag_fields({"title": title, "artist": artist, "album": album})
    if not title and not artist:
        return
    try:
        from mutagen import File as MutagenFile

        audio = MutagenFile(path, easy=True)
        if audio is None:
            return
        if getattr(audio, "tags", None) is None:
            try:
                audio.add_tags()
            except Exception:
                pass
        if title:
            audio["title"] = title
        if artist:
            audio["artist"] = artist
        if album:
            audio["album"] = album
        audio.save()
    except Exception as e:
        logger.warning("Failed to write audio tags for %s: %s", path, e)


_COVER_FETCH_TRANSPORT: "httpx.SyncBaseTransport | None" = None
_COVER_FETCH_MAX_BYTES = 10 * 1024 * 1024


def _sniff_image_mime(data: bytes) -> str:
    """魔数嗅探图片类型；非图片返回空串（Content-Type 会说谎，字节不会）。"""
    if data.startswith(b"\xFF\xD8\xFF"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    return ""


# TOP[top.cover] 封面下载/内嵌/缓存与清理（负面缓存、内网地址判定、歌单回补）
def download_cover_bytes(url: str) -> "tuple[bytes, str] | None":
    """下载封面图字节（内嵌用）。非图片/超限/酷我文本假图/网络失败一律返回 None。"""
    url = str(url or "").strip()
    if not url.startswith(("http://", "https://")):
        return None
    if _KW_TEXT_COVER_HOST in url:
        return None
    try:
        kwargs: dict = {"timeout": 8.0, "follow_redirects": True}
        if _COVER_FETCH_TRANSPORT is not None:
            kwargs["transport"] = _COVER_FETCH_TRANSPORT
        with httpx.Client(**kwargs) as client:
            r = client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        data = r.content or b""
        if not data or len(data) > _COVER_FETCH_MAX_BYTES:
            return None
        mime = _sniff_image_mime(data)
        if not mime:
            return None
        return data, mime
    except Exception as e:
        logger.debug("cover fetch failed for %s: %s", url, type(e).__name__)
        return None


def _has_embedded_cover(path: str) -> bool:
    try:
        from mutagen import File as MutagenFile

        mf = MutagenFile(path)
        tags = getattr(mf, "tags", None)
        if tags is None:
            return False
        if hasattr(tags, "pictures"):  # FLAC/APE
            return bool(tags.pictures)
        if hasattr(tags, "getall"):  # ID3
            return bool(tags.getall("APIC"))
        return bool(tags.get("covr") or tags.get("METADATA_BLOCK_PICTURE"))  # MP4/OGG
    except Exception:
        return False


def embed_audio_cover(path: str, data: bytes, mime: str) -> bool:
    """把封面字节内嵌进音频文件（MP3 APIC / FLAC Picture / MP4 covr / OGG）。

    飞牛扫描器（dhowden/tag）只认内嵌图，不读外置 cover 图片文件，内嵌是
    官方 App 显示封面的唯一途径。文件已有内嵌图（源站自带）则跳过；不支持的
    格式（wav 等）返回 False。
    """
    try:
        import base64

        from mutagen import File as MutagenFile
        from mutagen.flac import Picture
        from mutagen.id3 import APIC, ID3
        from mutagen.mp4 import MP4Cover

        audio = MutagenFile(path)
        if audio is None or _has_embedded_cover(path):
            return False
        cls = audio.__class__.__name__
        if cls == "MP3":
            try:
                id3 = ID3(path)
            except Exception:
                id3 = ID3()
            id3.delall("APIC")
            id3.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=data))
            id3.save(path, v2_version=3)
            return True
        if getattr(audio, "tags", None) is None:
            try:
                audio.add_tags()
            except Exception:
                return False
        if cls == "MP4":
            if mime not in ("image/jpeg", "image/png"):
                return False
            fmt = MP4Cover.FORMAT_PNG if mime == "image/png" else MP4Cover.FORMAT_JPEG
            audio["covr"] = [MP4Cover(data, imageformat=fmt)]
            audio.save()
            return True
        if cls == "FLAC":
            pic = Picture()
            pic.type = 3
            pic.mime = mime
            pic.desc = "Cover"
            pic.data = data
            audio.add_picture(pic)
            audio.save()
            return True
        if cls in ("OggVorbis", "OggOpus", "OggSpeex"):
            pic = Picture()
            pic.type = 3
            pic.mime = mime
            pic.desc = "Cover"
            pic.data = data
            audio["METADATA_BLOCK_PICTURE"] = [base64.b64encode(pic.write()).decode("ascii")]
            audio.save()
            return True
        return False
    except Exception as e:
        logger.debug("cover embed failed for %s: %s", path, e)
        return False


def detect_library_dir() -> str:
    """优先环境变量，否则读飞牛 music.db 的共享库路径，最后回退到仓库 cache/。"""
    explicit = str(CONF.get("library_dir") or "").strip()
    if explicit:
        return explicit
    db = str(CONF.get("music_db") or "")
    if db and os.path.exists(db):
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            try:
                rows = con.execute("SELECT path FROM shared_library ORDER BY id").fetchall()
            finally:
                con.close()
            for (path,) in rows:
                if path and os.path.isdir(path):
                    return path
        except Exception as e:
            logger.warning("Failed to read shared_library path: %s", e)
    return CONF["cache_dir"]


_TEE_SAVE_DIR_WARNED = False


def tee_save_dir() -> str:
    """边听边存落盘目录：配置路径可用则用，否则回退自动探测的曲库目录。"""
    explicit = str(CONF.get("tee_save_dir") or "").strip()
    if not explicit:
        return detect_library_dir()
    usable = False
    try:
        os.makedirs(explicit, exist_ok=True)
        usable = os.path.isdir(explicit) and os.access(explicit, os.W_OK)
    except Exception:
        usable = False
    if usable:
        return explicit
    global _TEE_SAVE_DIR_WARNED
    if not _TEE_SAVE_DIR_WARNED:
        _TEE_SAVE_DIR_WARNED = True
        logger.warning(
            "FNMUSIC_TEE_SAVE_DIR=%s 不可用（无法创建或不可写），边听边存回退到 %s",
            explicit, detect_library_dir(),
        )
    return detect_library_dir()


def iter_media_dirs() -> list[str]:
    dirs: list[str] = []
    explicit = str(CONF.get("tee_save_dir") or "").strip()
    for d in (([explicit] if explicit else []) + [detect_library_dir(), CONF["cache_dir"]]):
        if d and d not in dirs:
            dirs.append(d)
    return dirs


def adopt_library_perms(path: str) -> None:
    try:
        parent = os.path.dirname(path) or "."
        st = os.stat(parent)
        os.chown(path, st.st_uid, st.st_gid)
        os.chmod(path, 0o644)
    except Exception:
        pass


def find_cache_file(guid: str) -> str | None:
    recalled = recalled_media_path(guid)
    if recalled:
        return recalled
    safe = cache_safe_guid(guid)
    for d in iter_media_dirs():
        if not os.path.isdir(d):
            continue
        for ext in CACHE_EXTS:
            exact = os.path.join(d, f"{safe}.{ext}")
            if os.path.exists(exact) and os.path.getsize(exact) > 0:
                return exact
    return _find_file_by_snapshot_tags(guid)


# 标签兜底查库的阴性缓存：确认不在曲库的 guid 短期内不再反复扫快照/查库
_TAGS_LOOKUP_MISS: dict[str, float] = {}
_TAGS_MISS_TTL_S = 300.0


def _find_file_by_snapshot_tags(guid: str) -> "str | None":
    """.ref 与精确名都 miss 时的自愈兜底：按快照标签在官方曲库反查文件。

    命中说明歌已落库只是 .ref 丢了（或本就有同名同曲的本地文件），
    remember_media_path 自愈后下次直接走 recall。必须 title+artist 双条件
    才可能命中（album 有则再收紧），查库只在 miss 时发生。
    """
    now = time.monotonic()
    miss_at = _TAGS_LOOKUP_MISS.get(guid)
    if miss_at is not None and now - miss_at < _TAGS_MISS_TTL_S:
        return None
    snap = _lookup_online_snapshot(guid)
    title = artist = album = ""
    if snap:
        title, artist, album = _tag_fields(snap)
    path = library_file_by_tags(title, artist, album) if title and artist else None
    if not path:
        if len(_TAGS_LOOKUP_MISS) > 1024:
            _TAGS_LOOKUP_MISS.clear()
        _TAGS_LOOKUP_MISS[guid] = now
        return None
    remember_media_path(guid, path)
    logger.info("Recalled library file for %s via tags: %s", guid, path)
    return path


def promote_cache_hit(guid: str, audio_path: str) -> str:
    """旧 cache/ 音频：若曲库已有对应文件或歌词，则对齐过去。"""
    recalled = recalled_media_path(guid)
    if recalled:
        return recalled
    lib = detect_library_dir()
    try:
        if os.path.abspath(os.path.dirname(audio_path)) == os.path.abspath(lib):
            remember_media_path(guid, audio_path)
            return audio_path
    except Exception:
        return audio_path
    # Bare legacy IDs cannot prove source/track identity.
    return audio_path


def _is_rolling_cache_stem(path: str, guid: str) -> bool:
    """是否为 cache 目录下该 guid 的滚动缓存产物（cache_safe_guid 命名）。"""
    rolling = os.path.join(CONF["cache_dir"], cache_safe_guid(guid))
    try:
        return os.path.abspath(os.path.splitext(path)[0]) == os.path.abspath(rolling)
    except Exception:
        return False


def library_media_path(guid: str, title: str, ext: str, artist: str = "", directory: str | None = None) -> str:
    lib = directory or detect_library_dir()
    recalled = recalled_media_path(guid)
    if recalled and not _is_rolling_cache_stem(recalled, guid):
        return recalled
    stem = recalled_media_stem(guid)
    if stem and not _is_rolling_cache_stem(stem, guid):
        return f"{stem}.{ext}"
    os.makedirs(lib, exist_ok=True)
    return unique_library_path(lib, library_basename(title, artist), ext)


def find_lyric_file(guid: str) -> str | None:
    stem = recalled_media_stem(guid)
    if stem:
        sibling = f"{stem}.lrc"
        if os.path.exists(sibling) and os.path.getsize(sibling) > 0:
            return sibling
    audio = find_cache_file(guid)
    if audio:
        sibling = os.path.splitext(audio)[0] + ".lrc"
        if os.path.exists(sibling) and os.path.getsize(sibling) > 0:
            return sibling
    safe = cache_safe_guid(guid)
    for d in iter_media_dirs():
        if not os.path.isdir(d):
            continue
        exact = os.path.join(d, f"{safe}.lrc")
        if os.path.exists(exact) and os.path.getsize(exact) > 0:
            return exact
    return None


def lyric_cache_path(guid: str, title: str = "", artist: str = "") -> str:
    """歌词三档归属（音频到哪，歌词到哪）：已有歌词复用原位；有音频落在音频旁；
    无音频只落 cache（guid 命名）。绝不把曲库目录当无音频时的兜底——那会制造
    只有歌词没有音频的孤儿 .lrc（曲库在云盘上时还会产生上传流量）。
    """
    found = find_lyric_file(guid)
    if found:
        return found
    audio = find_cache_file(guid)
    if audio:
        return os.path.splitext(audio)[0] + ".lrc"
    return os.path.join(CONF["cache_dir"], f"{cache_safe_guid(guid)}.lrc")


def read_lyric_cache(guid: str) -> str:
    path = find_lyric_file(guid)
    if not path:
        return ""
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except Exception as e:
        logger.warning("Failed to read lyric cache %s: %s", path, e)
        return ""


def write_lyric_cache(guid: str, text: str, title: str = "", artist: str = "") -> None:
    text = (text or "").strip()
    if not text:
        return
    if text == read_lyric_cache(guid):
        return
    path = lyric_cache_path(guid, title=title, artist=artist)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    part_path = f"{path}.{uuid4().hex[:8]}.part"
    try:
        with open(part_path, "w", encoding="utf-8") as f:
            f.write(text)
            f.write("\n")
        os.replace(part_path, path)
        adopt_library_perms(path)
        remember_media_path(guid, path)
    except Exception as e:
        logger.warning("Failed to write lyric cache %s: %s", path, e)
        if os.path.exists(part_path):
            try:
                os.remove(part_path)
            except Exception:
                pass


def promote_shadow_lyric(guid: str, audio_path: str) -> None:
    """音频落曲库后，把 cache 里的影子歌词（无音频时代的 guid 命名副本）提升为
    音频旁 sidecar，词曲贴身；曲库已有 sidecar 时不覆盖。跨文件系统时退化为复制。
    """
    dest = os.path.splitext(audio_path)[0] + ".lrc"
    if os.path.exists(dest):
        return
    shadow = os.path.join(CONF["cache_dir"], f"{cache_safe_guid(guid)}.lrc")
    if not os.path.exists(shadow):
        return
    try:
        os.replace(shadow, dest)
    except OSError:
        try:
            shutil.copyfile(shadow, dest)
            os.remove(shadow)
        except Exception as e:
            logger.warning("shadow lyric promote failed for %s: %s", guid, e)
            return
    adopt_library_perms(dest)


async def cache_lyrics_from_musicdl(musicdl_client: httpx.AsyncClient, guid: str) -> dict | None:
    """与音频 tee 并行：把 musicdl /info 里的 LRC 落到曲库同目录 sidecar。"""
    song_id = song_id_from_online_guid(guid)
    try:
        r = await musicdl_client.get("/info", params={"id": song_id}, timeout=10.0)
        if r.status_code != 200:
            return None
        data = r.json()
        if not (isinstance(data, dict) and data.get("ok") is not False):
            return None
        write_lyric_cache(
            guid,
            str(data.get("lyric") or ""),
            title=str(data.get("title") or ""),
            artist=str(data.get("artist") or ""),
        )
        return data
    except Exception as e:
        logger.warning("lyric sidecar fetch failed for %s: %s", guid, e)
        return None


# TOP[top.lyric] 歌词解析与回补：音源歌词 → 本地缓存 → musicdl 兜底
async def resolve_online_lyric(request: Request, guid: str) -> str:
    """本地 .lrc 优先；没有再向源站要，拿到就落盘。"""
    cached = read_lyric_cache(guid)
    if cached:
        return cached

    if not _source_enabled(guid):
        return ""
    src = source_from_online_guid(guid)
    if src == "netease":
        musicbox_client = get_musicbox_client(request.app)
        raw_song_id = song_id_from_online_guid(guid)
        song_id = raw_song_id.split(":")[-1]
        try:
            r = await musicbox_client.get(f"/api/v1/song/{song_id}/lyric", timeout=10.0)
            if r.status_code == 200:
                res_data = r.json()
                if isinstance(res_data, dict) and res_data.get("ok") is not False:
                    l_data = res_data.get("data")
                    if isinstance(l_data, dict):
                        lyric_text = str(l_data.get("lyric") or "").strip()
                        if lyric_text:
                            info = await _online_info(request, guid)
                            write_lyric_cache(
                                guid,
                                lyric_text,
                                title=str((info or {}).get("title") or ""),
                                artist=str((info or {}).get("artist") or ""),
                            )
                            return lyric_text
        except Exception as e:
            logger.warning("musicbox lyric fetch failed for %s: %s", guid, e)
        return ""

    data = await _online_info(request, guid)
    text = str((data or {}).get("lyric") or "").strip()
    if text:
        write_lyric_cache(
            guid,
            text,
            title=str((data or {}).get("title") or ""),
            artist=str((data or {}).get("artist") or ""),
        )
    else:
        # fast=1 没拿到词：后台全模式回补（带 title/artist），下次拉取即得
        _ensure_lyric_backfill(request, guid, data)
    return text


# === 2.6.36 歌词后台回补：fast 路径拿不到词（播放源无缓存词）时，
# 后台用全模式（跨 5 平台搜索回退，服务端带 title/artist）再要一次，
# 成功落 .lrc 缓存，客户端下次拉取即得。10min 冷却 + single-flight。 ===
_LYRIC_BACKFILL_INFLIGHT: set[str] = set()
_LYRIC_BACKFILL_AT: dict[str, float] = {}
_LYRIC_BACKFILL_COOLDOWN_S = 600.0


async def _lyric_backfill_task(request: Request, guid: str, data: "dict | None") -> None:
    await asyncio.sleep(_LYRIC_STAGGER_S)  # 让主流/封面先跑，20s 预算往后让
    d = data if isinstance(data, dict) else {}
    title = str(d.get("title") or "").strip()
    artist = str(d.get("artist") or "").strip()
    if not (title or artist):
        song = ceru.song_for_guid(guid)
        if song:
            title = title or ceru.song_title(song)
            artist = artist or ceru.song_artist(song)
    if not (title or artist):
        try:
            info = await _online_info(request, guid, include_lyric=False)
            if info:
                title = title or str(info.get("title") or "").strip()
                artist = artist or str(info.get("artist") or "").strip()
        except Exception:
            pass
    if not (title or artist):
        return
    lx_client = get_lx_client(request.app)
    try:
        r = await lx_client.get(
            "/api/v1/track/lyric",
            params={
                "id": song_id_from_online_guid(guid),
                "title": title,
                "artist": artist,
            },
            timeout=20.0,
        )
        payload = r.json() if r.status_code == 200 else {}
        if isinstance(payload, dict) and payload.get("ok") is not False:
            text = str((payload.get("data") or {}).get("lyric") or "").strip()
            if text:
                write_lyric_cache(guid, text, title=title, artist=artist)
                logger.info("lyric backfill ok for %s (%d chars)", guid, len(text))
    except Exception as e:
        logger.debug("lyric backfill failed for %s: %s", guid, e)


def _ensure_lyric_backfill(request: Request, guid: str, data: "dict | None" = None) -> bool:
    if not is_online_guid(guid) or source_from_online_guid(guid) != "lx":
        return False
    if read_lyric_cache(guid):
        return False
    if not _source_enabled(guid):
        return False
    if guid in _LYRIC_BACKFILL_INFLIGHT:
        return False
    global _LYRIC_JOB_ACTIVE
    if _LYRIC_JOB_ACTIVE >= _LYRIC_JOB_MAX:
        return False
    last = _LYRIC_BACKFILL_AT.get(guid)
    if last is not None and time.monotonic() - last < _LYRIC_BACKFILL_COOLDOWN_S:
        return False
    _LYRIC_JOB_ACTIVE += 1
    _LYRIC_BACKFILL_INFLIGHT.add(guid)
    _LYRIC_BACKFILL_AT[guid] = time.monotonic()

    def _done(task: "asyncio.Task") -> None:
        global _LYRIC_JOB_ACTIVE
        _LYRIC_JOB_ACTIVE -= 1
        _LYRIC_BACKFILL_INFLIGHT.discard(guid)
        if not task.cancelled():
            try:
                task.exception()
            except Exception:  # noqa: BLE001
                pass

    task = asyncio.create_task(_lyric_backfill_task(request, guid, data))
    task.add_done_callback(_done)
    return True


def media_type_for_ext(ext: str) -> str:
    return {
        "mp3": "audio/mpeg",
        "flac": "audio/flac",
        "wav": "audio/wav",
        "ogg": "audio/ogg",
        "opus": "audio/ogg",
        "m4a": "audio/mp4",
        "aac": "audio/aac",
        "ape": "audio/x-ape",
        "wv": "audio/x-wavpack",
        "dsf": "audio/x-dsd",
        "dff": "audio/x-dff",
        "tta": "audio/x-tta",
        "wma": "audio/x-ms-wma",
        "aiff": "audio/aiff",
    }.get(ext.lower(), "application/octet-stream")


def ext_from_content_type(content_type: str) -> str:
    ct = (content_type or "").lower()
    if "flac" in ct:
        return "flac"
    if "wavpack" in ct or "x-wv" in ct:
        return "wv"
    if "wav" in ct or "wave" in ct:
        return "wav"
    if "opus" in ct:
        return "opus"
    if "ogg" in ct:
        return "ogg"
    if "ape" in ct:
        return "ape"
    if "aiff" in ct:
        return "aiff"
    if "mp4" in ct or "m4a" in ct:
        return "m4a"
    if "aac" in ct:
        return "aac"
    if "mpeg" in ct or "mp3" in ct:
        return "mp3"
    return play_format_from_ext(ct.split("/")[-1] if "/" in ct else "mp3")


def parse_http_range(range_header: str | None, file_size: int) -> tuple[int, int] | None:
    if not range_header:
        return None
    m = re.match(r"bytes=(\d*)-(\d*)", range_header.strip(), re.I)
    if not m:
        return None
    start_s, end_s = m.group(1), m.group(2)
    if start_s == "" and end_s == "":
        return None
    if start_s == "":
        suffix = int(end_s)
        start = max(file_size - suffix, 0)
        end = file_size - 1
    else:
        start = int(start_s)
        end = int(end_s) if end_s else file_size - 1
    end = min(end, file_size - 1)
    if start < 0 or start >= file_size or start > end:
        return None
    return start, end


def serve_file_with_range(path: str, range_header: str | None, media_type: str,
                          extra_headers: dict | None = None) -> Response:
    file_size = os.path.getsize(path)
    rng = parse_http_range(range_header, file_size)

    def iter_file(offset: int, length: int) -> AsyncGenerator[bytes, None]:
        async def gen() -> AsyncGenerator[bytes, None]:
            remaining = length
            with open(path, "rb") as fp:
                fp.seek(offset)
                while remaining > 0:
                    chunk = fp.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk

        return gen()

    extra = extra_headers or {}
    if rng is None:
        headers = {
            "Content-Type": media_type,
            "Content-Length": str(file_size),
            "Accept-Ranges": "bytes",
        }
        headers.update(extra)
        return StreamingResponse(
            iter_file(0, file_size),
            status_code=200,
            headers=headers,
        )

    start, end = rng
    length = end - start + 1
    headers = {
        "Content-Type": media_type,
        "Content-Length": str(length),
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
    }
    headers.update(extra)
    return StreamingResponse(
        iter_file(start, length),
        status_code=206,
        headers=headers,
    )


def get_upstream_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "upstream_client", None)
    if client is None:
        transport = httpx.AsyncHTTPTransport(uds=CONF["upstream_sock"])
        client = httpx.AsyncClient(transport=transport, base_url="http://unix", timeout=30.0)
        fastapi_app.state.upstream_client = client
    return client


def get_musicdl_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "musicdl_client", None)
    if client is None:
        client = httpx.AsyncClient(base_url=CONF["musicdl_url"], timeout=45.0)
        fastapi_app.state.musicdl_client = client
    return client


def get_musicbox_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "musicbox_client", None)
    if client is None:
        client = httpx.AsyncClient(base_url=CONF["musicbox_url"], timeout=20.0)
        fastapi_app.state.musicbox_client = client
    return client


def get_lx_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "lx_client", None)
    if client is None:
        client = httpx.AsyncClient(base_url=CONF["lx_url"], timeout=25.0)
        fastapi_app.state.lx_client = client
    return client


def get_llm_client(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "llm_client", None)
    if client is None:
        client = httpx.AsyncClient(timeout=dailyrec.LLM_TIMEOUT_S)
        fastapi_app.state.llm_client = client
    return client


# TOP[top.forward] 官方 API 透传：未接管的请求原样转发官方后端（Unix socket）
async def forward_to_upstream(request: Request, client: httpx.AsyncClient) -> Response:
    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"

    headers = copy_incoming_headers(request)
    body = await request.body()

    req = client.build_request(
        method=request.method,
        url=url_path,
        headers=headers,
        content=body if body else None,
    )
    # 幂等方法在"连接还没换来任何响应"的失败（官方服务重启窗口的陈旧连接 /
    # uds 瞬断）上重试一次：响应已开始流式传输后的失败不重试（无法安全回放）。
    resp = None
    attempts = 2 if request.method.upper() in ("GET", "HEAD") else 1
    for attempt in range(attempts):
        try:
            resp = await client.send(req, stream=True)
            break
        except (httpx.TransportError, httpx.RemoteProtocolError):
            if attempt + 1 >= attempts:
                raise
            logger.info("upstream forward retry (%s %s): connection-level failure",
                        request.method, request.url.path)
    resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})
    # 上游请求强制 accept-encoding: identity，其 Content-Length 即精确字节数；原样
    # 透传可让依赖总长度的播放内核走定长帧（对齐官方直连行为）。无 body 的状态
    # 码不能带；上游无视 identity 仍压缩时长度描述的是编码体，透传必错位，弃用。
    content_length = resp.headers.get("content-length")
    content_encoding = (resp.headers.get("content-encoding") or "").strip().lower()
    if (content_length and content_encoding in ("", "identity")
            and resp.status_code >= 200 and resp.status_code not in (204, 304)):
        resp_headers["content-length"] = content_length

    async def body_stream() -> AsyncGenerator[bytes, None]:
        try:
            async for chunk in resp.aiter_bytes():
                yield chunk
        finally:
            await resp.aclose()

    return StreamingResponse(
        body_stream(),
        status_code=resp.status_code,
        headers=resp_headers,
    )


async def fetch_upstream_envelope(request: Request, client: httpx.AsyncClient) -> Response | dict:
    """透传上游并解析 JSON 信封。失败时返回 Response，成功返回 dict。"""
    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)
    body = await request.body()
    req = client.build_request(
        method=request.method,
        url=url_path,
        headers=headers,
        content=body if body else None,
    )
    resp = await client.send(req)
    resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})
    if resp.status_code != 200:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    try:
        payload = resp.json()
    except Exception:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    if not isinstance(payload, dict):
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    payload["_ext_headers"] = resp_headers
    return payload


async def fetch_musicdl_search(client: httpx.AsyncClient, keyword: str, limit: int, sources: str | None = None) -> dict | None:
    if not keyword:
        return None
    scope = _FETCH_SCOPE.get()

    async def _fetch_once(params: dict[str, Any], timeout: float) -> dict | None:
        try:
            r = await client.get("/search", params=params, timeout=timeout)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict):
                    raw_items = data.get("items")
                    if isinstance(raw_items, list):
                        data["items"] = [it for it in raw_items if is_playable_online_track(it)]
                    return data
        except Exception as e:
            logger.warning("Failed to fetch online search from musicdl: %s", e)
        return None

    async def _query():
        params: dict[str, Any] = {"keyword": keyword, "limit": limit}
        selected_sources = CONF["online_sources"] if sources is None else sources
        if selected_sources:
            params["sources"] = selected_sources
        timeout = max(1.0, float(CONF.get("search_timeout") or 15))
        started = time.monotonic()
        data = await _fetch_once(params, timeout)
        # musicdl 快速交回 0 条且带错误（全源 busy/熔断/瞬时失败）时不是
        # 终态：稍等片刻重试一次，避免把偶发空结果当成真"无结果"。
        if (isinstance(data, dict) and not data.get("items") and data.get("errors")
                and time.monotonic() - started < 5.0):
            await asyncio.sleep(1.5)
            retry = await _fetch_once(params, timeout)
            if isinstance(retry, dict) and retry.get("items"):
                data = retry
        if isinstance(data, dict) and data.get("errors"):
            logger.warning("musicdl search partial errors: %s", data.get("errors"))
        return data

    return await _MUSICDL_SEARCH_GATE.run(scope, keyword, _query)


async def fetch_musicbox_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict] | None:
    if not keyword:
        return None
    scope = _FETCH_SCOPE.get()

    async def _query():
        return await _musicbox_search_request(client, keyword, limit)

    return await _MUSICBOX_SEARCH_GATE.run(scope, keyword, _query)


async def _musicbox_search_request(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict] | None:
    try:
        r = await client.get(
            "/api/v1/search",
            params={"keyword": keyword, "limit": limit, "type": "song"},
            timeout=20.0,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, dict) or data.get("ok") is False:
            return None
        raw_list = data.get("data")
        if not isinstance(raw_list, list):
            return None
        items = []
        song_ids = []
        for it in raw_list:
            if not isinstance(it, dict):
                continue
            if not is_playable_online_track(it):
                continue
            sid = str(it.get("song_id") or it.get("id") or "")
            if not sid:
                continue
            title = str(it.get("song_name") or it.get("title") or it.get("name") or "")
            artist = str(it.get("artist") or "")
            album = str(it.get("album_name") or it.get("album") or "")
            duration = it.get("duration") or 0
            try:
                duration_s = float(duration)
            except (TypeError, ValueError):
                duration_s = 0.0
            quality = str(it.get("quality") or "").upper()
            ext = "flac" if any(q in quality for q in ("SQ", "HR", "无损")) else "mp3"
            items.append({
                "id": f"netease:{sid}",
                "source": "netease",
                "title": title,
                "version": str(it.get("version") or ""),
                "artist": artist,
                "album": album,
                "duration_s": duration_s,
                "ext": ext,
                "cover_url": "",
                "lyric": "",
            })
            song_ids.append(sid)

        if song_ids:
            try:
                detail_resp = await client.get(
                    "/api/v1/songs/detail",
                    params={"ids": ",".join(song_ids)},
                    timeout=15.0,
                )
                if detail_resp.status_code == 200:
                    detail_json = detail_resp.json()
                    if isinstance(detail_json, dict) and detail_json.get("ok") is not False:
                        detail_list = detail_json.get("data")
                        if isinstance(detail_list, list):
                            detail_map = {}
                            for d_item in detail_list:
                                if isinstance(d_item, dict):
                                    d_sid = str(d_item.get("song_id") or d_item.get("id") or "")
                                    if d_sid:
                                        detail_map[d_sid] = d_item
                            for item in items:
                                raw_sid = item["id"].split(":", 1)[-1]
                                d_info = detail_map.get(raw_sid)
                                if d_info:
                                    pic_url = str(d_info.get("album_pic_url") or "")
                                    if pic_url:
                                        item["cover_url"] = pic_url
                                    if d_info.get("has_sq") or d_info.get("has_hr"):
                                        item["ext"] = "flac"
            except Exception as detail_err:
                logger.warning("Failed to fetch songs detail for %s: %s", keyword, detail_err)

        return [it for it in items if is_playable_online_track(it)]
    except Exception as e:
        logger.warning("Failed to fetch musicbox search: %s", e)
        return None


class _SearchItems(list):
    """List-compatible normalized results with source degradation metadata."""
    def __init__(self, items, partial=False):
        super().__init__(items)
        self.partial = partial


async def fetch_lx_search(client: httpx.AsyncClient, keyword: str, limit: int, sources: "list[str] | str | None" = None) -> list[dict]:
    """洛雪音乐源搜索：返回统一 item（id = "lx:<source>:<identifier>"）。

    sources：平台白名单（列表或逗号串），None = 用 CONF["lx_sources"]；空 = 跟随 lx 服务配置。
    """
    if not keyword:
        return None  # type: ignore[return-value]
    scope = _FETCH_SCOPE.get()

    async def _query():
        return await _lx_search_request(client, keyword, limit, sources, scope)

    return await _LX_SEARCH_GATE.run(scope, keyword, _query)


async def _lx_search_request(client: httpx.AsyncClient, keyword: str, limit: int, sources, scope: str) -> list[dict]:
    params: dict[str, Any] = {
        "keyword": keyword,
        "limit": limit,
        "probe": 1 if CONF.get("search_probe") else 0,
    }
    selected = CONF.get("lx_sources") if sources is None else sources
    if isinstance(selected, str):
        selected = [s.strip() for s in selected.split(",") if s.strip()]
    if selected:
        params["sources"] = ",".join(selected)
    timeout = max(1.0, float(CONF.get("search_timeout") or 15))
    headers = {_SCOPE_HEADER: scope} if scope else None
    try:
        r = await client.get(
            "/api/v1/search",
            params=params,
            headers=headers,
            timeout=timeout,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, dict) or data.get("ok") is False:
            return None
        raw_list = data.get("items")
        if not isinstance(raw_list, list):
            return None
        items = []
        allow_paywall = not bool(CONF.get("search_probe"))
        for it in raw_list:
            if not isinstance(it, dict):
                continue
            if not is_playable_online_track(it, allow_paywall=allow_paywall):
                continue
            tid = str(it.get("id") or "")
            if not tid:
                continue
            duration = it.get("duration_s") or 0
            try:
                duration_s = float(duration)
            except (TypeError, ValueError):
                duration_s = 0.0
            try:
                file_size = int(it.get("file_size") or 0)
            except (TypeError, ValueError):
                file_size = 0
            items.append({
                "id": tid,
                "source": "lx",
                "lx_source": str(it.get("lx_source") or ""),
                "version": str(it.get("version") or ""),
                "title": str(it.get("title") or it.get("name") or ""),
                "artist": str(it.get("artist") or ""),
                "album": str(it.get("album") or ""),
                "duration_s": duration_s,
                "ext": str(it.get("ext") or "mp3") or "mp3",
                "cover_url": str(it.get("cover_url") or ""),
                "file_size": file_size,
                "lyric": "",
                "verified": it.get("verified") is True,
            })
        return _SearchItems([it for it in items if is_playable_online_track(it, allow_paywall=allow_paywall)], partial=bool(data.get("errors")))
    except Exception as e:
        logger.warning("Failed to fetch online search from lxmusic: %s", e)
        return None


# 音质档（从低到高）：网易 = musicbox QUALITY_WHITELIST 全集；
# lx = 洛雪源脚本三档（128k/320k/flac 对应 standard/high/lossless）
_NETEASE_QUALITY_LADDER = ["standard", "higher", "exhigh", "lossless", "hires", "jymaster"]
_LX_QUALITY_LADDER = ["standard", "high", "lossless"]

# 在线媒体（尤其洛雪直链）下载时必须带的请求头。
# lxmusic-service 用浏览器 UA 探活成功，但它回给 proxy 的 data 里不带 headers；
# proxy 若用 httpx 默认 UA（python-httpx/...）去取流，酷我 CDN 会直接 403，
# 表现为“源里能播、飞牛却跳歌”。这里按平台补齐 UA / Referer。
_MEDIA_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
_MEDIA_REFERER = {
    "kw": "http://www.kuwo.cn/",
    "kg": "https://www.kugou.com/",
    "tx": "https://y.qq.com/",
    "wy": "https://music.163.com/",
    "mg": "https://music.migu.cn/",
}

# 洛雪播放地址缓存（10 分钟）与“原曲→可用替代平台”记忆（1 小时）。
# 目的：重复播放/切回上一首直接命中，不再重复解析；原平台失败后直接走上次成功的替代源。
_LX_URL_CACHE: dict[str, tuple[dict, float]] = {}
# 网易直链同样存在“HEAD + GET”重复解析；按客户端对象隔离缓存，避免测试或
# 多副本场景把不同 mock/会话的 URL 混用。
_NETEASE_URL_CACHE: dict[tuple[int, str], tuple[str, float]] = {}
_NETEASE_URL_CACHE_TTL_S = 30 * 60.0
# 平台直链普遍 1~2 小时有效；缓存越久，重复播放/来回切歌就越不需要
# 再打用户源，限流窗口里这一点尤其关键。
_LX_URL_CACHE_TTL_S = 30 * 60.0
# 2.6.39 播放成功率对齐安卓：已确认失效的直链（首字节取不到 / 中途断流且重连
# 耗尽）在 TTL 内不再下发（等价安卓 failedUrls 排除）；该曲后续取链从更低音质
# 档起步（等价安卓播放报错 → 音质阶梯回退）。
_LX_DEAD_URLS: dict[str, dict[str, float]] = {}
_LX_DEAD_URL_TTL_S = 30 * 60.0
_LX_STREAM_FAILS: dict[str, int] = {}
_LX_STREAM_FAIL_AT: dict[str, float] = {}
_LX_STREAM_FAIL_TTL_S = 30 * 60.0
_LX_MAX_TIER_DOWNGRADE = 2


def _lx_dead_urls(song_id: str) -> set:
    """该曲目 TTL 内已确认失效的直链集合（惰性清理）。"""
    d = _LX_DEAD_URLS.get(song_id) or {}
    now = time.time()
    live = {u for u, ts in d.items() if now - ts < _LX_DEAD_URL_TTL_S}
    if live:
        _LX_DEAD_URLS[song_id] = {u: d[u] for u in live}
    else:
        _LX_DEAD_URLS.pop(song_id, None)
    return live


def _lx_tier_downgrade_steps(song_id: str) -> int:
    """该曲目流失败次数 → 下次取链的降档步数（封顶 _LX_MAX_TIER_DOWNGRADE）。"""
    ts = _LX_STREAM_FAIL_AT.get(song_id)
    if ts is None or time.time() - ts >= _LX_STREAM_FAIL_TTL_S:
        _LX_STREAM_FAIL_AT.pop(song_id, None)
        _LX_STREAM_FAILS.pop(song_id, None)
        return 0
    return min(int(_LX_STREAM_FAILS.get(song_id, 0)), _LX_MAX_TIER_DOWNGRADE)


def _lx_mark_url_dead(song_id: str, url: str) -> None:
    """一条直链确认失效：记死链 + 记一次流失败，并清掉该曲的 URL 缓存，
    让下一次取链走全新的解析（换档/换源/换曲候选）。"""
    if not song_id or not url:
        return
    now = time.time()
    d = _LX_DEAD_URLS.setdefault(song_id, {})
    d[url] = now
    if now - _LX_STREAM_FAIL_AT.get(song_id, 0.0) >= _LX_STREAM_FAIL_TTL_S:
        n = 0
    else:
        n = int(_LX_STREAM_FAILS.get(song_id, 0))
    n += 1
    _LX_STREAM_FAILS[song_id] = n
    _LX_STREAM_FAIL_AT[song_id] = now
    _LX_URL_CACHE.pop(song_id, None)
    if n <= 3 or n % 10 == 0:
        logger.warning("lx stream dead for %s (fails=%d): %s", song_id, n, url[:140])


def _lx_reset_stream_fails(song_id: str) -> None:
    """解析成功即重置降档计数（死链记录保留——那条具体链接仍然失效）。"""
    _LX_STREAM_FAILS.pop(song_id, None)
    _LX_STREAM_FAIL_AT.pop(song_id, None)


_LX_ALT_CACHE: dict[str, tuple[str, float]] = {}
_LX_ALT_CACHE_TTL_S = 60 * 60.0
_LX_PLATFORM_HEALTH: dict[str, dict[str, float]] = {}
_LX_ALT_META_CACHE: dict[str, dict] = {}
_LX_ALT_META_LOADED = False
_LX_ALT_META_SAVE_LAST = 0.0
_LX_ALT_META_PATH = os.path.join(CONF.get("cache_dir") or _HOME, "lx_alt_meta.json")
_CERU_LX_ITEM_CACHE: dict[str, dict] = {}
_CERU_LX_ITEM_LOADED = False
_CERU_LX_ITEM_SAVE_LAST = 0.0
_CERU_LX_ITEM_PATH = os.path.join(CONF.get("cache_dir") or _HOME, "ceru_lx_items.json")
_LX_PREFETCH_RUNNING: set[str] = set()
_LX_PREFETCH_TASKS: set[asyncio.Task] = set()
# 2.6.53（树夏方案移植）：预取探测验证表。song_id -> (url, ts, status)。
# direct=手机口径裸探测通过 → 播放主路径跳过 inline 探测直接 307；
# relay=链活着但防盗链要源请求头 → 照常走中转；
# dead=链已确认失效（4xx）→ 记死链、后台重取；
# unknown=网络抖动/超时 → 不动缓存，下轮再验。
# 探测只打 CDN（Range 小请求），不消耗用户源取链配额。
_LX_URL_PROBE: dict[str, tuple[str, float, str]] = {}
_LX_PROBE_VERIFYING: set[str] = set()
_LX_PROBE_FRESH_S = float(os.environ.get("FNMUSIC_PREFETCH_PROBE_FRESH_S", "600"))
# 飞牛客户端自己维护播放队列，proxy 拿不到“下一首是谁”。退而求其次：
# 记住最近一次下发的歌单页顺序，播放当前首时就预取页内下一首的直链，
# 让切歌尽量命中已解析的 URL（对齐桌面端 prefetchNextTrack 的效果）。
_CERU_LAST_PAGE_GUIDS: list[str] = []
_CERU_PAGE_INDEX: dict[str, int] = {}
_CERU_PREFETCH_AT: dict[str, float] = {}
_CERU_PREFETCH_COOLDOWN_S = 120.0
# 向后预取几首：越大切歌越可能命中已解析直链，也越多后台取链。3 首是
# "切歌基本不等" 与 "不打爆公益源" 的折中；忙时服务端闸门会直接甩掉多余预取。
_CERU_PREFETCH_WINDOW = int(os.environ.get("FNMUSIC_PREFETCH_WINDOW", "3"))
# 按平台冷却：上游故障通常只影响单个平台（例如酷狗网关 ECONNRESET），
# 锁整个歌单会造成“整页连续跳歌”，所以只冷却出问题的平台，其余平台继续用。
_LX_PLATFORM_COOLDOWN: dict[str, float] = {}
_LX_PLATFORM_COOLDOWN_S = 10.0
# 单曲解析失败后的短时负缓存：切歌队列会在几秒内反复请求同一首，
# 没有负缓存就会把同一发失败请求连续打给上游，既慢又加速限流。
_LX_FAIL_COOLDOWN: dict[str, float] = {}
_LX_FAIL_COOLDOWN_S = 15.0
# 上游解析网关故障（连接重置/挂断）不是单曲无货，命中后短暂全局退避，
# 避免整张歌单在同一故障窗口里被逐首打穿。
_LX_UPSTREAM_FAULT_MARKERS = (
    "econnreset",
    "und_err_socket",
    "socket hang up",
    "connection reset",
    "fetch failed",
    "请求过于频繁",
    "too many requests",
)
# 服务端取链队列已满（503）的退避。503 是毫秒级返回的，代理若照旧「立刻换候选/
# 换档重试」就会雪崩：100 首并发起播实测队列被打到 20+ 深、26 首在 100ms 内吃 404，
# 完全是自己把队列撞爆，不是上游慢。所以：先等一小会儿原地补一枪，仍满则打开
# 全局 2s 短时退避（对所有取链收手），并且不再写单曲 15s 冷却——那不是无货。
_LX_QUEUE_RETRY_DELAY_S = 1.2
_LX_QUEUE_BACKOFF_S = 2.0
_LX_QUEUE_BACKOFF_UNTIL = 0.0
_BG_HEADER = "X-Fnmusic-Bg"
_COVER_VERSION_RE = re.compile(r":cover\d+$")
_COVER_ENTITY_VERSION_RE = re.compile(r":(?:album|artist):cover\d+$")


def _lx_platform_of(song_id: str) -> str:
    parts = str(song_id or "").split(":")
    return parts[2] if len(parts) >= 3 and parts[1] == "lx" else ""


def _lx_platform_cooled(song_id: str) -> bool:
    plat = _lx_platform_of(song_id)
    return bool(plat) and time.time() < _LX_PLATFORM_COOLDOWN.get(plat, 0.0)


# TOP[top.lx.health] 洛雪源健康度：成功/失败统计、平台冷却、替代源元数据
def _lx_health_report(song_id: str, ok: bool) -> None:
    src = _lx_platform_of(song_id)
    if not src:
        return
    h = _LX_PLATFORM_HEALTH.setdefault(src, {"ok": 0.0, "fail": 0.0, "last_fail": 0.0})
    if ok:
        h["ok"] = float(h.get("ok") or 0) + 1
    else:
        h["fail"] = float(h.get("fail") or 0) + 1
        h["last_fail"] = time.time()


def _lx_health_rank(song_id: str) -> float:
    src = _lx_platform_of(song_id)
    h = _LX_PLATFORM_HEALTH.get(src) or {}
    ok = float(h.get("ok") or 0)
    fail = float(h.get("fail") or 0)
    return fail / max(1.0, ok + fail)


def _load_lx_alt_meta() -> None:
    global _LX_ALT_META_LOADED
    if _LX_ALT_META_LOADED:
        return
    _LX_ALT_META_LOADED = True
    try:
        with open(_LX_ALT_META_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("items") if isinstance(data, dict) else None
        if isinstance(items, dict):
            _LX_ALT_META_CACHE.update({k: v for k, v in items.items() if isinstance(v, dict)})
    except Exception:
        pass


def _save_lx_alt_meta(force: bool = False) -> None:
    global _LX_ALT_META_SAVE_LAST
    now = time.monotonic()
    if not force and now - _LX_ALT_META_SAVE_LAST < 5.0:
        return
    _LX_ALT_META_SAVE_LAST = now
    try:
        os.makedirs(os.path.dirname(_LX_ALT_META_PATH) or ".", exist_ok=True)
        tmp = f"{_LX_ALT_META_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"savedAt": int(time.time()), "items": _LX_ALT_META_CACHE}, f, ensure_ascii=False)
        os.replace(tmp, _LX_ALT_META_PATH)
    except Exception as e:
        logger.debug("lx alt meta save failed: %s", e)


def _lx_alt_meta_for(guid: str) -> dict:
    _load_lx_alt_meta()
    return dict(_LX_ALT_META_CACHE.get(song_id_from_online_guid(guid)) or {})


def _load_ceru_lx_items() -> None:
    global _CERU_LX_ITEM_LOADED
    if _CERU_LX_ITEM_LOADED:
        return
    _CERU_LX_ITEM_LOADED = True
    try:
        with open(_CERU_LX_ITEM_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("items") if isinstance(data, dict) else None
        if isinstance(items, dict):
            _CERU_LX_ITEM_CACHE.update({k: v for k, v in items.items() if isinstance(v, dict)})
    except Exception:
        pass


def _save_ceru_lx_items(force: bool = False) -> None:
    global _CERU_LX_ITEM_SAVE_LAST
    now = time.monotonic()
    if not force and now - _CERU_LX_ITEM_SAVE_LAST < 5.0:
        return
    _CERU_LX_ITEM_SAVE_LAST = now
    try:
        os.makedirs(os.path.dirname(_CERU_LX_ITEM_PATH) or ".", exist_ok=True)
        tmp = f"{_CERU_LX_ITEM_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"savedAt": int(time.time()), "items": _CERU_LX_ITEM_CACHE}, f, ensure_ascii=False)
        os.replace(tmp, _CERU_LX_ITEM_PATH)
    except Exception as e:
        logger.debug("ceru lx item cache save failed: %s", e)


def _ceru_cached_lx_item(song: dict) -> dict:
    _load_ceru_lx_items()
    return dict(_CERU_LX_ITEM_CACHE.get(ceru.song_track_key(song)) or {})


def _ceru_store_lx_item(song: dict, item: dict) -> None:
    if not item or not item.get("id"):
        return
    _CERU_LX_ITEM_CACHE[ceru.song_track_key(song)] = dict(item)
    _save_ceru_lx_items()


def _ceru_forget_lx_item(song: dict) -> None:
    """丢弃校验不通过的旧映射（同歌手的另一首歌曾被错误缓存）。"""
    key = ceru.song_track_key(song)
    if _CERU_LX_ITEM_CACHE.pop(key, None) is not None:
        _save_ceru_lx_items()


def _ceru_resolve_item_for_song(song: dict) -> dict:
    """直连映射优先，其次使用通过一致性校验的持久化缓存。"""
    item = _ceru_item_for_song(song)
    if item and item.get("id"):
        return item
    cached = _ceru_cached_lx_item(song)
    if cached and cached.get("id"):
        if _ceru_item_matches_song(cached, song):
            return cached
        # 实测出现过“你怕啥子嘛”被缓存成同歌手的“我跪佛前问半生”。
        _ceru_forget_lx_item(song)
    return {}


def _lx_url_probe_ok(song_id: str, url: str) -> bool:
    """该直链是否被预取裸探测验证过（同 URL + direct + 新鲜窗口内）。"""
    if not song_id or not url:
        return False
    entry = _LX_URL_PROBE.get(song_id)
    if not entry:
        return False
    cached_url, ts, status = entry
    if status != "direct" or cached_url != url:
        return False
    return time.time() - ts < _LX_PROBE_FRESH_S


def _lx_prefetch_fresh(song_id: str) -> bool:
    """该曲是否可跳过预取：缓存里的链已被裸探测验证为可直接播且仍新鲜。"""
    entry = _LX_URL_PROBE.get(song_id or "")
    if not entry:
        return False
    cached = _LX_URL_CACHE.get(song_id)
    url = str((cached[0] or {}).get("url") or "") if cached else ""
    return (entry[2] == "direct" and bool(url) and entry[0] == url
            and time.time() - entry[1] < _LX_PROBE_FRESH_S)


def _prune_lx_probe_table() -> None:
    now = time.time()
    for key in [k for k, (_, ts, _) in _LX_URL_PROBE.items()
                if now - ts > _LX_PROBE_FRESH_S * 4]:
        _LX_URL_PROBE.pop(key, None)


def _lx_prefetch_mark_dead(song_id: str, url: str) -> None:
    """预取探测发现死链：只记死链 + 清 URL 缓存（下次取链走全新解析），
    不计流失败——预取探测 4xx 不等于真实断流，不触发音质降档。"""
    if not song_id or not url:
        return
    _LX_DEAD_URLS.setdefault(song_id, {})[url] = time.time()
    _LX_URL_CACHE.pop(song_id, None)


async def _prefetch_probe_verify(song_id: str, url: str,
                                 headers: "dict | None" = None) -> None:
    """预取后按手机口径裸请求验证直链并分级记录（只打 CDN，不消耗用户源配额）。

    - 裸 200/206 音频 → direct：播放主路径跳过 inline 探测直接 307；
    - 裸 404 → dead：链已失效，记死链，由调用方后台重取；
    - 裸 403/其他 4xx → 带源请求头复验，区分防盗链与死链：
      带头成功 → relay（链活着，手机需走中转）；带头 4xx → dead；
    - 超时/网络异常 → unknown：不动缓存，等下一轮再验。
    """
    if not song_id or not url or not CONF.get("wan_direct_enabled"):
        return
    if _lx_url_probe_ok(song_id, url):
        return
    if song_id in _LX_PROBE_VERIFYING:
        return
    _LX_PROBE_VERIFYING.add(song_id)
    try:
        if len(_LX_URL_PROBE) > 2000:
            _prune_lx_probe_table()
        ok, detail = await _wan_direct_probe(url, "bytes=0-1")
        now = time.time()
        if ok:
            _LX_URL_PROBE[song_id] = (url, now, "direct")
            return
        if detail.startswith("status=404"):
            status = "dead"
        elif detail.startswith("status=4"):
            ok2, detail2 = await _wan_direct_probe(url, "bytes=0-1",
                                                    headers=headers or None)
            if ok2:
                status = "relay"
            elif detail2.startswith("status=4"):
                status = "dead"
            else:
                status = "unknown"
        else:
            status = "unknown"
        _LX_URL_PROBE[song_id] = (url, now, status)
        if status == "dead":
            logger.info("lx prefetch probe dead url for %s (%s)", song_id, detail)
            _lx_prefetch_mark_dead(song_id, url)
    except Exception as e:  # noqa: BLE001
        logger.debug("lx prefetch probe failed for %s: %s", song_id, e)
    finally:
        _LX_PROBE_VERIFYING.discard(song_id)


async def _prefetch_lx_urls(song_ids: list[str]) -> None:
    sem = asyncio.Semaphore(2)

    async def one(song_id: str) -> None:
        if not song_id:
            return
        try:
            async with sem:
                resolved = await resolve_lx_url(get_lx_client(app), song_id, background=True)
                url = str((resolved or {}).get("url") or "")
                headers = (resolved or {}).get("headers") or {}
                if not url:
                    # 取链没排上（队列退避/冷却）：验证缓存里已有的链。
                    # 复验只打 CDN 不烧配额——长收听会话里链接保鲜就靠这一环。
                    cached = _LX_URL_CACHE.get(song_id)
                    if cached:
                        url = str((cached[0] or {}).get("url") or "")
                        headers = (cached[0] or {}).get("headers") or {}
                if url:
                    await _prefetch_probe_verify(song_id, url, headers)
                    if _LX_URL_PROBE.get(song_id, ("", 0.0, ""))[2] == "dead":
                        # 死链确认：后台重取 + 再验一轮（只一轮，不循环烧配额）
                        retry = await resolve_lx_url(get_lx_client(app), song_id, background=True)
                        retry_url = str((retry or {}).get("url") or "")
                        if retry_url:
                            await _prefetch_probe_verify(
                                song_id, retry_url, (retry or {}).get("headers") or {})
        except Exception:  # noqa: BLE001
            pass
        finally:
            _LX_PREFETCH_RUNNING.discard(song_id)

    try:
        await asyncio.gather(*(one(song_id) for song_id in song_ids), return_exceptions=True)
    finally:
        for song_id in song_ids:
            _LX_PREFETCH_RUNNING.discard(song_id)


# TOP[top.lx.prefetch] 播放地址后台预取 + 下一曲预热（压低首播延迟）
def schedule_lx_prefetch(song_ids: list[str]) -> None:
    pending: list[str] = []
    for song_id in song_ids:
        if not song_id or song_id in _LX_PREFETCH_RUNNING or _lx_prefetch_fresh(song_id):
            continue
        _LX_PREFETCH_RUNNING.add(song_id)
        pending.append(song_id)
    if not pending:
        return
    try:
        task = asyncio.get_running_loop().create_task(_prefetch_lx_urls(pending))
    except RuntimeError:
        return
    _LX_PREFETCH_TASKS.add(task)
    task.add_done_callback(_LX_PREFETCH_TASKS.discard)


def _remember_ceru_page(tracks: list[dict]) -> None:
    """记住最近下发的歌单页顺序，供“预取下一首”推断使用。"""
    global _CERU_LAST_PAGE_GUIDS, _CERU_PAGE_INDEX
    guids = [
        str(t.get("guid") or "")
        for t in tracks
        if str(t.get("guid") or "").startswith("online:lx:")
    ]
    if len(guids) < 2:
        return
    _CERU_LAST_PAGE_GUIDS = guids
    _CERU_PAGE_INDEX = {g: i for i, g in enumerate(guids)}


def _schedule_next_track_prefetch(guid: str, request: "Request | None" = None) -> None:
    """播放当前首时预热页内后续几首的直链，让切歌尽量命中已解析 URL。

    窗口取 3 首（安卓版也是提前解析播放队列里的后几首）：真实听歌同一时刻只有
    1-2 首要取链，队列几乎是空的，切歌直接命中缓存 → 起播接近秒开，也不会出现
    "按下下一首才开始排队" 的尾部超时。预取一律带后台标记，服务端忙时先甩它，
    绝不占用用户正在等的这一首。
    """
    idx = _CERU_PAGE_INDEX.get(str(guid or ""))
    if idx is None:
        return
    now = time.time()
    pending: list[str] = []
    for next_guid in _CERU_LAST_PAGE_GUIDS[idx + 1: idx + 1 + _CERU_PREFETCH_WINDOW]:
        if now < _CERU_PREFETCH_AT.get(next_guid, 0.0):
            continue
        song_id = song_id_from_online_guid(next_guid)
        if not song_id:
            continue
        _CERU_PREFETCH_AT[next_guid] = now + _CERU_PREFETCH_COOLDOWN_S
        pending.append(song_id)
    if pending:
        schedule_lx_prefetch(pending)
    # 2.6.54：预取只负责"取好直链"，预开再进一步——把紧邻下一首的流真正打开
    # （连上 CDN 并取到首包）。命中时切歌首包零等待；失败静默回落，不碰成功率。
    if request is not None and idx + 1 < len(_CERU_LAST_PAGE_GUIDS):
        try:
            asyncio.create_task(_preopen_next_track(request, _CERU_LAST_PAGE_GUIDS[idx + 1]))
        except RuntimeError:
            pass


def _remember_list_order(payload: "dict | None") -> None:
    """记住列表 payload 里在线曲目的顺序，供“预取下一首”推断。

    2.6.53：从 Ceru 歌单页扩到所有列表下发点（搜索/收藏/歌单页/专辑详情）。
    只认标准单列表形态 data.list / data.tracks；假 guid 统一反解回真实 guid；
    在线曲目不足 2 条不做（预取窗口需要至少两首才有“下一首”）。
    """
    if not isinstance(payload, dict):
        return
    data = payload.get("data")
    if not isinstance(data, dict):
        return
    tracks = data.get("list")
    if not isinstance(tracks, list):
        tracks = data.get("tracks")
    if not isinstance(tracks, list):
        return
    guids: list[str] = []
    seen: set[str] = set()
    for t in tracks:
        if not isinstance(t, dict):
            continue
        g = resolve_real_guid(str(t.get("guid") or ""))
        if g.startswith("online:") and g not in seen:
            seen.add(g)
            guids.append(g)
    if len(guids) < 2:
        return
    _remember_ceru_page([{"guid": g} for g in guids])


def _warm_page_head(tracks: "list | None", limit: int = 1) -> None:
    """打开列表页时预热页首 N 首在线曲（与既有 Ceru 歌单页同口径）：
    用一次后台额度换首播接近秒开；预取带后台标记，不挤占用户请求。
    """
    ids: list[str] = []
    for t in tracks or []:
        if not isinstance(t, dict):
            continue
        g = str(t.get("guid") or "") or online_guid_from_item(t)
        g = resolve_real_guid(g)
        if g.startswith("online:lx:") and len(ids) < limit:
            sid = song_id_from_online_guid(g)
            if sid and sid not in ids:
                ids.append(sid)
        if len(ids) >= limit:
            break
    if ids:
        schedule_lx_prefetch(ids)


# TOP[top.quality] 音质偏好（高音质/平衡/流畅）对质量梯的排序规则
def quality_order(ladder: "list[str] | tuple[str, ...]", mode: "str | None", primary: "str | None" = None) -> list[str]:
    """按音质模式排档序（ladder 从低到高）。

    high（默认）：锚定 primary（缺省取最高档）向下逐级，失败逐级降档；
    balanced：取中间档（偶数档取中间偏高），失败先向下再向上；
    smooth：从低到高，优先最省流量的档。
    """
    seq = [q for q in ladder if q]
    if not seq:
        return []
    mode = str(mode or "high").strip().lower()
    if mode == "smooth":
        return list(seq)
    if mode == "balanced":
        mid = len(seq) // 2
        return [seq[mid]] + list(reversed(seq[:mid])) + seq[mid + 1:]
    if primary in seq:
        idx = seq.index(primary)
    else:
        idx = len(seq) - 1
    return [seq[idx]] + list(reversed(seq[:idx]))


async def _lx_ladder_resolve(client: httpx.AsyncClient, song_id: str,
                             qualities: list, *, fresh: bool,
                             background: bool = False) -> "dict | None":
    """按档序逐档向 lxmusic-service 取链；fresh=1 时强制服务端跳过探活缓存。

    background=True 表示预取/预热这类「用户没在等」的取链：服务端会把它排到
    播放请求之后，队列积压时优先甩它。
    """
    global _LX_QUEUE_BACKOFF_UNTIL
    headers = {_BG_HEADER: "1"} if background else {}

    async def _attempt(q: str) -> tuple[str, "dict | None"]:
        """一档一枪：ok=拿到直链，busy=服务端排队已满，miss=这一档没货。"""
        params = {"id": song_id, "quality": q}
        if fresh:
            params["fresh"] = 1
        try:
            r = await client.get(
                "/api/v1/track/url",
                params=params,
                headers=headers,
                # 略高于 lxmusic 端点总预算（LX_URL_TIMEOUT，默认 20s）：让端点自己
                # 返回 404/502 完成降档缓存，而不是在 proxy 侧掐断后反复重解析
                timeout=22.0,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("resolve_lx_url error for %s (quality=%s): %s", song_id, q, e)
            return "miss", None
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:  # noqa: BLE001
                return "miss", None
            if isinstance(data, dict) and data.get("ok") is not False:
                inner = data.get("data")
                if isinstance(inner, dict) and inner.get("url"):
                    return "ok", inner
            return "miss", None
        if r.status_code == 503:
            return "busy", None
        # 洛雪用户源限流时后续档位也一定失败，立即停止并让上层换源/稍后重试，
        # 不再用 lossless→high→standard 连续撞三次把限流窗口打得更久。
        try:
            err_text = str((r.json() or {}).get("error") or "")
        except Exception:  # noqa: BLE001
            err_text = ""
        low = err_text.lower()
        if any(marker in low for marker in _LX_UPSTREAM_FAULT_MARKERS):
            logger.warning("lx rate limited for %s: %s", song_id, err_text)
            plat = _lx_platform_of(song_id)
            if plat:
                _LX_PLATFORM_COOLDOWN[plat] = time.time() + _LX_PLATFORM_COOLDOWN_S
            return "miss", None
        return "miss", None

    for q in qualities:
        if _lx_platform_cooled(song_id):
            break
        kind, data = await _attempt(q)
        if kind == "ok":
            return data
        if kind == "busy":
            # 队列满不是无货：等一轮再补一枪（队列浅时基本就过了）。
            await asyncio.sleep(_LX_QUEUE_RETRY_DELAY_S)
            kind2, data2 = await _attempt(q)
            if kind2 == "ok":
                return data2
            # 仍满：打开全局短时退避，让所有在途/后续取链一起收手，
            # 否则「秒失败→立刻重试」会把队列彻底打爆。
            _LX_QUEUE_BACKOFF_UNTIL = time.time() + _LX_QUEUE_BACKOFF_S
            logger.info("lx resolve queue still full for %s (quality=%s), backoff %.1fs",
                        song_id, q, _LX_QUEUE_BACKOFF_S)
            return None
    return None


# TOP[top.stream.resolve] 洛雪播放地址解析：质量梯选择、死链降档、失败冷却
async def resolve_lx_url(client: httpx.AsyncClient, song_id: str, *,
                         background: bool = False) -> "dict | None":
    """洛雪音乐源直链解析：song_id 形如 "lx:kg:<hash>"。"""
    global _LX_QUEUE_BACKOFF_UNTIL
    original_id = str(song_id or "")
    if time.time() < _LX_QUEUE_BACKOFF_UNTIL:
        # 全局取链退避窗口内不再撞队列（预取/预热/候选重试都在这一层收手）
        return None
    cached_hit = _LX_URL_CACHE.get(original_id)
    if cached_hit and time.time() - cached_hit[1] < _LX_URL_CACHE_TTL_S:
        # 2.6.39：缓存里那条链已被确认失效（死链排除，对齐安卓 failedUrls）→ 作废重取
        if str((cached_hit[0] or {}).get("url") or "") in _lx_dead_urls(original_id):
            _LX_URL_CACHE.pop(original_id, None)
        else:
            return dict(cached_hit[0])
    alt = _LX_ALT_CACHE.get(original_id)
    if alt and time.time() - alt[1] < _LX_ALT_CACHE_TTL_S:
        try:
            alt_resolved = await resolve_lx_url(client, alt[0], background=background)
        except Exception:
            alt_resolved = None
        if alt_resolved:
            _LX_URL_CACHE[original_id] = (dict(alt_resolved), time.time())
            return alt_resolved

    # 冷却检查必须放在“替代源尝试”之后：原平台挂了/原曲目刚失败过，
    # 不代表缓存里的替代曲目（另一个平台）也不可用。此前冷却在最前面，
    # 会把 alt 自救和 URL 缓存之后唯一的活路一起堵死，转码拿不到源就只能
    # 发 App 不认的假列表 → 拒收 → 跳歌 → 连环啪啪跳。
    if time.time() < _LX_FAIL_COOLDOWN.get(original_id, 0.0):
        return None
    if _lx_platform_cooled(song_id):
        return None

    # 对齐桌面端 getPlaybackQualityCandidates：一个音质档失败后逐档重试，
    # 而不是一次失败就让客户端跳歌。档序由管理页「音质偏好」决定：
    # 高音质=lossless(flac) 优先，失败降 high(320k) 再降 standard(128k)；
    # 平衡=320k 优先；流畅=128k 优先（128k 请求最省额度、CDN 最稳）。
    # 若失败原因是上游网关故障（循环内会打开平台冷却并立即中止），
    # 不去白烧剩余额度。
    # 2.6.39：该曲直链有失效记录时从更低档起步（对齐安卓播放报错→降档回退）。
    qualities = quality_order(_LX_QUALITY_LADDER, CONF.get("quality_mode"),
                              CONF.get("lx_quality"))
    steps = _lx_tier_downgrade_steps(original_id)
    if steps > 0 and len(qualities) > 1:
        qualities = qualities[steps:] + qualities[:steps]

    resolved = await _lx_ladder_resolve(client, song_id, qualities, fresh=False,
                                        background=background)
    if resolved and str(resolved.get("url") or "") in _lx_dead_urls(original_id):
        # 服务端探活缓存仍在下发同一死链：fresh=1 强制重解析一次
        #（等价安卓把 failedUrls 排除后重新取链）。
        logger.info("lx dead url still served upstream for %s, fresh re-resolve", original_id)
        fresh2 = await _lx_ladder_resolve(client, song_id, qualities, fresh=True,
                                          background=background)
        if fresh2 and str(fresh2.get("url") or "") not in _lx_dead_urls(original_id):
            resolved = fresh2
        else:
            # 强制重解析仍拿不到活链：本轮判失败进冷却，绝不把死链写回缓存
            # （否则每轮首播都要白烧两次取链才拿回同一条死链）。
            resolved = None
    if resolved:
        _LX_URL_CACHE[original_id] = (dict(resolved), time.time())
        _LX_FAIL_COOLDOWN.pop(original_id, None)
        _lx_health_report(original_id, True)
        _lx_reset_stream_fails(original_id)
        return resolved
    if time.time() < _LX_QUEUE_BACKOFF_UNTIL:
        # 排队没排上不是这首歌无货：只按退避窗口收手，既不写 15s 单曲冷却，
        # 也不给平台记失败（否则一次拥塞会把同平台后续歌曲全部误杀）。
        return None
    _LX_FAIL_COOLDOWN[original_id] = time.time() + _LX_FAIL_COOLDOWN_S
    _lx_health_report(original_id, False)
    return None


async def resolve_netease_url(client: httpx.AsyncClient, song_id: str) -> str | None:
    original_id = str(song_id or "")
    cache_key = (id(client), original_id)
    cached_hit = _NETEASE_URL_CACHE.get(cache_key)
    if cached_hit and time.time() - cached_hit[1] < _NETEASE_URL_CACHE_TTL_S:
        return cached_hit[0]

    primary = str(CONF.get("netease_quality") or "lossless").strip()
    qualities = quality_order(_NETEASE_QUALITY_LADDER, CONF.get("quality_mode"), primary)
    if primary and primary not in qualities:
        qualities.insert(0, primary)

    for q in qualities:
        try:
            r = await client.get(f"/api/v1/song/{original_id}/url", params={"quality": q}, timeout=10.0)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict) and data.get("ok") is not False:
                    inner = data.get("data")
                    if isinstance(inner, dict):
                        code = inner.get("code")
                        url = inner.get("url")
                        if code == 200 and url:
                            url = str(url)
                            _NETEASE_URL_CACHE[cache_key] = (url, time.time())
                            return url
        except Exception as e:
            logger.warning("resolve_netease_url error for %s (quality=%s): %s", song_id, q, e)
    return None


def ensure_search_list(upstream_json: dict) -> list:
    """保证 data.list 存在，本地 0 条时仍能追加在线条目。"""
    data = upstream_json.get("data")
    if not isinstance(data, dict):
        data = {}
        upstream_json["data"] = data
    target = get_by_path(upstream_json, CONF["search_list_path"])
    if isinstance(target, list):
        return target
    for key in ("list", "items", "tracks", "records"):
        if isinstance(data.get(key), list):
            if key != "list":
                data["list"] = data[key]
            return data["list"]
    data["list"] = []
    if "total" not in data:
        data["total"] = 0
    return data["list"]


def merge_online_tracks(
    upstream_json: dict,
    online_data: list[dict] | dict | None,
    page: int = 1,
    size: int = 50,
    selected: bool = False,
) -> dict:
    target_list = ensure_search_list(upstream_json)
    if not online_data:
        return upstream_json

    if isinstance(online_data, dict):
        raw_items = online_data.get("items", [])
    elif isinstance(online_data, list):
        raw_items = online_data
    else:
        raw_items = []

    if not raw_items:
        return upstream_json

    existing_keys = set()
    for item in target_list:
        t = title_from_track(item)
        a = artist_from_track(item)
        if t and a:
            existing_keys.add((t, a))

    online_limit = CONF["online_limit"]
    if not selected:
        start = 0 if page == 1 else online_limit + (page - 2) * size
        raw_page = raw_items[start:start + (online_limit if page == 1 else size)]
    else:
        raw_page = raw_items
    filtered_online = []
    for online_item in raw_page:
        if not is_playable_online_track(online_item, require_id=True):
            continue
        ot = str(online_item.get("title") or online_item.get("name") or "").strip().lower()
        oa = str(online_item.get("artist") or "").strip().lower()
        if ot and oa and (ot, oa) in existing_keys:
            continue
        filtered_online.append(online_item)

    page_online = filtered_online

    for it in page_online:
        target_list.append(build_online_track(it))

    parts = CONF["search_list_path"].split(".")
    parent = upstream_json
    for p in parts[:-1]:
        if isinstance(parent, dict) and p in parent:
            parent = parent[p]
    if isinstance(parent, dict):
        orig_total = parent.get("total")
        if not isinstance(orig_total, int):
            orig_total = len(target_list) - len(page_online)
        parent["total"] = orig_total + sum(1 for item in raw_items if is_playable_online_track(item, require_id=True) and (title_from_track(item), artist_from_track(item)) not in existing_keys)

    return upstream_json


_FAKE_GUID_REVERSE: dict[str, str] = {}
_REGISTRY_WARMED = False
_ONLINE_ID_RE = re.compile(r"online:[A-Za-z0-9_:\-]+")


# TOP[top.guid] 在线 guid 伪装/反查：客户端只见 32 位官方 id，服务端保持注册表
def fake_official_guid(real_guid: str) -> str:
    """在线 guid 确定性映射为官方 32-hex 形态。

    官方 App 会按 id 格式过滤条目（非 32-hex 的 online: 前缀 id 整条被丢弃，
    症状为收藏/播放历史列表空白或条目消失），且要求收藏/播放回读的 guid 与
    客户端自身持有的一致，因此所有下发的在线 id 必须统一伪装成官方形态。
    """
    fake = hashlib.md5(f"fnmusic-ext::{real_guid}".encode()).hexdigest()
    _FAKE_GUID_REVERSE.setdefault(fake, real_guid)
    return fake


# 专辑伪装 guid 登记表（issue #22）：fake 32-hex → 专辑元信息。
# 与 _FAKE_GUID_REVERSE 同生命周期（内存 + ensure_registry_warm 磁盘重建）；
# /search/album 的在线专辑锚点额外落盘持久化（这类锚点不存在于任何快照存储，
# 重启后 warm 重建不到，见 _persist_album_registry/_load_album_registry_persisted）。
_FAKE_ALBUM_REGISTRY: dict[str, dict] = {}
_ALBUM_REGISTRY_PATH = (
    os.environ.get("FNMUSIC_ALBUM_REGISTRY", "").strip()
    or os.path.join(_HOME, "album_registry.json")
)
_ALBUM_REGISTRY_PERSIST_MAX = 256
_ALBUM_REGISTRY_TTL_S = 14 * 86400
_ALBUM_PERSIST_MIN_INTERVAL_S = 5.0
_ALBUM_PERSIST_LAST = 0.0


def _album_name_from_obj(track_obj: dict) -> str:
    """从曲目对象（VO 快照或 info 形状）提取专辑名。"""
    album = track_obj.get("album")
    if isinstance(album, dict):
        return str(album.get("name") or "").strip()
    return str(track_obj.get("albumName") or track_obj.get("album") or "").strip()


def _person_name(value: Any) -> str:
    """歌手字段：字符串原样，对象取 name，列表取第一个有名字的成员。"""
    if isinstance(value, dict):
        return str(value.get("name") or "").strip()
    if isinstance(value, list):
        for item in value:
            name = _person_name(item)
            if name:
                return name
        return ""
    return str(value or "").strip()


def _tag_fields(info: dict | None) -> tuple[str, str, str]:
    """落盘/历史用的标题、歌手、专辑。恒为纯字符串。

    推荐歌单等前端曲目里 album 是 {name, guid, artists, coverId}，
    不能 str() 整个对象。对象 name 为空时再看 albumName。
    """
    src = info if isinstance(info, dict) else {}
    title_raw = src.get("title")
    if isinstance(title_raw, dict) or not str(title_raw or "").strip():
        title = _person_name(title_raw) or _person_name(src.get("name"))
    else:
        title = str(title_raw).strip()
    artist = _person_name(src.get("artist")) or _person_name(src.get("artists"))
    album = _album_name_from_obj(src) or str(src.get("albumName") or "").strip()
    return title, artist, album


def _cover_url_of(info: dict | None) -> str:
    src = info if isinstance(info, dict) else {}
    return str(src.get("cover_url") or src.get("coverUrl") or src.get("coverURL") or "").strip()


def _item_meta_richness(it: dict) -> int:
    """条目元数据丰富度：title/album 各 1 分。用于"更全的快照覆盖残缺的"合并。"""
    it = it if isinstance(it, dict) else {}
    return int(bool(str(it.get("title") or "").strip())) + int(bool(str(it.get("album") or "").strip()))


def _persist_album_registry(force: bool = False) -> None:
    """把内存专辑登记表落盘（节流 + 上限 + TTL），供重启后 ensure_registry_warm 恢复。

    只持久化 persist=True 的登记（/search/album 的在线专辑锚点与聚合代表曲目），
    而非全部曲目衍生登记——后者可从收藏/历史/推荐缓存重建，落盘只会膨胀。
    """
    global _ALBUM_PERSIST_LAST
    now = time.monotonic()
    if not force and now - _ALBUM_PERSIST_LAST < _ALBUM_PERSIST_MIN_INTERVAL_S:
        return
    _ALBUM_PERSIST_LAST = now
    try:
        os.makedirs(os.path.dirname(_ALBUM_REGISTRY_PATH) or ".", exist_ok=True)
        now_ts = int(time.time())
        entries = []
        for entry in _FAKE_ALBUM_REGISTRY.values():
            if not entry.get("persist"):
                continue
            entries.append({
                "track_guid": str(entry.get("track_guid") or ""),
                "source": str(entry.get("source") or ""),
                "album": str(entry.get("album") or ""),
                "album_id": str(entry.get("album_id") or ""),
                "item": entry.get("item") if isinstance(entry.get("item"), dict) else {},
                "saved_at": now_ts,
            })
        payload = {"version": 1, "savedAt": now_ts, "entries": entries[-_ALBUM_REGISTRY_PERSIST_MAX:]}
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(_ALBUM_REGISTRY_PATH) or ".", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(tmp, _ALBUM_REGISTRY_PATH)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
    except Exception as e:
        logger.debug("album registry persist failed: %s", type(e).__name__)


def _load_album_registry_persisted() -> int:
    """重启后从磁盘恢复 /search/album 登记的在线专辑（跨会话可反解，消除
    "重启后点旧搜索结果进专辑回落官方无此专辑"的窗口）。返回恢复条数。"""
    try:
        with open(_ALBUM_REGISTRY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0
    entries = data.get("entries") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        return 0
    cutoff = time.time() - _ALBUM_REGISTRY_TTL_S
    restored = 0
    for row in entries:
        if not isinstance(row, dict):
            continue
        track_guid = str(row.get("track_guid") or "")
        saved_at = row.get("saved_at")
        if not track_guid or (isinstance(saved_at, (int, float)) and saved_at < cutoff):
            continue
        fake = fake_official_guid(f"{track_guid}:album")
        if fake in _FAKE_ALBUM_REGISTRY:
            continue
        _FAKE_ALBUM_REGISTRY[fake] = {
            "source": str(row.get("source") or source_from_online_guid(track_guid)),
            "album": str(row.get("album") or ""),
            "track_guid": track_guid,
            "item": row.get("item") if isinstance(row.get("item"), dict) else {},
            "album_id": str(row.get("album_id") or ""),
            "persist": True,
        }
        restored += 1
    return restored


def register_fake_album(
    track_guid: str, album_name: str, item: dict | None = None, album_id: str = "", persist: bool = False
) -> None:
    """登记专辑伪装 guid → (source, 专辑名, 所属曲目条目)，供专辑详情拦截反解。

    曲目 VO 构造时同步登记（build_online_track 统一挂接，搜索/推荐/收藏/历史
    全路径覆盖）；假 id 是确定性 md5，重启后由 ensure_registry_warm 从收藏/
    历史/歌单附加/推荐缓存重建时同样登记，保证跨会话可反解。
    album_id：netease 真实专辑 id（/search/album 在线专辑直达详情用，可空）。
    persist：/search/album 的在线专辑登记传 True——这类锚点不落在任何持久快照里，
    重启后无法由 warm 重建，必须落盘（其余曲目衍生登记不需要，避免写放大）。
    """
    src = source_from_online_guid(track_guid)
    if not src:
        return
    fake = fake_official_guid(f"{track_guid}:album")
    entry = _FAKE_ALBUM_REGISTRY.get(fake)
    if entry is None:
        _FAKE_ALBUM_REGISTRY[fake] = {
            "source": src,
            "album": str(album_name or "").strip(),
            "track_guid": track_guid,
            "item": dict(item) if isinstance(item, dict) and item else {},
            "album_id": str(album_id or ""),
            "persist": bool(persist),
        }
        if persist:
            _persist_album_registry()
        return
    # 已登记：残缺快照可被更全的后到登记覆盖——首次解析失败时 build_metadata_payload
    # 会用 stub（仅 id/source，issue #28 真机复现）先占位，若"首个非空即终"，
    # 之后的全量信息永远进不来，专辑合成/封面/命名全部拿到残缺条目。
    if album_name and not entry.get("album"):
        entry["album"] = str(album_name).strip()
    if album_id and not entry.get("album_id"):
        entry["album_id"] = str(album_id)
    if isinstance(item, dict) and item:
        cur = entry.get("item") if isinstance(entry.get("item"), dict) else {}
        if _item_meta_richness(item) > _item_meta_richness(cur):
            entry["item"] = dict(item)
    if persist:
        # 更新路径同样尝试落盘（节流 5s/次；更全的专辑名/album_id/item 需要跟着持久化）
        if not entry.get("persist"):
            entry["persist"] = True
        _persist_album_registry()


def resolve_fake_album(candidate: str) -> dict | None:
    """客户端回传的专辑伪装 guid → 登记条目；非伪装专辑（含官方 guid）返回 None。"""
    if not candidate:
        return None
    fake = candidate[6:] if candidate.startswith("track_") else candidate
    if not re.fullmatch(r"[0-9a-f]{32}", fake or ""):
        return None
    ensure_registry_warm()
    entry = _FAKE_ALBUM_REGISTRY.get(fake)
    if entry is None:
        # 专辑登记丢失（如旧会话仅命中曲目反查表）：按确定性映射重建最小条目
        real = _FAKE_GUID_REVERSE.get(fake) or ""
        if real.endswith(":album") and is_online_guid(real):
            track_guid = real[: -len(":album")]
            entry = {
                "source": source_from_online_guid(track_guid),
                "album": "",
                "track_guid": track_guid,
                "item": {},
            }
            _FAKE_ALBUM_REGISTRY[fake] = entry
    return entry or None


def album_entry_from_real_guid(real_guid: str) -> "dict | None":
    """真实形态专辑 guid（"online:...:album"）→ 登记条目；封面拦截按 cover_url 直出用。"""
    raw = str(real_guid or "")
    if not raw.endswith(":album"):
        return None
    fake = fake_official_guid(raw)
    return _FAKE_ALBUM_REGISTRY.get(fake)


def _register_fakes_from_items(items) -> None:
    for it in items or []:
        if not isinstance(it, dict):
            continue
        g = str(it.get("guid") or "")
        nested = it.get("track") if isinstance(it.get("track"), dict) else None
        if is_online_guid(g):
            fake_official_guid(g)
            # 专辑假 id 一并登记（issue #22）：收藏/历史/歌单附加/推荐缓存里的
            # 快照带专辑名，重建时保住专辑详情的反解依据
            src_obj = nested if nested is not None else it
            register_fake_album(g, _album_name_from_obj(src_obj), _snapshot_to_info(src_obj))
        elif nested is not None:
            _register_fakes_from_items([nested])


def _iter_registry_jsons(directory: str):
    """列出目录下的 json 文件（含一层子目录，兼容 recommend_cache/<user>/x.json）。"""
    try:
        for name in sorted(os.listdir(directory)):
            path = os.path.join(directory, name)
            if name.endswith(".json") and os.path.isfile(path):
                yield path
            elif os.path.isdir(path):
                for sub in sorted(os.listdir(path)):
                    if sub.endswith(".json"):
                        yield os.path.join(path, sub)
    except Exception:
        return


def _register_fakes_from_recommend_bundle(data) -> None:
    if not isinstance(data, dict):
        return
    _register_fakes_from_items(data.get("tracks"))
    playlist = data.get("playlist")
    if isinstance(playlist, dict):
        for key in ("guid", "coverId"):
            g = str(playlist.get(key) or "")
            if is_online_guid(g):
                fake_official_guid(g)


def _register_fakes_from_plt(data) -> bool:
    """playlist_tracks 存储：items 是 {歌单guid: [条目]} 字典形状，逐桶注册曲目假 id。"""
    if isinstance(data, dict) and isinstance(data.get("items"), dict):
        for bucket in data["items"].values():
            _register_fakes_from_items(bucket)
        return True
    return False


def ensure_registry_warm() -> None:
    """从收藏/历史/歌单附加/推荐缓存重建 fake→real 映射（假 id 是确定性 md5，可完整重建）。

    服务重启后内存注册表为空，而客户端仍持有重启前学到的假 id；此时上报的
    播放/收藏事件若反解失败会被当作官方事件透传而丢失。收藏与历史存储里
    出现过的 guid 覆盖客户端会回传的曲目假 id；歌单附加条目同理；推荐缓存
    里的歌单/曲目 guid 覆盖客户端会回传的歌单封面假 id（歌单 coverId 也走伪装下发）。
    """
    global _REGISTRY_WARMED
    if _REGISTRY_WARMED:
        return
    _REGISTRY_WARMED = True
    try:
        for guid in ceru.registered_guids():
            if is_online_guid(guid):
                for suffix in (
                    "", ":album", ":artist",
                    ":cover2", ":album:cover2", ":artist:cover2",
                    ":cover3", ":album:cover3", ":artist:cover3",
                    ":cover4", ":album:cover4", ":artist:cover4",
                    ":cover5", ":album:cover5", ":artist:cover5",
                ):
                    fake_official_guid(f"{guid}{suffix}")
    except Exception as e:
        logger.debug("ceru guid registry warm failed: %s", e)
    for directory in (CONF.get("fav_dir") or os.path.join(_HOME, "online_favorites"),
                      CONF.get("plt_dir") or os.path.join(_HOME, "playlist_tracks"),
                      dailyrec.play_history_dir(),
                      dailyrec.recommend_cache_dir(),
                      nmpl.cache_dir()):
        for path in _iter_registry_jsons(directory):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue
            if isinstance(data, dict) and data.get("tracks") is not None:
                _register_fakes_from_recommend_bundle(data)
                continue
            if _register_fakes_from_plt(data):
                continue
            _register_fakes_from_items(data.get("items") if isinstance(data, dict) else data)
    # /search/album 的在线专辑锚点（不在任何快照存储里）从落盘登记恢复
    try:
        _load_album_registry_persisted()
    except Exception as e:
        logger.debug("album registry restore failed: %s", type(e).__name__)


def resolve_real_guid(candidate: str) -> str:
    if not candidate:
        return candidate
    if candidate in _FAKE_GUID_REVERSE:
        return _FAKE_GUID_REVERSE[candidate]
    if candidate.startswith("track_") and len(candidate) > 6:
        stripped = candidate[6:]
        if stripped in _FAKE_GUID_REVERSE:
            return _FAKE_GUID_REVERSE[stripped]
        if re.fullmatch(r"[0-9a-f]{32}", stripped):
            ensure_registry_warm()
            if stripped in _FAKE_GUID_REVERSE:
                return _FAKE_GUID_REVERSE[stripped]
    if re.fullmatch(r"[0-9a-f]{32}", candidate or ""):
        ensure_registry_warm()
        if candidate in _FAKE_GUID_REVERSE:
            return _FAKE_GUID_REVERSE[candidate]
    return candidate


def disguise_client_json(obj):
    """递归把客户端可见数据里的 online: 前缀 id 全部替换为官方 32-hex 形态。

    所有下发出口（搜索/元数据/歌词/收藏/历史/每日推荐曲目）统一伪装；
    入口（extract_guid/create/delete/event）经 resolve_real_guid 反解回真实 guid。
    coverId 按官方惯例带 track_ 前缀；字符串内嵌的 guid（如 audioSpec.path）
    一并替换。纯内存变换，不落盘、不触碰官方库，restore 后自然消失。
    """
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if isinstance(v, str) and v.startswith("online:"):
                if k in ("coverId", "cover_id"):
                    out[k] = "track_" + fake_official_guid(v)
                else:
                    out[k] = fake_official_guid(v)
            else:
                out[k] = disguise_client_json(v)
        return out
    if isinstance(obj, list):
        return [disguise_client_json(x) for x in obj]
    if isinstance(obj, str):
        return _ONLINE_ID_RE.sub(lambda m: fake_official_guid(m.group(0)), obj)
    return obj


def extract_guid(request: Request, path_guid: str | None = None) -> str:
    if path_guid:
        return resolve_real_guid(path_guid)
    return resolve_real_guid(
        request.query_params.get("guid")
        or request.query_params.get("trackGUID")
        or request.query_params.get("trackGuid")
        or request.query_params.get("coverId")
        or request.query_params.get("id")
        or request.query_params.get("trackId")
        or ""
    )


async def extract_guid_from_body(request: Request) -> str:
    guid = extract_guid(request)
    if guid:
        return guid
    try:
        body = await request.json()
    except Exception:
        return ""
    if isinstance(body, dict):
        return resolve_real_guid(str(
            body.get("guid")
            or body.get("trackGUID")
            or body.get("trackGuid")
            or body.get("id")
            or body.get("trackId")
            or ""
        ))
    return ""


def empty_ok() -> JSONResponse:
    return JSONResponse(content={"code": 0, "msg": "ok", "data": {}})


def build_lyric_list_payload(guid: str, lyric_text: str) -> dict:
    """对齐飞牛 $n.lyric.list → xr(list, preferred)。

    每条需有非空 content；source=2 表示 EXTERNAL_LRC（非内嵌，不强制 offset）。
    """
    text = (lyric_text or "").strip()
    if not text:
        return {"code": 0, "msg": "ok", "data": {"list": [], "preferred": ""}}
    lyric_guid = f"{guid}:lyric"
    now = int(time.time())
    item = {
        "guid": lyric_guid,
        "content": text,
        "source": 2,
        "isLRC": True,
        "offset": 0,
        "createdAt": now,
        "updatedAt": now,
    }
    return {
        "code": 0,
        "msg": "ok",
        "data": {"list": [item], "preferred": lyric_guid},
    }


def stub_online_info(guid: str) -> dict:
    song_id = song_id_from_online_guid(guid)
    return {
        "id": song_id,
        "source": source_from_online_guid(guid),
        "title": "",
        "artist": "",
        "album": "",
        "duration_s": 0,
        "ext": "mp3",
        "file_size": 0,
        "cover_url": "",
        "lyric": "",
    }


# TOP[top.metadata] 播放页元数据组装：isFavorite/coverId 与列表同口径（防星标闪/封面闪）
def build_metadata_payload(guid: str, data: dict | None) -> dict:
    """飞牛 resolveTrackPlayback._h() 会无防护读取 data.track.genres.join / album / artists。

    缺 genres 或 album 不是对象时直接抛错，播放器跳过且不会请求 stream。
    """
    info = dict(data or {})
    info.setdefault("id", song_id_from_online_guid(guid))
    info.setdefault("source", source_from_online_guid(guid))
    vo = build_online_track(info)
    album_obj = vo["album"] if isinstance(vo.get("album"), dict) else {
        "name": str(vo.get("album") or ""),
        "guid": f"{guid}:album",
        "artists": vo.get("artists") or [],
        "coverId": guid,
    }
    track = {
        "guid": guid,
        "id": guid,
        "title": vo.get("title") or "",
        "artists": vo.get("artists") or [],
        "album": album_obj,
        "genres": list(vo.get("genres") or []),
        "duration": vo.get("duration") or 0,
        "coverId": guid,
        "coverUrl": vo.get("coverUrl") or "",
        "format": vo.get("format") or "mp3",
        "hasLyric": bool(vo.get("hasLyric") or info.get("lyric")),
        "isFavorite": False,
        "isCue": False,
        "accessStatus": 0,
        "audioSpec": vo["audioSpec"],
    }
    return {
        "code": 0,
        "msg": "ok",
        "data": {
            **vo,
            "guid": guid,
            "id": guid,
            "album": album_obj,
            "audioSpec": vo["audioSpec"],
            "track": track,
        },
    }


def _conf_log_value(key: str, value: Any) -> Any:
    lowered = key.lower()
    if any(part in lowered for part in _REDACT_KEY_PARTS):
        return "***" if value else ""
    return value


def _background_jobs_enabled() -> bool:
    """后台任务只在真实服务环境启用（takeover 注入 FNMUSIC_BACKGROUND_JOBS=1）；
    默认关闭，单元测试与手动导入 app 永不触发，避免误扫真实曲库目录。
    """
    return os.environ.get("FNMUSIC_BACKGROUND_JOBS", "").lower() in ("1", "true", "yes")


_LAST_PLAY_AT = 0.0

# === 收藏封面/歌词全量补齐（限速、可续跑、补完即静默）===
# 缺什么补什么：已有封面磁盘缓存 / 已有 .lrc 的直接跳过，所以跑完一轮之后
# 每轮都接近零请求。官方接口额度仍由既有每日预算卡住（跨源搜索 600/天、
# 酷我按 ID 500/天），这里只按节奏喂，不会持续打接口；正在播放时自动避让。
_ASSET_SWEEP_ENABLED = os.environ.get("FNMUSIC_ASSET_SWEEP", "1").lower() in ("1", "true", "yes")
_ASSET_SWEEP_START_DELAY_S = float(os.environ.get("FNMUSIC_ASSET_SWEEP_DELAY", "75"))
_ASSET_SWEEP_GAP_S = float(os.environ.get("FNMUSIC_ASSET_SWEEP_GAP", "0.9"))
_ASSET_SWEEP_PLAY_AVOID_S = float(os.environ.get("FNMUSIC_ASSET_SWEEP_AVOID", "25"))
_ASSET_SWEEP_RESCAN_S = float(os.environ.get("FNMUSIC_ASSET_SWEEP_RESCAN", "21600"))


def _asset_sweep_state_path() -> str:
    return os.path.join(CONF.get("cache_dir") or _HOME, "asset_sweep.json")


def _background_request() -> "Request":
    """后台任务用的最小 Request：只需要 app / headers / query 这三样。"""
    scope = {
        "type": "http", "http_version": "1.1", "method": "GET", "path": "/internal",
        "raw_path": b"/internal", "query_string": b"", "root_path": "", "scheme": "http",
        "headers": [(b"host", b"127.0.0.1")], "client": ("127.0.0.1", 0),
        "server": ("127.0.0.1", 0), "app": app,
    }
    return Request(scope)


async def _asset_sweeper() -> None:
    while True:
        try:
            await asyncio.sleep(_ASSET_SWEEP_START_DELAY_S)
            state_path = _asset_sweep_state_path()
            state: dict = {}
            try:
                state = json.loads(open(state_path, encoding="utf-8").read()) or {}
            except Exception:  # noqa: BLE001
                state = {}
            index = await _ceru_favorites_index()
            songs = [s for s in (index.get("by_key") or {}).values() if isinstance(s, dict)]
            if not songs:
                await asyncio.sleep(120)
                continue
            cursor = int(state.get("cursor") or 0)
            if cursor >= len(songs):
                # 这一轮全查过了：只在真正过了复扫间隔后才从头再来（增量补齐新歌）
                if time.time() - float(state.get("finishedAt") or 0.0) < _ASSET_SWEEP_RESCAN_S:
                    await asyncio.sleep(300)
                    continue
                cursor = 0
            request = _background_request()
            filled = 0
            for pos in range(cursor, len(songs)):
                if time.time() - _LAST_PLAY_AT < _ASSET_SWEEP_PLAY_AVOID_S:
                    await asyncio.sleep(_ASSET_SWEEP_PLAY_AVOID_S)  # 用户在听：让路
                song = songs[pos]
                try:
                    track = _ceru_light_track(song)
                except Exception:  # noqa: BLE001
                    track = None
                guid = str((track or {}).get("guid") or "")
                pos_next = pos + 1
                if not guid.startswith("online:lx:"):
                    continue
                need_cover = _read_cover_cache(guid) is None
                need_lyric = not read_lyric_cache(guid)
                if need_cover or need_lyric:
                    _preheat_track_assets(request, guid)
                    filled += 1
                    await asyncio.sleep(_ASSET_SWEEP_GAP_S)
                if pos_next % 25 == 0:
                    state.update({"cursor": pos_next, "updatedAt": int(time.time())})
                    try:
                        tmp = f"{state_path}.tmp"
                        with open(tmp, "w", encoding="utf-8") as fh:
                            json.dump(state, fh)
                        os.replace(tmp, state_path)
                    except OSError:
                        pass
            state.update({"cursor": len(songs), "finishedAt": int(time.time())})
            try:
                tmp = f"{state_path}.tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(state, fh)
                os.replace(tmp, state_path)
            except OSError:
                pass
            logger.info("收藏封面/歌词补齐扫描完成一轮：%d 首入队补齐，共 %d 首", filled, len(songs))
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.warning("asset sweeper failed: %s", e)
            await asyncio.sleep(600)


async def _lyric_orphan_sweeper() -> None:
    """启动即清一次、之后每 30 分钟复扫：清掉本插件写进曲库、音频已消失的孤儿
    .lrc（安全边界见 cache_gc.sweep_orphan_lyrics，用户自有歌词永不触碰）。
    """
    while True:
        try:
            dirs: list[str] = []
            for d in (tee_save_dir(), detect_library_dir()):
                if d and d not in dirs and d != CONF["cache_dir"]:
                    dirs.append(d)
            removed = await asyncio.to_thread(sweep_orphan_lyrics, CONF["cache_dir"], dirs)
            if removed:
                logger.info("orphan lyric sweep removed %d file(s): %s", len(removed), removed)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("orphan lyric sweep failed: %s", e)
        await asyncio.sleep(1800)


@asynccontextmanager
# TOP[top.proxy] 代理进程生命周期：启动自检、env 热重载循环、后台清扫任务
async def lifespan(fastapi_app: FastAPI):
    global _PROBE_CLIENT
    logger.info("=== fnmusic-ext v%s configuration ===", get_version())
    for k, v in CONF.items():
        logger.info("  %s = %s", k, _conf_log_value(k, v))
    logger.info("  llm_enabled = %s", dailyrec.llm_enabled())
    logger.info("==================================")

    sweeper_task = asyncio.create_task(_lyric_orphan_sweeper()) if _background_jobs_enabled() else None
    asset_task = asyncio.create_task(_asset_sweeper()) if (
        _background_jobs_enabled() and _ASSET_SWEEP_ENABLED
    ) else None
    env_task = asyncio.create_task(_env_watch_loop()) if (
        _background_jobs_enabled() and CONF.get("env_watch", True)
    ) else None
    ceru_task = asyncio.create_task(ceru.sync_poll_loop()) if (
        _background_jobs_enabled() and _ceru_on()
    ) else None
    created_upstream = False
    created_musicdl = False
    created_musicbox = False
    created_lx = False
    created_llm = False

    if getattr(fastapi_app.state, "upstream_client", None) is None:
        fastapi_app.state.upstream_client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=CONF["upstream_sock"]),
            base_url="http://unix",
            timeout=30.0,
        )
        created_upstream = True

    if getattr(fastapi_app.state, "musicdl_client", None) is None:
        fastapi_app.state.musicdl_client = httpx.AsyncClient(
            base_url=CONF["musicdl_url"],
            timeout=45.0,
        )
        created_musicdl = True

    if getattr(fastapi_app.state, "musicbox_client", None) is None:
        fastapi_app.state.musicbox_client = httpx.AsyncClient(
            base_url=CONF["musicbox_url"],
            timeout=20.0,
        )
        created_musicbox = True

    if getattr(fastapi_app.state, "lx_client", None) is None:
        fastapi_app.state.lx_client = httpx.AsyncClient(
            base_url=CONF["lx_url"],
            timeout=25.0,
        )
        created_lx = True

    if getattr(fastapi_app.state, "llm_client", None) is None:
        fastapi_app.state.llm_client = httpx.AsyncClient(timeout=dailyrec.LLM_TIMEOUT_S)
        created_llm = True

    try:
        yield
    finally:
        tasks = [entry["task"] for entry in _SEARCH_CACHE.values() if entry.get("task") and not entry["task"].done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if sweeper_task:
            sweeper_task.cancel()
            await asyncio.gather(sweeper_task, return_exceptions=True)
        if asset_task:
            asset_task.cancel()
            await asyncio.gather(asset_task, return_exceptions=True)
        if env_task:
            env_task.cancel()
            await asyncio.gather(env_task, return_exceptions=True)
        if ceru_task:
            ceru_task.cancel()
            await asyncio.gather(ceru_task, return_exceptions=True)
        await ceru.close()
        try:
            await tc.shutdown_all()
        except Exception:  # noqa: BLE001
            pass
        if created_upstream and getattr(fastapi_app.state, "upstream_client", None):
            await fastapi_app.state.upstream_client.aclose()
            fastapi_app.state.upstream_client = None
        if created_musicdl and getattr(fastapi_app.state, "musicdl_client", None):
            await fastapi_app.state.musicdl_client.aclose()
            fastapi_app.state.musicdl_client = None
        if created_musicbox and getattr(fastapi_app.state, "musicbox_client", None):
            await fastapi_app.state.musicbox_client.aclose()
            fastapi_app.state.musicbox_client = None
        if created_lx and getattr(fastapi_app.state, "lx_client", None):
            await fastapi_app.state.lx_client.aclose()
            fastapi_app.state.lx_client = None
        if _PROBE_CLIENT is not None and not _PROBE_CLIENT.is_closed:
            await _PROBE_CLIENT.aclose()
        _PROBE_CLIENT = None
        if created_llm and getattr(fastapi_app.state, "llm_client", None):
            await fastapi_app.state.llm_client.aclose()
            fastapi_app.state.llm_client = None


app = FastAPI(title="fnmusic-ext", lifespan=lifespan)


@app.middleware("http")
async def log_client_requests(request: Request, call_next):
    """记录 /music/ 请求的方法/路径/状态/UA，供 App 端兼容问题远程定位。

    不记 query（guid 无必要），UA 折叠空白并截断，配合 takeover 日志白名单的固定格式。
    """
    response = await call_next(request)
    if request.url.path.startswith("/music/"):
        ua = re.sub(r"\s+", " ", request.headers.get("user-agent") or "-")[:100]
        logger.info(
            "client request %s %s status=%s ua=%s",
            request.method,
            request.url.path,
            response.status_code,
            ua,
        )
    return response


@app.middleware("http")
async def no_store_dynamic_api(request: Request, call_next):
    """动态 API 响应统一 no-store。

    飞牛远程访问网关（以及任何中间层/客户端 HTTP 缓存）对没有 Cache-Control
    的 GET 响应可能按默认策略缓存：升级后内网直连拿新数据、外网拿旧缓存，
    表现为同一歌单内外网显示不一致（如标星/曲目变化不生效）。音频流与封面
    响应均已显式设置缓存头，不受影响。"""
    response = await call_next(request)
    if (
        request.method == "GET"
        and request.url.path.startswith("/music/api/v1/")
        and "cache-control" not in response.headers
    ):
        response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/_ext/livez")
async def ext_livez():
    return {"ok": True, "service": "fnmusic-ext", "pid": os.getpid()}


@app.get("/_ext/healthz")
async def ext_healthz(request: Request):
    async def probe(name: str, client: httpx.AsyncClient, path: str) -> dict:
        try:
            response = await asyncio.wait_for(client.get(path, timeout=2.0), timeout=2.4)
            healthy = response.status_code < 500 if name == "upstream" else response.status_code == 200
            detail: dict = {"status": "ok" if healthy else "fail", "http_status": response.status_code}
            if name != "upstream" and healthy:
                try:
                    payload = response.json()
                    if isinstance(payload, dict):
                        detail["dependency"] = payload
                        if payload.get("ok") is False:
                            detail["status"] = "fail"
                    else:
                        detail["status"] = "fail"
                except ValueError:
                    detail["status"] = "fail"
                    detail["error"] = "invalid health JSON"
            return detail
        except Exception as exc:
            return {"status": "fail", "error": type(exc).__name__}

    checks = [("upstream", True, get_upstream_client, "/music/api/v1/search/track?keyword=healthz_probe"),
              ("musicdl", CONF.get("musicdl_enabled"), get_musicdl_client, "/healthz"),
              ("musicbox", CONF.get("netease_enabled"), get_musicbox_client, "/healthz"),
              ("lxmusic", CONF.get("lx_enabled"), get_lx_client, "/healthz")]
    enabled = [(name, getter, path) for name, on, getter, path in checks if on]
    results = await asyncio.gather(*(probe(name, getter(request.app), path) for name, getter, path in enabled))
    details = {name: {"status": "disabled"} for name, on, _, _ in checks if not on}
    details.update({name: result for (name, _, _), result in zip(enabled, results)})
    statuses = {name: value["status"] for name, value in details.items()}
    failed = [name for name, status in statuses.items() if status == "fail"]
    source_ok = any(statuses[name] == "ok" for name in ("musicdl", "musicbox", "lxmusic"))
    return {"ok": statuses["upstream"] == "ok" and source_ok, "version": get_version(),
            **statuses, "llm": "enabled" if dailyrec.llm_enabled() else "disabled",
            "recommend": {
                "mode": "per-user: source-slot -> llm -> local-random",
                "netease": bool(CONF.get("netease_enabled")),
                "lx": bool(CONF.get("lx_enabled")),
                "llm": "enabled" if dailyrec.llm_enabled() else "disabled",
                "source_slot_claimed": dailyrec.source_slot_claimed(),
                "recent": dailyrec.last_recommend_summary(),
            },
            "degraded": bool(failed), "failures": failed, "details": details}


@app.get("/_ext/ceru/status")
# TOP[top.ceru.status] 多端同步状态接口（WebUI 同步页数据源）
async def ext_ceru_status():
    """Ceru 同步接入自检（Unix socket 可直接 curl，供排障确认数据已拉取）。"""
    enabled_flag = bool(CONF.get("ceru_sync_enabled"))
    if not _ceru_on():
        return {
            "ok": False,
            "enabled": enabled_flag,
            "host": CONF.get("ceru_sync_host") or "",
            "reason": "ceru sync not enabled or host not configured",
        }
    try:
        playlists = await ceru.list_playlists()
        favorites = await ceru.list_favorites()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "enabled": True, "error": str(exc)}
    return {
        "ok": True,
        "enabled": True,
        "host": CONF.get("ceru_sync_host") or "",
        "playlists": len(playlists),
        "favorites": len(favorites.get("song") or []),
        "titles": [str(p.get("title") or "") for p in playlists[:8]],
    }


@app.get("/music/api/v1/search/track")
@app.get("/music/api/v1/search/track/{subpath:path}")
# TOP[top.search] 搜索聚合主入口：本地曲库优先 + 音源窗口分页 + 可播过滤
async def search_track(request: Request):
    upstream_client = get_upstream_client(request.app)
    musicdl_client = get_musicdl_client(request.app)
    musicbox_client = get_musicbox_client(request.app)
    keyword = extract_keyword(request)
    if keyword:
        _note_user_keyword(_credential_scope(request), keyword)

    page_str = request.query_params.get("page")
    try:
        page = int(page_str) if page_str else 1
    except (TypeError, ValueError):
        page = 1
    if page < 1:
        page = 1

    size_str = request.query_params.get("size")
    try:
        size = int(size_str) if size_str else 50
    except (TypeError, ValueError):
        size = 50
    if size < 1:
        size = 50

    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)

    req = upstream_client.build_request("GET", url_path, headers=headers)
    upstream_resp = await upstream_client.send(req)

    resp_headers = filter_headers(upstream_resp.headers, exclude_keys={"content-length", "content-encoding"})

    if upstream_resp.status_code != 200:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    try:
        upstream_json = upstream_resp.json()
    except Exception:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    if not isinstance(upstream_json, dict) or upstream_json.get("code") != 0:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    if not keyword:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    scope = _credential_scope(request)
    if _USER_SEARCH_WORD.get(scope) != keyword:
        return JSONResponse(content=disguise_client_json(upstream_json), status_code=upstream_resp.status_code, headers=resp_headers)

    key = _search_scope(request) + ":" + keyword
    _clean_search_cache()
    entry = _SEARCH_CACHE.get(key)
    if entry is None:
        entry = {"items": [], "ts": 0, "keyword": keyword,
                 "scope": _search_scope(request), "credentials": scope, "config": _source_config(), "task": None}
        _set_search_cache(key, entry)
    entry["gen"] = _USER_SEARCH_GEN.get(scope, 0)
    entry["superseded"] = False
    entry["accessed"] = time.time()
    task = entry.get("task")
    if (not task or task.done()) and time.time() - entry["ts"] >= _search_ttl(entry):
        task = asyncio.create_task(_aggregate_search(request, keyword, entry))
        entry["task"] = task
    # 在线搜索只有一个超时。有在线条目就立刻回复（本地在前、在线在后）；
    # 到点仍没有，就放弃这次还没回来的结果，只交本地列表。
    if task and not task.done():
        budget = max(0.05, float(CONF["search_timeout"])) + max(0.0, float(CONF.get("search_debounce_s") or 0))
        deadline = asyncio.get_running_loop().time() + budget
        while not task.done() and asyncio.get_running_loop().time() < deadline:
            if entry.get("items"):
                break
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            await asyncio.wait({task}, timeout=min(0.05, remaining))
        if not entry.get("items") and not task.done():
            entry["abandoned"] = True
            entry["items"] = []
            entry["partial"] = True
            entry["ts"] = time.time()
            task.cancel()
    local_list = ensure_search_list(upstream_json)
    local_keys = {(title_from_track(x), artist_from_track(x)) for x in local_list}
    total_online = sum(1 for x in entry["items"] if (title_from_track(x), artist_from_track(x)) not in local_keys)
    original_total = upstream_json.get("data", {}).get("total", len(local_list))
    if not isinstance(original_total, int):
        original_total = len(local_list)
    # 官方搜索越界页不返回空列表而是钳制回第 1 页（total=4 时 page=2 仍返回
    # 同样 4 条，收藏/歌单条目接口无此行为）。本页全局起点已越过官方段时必须
    # 清空官方列表，否则官方条目会拼上在线切片在每个后续页重复出现。
    if (page - 1) * size >= original_total and local_list:
        local_list.clear()
    # 官方搜索忽略 size 全量返回结果集（total=11 时 size=10 的 page=1/2 均返回
    # 同样 11 条，2026-09-25 官方更新实测）：返回条数超过单页窗口说明拿到的是
    # 全量列表而非请求页，按请求窗口原地切片，保证本地段全局分页不跨页重复。
    if len(local_list) > size:
        win_start = (page - 1) * size
        local_list[:] = local_list[win_start:win_start + size]
    # 本地优先全局布局：本地条目占据全局前 local_total 位，在线条目紧随其后。
    # 本页在线切片 = 全局分页区间与在线区间的交集；纯本地页（区间未触及在线段）
    # 在线切片为空，上游结果原样透传，只有 total 计入在线条数驱动客户端继续翻页。
    selected = _online_window(entry, page, size, original_total)
    merged = merge_online_tracks(upstream_json, selected, page=1, size=size, selected=True)
    merged["data"]["total"] = original_total + total_online
    logger.info("search debug: kw=%s page=%s size=%s official_total=%s online_items=%s online_total=%s final_total=%s",
                keyword, page, size, original_total, len(entry["items"]), total_online, merged["data"]["total"])
    fav_set = await _online_favorite_set(request)
    if fav_set and isinstance(merged.get("data"), dict) and isinstance(merged["data"].get("list"), list):
        for it in merged["data"]["list"]:
            if isinstance(it, dict) and str(it.get("guid") or "") in fav_set:
                it["isFavorite"] = True
    _remember_list_order(merged)
    _warm_page_head(selected)
    return JSONResponse(content=disguise_client_json(merged), status_code=upstream_resp.status_code, headers=resp_headers)


async def _aggregate_search(request: Request, keyword: str, entry: dict) -> None:
    scope = entry.get("credentials") or _credential_scope(request)
    if _user_search_stale(entry, scope):
        entry["items"] = []
        entry["superseded"] = True
        entry["ts"] = 0
        return
    window = await _wait_search_debounce(scope)
    if _user_search_stale(entry, scope) or (window is not None and _SEARCH_DEBOUNCE.get(scope) is not window):
        entry["items"] = []
        entry["superseded"] = True
        entry["ts"] = 0
        return
    token = _FETCH_SCOPE.set(scope)
    sources = []
    if CONF.get("netease_enabled"):
        sources.append(fetch_musicbox_search(get_musicbox_client(request.app), keyword, CONF["netease_search_limit"]))
    if CONF.get("musicdl_enabled"):
        sources.append(fetch_musicdl_search(get_musicdl_client(request.app), keyword, CONF["online_limit"]))
    if CONF.get("lx_enabled"):
        sources.append(fetch_lx_search(get_lx_client(request.app), keyword, CONF["lx_search_limit"]))
    tasks = [asyncio.create_task(coro) for coro in sources]
    pending = set(tasks)
    partial = False
    results: dict[asyncio.Task, list] = {}
    try:
        deadline = asyncio.get_running_loop().time() + max(0.05, float(CONF["search_timeout"]))
        while pending and not _user_search_stale(entry, scope):
            done, pending = await asyncio.wait(pending, timeout=max(0, deadline - asyncio.get_running_loop().time()), return_when=asyncio.FIRST_COMPLETED)
            if not done:
                partial = True
                break
            for task in tasks:
                if task not in done:
                    continue
                try:
                    data = task.result()
                except Exception:
                    data = None
                if entry.get("abandoned") or data is _SUPERSEDED or _user_search_stale(entry, scope):
                    if data is _SUPERSEDED or _user_search_stale(entry, scope):
                        entry["superseded"] = True
                    entry["items"] = []
                    results[task] = []
                    continue
                partial |= data is None or bool(getattr(data, "partial", False)) or (isinstance(data, dict) and bool(data.get("errors") or data.get("ok") is False))
                items = data.get("items", []) if isinstance(data, dict) else (data or [])
                results[task] = items
                if not entry["items"]:
                    ordered = [item for source_task in tasks for item in results.get(source_task, [])]
                else:
                    ordered = entry["items"] + items
                entry["items"] = deduplicate_online_items(ordered)[:2000]
        if entry.get("abandoned") or _user_search_stale(entry, scope):
            entry["items"] = []
            if _user_search_stale(entry, scope):
                entry["superseded"] = True
                entry["ts"] = 0
            elif not entry.get("ts"):
                entry["partial"] = True
                entry["ts"] = time.time()
            return
        entry["partial"] = partial
        # 全源失败（source busy/熔断/异常）交回的 0 条不是真"无结果"：不落缓存时间戳，
        # 同词紧跟着重搜会立即重新聚合，而不是吃 10 秒空缓存一直返回空。
        entry["ts"] = 0 if partial and not entry.get("items") else time.time()
    finally:
        _FETCH_SCOPE.reset(token)
        for task in tasks:
            if not task.done():
                task.cancel()
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            if entry.get("abandoned"):
                entry["items"] = []
                entry["partial"] = True
                if not entry.get("ts"):
                    entry["ts"] = time.time()


def _online_window(entry: dict, page: int, size: int, local_total: int) -> list[dict]:
    """本地优先布局下取本页的在线切片。

    全局布局：[本地 0..local_total) [在线 local_total..local_total+len(items))。
    本页全局区间 = [(page-1)*size, page*size)；与在线段求交集后映射到 items 下标。
    items 只追加不重排，同一 (page,size,local_total) 的切片稳定；items 后续
    增长只会让更靠后的页多出条目（已返回页的前缀不动）。
    """
    start_global = (page - 1) * size
    end_global = page * size
    start = max(0, start_global - local_total)
    end = max(0, end_global - local_total)
    if start >= end:
        return []
    window = [item for item in entry["items"][start:end] if _source_enabled(online_guid_from_item(item))]
    return window


@app.get("/music/api/v1/search/suggest")
@app.get("/music/api/v1/search/suggest/{subpath:path}")
async def search_suggest(request: Request):
    if not CONF["merge_suggest"]:
        return await forward_to_upstream(request, get_upstream_client(request.app))

    upstream_client = get_upstream_client(request.app)
    musicdl_client = get_musicdl_client(request.app)
    keyword = extract_keyword(request)

    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)

    musicdl_task: asyncio.Task | None = None
    suggest_scope = _FETCH_SCOPE.set(_credential_scope(request) + ":suggest")
    if keyword and CONF.get("musicdl_enabled"):
        musicdl_task = asyncio.create_task(fetch_musicdl_search(musicdl_client, keyword, 5))
    _FETCH_SCOPE.reset(suggest_scope)

    req = upstream_client.build_request("GET", url_path, headers=headers)
    upstream_resp = await upstream_client.send(req)
    resp_headers = filter_headers(upstream_resp.headers, exclude_keys={"content-length", "content-encoding"})

    if upstream_resp.status_code != 200:
        if musicdl_task:
            musicdl_task.cancel()
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    try:
        upstream_json = upstream_resp.json()
    except Exception:
        if musicdl_task:
            musicdl_task.cancel()
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    if not isinstance(upstream_json, dict) or upstream_json.get("code") != 0:
        if musicdl_task:
            musicdl_task.cancel()
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    musicdl_data = None
    if musicdl_task:
        try:
            musicdl_data = await asyncio.wait_for(asyncio.shield(musicdl_task), timeout=10.0)
        except Exception as e:
            logger.warning("Suggest musicdl error: %s", e)
            musicdl_task.cancel()

    data_field = upstream_json.get("data")
    if isinstance(data_field, list) and isinstance(musicdl_data, dict) and musicdl_data.get("items"):
        for item in musicdl_data.get("items", [])[:5]:
            title = item.get("title")
            if title and title not in data_field:
                data_field.append(title)

    return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)


def _lookup_online_snapshot(guid: str) -> "dict | None":
    """按 guid 在在线播放历史/在线收藏的用户快照里反查曲目元数据（issue #28）。

    两个存储都按用户分文件且量小（历史 ≤500 条/用户），tee 转正是低频路径，
    顺序扫描可接受；命中即返回含 title/artist/album 的快照 dict。
    """
    # ① 在线播放历史（play_history/<user>.json，条目最新的在末尾，倒序找）
    try:
        root = dailyrec.play_history_dir()
        for name in sorted(os.listdir(root)):
            if not name.endswith(".json"):
                continue
            try:
                with open(os.path.join(root, name), encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue
            items = data.get("items") if isinstance(data, dict) else data
            if not isinstance(items, list):
                continue
            for it in reversed(items):
                if isinstance(it, dict) and it.get("guid") == guid:
                    snap = it.get("track") if isinstance(it.get("track"), dict) else None
                    if snap and (snap.get("title") or snap.get("artist")):
                        return snap
    except OSError:
        pass
    # ② 在线收藏（online_favorites/<user>.json，形状同收藏下发条目）
    try:
        fav_root = CONF.get("fav_dir") or os.path.join(_HOME, "online_favorites")
        for name in sorted(os.listdir(fav_root)):
            if not name.endswith(".json"):
                continue
            try:
                with open(os.path.join(fav_root, name), encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue
            items = data.get("items") if isinstance(data, dict) else data
            if not isinstance(items, list):
                continue
            for it in reversed(items):
                if isinstance(it, dict) and it.get("guid") == guid:
                    snap = it.get("track") if isinstance(it.get("track"), dict) else None
                    if snap and (snap.get("title") or snap.get("artist")):
                        return snap
    except OSError:
        pass
    # ③ 歌单附加条目（playlist_tracks/<user>.json，{"items": {歌单guid: [条目]}}）：
    # 只加过歌单、从未收藏/播放的歌也要能反查（官方绑定与 tee 兜底的元数据来源）
    try:
        plt_root = plt_dir()
        for name in sorted(os.listdir(plt_root)):
            if not name.endswith(".json"):
                continue
            try:
                with open(os.path.join(plt_root, name), encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue
            buckets = data.get("items") if isinstance(data, dict) else None
            if not isinstance(buckets, dict):
                continue
            for bucket in buckets.values():
                if not isinstance(bucket, list):
                    continue
                for it in reversed(bucket):
                    if isinstance(it, dict) and it.get("guid") == guid:
                        snap = it.get("track") if isinstance(it.get("track"), dict) else None
                        if snap and (snap.get("title") or snap.get("artist")):
                            return snap
    except OSError:
        pass
    return None


def _track_in_list(tracks: Any, guid: str) -> dict | None:
    if not isinstance(tracks, list):
        return None
    for track in tracks:
        if not isinstance(track, dict) or str(track.get("guid") or "") != guid:
            continue
        title, artist, _album = _tag_fields(track)
        if title or artist:
            return track
    return None


def _lookup_playlist_cache_track(guid: str) -> dict | None:
    """推荐歌单、网易账号歌单的磁盘缓存按 guid 反查下发曲目。

    这两类列表不进搜索缓存。缓存里的曲目是前端形态（album 为对象），
    调用方必须用 _tag_fields 取标签，不能 str(album)。
    """
    if not guid:
        return None
    for root in (dailyrec.recommend_cache_dir(), nmpl.cache_dir()):
        for path in _iter_registry_jsons(root):
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            hit = _track_in_list(data.get("tracks"), guid)
            if hit:
                return hit
    return None


def _merge_missing_tags(title: str, artist: str, album: str, snap: dict) -> tuple[str, str, str]:
    """只补空字段。已经是纯字符串的专辑不被缓存里的另一份专辑覆盖。"""
    snap_title, snap_artist, snap_album = _tag_fields(snap)
    return title or snap_title, artist or snap_artist, album or snap_album


def _tee_metadata_fallback(
    guid: str, title: str, artist: str, album: str, dest_info: dict | None = None,
) -> tuple[str, str, str]:
    """tee 落盘元数据缺失时的回查兜底，避免以 unknown 进曲库。

    顺序：推荐/网易账号歌单缓存（首播时历史往往还没写）→ 播放历史/收藏/自建歌单。
    缺哪个字段补哪个；已有的纯字符串专辑保持不变。封面 URL 只在调用方还没有时写入 dest_info。
    """
    title, artist, album = (title or "").strip(), (artist or "").strip(), (album or "").strip()
    need_cover = not _cover_url_of(dest_info)
    if not (title and artist and album and not need_cover):
        filled = False
        for snap in (_lookup_playlist_cache_track(guid), _lookup_online_snapshot(guid)):
            if not snap:
                continue
            title, artist, album = _merge_missing_tags(title, artist, album, snap)
            if need_cover and isinstance(dest_info, dict):
                cover = _cover_url_of(snap)
                if cover:
                    dest_info["cover_url"] = cover
                    need_cover = False
            filled = True
            if title and artist and album and not need_cover:
                break
        if filled:
            logger.info("tee metadata fallback hit for %s: %s - %s", guid, artist, title)
    if not (title and artist):
        logger.warning(
            "tee finalize missing metadata for %s (title=%r artist=%r)：将以 unknown 命名落盘，"
            "该曲目的元数据解析链路需要排查", guid, title, artist,
        )
    return title, artist, album


def _tee_finalize(part: str, guid: str, ext: str, info: dict | None, tee_enabled: bool) -> dict | None:
    """落盘转正：边听边存开→进曲库（定名/权限/标签，歌词仅在 FNMUSIC_LYRIC_AUTO_DL
    开启时随迁），关→进滚动缓存（不写歌词）。

    part 必须已完整写好且长度校验通过；成功后由调用方触发曲库扫描通知。
    无损档解码校验不过直接丢弃（上游损坏流绝不入库）并拉黑该 guid。
    返回实际落盘元数据 {"dest","title","artist","album"}（官方绑定按此匹配官方曲库）。
    """
    if str(ext or "").lower() in _LOSSLESS_EXTS and not _audio_file_ok(part):
        _lossless_blacklist(guid)
        logger.warning("tee finalize rejected corrupt lossless for %s", guid)
        try:
            os.remove(part)
        except OSError:
            pass
        return None
    src = dict(info or {})
    title, artist, album = _tag_fields(src)
    if tee_enabled:
        title, artist, album = _tee_metadata_fallback(guid, title, artist, album, src)
        dest = library_media_path(guid, title, ext, artist=artist, directory=tee_save_dir())
        os.replace(part, dest)
        remember_media_path(guid, dest)
        adopt_library_perms(dest)
        write_audio_tags(dest, title, artist, album)
        # 自动下载封面：音乐文件已完整落库才走到这里（下载失败根本进不了
        # finalize），封面字节直接内嵌进音频——不产生独立封面文件，无孤儿
        cover_url = _cover_url_of(src)
        if cover_url and CONF.get("auto_cover", True):
            try:
                fetched = download_cover_bytes(cover_url)
                if fetched and embed_audio_cover(dest, fetched[0], fetched[1]):
                    logger.info("cover embedded for %s: %s", guid, dest)
            except Exception as e:
                logger.debug("cover embed step skipped for %s: %s", guid, e)
        # 自动下载歌词开启时，无音频时代落在 cache 的影子歌词跟随音频进曲库，词曲贴身
        if CONF.get("lyric_auto_dl"):
            promote_shadow_lyric(guid, dest)
    else:
        # 边听边存关闭：只写滚动缓存（cache_safe_guid 命名，find_cache_file 精确名可命中）
        dest = os.path.join(CONF["cache_dir"], f"{cache_safe_guid(guid)}.{ext}")
        os.replace(part, dest)
    logger.info("tee finalize done for %s: dest=%s", guid, dest)
    # 歌词只随"音乐完整落库成功"写入（tee 转正即到此处），且仅在自动下载歌词
    # 开启时；下载失败根本进不了 finalize，绝不产生先落歌词的孤儿文件
    if tee_enabled and CONF.get("lyric_auto_dl"):
        lyric = str(src.get("lyric") or src.get("lrc") or "")
        if lyric.strip():
            write_lyric_cache(guid, lyric, title, artist)
    if not tee_enabled:
        try:
            purge_rolling(CONF["cache_dir"], keep=int(CONF.get("tee_cache_max", 2)))
        except Exception as e:
            logger.warning("Rolling cache purge failed: %s", e)
    return {"dest": dest, "title": title, "artist": artist, "album": album}


async def _auto_lyric_after_finalize(request: Request, guid: str, meta: dict | None) -> None:
    """音乐完整落库成功后补齐歌词（FNMUSIC_LYRIC_AUTO_DL，默认关）。

    仅边听边存转正路径调用，且必须排在 _schedule_library_scan 之前——保证
    官方扫描入库前 .lrc 已落在音频旁。本地已有歌词（含影子提升产物）零请求；
    缺失时向源站补拉（酷我上游接口已失效返回空即跳过）。下载失败不会走到
    这里，不会产生只有歌词没有音频的孤儿文件。
    """
    if not CONF.get("lyric_auto_dl"):
        return
    dest = str((meta or {}).get("dest") or "")
    if not dest or _is_rolling_cache_stem(dest, guid):
        return
    if find_lyric_file(guid):
        return
    try:
        text = await asyncio.wait_for(resolve_online_lyric(request, guid), timeout=10.0)
    except Exception as e:
        logger.info("auto lyric fetch failed for %s: %s", guid, type(e).__name__)
        return
    if text:
        logger.info("auto lyric saved for %s (%d chars)", guid, len(text))
    else:
        logger.info("auto lyric unavailable for %s (source returned no lyric)", guid)


_SCAN_TTL_S = 90.0
_SCAN_MERGE_S = 3.0
_scan_last_attempt = 0.0
_scan_task: "asyncio.Task | None" = None


def _schedule_library_scan(headers: dict | None) -> None:
    """落盘进曲库后通知官方重扫（FNMUSIC_LIBRARY_SCAN_PATH 为空=禁用）。

    官方扫描接口无公开资料：路径可配置、默认禁用；3 秒合并窗口（连续落盘多首
    只发一次）+ 90 秒 TTL（成功与失败都节流，路径配错不会风暴）。凭证头取自
    触发请求（copy_incoming_headers），只在任务内一次性使用，不落盘。
    """
    global _scan_task, _scan_last_attempt
    if not str(CONF.get("library_scan_path") or "").strip() or not headers:
        return
    if time.monotonic() - _scan_last_attempt < _SCAN_TTL_S:
        return
    if _scan_task is not None and not _scan_task.done():
        return

    async def _run() -> None:
        global _scan_last_attempt
        await asyncio.sleep(_SCAN_MERGE_S)
        scan_path = str(CONF.get("library_scan_path") or "").strip()
        if not scan_path:
            return
        _scan_last_attempt = time.monotonic()
        try:
            client = get_upstream_client(app)
            req = client.build_request("POST", scan_path, headers=dict(headers))
            resp = await client.send(req)
            if resp.status_code == 200:
                logger.info("Library scan triggered via %s", scan_path)
            else:
                logger.warning(
                    "Library scan %s returned %s (check FNMUSIC_LIBRARY_SCAN_PATH)",
                    scan_path, resp.status_code,
                )
        except Exception as e:
            logger.warning("Library scan trigger failed: %s", e)

    _scan_task = asyncio.create_task(_run())


# TOP[top.tee] 边听边存：Tee 落盘、收尾入库、封面内嵌、自动下载歌词
def stream_tee_response(
    resp: httpx.Response,
    guid: str,
    range_header: str | None,
    coro_factory: Callable[[], Coroutine[Any, Any, Any]] | None = None,
    client_to_close: httpx.AsyncClient | None = None,
    resolved_ext: str | None = None,
    pre_info: dict | None = None,
    chunks: Any = None,
    first_chunk: bytes = b"",
    scan_headers_factory: Callable[[], dict] | None = None,
    post_finalize: Callable[[dict | None], Coroutine[Any, Any, None]] | None = None,
    reconnect: Callable[[int], Coroutine[Any, Any, tuple[httpx.Response, Any, bytes] | None]] | None = None,
    on_death: Callable[[], None] | None = None,
) -> Response:
    headers = {"Accept-Ranges": "bytes"}
    # 禁止前置 nginx 缓冲音频分片，避免外网播放首包被延迟到缓冲填满后才发出。
    headers["X-Accel-Buffering"] = "no"
    for key in ("content-type", "content-length", "content-range"):
        if resp.headers.get(key):
            headers[key] = resp.headers[key]
    if resolved_ext:
        headers["content-type"] = media_type_for_ext(resolved_ext)
    length = resp.headers.get("content-length", "")
    expected = int(length) if length.isdigit() else None
    full_resource = resp.status_code == 200
    if resp.status_code == 206:
        # Content-Length proves only this segment, not the entire recording.
        match = re.fullmatch(r"bytes\s+0-(\d+)/(\d+)", resp.headers.get("content-range", "").strip(), re.I)
        full_resource = bool(match and int(match[1]) + 1 == int(match[2]) and int(match[2]) > 0)
        if full_resource:
            total = int(match[2])
            full_resource = expected is None or expected == total
            expected = total
    ext = resolved_ext or ext_from_content_type(resp.headers.get("content-type", ""))

    async def body() -> AsyncGenerator[bytes, None]:
        # Pull-through provides backpressure: no unbounded producer queue and no
        # downloader outliving its consumer. Only a clean EOF may finalize.
        part = None
        fp = None
        written = 0
        eof = False
        info_task = None
        tee_active_token = False
        upstream_aborted = False
        # 必须在 try 之前绑定：finally 会引用这两个变量，若首次赋值放在 try 内，
        # 一旦生成器在赋值前被关闭（客户端提前断开/换源重定向），finally 就会
        # 抛 UnboundLocalError，把正常音频流直接打断。
        tee_enabled = bool(CONF.get("tee_save_enabled"))
        resume_part = None
        # 不能用 resp 这个名字在本函数内赋值：一旦赋值，Python 会把 resp 视为
        # body() 的局部变量，收尾时 await resp.aclose() 就会 UnboundLocalError，
        # 把正常音频流中途掐断（浏览器表现为播不了/播一半停）。
        current_resp = resp
        try:
            if should_cache(range_header) and full_resource:
                directory = tee_save_dir() if tee_enabled else CONF["cache_dir"]
                os.makedirs(directory, exist_ok=True)
                part = os.path.join(directory, f"{cache_safe_guid(guid)}.{uuid4().hex}.part")
                fp = open(part, "wb")
                tee_active_token = _tee_active_acquire(guid)
                if pre_info is None and coro_factory:
                    info_task = asyncio.create_task(coro_factory())
            iterator = chunks if chunks is not None else resp.aiter_bytes()
            if first_chunk:
                if fp:
                    fp.write(first_chunk)
                written += len(first_chunk)
                yield first_chunk
            reconnect_attempts = 0
            try:
                while True:
                    try:
                        async for chunk in iterator:
                            if chunk:
                                if fp:
                                    fp.write(chunk)
                                written += len(chunk)
                                yield chunk
                        break
                    except Exception as exc:
                        # CDN 中途掉线：从已写入位置 Range 续传，继续同一个
                        # 客户端响应。只有“无法续传/续传又失败”才把这条直链
                        # 记为失效（对齐安卓 failedUrls）；续传成功不算流失败，
                        # 避免一次瞬时抖动把整档音质拖低。
                        if on_death and (reconnect is None
                                         or reconnect_attempts >= 3 or written <= 0):
                            on_death()
                        if reconnect is None or reconnect_attempts >= 3 or written <= 0:
                            raise
                        reconnect_attempts += 1
                        logger.warning(
                            "Stream interrupted for %s at %d bytes, reconnecting (%d/3): %s",
                            guid, written, reconnect_attempts, type(exc).__name__,
                        )
                        with anyio.CancelScope(shield=True):
                            try:
                                await resp.aclose()
                            except Exception:
                                pass
                        try:
                            resumed = await reconnect(written)
                        except Exception:
                            if on_death:
                                on_death()
                            raise
                        if resumed is None:
                            if on_death:
                                on_death()
                            raise
                        new_resp, iterator, first = resumed
                        current_resp = new_resp
                        if first:
                            if fp:
                                fp.write(first)
                            written += len(first)
                            yield first
            except Exception as exc:
                # An aborted stream must not enter the cache, but it must leave
                # a trace: CDN stalls mid-file were previously invisible in the
                # journal. Client disconnects raise CancelledError (not caught
                # here) and stay silent on purpose.
                upstream_aborted = True
                logger.warning("Stream aborted mid-way for %s: %s", guid, type(exc).__name__)
                raise
            eof = True
            if fp:
                fp.close()
                fp = None
            if part and eof and written >= 1024 and (expected is None or written == expected):
                # 无论客户端连接此时是否已关闭（curl 接收完直接 EOF 退出，Starlette 会 aclose 生成器），
                # 完整的音频已全部接收完毕，落盘与标签写入必须受 shield 保护完整执行完毕，
                # 且立即解绑 part，绝不能被 GeneratorExit / CancelledError 提前打断导致 part 在 finally 中被误删。
                to_finalize = part
                part = None
                with anyio.CancelScope(shield=True):
                    info = pre_info
                    if info is None and info_task:
                        try:
                            info = await asyncio.wait_for(info_task, timeout=8.0)
                        except Exception:
                            info = None
                    try:
                        meta = await asyncio.to_thread(_tee_finalize, to_finalize, guid, ext, info, tee_enabled)
                        # 自动下载歌词：补拉必须在通知官方扫描之前完成（入库即带歌词）
                        if post_finalize is not None and meta:
                            await post_finalize(meta)
                        scan_headers = scan_headers_factory() if scan_headers_factory is not None else None
                        if scan_headers:
                            _schedule_library_scan(scan_headers)
                        if meta and tee_enabled:
                            # 官方绑定意图（收藏/歌单）统一在下载落库完成后调度
                            _dispatch_official_binding(guid, scan_headers, meta)
                    except Exception as exc:
                        logger.warning("tee finalize execution failed for %s: %s", guid, type(exc).__name__)
                        if os.path.exists(to_finalize):
                            try:
                                os.remove(to_finalize)
                            except OSError:
                                pass
        finally:
            if fp:
                fp.close()
                fp = None
            if part and os.path.exists(part):
                # 切歌/客户端退出不中断下载：未写完的 part 交接给后台续传任务完成
                # （仅边听边存开启的整轨流且非上游错误；written 不足或续传满载时
                # 按旧语义删除，上游流错误绝不进续传）
                if (tee_enabled and full_resource and not upstream_aborted
                        and written >= 1024 and _tee_handoff_slot_available()):
                    resume_part = part
                    part = None
                else:
                    try:
                        os.remove(part)
                    except OSError:
                        pass
                    part = None
            with anyio.CancelScope(shield=True):
                if tee_active_token:
                    _tee_active_release(guid)
                if info_task and not info_task.done():
                    info_task.cancel()
                    await asyncio.gather(info_task, return_exceptions=True)
                try:
                    await current_resp.aclose()
                except Exception:
                    pass
                if client_to_close:
                    await client_to_close.aclose()
                if resume_part:
                    _register_tee_handoff(guid, resume_part, written, expected, ext, scan_headers_factory)

    return StreamingResponse(body(), status_code=resp.status_code, headers=headers)


def _credential_scope(request: Request) -> str:
    return hashlib.sha256(json.dumps([request.headers.get(k, "") for k in
        ("cookie", "authorization", "x-trim-music-temp-token")]).encode()).hexdigest()


def _retained_track(request: Request, guid: str) -> tuple[dict | None, dict | None]:
    _clean_search_cache()
    for entry in reversed(list(_SEARCH_CACHE.values())):
        if entry.get("credentials") != _credential_scope(request) or entry.get("config", _source_config()) != _source_config():
            continue
        for item in entry.get("items", []):
            if online_guid_from_item(item) == guid:
                return item, entry
            for alternative in item.get("_alternatives", []):
                if online_guid_from_item(alternative) == guid:
                    return alternative, entry
    return None, None


# TOP[top.stream.fallback] 换源恢复：主源失败按健康度换替代源（无损黑名单防反复横跳）
async def _recover_source(request: Request, guid: str, entry: dict | None) -> bool:
    """One bounded source re-search after a backend loses its in-memory IDs."""
    if not entry or not _source_enabled(guid):
        return False
    source = source_from_online_guid(guid)
    recovery_key = "recovered:" + source
    if time.monotonic() - entry.get(recovery_key, -1000) < 30:
        return False
    entry[recovery_key] = time.monotonic()
    keyword = entry.get("keyword", "")
    token = _FETCH_SCOPE.set(_credential_scope(request) + ":play")
    try:
        if source == "netease":
            coro = fetch_musicbox_search(get_musicbox_client(request.app), keyword, CONF["netease_search_limit"])
        elif source == "lx":
            # 按目标 GUID 的平台精确重搜（GUID 第 3 段），对齐 musicdl 分支按引擎重搜的行为
            parts = guid.split(":")
            coro = fetch_lx_search(get_lx_client(request.app), keyword, CONF["lx_search_limit"],
                                   sources=[parts[2]] if len(parts) >= 4 else None)
        else:
            selected = [name.strip() for name in str(CONF.get("online_sources") or "").split(",")
                        if name.strip().lower().removesuffix("musicclient") == source.lower()]
            coro = fetch_musicdl_search(get_musicdl_client(request.app), keyword, CONF["online_limit"],
                                       ",".join(selected) or source)
        try:
            result = await asyncio.wait_for(coro, timeout=3.0)
        except Exception:
            return False
        if result is _SUPERSEDED or not result:
            return False
        items = result.get("items", []) if isinstance(result, dict) else result
        if not isinstance(items, list):
            return False
        return any(online_guid_from_item(item) == guid for item in items)
    finally:
        _FETCH_SCOPE.reset(token)


# TOP[top.stream.open] 在线播放开流主链路：本地缓存 → 取址 → 直连/代理 → Tee
async def _open_online_stream(request: Request, guid: str, range_header: str | None,
                              force_mp3: bool = False):
    """Resolve and read first bytes before committing HTTP headers to the client."""
    global _LAST_PLAY_AT
    _LAST_PLAY_AT = time.time()
    source = source_from_online_guid(guid)
    info, _ = _retained_track(request, guid)
    headers = {"Accept-Encoding": "identity"}
    if range_header:
        headers["Range"] = range_header
    via_musicdl = False
    owned = None
    resp = None
    ext = None
    reconnect_builder = None
    on_death = None
    try:
        if source in ("netease", "lx"):
            if source == "netease":
                url = await resolve_netease_url(get_musicbox_client(request.app), song_id_from_online_guid(guid).split(":")[-1])
                if not url:
                    return None
            else:
                lx_client = get_lx_client(request.app)
                try:
                    resolved = await asyncio.wait_for(
                        # lxmusic-service 内部预算 20s（含多源回退 + 降档 + 探活），
                        # 这里给足 12s，避免解析已完成却被 proxy 提前判失败。
                        resolve_lx_url(lx_client, song_id_from_online_guid(guid)), timeout=12.0
                    )
                except Exception:
                    resolved = None
                if not resolved:
                    # 直接 ID 解析失败时，用标题+歌手在当前洛雪源重新搜索一个可播候选，
                    # 减少“这首歌跳过”的概率。
                    if info is None:
                        try:
                            info = await asyncio.wait_for(
                                _fetch_online_info(request, guid, include_lyric=False), timeout=3.0
                            )
                        except Exception:
                            info = None
                    if not str((info or {}).get("title") or "").strip():
                        ceru_song = ceru.song_for_guid(guid)
                        if ceru_song:
                            info = {
                                **(info or {}),
                                "title": ceru.song_title(ceru_song),
                                "artist": ceru.song_artist(ceru_song),
                                "album": ceru.song_album(ceru_song),
                            }
                    original_meta = _LX_ALT_META_CACHE.setdefault(song_id_from_online_guid(guid), {})
                    for key in ("title", "artist", "album", "cover_url", "duration_s", "ext"):
                        if info.get(key) not in (None, "") and not original_meta.get(key):
                            original_meta[key] = info[key]
                    if original_meta:
                        _save_lx_alt_meta()
                    title = str((info or {}).get("title") or "").strip()
                    artist = str((info or {}).get("artist") or "").strip()
                    keyword = f"{title} {artist}".strip()
                    if keyword:
                        try:
                            candidates = await asyncio.wait_for(
                                fetch_lx_search(lx_client, keyword, 12), timeout=8.0
                            )
                        except Exception:
                            candidates = []
                        # 优先标题一致、歌手匹配的候选，再逐个尝试解析
                        norm = lambda s: re.sub(r"[\s\-—·・&/、,，|]", "", str(s or "")).lower()
                        cand_title = norm(title)
                        cand_artist = norm(artist)
                        def cand_title_of(cand):
                            return norm(cand.get("title") or cand.get("name"))
                        def rank(cand):
                            c_title = cand_title_of(cand)
                            c_artist = norm(cand.get("artist") or cand.get("singer"))
                            score = 0
                            if cand_title and c_title == cand_title:
                                score += 4
                            elif cand_title and c_title and (cand_title in c_title or c_title in cand_title):
                                score += 2
                            if cand_artist and c_artist and (cand_artist == c_artist or cand_artist in c_artist or c_artist in cand_artist):
                                score += 2
                            score -= _lx_health_rank(str(cand.get("id") or ""))
                            # 正在冷却的平台排到最后，优先用健康平台兜底。
                            if _lx_platform_cooled(str(cand.get("id") or "")):
                                score -= 10
                            return -score
                        pool = [c for c in (candidates or []) if isinstance(c, dict)]
                        if cand_title:
                            # 只接受标题一致的候选：搜索结果里经常混入同歌手的
                            # 其它歌，直接取 top1 会把“你怕啥子嘛”播成别的歌。
                            exact = [c for c in pool if cand_title_of(c) == cand_title]
                            near = [
                                c for c in pool
                                if cand_title_of(c)
                                and (cand_title in cand_title_of(c) or cand_title_of(c) in cand_title)
                            ]
                            pool = exact or near
                        ranked = sorted(pool, key=rank)
                        for cand in ranked[:5]:
                            alt_id = str((cand or {}).get("id") or "")
                            if not alt_id or alt_id == song_id_from_online_guid(guid):
                                continue
                            if _lx_platform_cooled(alt_id):
                                continue
                            try:
                                alt = await asyncio.wait_for(resolve_lx_url(lx_client, alt_id), timeout=10.0)
                            except Exception:
                                alt = None
                            if alt:
                                resolved = alt
                                alt_guid = online_guid_from_item(cand)
                                if alt_guid and alt_guid != guid:
                                    # 换源只换音频流，封面/歌词仍认原歌曲。
                                    _CERU_SOURCE_OVERRIDES[alt_guid] = guid
                                alt_meta = _LX_ALT_META_CACHE.setdefault(song_id_from_online_guid(guid), {})
                                for key in ("title", "artist", "album", "cover_url", "duration_s", "ext"):
                                    if cand.get(key) not in (None, "") and not alt_meta.get(key):
                                        alt_meta[key] = cand.get(key)
                                _save_lx_alt_meta()
                                _LX_ALT_CACHE[song_id_from_online_guid(guid)] = (
                                    alt_id, time.time()
                                )
                                break
                if not resolved:
                    return None
                url = resolved["url"]
                # 2.6.39 断流死链回收（对齐安卓 failedUrls）：首字节取不到或
                # 中途断流时把这条直链标记失效，下次取链自动跳过它。
                # 默认参数绑定当前值，避免闭包延迟求值拿到后续赋值的 url。
                def on_death(_s=song_id_from_online_guid(guid), _u=url) -> None:
                    _lx_mark_url_dead(_s, _u)
                ext = resolved.get("ext")
                for key, value in (resolved.get("headers") or {}).items():
                    if key.lower() in ("referer", "user-agent", "origin", "cookie"):
                        headers[key] = str(value)
                # 关键：CDN 防盗链。lxmusic-service 已用浏览器 UA 探活通过，
                # 但响应不带 headers；proxy 直接取流会用 python-httpx UA 被
                # 酷我等平台 403。缺什么补什么，已有则以源返回的为准。
                if not any(k.lower() == "user-agent" for k in headers):
                    headers["User-Agent"] = _MEDIA_UA
                if not any(k.lower() == "referer" for k in headers):
                    plat = _lx_platform_of(song_id_from_online_guid(guid))
                    referer = _MEDIA_REFERER.get(plat)
                    if referer:
                        headers["Referer"] = referer
            if info is None:
                try:
                    info = await asyncio.wait_for(_fetch_online_info(request, guid), timeout=0.75)
                except Exception as exc:
                    logger.info("online info fast-path missed for %s: %s", guid, type(exc).__name__)
            ext = ext or (info or {}).get("ext")
            # Read timeout must tolerate CDN throttling mid-file: a flat 10s
            # timeout kills long pauses between chunks and surfaces as playback
            # stuck at ~70-80% of the track.
            owned = httpx.AsyncClient(
                timeout=httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=10.0),
                follow_redirects=True,
            )
            client = owned
            req = client.build_request("GET", url, headers=headers)
            reconnect_builder = lambda h: client.build_request("GET", url, headers=h)
        else:
            client = get_musicdl_client(request.app)
            via_musicdl = True
            params = {"id": song_id_from_online_guid(guid), "proxy": "true"}
            # 无损档已被判定损坏的曲目（或上层明确要求）直接取 mp3 档；
            # 带断点续传的请求不切档——前后字节必须来自同一条流
            if (force_mp3 or _lossless_is_blacklisted(guid)) and not range_header:
                params["quality"] = "mp3"
            req = client.build_request("GET", "/stream", params=params, headers=headers)
            reconnect_builder = lambda h: client.build_request("GET", "/stream", params=params, headers=h)
        resp = await client.send(req, stream=True)

        async def reconnect(offset: int):
            if offset <= 0:
                return None
            retry_headers = dict(headers)
            retry_headers["Range"] = f"bytes={offset}-"
            retry_req = None
            # 洛雪续传只回原直链：Range 偏移按原编码的字节数算，重新解析拿到
            # 的“另一条”链音质/编码可能不同，硬拼字节会把音频拼花。token 过期
            # 导致原链 403 时这里返回 None → on_death 记死链，客户端重播时
            # resolve_lx_url 走死链排除 + fresh=1 重新取链。
            if source == "netease":
                try:
                    fresh_url = await resolve_netease_url(
                        get_musicbox_client(request.app), song_id_from_online_guid(guid).split(":")[-1]
                    )
                except Exception:
                    fresh_url = None
                if fresh_url:
                    retry_req = client.build_request("GET", fresh_url, headers=retry_headers)
            if retry_req is None:
                if reconnect_builder is None:
                    return None
                retry_req = reconnect_builder(retry_headers)
            retry_resp = await client.send(retry_req, stream=True)
            ctype = retry_resp.headers.get("content-type", "").lower()
            # 续传必须拿到 206 分段：200 表示服务端忽略 Range 从头下发，拼进
            # 已播放的偏移只会把音频拼花，直接放弃。
            if (retry_resp.status_code not in (200, 206)
                    or (retry_resp.status_code == 200 and offset > 0)
                    or any(x in ctype for x in ("text/", "json"))):
                await retry_resp.aclose()
                return None
            retry_chunks = retry_resp.aiter_bytes()
            retry_first = await anext(retry_chunks, b"")
            if not retry_first:
                await retry_resp.aclose()
                return None
            return retry_resp, retry_chunks, retry_first

        content_type = resp.headers.get("content-type", "").lower()
        if (resp.status_code not in (200, 206)
                or any(x in content_type for x in ("text/", "json"))
                or resp.headers.get("content-encoding", "identity").strip().lower() not in ("", "identity")):
            # Reject servers ignoring identity: decoded bytes cannot use encoded
            # Content-Length/Range offsets, and must not enter the audio cache.
            if on_death:
                on_death()
            return None
        chunks = resp.aiter_bytes()
        first = await anext(chunks, b"")
        if not first:
            if on_death:
                on_death()
            return None
        # 仅 musicdl 通道会服务端透明降级（坏无损流→官方 mp3 档）：以实际
        # content-type 为准修正扩展名，避免 mp3 字节按 .flac 命名入库。
        # 网易/洛雪直链相反——上游会错报 audio/mpeg，扩展名以解析结果为准。
        if via_musicdl and "audio/mpeg" in content_type and (not ext or ext in _LOSSLESS_EXTS):
            ext = "mp3"
        # 第 7 个元素必须是真正可 await 的 reconnect(offset)：此前误把
        # reconnect_builder（只产 httpx.Request 的 lambda）传给了
        # stream_tee_response，await 到 Request 必抛 TypeError，断流重连
        # 从未生效——CDN 一断就跳歌。第 8 个元素 on_death 供断流时回收死链。
        result = (resp, owned, ext, info, chunks, first, reconnect, on_death)
        resp = owned = None  # transfer ownership to response iterator
        return result
    finally:
        if resp:
            await resp.aclose()
        # 提前判失败的路径（解析死链/首字节为空/上游拒收）同样要收掉新建的
        # client，否则每次失败泄漏一个连接池，并发场景加剧 502。
        if owned:
            await owned.aclose()
        if owned:
            await owned.aclose()


_FULL_FETCH_COOLDOWN_S = 1800.0
_full_fetch_tasks: "dict[str, asyncio.Task]" = {}
_full_fetch_failed: "dict[str, float]" = {}

# --- 损坏流防护（2026-09-29 实测）-----------------------------------------
# kuwo 官方 CDN 的无损档交付不可解码字节（fLaC 头与 Content-Length 都正常，
# musicdl 库探活只验 URL/扩展名），坏文件一旦入库，官方转码器编到坏点即报
# "transcoding error"、App 端表现为转码下载失败。对策：入库前全量解码校验，
# 坏流丢弃并按 guid 拉黑无损档——此后该曲目取流自动降级 mp3 档（musicdl
# 服务 /stream?quality=mp3）。
_LOSSLESS_EXTS = {"flac", "wav", "ape", "m4a", "alac", "ogg", "opus"}
_LOSSLESS_BAD: "dict[str, float]" = {}
_LOSSLESS_BAD_TTL_S = 6 * 3600.0


def _lossless_blacklist(guid: str) -> None:
    _LOSSLESS_BAD[guid] = time.monotonic()


def _lossless_is_blacklisted(guid: str) -> bool:
    ts = _LOSSLESS_BAD.get(guid)
    if ts is None:
        return False
    if time.monotonic() - ts > _LOSSLESS_BAD_TTL_S:
        _LOSSLESS_BAD.pop(guid, None)
        return False
    return True


def _audio_file_ok(path: str) -> bool:
    """完整音频文件的解码校验：ffmpeg 无任何错误输出才算通过。

    只对已通过长度校验的完整文件调用——截断的完好文件同样会报解码错误，
    报错文本无法区分二者，完整性必须由字节数校验先行保证。
    ffmpeg 不可用（测试守卫/宿主机缺件）时无从校验，只能放行。
    """
    if not tc.FFMPEG_BIN:
        return True
    try:
        r = subprocess.run(
            [tc.FFMPEG_BIN, "-v", "error", "-nostdin", "-i", path, "-f", "null", "-"],
            capture_output=True, text=True, timeout=300,
        )
        return r.returncode == 0 and not (r.stderr or "").strip()
    except Exception:  # noqa: BLE001
        return False


def _prune_full_fetch_state(now: float) -> None:
    if len(_full_fetch_failed) > 1000:
        for g in [g for g, at in _full_fetch_failed.items() if now - at >= _FULL_FETCH_COOLDOWN_S]:
            del _full_fetch_failed[g]


def _register_background_fetch(request: Request, guid: str, gate_key: str) -> bool:
    """后台整轨下载通用注册逻辑，按 gate_key 指定的 CONF 键做门禁。"""
    if not CONF.get(gate_key) or find_cache_file(guid):
        return False
    if guid in _tee_active:
        # tee 流式下载同 guid 进行中：不重复下载，tee 转正/切歌续传路径会调度后续动作
        return False
    task = _full_fetch_tasks.get(guid)
    if task is not None and not task.done():
        return False
    now = time.monotonic()
    _prune_full_fetch_state(now)
    if now - _full_fetch_failed.get(guid, -_FULL_FETCH_COOLDOWN_S) < _FULL_FETCH_COOLDOWN_S:
        return False
    headers = copy_incoming_headers(request)
    _full_fetch_tasks[guid] = asyncio.create_task(_full_fetch_download(guid, headers))
    return True


def _register_full_fetch(request: Request, guid: str) -> None:
    """定长窗口拉流客户端的边听边存兜底：注册后台整轨下载，客户端请求照常服务。

    should_cache 只认"无 Range / bytes=0-"，窗口内核（如 bytes=0-1048575 一段）
    永远过不了 tee 写盘门；由服务端另起整轨下载落盘补齐。同 guid 去重、
    失败后冷却期内不重试，防止坏源反复打上游。
    """
    _register_background_fetch(request, guid, "tee_save_enabled")


def _register_fav_autobind(request: Request, guid: str, user_guid: str = "",
                           playlist_guid: str | None = None) -> None:
    """收藏/加入歌单的在线歌曲自动绑定本地：登记官方绑定意图并确保整轨下载落库。

    下载不与边听边存重复：tee 正在流式下载或后台任务在途时只登记意图，由对应
    完成路径（tee 转正 / 后台整轨下载 / 切歌续传）统一调度官方绑定。文件已在
    库时无需下载，直接尝试官方绑定（官方可能已扫入库）。
    """
    if not CONF.get("fav_auto_bind"):
        return
    headers = copy_incoming_headers(request)
    if user_guid:
        _register_bind_intent(guid, user_guid, headers, playlist_guid)
    if find_cache_file(guid):
        _dispatch_official_binding(guid, headers, None)
        return
    _register_background_fetch(request, guid, "fav_auto_bind")


async def _info_for_background_save(request: Request, guid: str, info: dict | None) -> dict:
    """后台整轨/续传落盘前补齐元数据。

    本地推荐歌单、网易账号歌单、历史/收藏里已有标题就用那份，不再打源站。
    都没有时再拉一次歌曲信息（数秒，不占播放起播的 0.75 秒窗口）。
    """
    base = dict(info or {})
    if _tag_fields(base)[0]:
        return base
    local = _lookup_playlist_cache_track(guid) or _lookup_online_snapshot(guid)
    if local and _tag_fields(local)[0]:
        merged = dict(local)
        if base.get("ext"):
            merged["ext"] = base["ext"]
        if base.get("lyric"):
            merged["lyric"] = base["lyric"]
        return merged
    try:
        fetched = await asyncio.wait_for(_fetch_online_info(request, guid), timeout=4.0)
    except Exception as exc:
        logger.info("background online info missed for %s: %s", guid, type(exc).__name__)
        return base
    if isinstance(fetched, dict) and _tag_fields(fetched)[0]:
        if base.get("ext") and not fetched.get("ext"):
            fetched = dict(fetched)
            fetched["ext"] = base["ext"]
        return fetched
    logger.info("background online info empty for %s", guid)
    return base


async def _full_fetch_download(guid: str, cred_headers: dict, force_mp3: bool = False) -> None:
    """后台整轨下载 online guid 并落盘（独立于客户端连接，不占播放路径预算）。

    无损档解码校验失败时按 guid 拉黑并自动以 mp3 档重试一次（服务端
    quality=mp3），两次都坏才宣告失败——坏字节绝不入库。
    """
    part = None
    resp = None
    owned = None
    try:
        # 合成最小 Request scope（触发请求的凭证头 + app 实例），完整复用在线取
        # 流的解析/元数据/首字节校验逻辑；Range 传 None 保证拿到完整资源。
        fake_request = _synth_request(cred_headers)
        opened = await _open_online_stream(fake_request, guid, None, force_mp3=force_mp3)
        if not opened:
            raise RuntimeError("open failed")
        resp, owned, ext, info, chunks, first, _reconnect, _on_death = opened
        expected = None
        length = resp.headers.get("content-length", "")
        if length.isdigit():
            expected = int(length)
        directory = tee_save_dir()
        os.makedirs(directory, exist_ok=True)
        part = os.path.join(directory, f"{cache_safe_guid(guid)}.{uuid4().hex}.part")
        written = 0
        with open(part, "wb") as fp:
            if first:
                fp.write(first)
                written += len(first)
            async for chunk in chunks:
                if chunk:
                    fp.write(chunk)
                    written += len(chunk)
        if written < 1024 or (expected is not None and written != expected):
            raise RuntimeError(f"size mismatch written={written} expected={expected}")
        info = await _info_for_background_save(fake_request, guid, info)
        ext = ext or (info or {}).get("ext") or "mp3"
        if ext in _LOSSLESS_EXTS and not await asyncio.to_thread(_audio_file_ok, part):
            _lossless_blacklist(guid)
            logger.warning("audio decode check failed for %s (ext=%s bytes=%d)", guid, ext, written)
            # 坏流不落库：关掉当前连接后改要 mp3 档重来一次
            with anyio.CancelScope(shield=True):
                if resp:
                    await resp.aclose()
                if owned:
                    await owned.aclose()
                resp = owned = None
            if not force_mp3:
                logger.warning("retrying %s with mp3 tier after corrupt lossless stream", guid)
                await _full_fetch_download(guid, cred_headers, force_mp3=True)
                return
            raise RuntimeError("corrupt stream even at mp3 tier")
        meta = await asyncio.to_thread(_tee_finalize, part, guid, ext, info or {}, True)
        part = None
        logger.info("Background full fetch saved %s (%d bytes)", guid, written)
        await _auto_lyric_after_finalize(fake_request, guid, meta)
        _schedule_library_scan(cred_headers)
        _dispatch_official_binding(guid, cred_headers, meta)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        _full_fetch_failed[guid] = time.monotonic()
        logger.warning("Background full fetch failed for %s: %s", guid, type(e).__name__)
    finally:
        if part and os.path.exists(part):
            try:
                os.remove(part)
            except OSError:
                pass
        with anyio.CancelScope(shield=True):
            if resp:
                await resp.aclose()
            if owned:
                await owned.aclose()
        if _full_fetch_tasks.get(guid) is asyncio.current_task():
            del _full_fetch_tasks[guid]


# === 边听边存 tee 活跃跟踪 + 切歌续传交接 ===
# 需求：开启边听边存后听歌即下载，切歌不中断下载；收藏/加歌单与正在进行的
# tee 下载不重复拉流。

_tee_active: "dict[str, int]" = {}
_tee_handoff_active: "set[str]" = set()


def _tee_active_acquire(guid: str) -> bool:
    _tee_active[guid] = _tee_active.get(guid, 0) + 1
    return True


def _tee_active_release(guid: str) -> None:
    left = _tee_active.get(guid, 0) - 1
    if left > 0:
        _tee_active[guid] = left
    else:
        _tee_active.pop(guid, None)


def _tee_handoff_slot_available() -> bool:
    """续传并发上限（FNMUSIC_TEE_HANDOFF_MAX，0=关闭）：防快速跳歌形成下载洪泛。"""
    try:
        limit = int(CONF.get("tee_handoff_max") or 0)
    except (TypeError, ValueError):
        limit = 0
    return limit > 0 and len(_tee_handoff_active) < limit


def _remove_quiet(path: str | None) -> None:
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def _synth_request(cred_headers: dict) -> Request:
    """合成最小 Request scope（凭证头 + app 实例），供后台下载复用在线取流逻辑。"""
    scope_headers = [
        (str(k).lower().encode("latin-1"), str(v).encode("latin-1"))
        for k, v in (cred_headers or {}).items()
    ]
    return Request({"type": "http", "headers": scope_headers, "app": app})


def _register_tee_handoff(guid: str, part: str, written: int, expected: int | None,
                          ext: str | None, headers_factory: "Callable[[], dict] | None") -> None:
    """tee 流式下载被客户端中断（切歌/退出）后，把未完成的 part 交给后台续传完成。"""
    if find_cache_file(guid):
        _remove_quiet(part)
        return
    task = _full_fetch_tasks.get(guid)
    if task is not None and not task.done():
        # 已有在途整轨下载：交给它完成，不重复续传
        _remove_quiet(part)
        return
    now = time.monotonic()
    _prune_full_fetch_state(now)
    if now - _full_fetch_failed.get(guid, -_FULL_FETCH_COOLDOWN_S) < _FULL_FETCH_COOLDOWN_S:
        _remove_quiet(part)
        return
    headers = headers_factory() if headers_factory is not None else {}
    task = asyncio.create_task(_tee_handoff_download(guid, part, written, expected, ext, headers))
    _full_fetch_tasks[guid] = task
    _tee_handoff_active.add(guid)

    def _done(t: "asyncio.Task") -> None:
        _tee_handoff_active.discard(guid)
        if _full_fetch_tasks.get(guid) is t:
            del _full_fetch_tasks[guid]

    task.add_done_callback(_done)


async def _tee_handoff_download(guid: str, part: str, written: int, expected: int | None,
                                ext: str | None, cred_headers: dict) -> None:
    """续传被中断的 tee 下载；源不支持断点续传或长度不符时回退整轨重下。"""
    try:
        resumed = await _resume_part_download(guid, part, written, expected, ext, cred_headers)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning("Tee handoff resume error for %s: %s", guid, type(e).__name__)
        resumed = False
    if resumed:
        return
    _remove_quiet(part)
    await _full_fetch_download(guid, cred_headers)


async def _resume_part_download(guid: str, part: str, written: int, expected: int | None,
                                ext: str | None, cred_headers: dict) -> bool:
    """Range 断点续拉剩余字节追加进 part；不可续传返回 False（调用方回退整轨重下）。"""
    fake_request = _synth_request(cred_headers)
    opened = await _open_online_stream(fake_request, guid, f"bytes={written}-")
    if not opened:
        return False
    resp, owned, stream_ext, info, chunks, first, _reconnect, _on_death = opened
    try:
        if resp.status_code != 206:
            # 源忽略 Range 返回 200：无法续传
            return False
        match = re.fullmatch(
            r"bytes\s+(\d+)-(\d+)/(\d+)", resp.headers.get("content-range", "").strip(), re.I,
        )
        if not match or int(match.group(1)) != written:
            return False
        total = int(match.group(3))
        if total <= 0 or (expected is not None and total != expected):
            return False
        append_written = 0
        with open(part, "ab") as fp:
            if first:
                fp.write(first)
                append_written += len(first)
            async for chunk in chunks:
                if chunk:
                    fp.write(chunk)
                    append_written += len(chunk)
        final_size = written + append_written
        if final_size < 1024 or final_size != total:
            raise RuntimeError(f"resume size mismatch final={final_size} total={total}")
        info = await _info_for_background_save(fake_request, guid, info)
        resolved_ext = ext or stream_ext or (info or {}).get("ext") or "mp3"
        meta = await asyncio.to_thread(_tee_finalize, part, guid, resolved_ext, info or {}, True)
        logger.info("Tee handoff resumed %s (+%d bytes, total %d)", guid, append_written, final_size)
        await _auto_lyric_after_finalize(fake_request, guid, meta)
        _schedule_library_scan(cred_headers)
        _dispatch_official_binding(guid, cred_headers, meta)
        return True
    finally:
        with anyio.CancelScope(shield=True):
            await resp.aclose()
            if owned:
                await owned.aclose()


# === 官方绑定：下载落库 → 等官方扫入库 → 写官方收藏/歌单 → 删本地映射 ===
# 官方接口只认曲库内 guid：先只读轮询官方 music.db 拿到新入库曲目的官方 guid，
# 再用触发时刻的凭证头直调官方写接口；成功后删除本地映射条目（官方列表唯一
# 显示，避免同一首歌官方/本地映射重复出现两次），失败保留本地映射兜底并
# 由列表读取路径自愈重试。凭证头仅留内存，绝不落盘。

_BIND_POLL_INTERVAL_S = 3.0
_BIND_RETRY_THROTTLE_S = 60.0
_BIND_RETRY_BATCH = 5
# guid -> user_guid -> {"fav": bool, "playlists": set[str], "headers": dict}
_bind_pending: "dict[str, dict[str, dict]]" = {}
_bind_tasks: "dict[tuple[str, str], asyncio.Task]" = {}
_bind_retry_last: "dict[str, float]" = {}


def _register_bind_intent(guid: str, user_guid: str, headers: "dict | None",
                          playlist_guid: str | None = None) -> None:
    """登记官方绑定意图（收藏或加入歌单），由下载完成路径统一调度执行。"""
    if not guid or not user_guid:
        return
    users = _bind_pending.setdefault(guid, {})
    intent = users.get(user_guid)
    if intent is None:
        intent = {"fav": False, "playlists": set(), "headers": dict(headers or {})}
        users[user_guid] = intent
    elif headers:
        intent["headers"] = dict(headers)
    if playlist_guid:
        intent["playlists"].add(playlist_guid)
    else:
        intent["fav"] = True


def official_track_guid_by_tags(title: str, artist: str, album: str = "") -> "str | None":
    """只读官方 music.db，按落盘标签匹配曲目的官方 guid（最新一条优先）。

    标签由 write_audio_tags 写入、官方扫描入库时读取，精确匹配即高保真；
    精确未命中再用大小写不敏感兜底一次（官方扫描器可能做过归一化）。
    """
    title_s = (title or "").strip()
    artist_s = (artist or "").strip()
    album_s = (album or "").strip()
    if not title_s or not artist_s:
        return None
    db = str(CONF.get("music_db") or "")
    if not db or not os.path.exists(db):
        return None
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    except Exception as e:
        logger.warning("Official bind db open failed: %s", e)
        return None
    try:
        for nocase in (False, True):
            collate = " COLLATE NOCASE" if nocase else ""
            sql = (
                "SELECT t.guid FROM track t "
                "JOIN track_artist ta ON ta.track_id = t.id "
                "JOIN artist a ON a.id = ta.artist_id "
                f"WHERE t.title = ?{collate} AND a.name = ?{collate}"
            )
            params: list = [title_s, artist_s]
            if album_s:
                sql += (
                    " AND EXISTS (SELECT 1 FROM album al"
                    f" WHERE al.id = t.album_id AND al.name = ?{collate})"
                )
                params.append(album_s)
            sql += " ORDER BY t.id DESC LIMIT 1"
            try:
                row = con.execute(sql, params).fetchone()
            except Exception as e:
                logger.warning("Official bind db query failed: %s", e)
                return None
            if row and row[0]:
                return str(row[0])
        return None
    finally:
        con.close()


def library_file_by_tags(title: str, artist: str, album: str = "") -> "str | None":
    """按标签在官方 music.db 反查曲库文件路径（audio_file.path，最新优先）。

    find_cache_file 的兜底数据源：与 official_track_guid_by_tags 同款两遍
    精确/NOCASE 查询，但取 a.path 且逐行校验文件确实存在（官方库里的
    悬空记录不算命中）。
    """
    title_s = (title or "").strip()
    artist_s = (artist or "").strip()
    album_s = (album or "").strip()
    if not title_s or not artist_s:
        return None
    db = str(CONF.get("music_db") or "")
    if not db or not os.path.exists(db):
        return None
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    except Exception as e:
        logger.warning("Library tags lookup db open failed: %s", e)
        return None
    try:
        for nocase in (False, True):
            collate = " COLLATE NOCASE" if nocase else ""
            sql = (
                "SELECT a.path FROM track t "
                "JOIN audio_file a ON a.id = t.audio_file_id "
                "JOIN track_artist ta ON ta.track_id = t.id "
                "JOIN artist ar ON ar.id = ta.artist_id "
                f"WHERE t.title = ?{collate} AND ar.name = ?{collate}"
            )
            params: list = [title_s, artist_s]
            if album_s:
                sql += (
                    " AND EXISTS (SELECT 1 FROM album al"
                    f" WHERE al.id = t.album_id AND al.name = ?{collate})"
                )
                params.append(album_s)
            sql += " ORDER BY t.id DESC"
            try:
                rows = con.execute(sql, params).fetchall()
            except Exception as e:
                logger.warning("Library tags lookup query failed: %s", e)
                return None
            for row in rows:
                path = str(row[0] or "").strip()
                if path and os.path.isfile(path) and os.path.getsize(path) > 0:
                    return path
        return None
    finally:
        con.close()


async def _official_upstream_write(method: str, path: str, headers: dict, payload: dict) -> bool:
    """以扩展名义直调官方写接口（复用触发请求凭证头）；code==0 视为成功。"""
    try:
        client = get_upstream_client(app)
        req = client.build_request(method, path, headers=dict(headers or {}), json=payload)
        resp = await client.send(req)
        if resp.status_code != 200:
            logger.warning("Official bind %s %s -> http %s", method, path, resp.status_code)
            return False
        body = resp.json()
    except Exception as e:
        logger.warning("Official bind %s %s failed: %s", method, path, type(e).__name__)
        return False
    if isinstance(body, dict) and body.get("code") == 0:
        return True
    logger.warning("Official bind %s %s -> %s", method, path, body)
    return False


def _fav_bind_pending(guid: str, user_guid: str) -> bool:
    try:
        return any(
            it.get("guid") == guid and it.get("bind") == "pending"
            for it in load_online_favorites(user_guid)
        )
    except Exception:
        return False


async def _remove_fav_bound(guid: str, user_guid: str) -> bool:
    """官方收藏写入成功后删本地映射条目（仅 bind=pending；老数据不动）。"""
    async with _FAV_LOCK:
        try:
            items = load_online_favorites(user_guid)
            kept = [it for it in items if not (it.get("guid") == guid and it.get("bind") == "pending")]
            if len(kept) == len(items):
                return False
            save_online_favorites(user_guid, kept)
            return True
        except Exception as e:
            logger.warning("Error removing bound favorite for %s/%s: %s", user_guid, guid, e)
            return False


def _plt_bind_pending(playlist_guid: str, guid: str, user_guid: str) -> bool:
    try:
        bucket = load_playlist_tracks(user_guid).get(playlist_guid) or []
        return any(it.get("guid") == guid and it.get("bind") == "pending" for it in bucket)
    except Exception:
        return False


async def _remove_plt_bound(playlist_guid: str, guid: str, user_guid: str) -> bool:
    async with _PLT_LOCK:
        try:
            items = load_playlist_tracks(user_guid)
            bucket = items.get(playlist_guid)
            if not bucket:
                return False
            kept = [it for it in bucket if not (it.get("guid") == guid and it.get("bind") == "pending")]
            if len(kept) == len(bucket):
                return False
            if kept:
                items[playlist_guid] = kept
            else:
                items.pop(playlist_guid, None)
            save_playlist_tracks(user_guid, items)
            return True
        except Exception as e:
            logger.warning("Error removing bound playlist track for %s/%s: %s", user_guid, guid, e)
            return False


def _dispatch_official_binding(guid: str, headers: "dict | None", meta: "dict | None") -> None:
    """下载落库完成后调度官方绑定：逐用户去重起任务；无意图时零开销。"""
    if not CONF.get("fav_auto_bind"):
        return
    users = _bind_pending.get(guid)
    if not users:
        return
    for user_guid in list(users):
        if headers:
            users[user_guid]["headers"] = dict(headers)
        key = (guid, user_guid)
        existing = _bind_tasks.get(key)
        if existing is not None and not existing.done():
            continue
        task = asyncio.create_task(_bind_official_task(guid, user_guid, meta))
        _bind_tasks[key] = task

        def _done(t: "asyncio.Task", key=key) -> None:
            if _bind_tasks.get(key) is t:
                del _bind_tasks[key]

        task.add_done_callback(_done)


async def _bind_official_task(guid: str, user_guid: str, meta: "dict | None") -> None:
    """把单个用户的收藏/歌单意图写入官方，成功后删本地映射（官方列表唯一显示）。"""
    intent = (_bind_pending.get(guid) or {}).get(user_guid)
    if not intent:
        return
    headers = dict(intent.get("headers") or {})
    title = str((meta or {}).get("title") or "")
    artist = str((meta or {}).get("artist") or "")
    album = str((meta or {}).get("album") or "")
    if not (title.strip() and artist.strip()):
        snap = _lookup_online_snapshot(guid)
        snap = _snapshot_to_info(snap) if isinstance(snap, dict) and snap else {}
        title = title.strip() or str(snap.get("title") or "")
        artist = artist.strip() or str(snap.get("artist") or "")
        album = album.strip() or str(snap.get("album") or "")
    if not (title.strip() and artist.strip()):
        logger.info("Official bind skip for %s: no usable metadata", guid)
        return
    # 官方只认曲库内 guid：等待官方把刚落盘文件扫入库（只读轮询，绝不写官方库）
    _schedule_library_scan(headers)
    try:
        timeout_s = max(1.0, float(CONF.get("official_bind_timeout_s") or 120))
    except (TypeError, ValueError):
        timeout_s = 120.0
    deadline = time.monotonic() + timeout_s
    official_guid = await asyncio.to_thread(official_track_guid_by_tags, title, artist, album)
    while not official_guid and time.monotonic() < deadline:
        await asyncio.sleep(_BIND_POLL_INTERVAL_S)
        official_guid = await asyncio.to_thread(official_track_guid_by_tags, title, artist, album)
    if not official_guid:
        logger.info(
            "Official bind wait timeout for %s (%s - %s)：官方曲库尚未收录，保留本地映射待自愈重试",
            guid, artist, title,
        )
        return
    if intent.get("fav") and _fav_bind_pending(guid, user_guid):
        ok = await _official_upstream_write(
            "POST", "/music/api/v1/favorite-track/create", headers, {"trackGUID": official_guid},
        )
        if ok and await _remove_fav_bound(guid, user_guid):
            _record_bind(user_guid, guid, official_guid)
            logger.info("Official favorite bound for %s (%s) -> %s", guid, user_guid, official_guid)
        elif ok:
            # 写官方期间用户已取消收藏：补偿撤销官方收藏，不留幽灵状态
            await _official_upstream_write(
                "POST", "/music/api/v1/favorite-track/delete", headers, {"trackGUID": official_guid},
            )
    for playlist_guid in sorted(set(intent.get("playlists") or ())):
        if not _plt_bind_pending(playlist_guid, guid, user_guid):
            continue
        ok = await _official_upstream_write(
            "POST", "/music/api/v1/playlist/add-track", headers,
            {"guid": playlist_guid, "trackGUIDs": [official_guid]},
        )
        if ok and await _remove_plt_bound(playlist_guid, guid, user_guid):
            _record_bind(user_guid, guid, official_guid, playlist_guid=playlist_guid)
            logger.info("Official playlist bound for %s (%s -> %s)", guid, playlist_guid, official_guid)
        elif ok:
            await _official_upstream_write(
                "POST", "/music/api/v1/playlist/remove-track", headers,
                {"guid": playlist_guid, "trackGUIDs": [official_guid]},
            )
    # 意图清理：本地已无 pending 条目则移除；失败项保留待列表读取自愈重试
    live = _bind_pending.get(guid, {}).get(user_guid)
    if live is not None:
        still_pending = (live.get("fav") and _fav_bind_pending(guid, user_guid)) or any(
            _plt_bind_pending(pl, guid, user_guid) for pl in (live.get("playlists") or ())
        )
        if not still_pending and _bind_pending.get(guid, {}).get(user_guid) is live:
            _bind_pending[guid].pop(user_guid, None)
            if not _bind_pending[guid]:
                _bind_pending.pop(guid, None)


# === 绑定登记（online guid -> 官方 guid，bind_registry.json） ===
# 官方绑定完成后本地映射已删，但客户端旧会话可能仍持伪装 id 操作取消收藏/
# 移出歌单；登记命中时翻译成官方 guid 转发官方删除，避免"取消没生效"。

_BIND_REGISTRY_LIMIT = 1000
_BIND_REGISTRY_TTL_S = 30 * 86400.0
_bind_registry: "dict[str, dict[str, dict]]" = {}
_bind_registry_loaded = False


def bind_registry_path() -> str:
    return os.path.join(_HOME, "bind_registry.json")


def _ensure_bind_registry() -> None:
    global _bind_registry_loaded
    if _bind_registry_loaded:
        return
    _bind_registry_loaded = True
    try:
        with open(bind_registry_path(), encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            for user, entries in data.items():
                if isinstance(entries, dict):
                    _bind_registry[str(user)] = {
                        str(g): e for g, e in entries.items() if isinstance(e, dict)
                    }
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning("Failed to load bind registry: %s", e)


def _persist_bind_registry() -> None:
    path = bind_registry_path()
    parent = os.path.dirname(path) or "."
    part_path = f"{path}.{uuid4().hex[:8]}.part"
    try:
        os.makedirs(parent, exist_ok=True)
        with open(part_path, "w", encoding="utf-8") as f:
            json.dump(_bind_registry, f, ensure_ascii=False, indent=2)
        os.replace(part_path, path)
    except Exception as e:
        logger.warning("Failed to persist bind registry: %s", e)
        _remove_quiet(part_path)


def _prune_bind_registry() -> None:
    now = time.time()
    for user in list(_bind_registry):
        entries = _bind_registry[user]
        for g, entry in list(entries.items()):
            try:
                stale = now - float(entry.get("at") or 0) >= _BIND_REGISTRY_TTL_S
            except (TypeError, ValueError):
                stale = True
            if stale:
                entries.pop(g, None)
        if not entries:
            _bind_registry.pop(user, None)
    while sum(len(v) for v in _bind_registry.values()) > _BIND_REGISTRY_LIMIT:
        oldest: "tuple[float, str, str] | None" = None
        for user, entries in _bind_registry.items():
            for g, entry in entries.items():
                try:
                    at = float(entry.get("at") or 0)
                except (TypeError, ValueError):
                    at = 0.0
                if oldest is None or at < oldest[0]:
                    oldest = (at, user, g)
        if oldest is None:
            break
        _bind_registry[oldest[1]].pop(oldest[2], None)
        if not _bind_registry[oldest[1]]:
            _bind_registry.pop(oldest[1], None)


def _bind_entry_alive(entry: dict) -> bool:
    return bool(entry.get("fav") or entry.get("playlists"))


def _record_bind(user_guid: str, guid: str, official_guid: str,
                 playlist_guid: str | None = None) -> None:
    """官方写入成功后登记 online guid → 官方 guid 映射（旧会话假 id 删除时翻译转发）。"""
    _ensure_bind_registry()
    entries = _bind_registry.setdefault(user_guid, {})
    entry = entries.get(guid)
    if not isinstance(entry, dict) or entry.get("official") != official_guid:
        entry = {"official": official_guid, "fav": False, "playlists": [], "at": time.time()}
        entries[guid] = entry
    if playlist_guid:
        pls = [p for p in (entry.get("playlists") or []) if p != playlist_guid]
        pls.append(playlist_guid)
        entry["playlists"] = pls
    else:
        entry["fav"] = True
    entry["at"] = time.time()
    _prune_bind_registry()
    _persist_bind_registry()


def lookup_bind_entry(user_guid: str, guid: str) -> "dict | None":
    _ensure_bind_registry()
    entry = _bind_registry.get(user_guid, {}).get(guid)
    return dict(entry) if isinstance(entry, dict) else None


def _forget_bind(user_guid: str, guid: str, fav: bool = False,
                 playlist_guid: str | None = None) -> None:
    """撤销登记中的收藏/歌单标记；两边都空则整条删除。"""
    _ensure_bind_registry()
    entries = _bind_registry.get(user_guid)
    if not entries:
        return
    entry = entries.get(guid)
    if not isinstance(entry, dict):
        return
    if fav:
        entry["fav"] = False
    if playlist_guid:
        entry["playlists"] = [p for p in (entry.get("playlists") or []) if p != playlist_guid]
    if not _bind_entry_alive(entry):
        entries.pop(guid, None)
    if not entries:
        _bind_registry.pop(user_guid, None)
    _persist_bind_registry()


def _forget_bind_playlist(user_guid: str, playlist_guid: str) -> None:
    """删除歌单后清掉登记里该歌单的标记（防孤儿转发）。"""
    _ensure_bind_registry()
    entries = _bind_registry.get(user_guid)
    if not entries:
        return
    changed = False
    for g in list(entries):
        entry = entries[g]
        pls = [p for p in (entry.get("playlists") or []) if p != playlist_guid]
        if pls != (entry.get("playlists") or []):
            entry["playlists"] = pls
            changed = True
            if not _bind_entry_alive(entry):
                entries.pop(g, None)
    if not entries:
        _bind_registry.pop(user_guid, None)
        changed = True
    if changed:
        _persist_bind_registry()


def _lookup_bind_any_user(raw_guid: str) -> "tuple[str, str, dict] | None":
    """跨用户按在线 guid 或其伪装 32hex 形态反查绑定登记。

    返回 (user_guid, online_guid, entry)。官方绑定完成后本地映射已删、伪装
    反查表可能不再认识该 id，这里兜底直接比对确定性 md5。
    """
    _ensure_bind_registry()
    raw = str(raw_guid or "").strip()
    if not raw or not _bind_registry:
        return None
    resolved = resolve_real_guid(raw)
    for user, entries in _bind_registry.items():
        if is_online_guid(resolved) and resolved in entries:
            return user, resolved, dict(entries[resolved])
    if re.fullmatch(r"[0-9a-fA-F]{32}", raw):
        for user, entries in _bind_registry.items():
            for g, entry in entries.items():
                if fake_official_guid(g) == raw:
                    return user, g, dict(entry)
    return None


def _maybe_retry_official_binds(request: Request, user_guid: str,
                                fav_items: "list[dict] | None" = None,
                                plt_map: "dict[str, list[dict]] | None" = None) -> None:
    """列表读取时的自愈：bind=pending 条目补发官方绑定或下载（用户级 60s 节流）。

    覆盖代理重启丢内存意图、官方迟扫入库、下载中断等场景；凭证头取当前请求，
    绝不持久化。
    """
    if not CONF.get("fav_auto_bind") or not user_guid:
        return
    fav_guids = [
        str(it.get("guid") or "") for it in (fav_items or [])
        if it.get("bind") == "pending" and it.get("guid")
    ]
    plt_guids: "dict[str, list[str]]" = {}
    for playlist_guid, bucket in (plt_map or {}).items():
        pending = [
            str(it.get("guid") or "") for it in (bucket or [])
            if it.get("bind") == "pending" and it.get("guid")
        ]
        if pending:
            plt_guids[playlist_guid] = pending
    if not fav_guids and not plt_guids:
        return
    now = time.monotonic()
    if now - _bind_retry_last.get(user_guid, -_BIND_RETRY_THROTTLE_S) < _BIND_RETRY_THROTTLE_S:
        return
    _bind_retry_last[user_guid] = now
    headers = copy_incoming_headers(request)
    uniq = list(dict.fromkeys(fav_guids + [g for gs in plt_guids.values() for g in gs]))
    for guid in fav_guids[:_BIND_RETRY_BATCH]:
        _register_bind_intent(guid, user_guid, headers, None)
    for playlist_guid, guids in plt_guids.items():
        for guid in guids[:_BIND_RETRY_BATCH]:
            _register_bind_intent(guid, user_guid, headers, playlist_guid)
    for guid in uniq[:_BIND_RETRY_BATCH]:
        if find_cache_file(guid):
            _dispatch_official_binding(guid, headers, None)
        else:
            _register_background_fetch(request, guid, "fav_auto_bind")


_STREAM_PREVIEW_TTL_S = 3.0
_STREAM_PREVIEW_CACHE: dict[tuple[str, str, str], dict] = {}

def _stream_preview_key(request: Request, guid: str, range_header: str | None) -> tuple[str, str, str]:
    return (guid, range_header or "", _credential_scope(request))

async def _close_stream_preview(preview: dict) -> None:
    opened = preview.get("opened")
    if not opened:
        return
    resp, owned, *_ = opened
    try:
        await resp.aclose()
    except Exception:
        pass
    if owned:
        try:
            await owned.aclose()
        except Exception:
            pass

def _schedule_stream_preview_expiry(key: tuple[str, str, str], preview: dict) -> None:
    async def expire() -> None:
        await asyncio.sleep(_STREAM_PREVIEW_TTL_S)
        if _STREAM_PREVIEW_CACHE.get(key) is preview:
            _STREAM_PREVIEW_CACHE.pop(key, None)
            await _close_stream_preview(preview)
    preview["cleanup_task"] = asyncio.create_task(expire())

def _take_stream_preview(key: tuple[str, str, str]) -> tuple | None:
    preview = _STREAM_PREVIEW_CACHE.pop(key, None)
    if not preview:
        return None
    cleanup = preview.get("cleanup_task")
    if cleanup and not cleanup.done():
        cleanup.cancel()
    if time.monotonic() >= preview["expires_at"]:
        asyncio.create_task(_close_stream_preview(preview))
        return None
    return preview["opened"]


# === 2.6.54 下一首预开流（preopen）===
# 2.6.53 之后插件侧切歌已近零耗时，但外网手机点"下一首"时仍要自己连 CDN
# （TLS 握手 + 首包 + 播放器缓冲，约 1~2s）。这里复刻安卓 ExoPlayer 的
# "预备队列"思路：当前首播期间，后台把紧邻下一首的流真正打开（连上 CDN、
# 取到首包），手机点下一首时直接从已开的流里出字节，首包等待降为一个网络
# 往返。只预开 1 首；命中那次切歌走中转（手机↔NAS↔CDN，音乐码率低，NAS
# 带宽开销可忽略）；TTL 内不切则自动关闭。预开只打 CDN、不消耗音源取链
# 配额（URL 一律由 2.6.53 的预取解析好，预开只消费缓存里的链）；任何失败
# 都静默回落现有路径（307 快路径/中转），成功率不受影响。
_PREOPEN_TTL_S = float(os.environ.get("FNMUSIC_PREOPEN_TTL_S", "120"))
_PREOPEN_MAX = int(os.environ.get("FNMUSIC_PREOPEN_MAX", "1"))
_PREOPEN_STREAMS: dict[str, dict] = {}
_PREOPEN_INFLIGHT: set[tuple[str, str]] = set()


def _preopen_key(scope: str, guid: str) -> str:
    return f"{scope}\x00{guid}"


def _lx_url_cached_fresh(song_id: str) -> bool:
    hit = _LX_URL_CACHE.get(song_id)
    return bool(hit and time.time() - hit[1] < _LX_URL_CACHE_TTL_S)


async def _preopen_wait_url(song_id: str, timeout_s: float) -> bool:
    """等 2.6.53 预取把下一首的直链解析进缓存（预取与预开同批触发）。

    预开绝不自行调音源取链：URL 已缓存 → 直接去连 CDN；预取还在跑 → 等它；
    预取已放弃（冷却/无货）→ 本次不预开，播放请求走正常路径兜底。
    """
    if not song_id or _lx_url_cached_fresh(song_id):
        return True
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        await asyncio.sleep(0.15)
        if _lx_url_cached_fresh(song_id):
            return True
        if song_id not in _LX_PREFETCH_RUNNING:
            return False
    return _lx_url_cached_fresh(song_id)


async def _preopen_expiry(key: str, entry: dict) -> None:
    await asyncio.sleep(_PREOPEN_TTL_S)
    if _PREOPEN_STREAMS.get(key) is entry:
        _PREOPEN_STREAMS.pop(key, None)
        await _close_stream_preview(entry)


def _take_preopen(guid: str, range_header: str | None, scope: str) -> "tuple | None":
    """切歌时领取已预开的流。只有"从头取"（无 Range 或 bytes=0-）才匹配：
    seek 请求的字节偏移和预开流对不上，丢弃预开流走正常路径（CDN 自会 Range）。"""
    entry = _PREOPEN_STREAMS.pop(_preopen_key(scope, guid), None)
    if not entry:
        return None
    cleanup = entry.get("cleanup_task")
    if cleanup and not cleanup.done():
        cleanup.cancel()
    if time.monotonic() >= entry["expires_at"] or not should_cache(range_header):
        asyncio.create_task(_close_stream_preview(entry))
        return None
    return entry["opened"]


async def _preopen_next_track(request: Request, next_guid: str) -> None:
    """后台预开页内紧邻下一首的流（fire-and-forget，失败静默）。"""
    if not next_guid or not is_online_guid(next_guid):
        return
    if find_cache_file(next_guid):
        return  # 本地文件 0ms 读，无需预开
    if not _source_enabled(next_guid):
        return
    scope = _credential_scope(request)
    key = _preopen_key(scope, next_guid)
    if key in _PREOPEN_STREAMS or len(_PREOPEN_STREAMS) >= _PREOPEN_MAX:
        return
    inflight = (scope, next_guid)
    if inflight in _PREOPEN_INFLIGHT:
        return
    _PREOPEN_INFLIGHT.add(inflight)
    try:
        song_id = song_id_from_online_guid(next_guid)
        if not await _preopen_wait_url(song_id, 6.0):
            return
        opened = await asyncio.wait_for(
            _open_online_stream(request, next_guid, "bytes=0-"), timeout=8.0)
        if not opened:
            return
        if key in _PREOPEN_STREAMS or len(_PREOPEN_STREAMS) >= _PREOPEN_MAX:
            await _close_stream_preview({"opened": opened})
            return
        entry = {"opened": opened, "expires_at": time.monotonic() + _PREOPEN_TTL_S}
        _PREOPEN_STREAMS[key] = entry
        entry["cleanup_task"] = asyncio.create_task(_preopen_expiry(key, entry))
        logger.info("preopen ready for %s", next_guid)
    except Exception:  # noqa: BLE001
        logger.debug("preopen next track failed", exc_info=True)
    finally:
        _PREOPEN_INFLIGHT.discard(inflight)


async def _stream_head_response(request: Request, guid: str, cached: str | None, range_header: str | None) -> Response:
    """HEAD 探测（部分手机播放器先 HEAD 后 GET）：缓存命中回真实大小头。"""
    if cached:
        ext = os.path.splitext(cached)[1].lstrip(".") or "mp3"
        full = serve_file_with_range(cached, range_header, media_type_for_ext(ext))
        return Response(status_code=full.status_code, headers=dict(full.headers))
    opened = None
    try:
        opened = await asyncio.wait_for(_open_online_stream(request, guid, range_header), timeout=15.0)
    except Exception:
        opened = None
    if not opened:
        return JSONResponse(content={"code": 404, "msg": "online source unavailable", "data": None}, status_code=404)
    resp, owned, _ext, _info, _chunks, _first, _reconnect, _on_death = opened
    headers = {"Accept-Ranges": "bytes", "Cache-Control": "no-store"}
    for key in ("content-type", "content-length", "content-range"):
        val = resp.headers.get(key)
        if val:
            headers[key] = val
    preview_key = _stream_preview_key(request, guid, range_header)
    old = _STREAM_PREVIEW_CACHE.pop(preview_key, None)
    if old:
        asyncio.create_task(_close_stream_preview(old))
    preview = {"opened": opened, "expires_at": time.monotonic() + _STREAM_PREVIEW_TTL_S}
    _STREAM_PREVIEW_CACHE[preview_key] = preview
    _schedule_stream_preview_expiry(preview_key, preview)
    _preheat_track_assets(request, guid)
    return Response(status_code=206 if "content-range" in headers else 200, headers=headers)


# === 2.6.36 首播预热：建流的同时后台静默取封面/歌词（fire-and-forget，
# 绝不挡首包）。6h 冷却 + single-flight：同一首歌的重复 HEAD/GET 不反复触发。 ===
_PREHEAT_INFLIGHT: set[str] = set()
_PREHEAT_AT: dict[str, float] = {}
_PREHEAT_COOLING_S = 6 * 3600.0
# 2.6.37: 全局流控。NAS 现场实测：单 uvicorn 进程 + nginx 前置，并发 8 条流
# 请求时 75% 失败（502/401），元凶是 2.6.36 后台管道无上限（每首播一首同时
# 跑 5 平台封面搜索 + 20s 全模式歌词搜索 + 下一首预取），把主流请求拖死。
# 桌面端同类回补只有并发 6 且建好缓存后不再搜索——这里更保守：预热全局
# 最多 2 首并发、歌词回补 1 条、封面搜索 2 条在途，满则直接放弃本轮
# （冷却/负缓存机制保证下轮还会试），主播放路径永远优先。
_PREHEAT_JOB_MAX = 2
_LYRIC_JOB_MAX = 1
_COVER_SEARCH_MAX = 2
_LYRIC_JOB_ACTIVE: "int" = 0
_COVER_SEARCH_ACTIVE: "int" = 0
_PREHEAT_STAGGER_S = 1.5
_LYRIC_STAGGER_S = 4.0


def _preheat_track_assets(request: Request, guid: str) -> None:
    if not is_online_guid(guid):
        return
    if guid in _PREHEAT_INFLIGHT:
        return
    if len(_PREHEAT_INFLIGHT) >= _PREHEAT_JOB_MAX:
        return
    last = _PREHEAT_AT.get(guid)
    if last is not None and time.monotonic() - last < _PREHEAT_COOLING_S:
        return
    _PREHEAT_AT[guid] = time.monotonic()
    _PREHEAT_INFLIGHT.add(guid)

    def _done(task: "asyncio.Task") -> None:
        _PREHEAT_INFLIGHT.discard(guid)
        if not task.cancelled():
            try:
                task.exception()
            except Exception:  # noqa: BLE001
                pass

    task = asyncio.create_task(_preheat_track_assets_task(request, guid))
    task.add_done_callback(_done)


async def _preheat_track_assets_task(request: Request, guid: str) -> None:
    await asyncio.sleep(_PREHEAT_STAGGER_S)  # 让主流/客户端先走完首包
    try:
        data = await _online_info(request, guid, include_lyric=False)
    except Exception as e:
        logger.debug("preheat info failed for %s: %s", guid, e)
        return
    if not data:
        return
    try:
        if not _read_cover_cache(guid):
            tail = await _cover_fallback_tail(request, guid, data)
            if isinstance(tail, RedirectResponse):
                # 直构/补全命中的是 302：把目标图下载落盘，客户端后续请求直接走缓存
                target = str(tail.headers.get("location") or "")
                if target:
                    got = await asyncio.to_thread(download_cover_bytes, target)
                    if got:
                        _write_cover_cache(guid, got[0], got[1])
    except Exception as e:
        logger.debug("preheat cover failed for %s: %s", guid, e)
    try:
        if not read_lyric_cache(guid):
            _ensure_lyric_backfill(request, guid, data)
    except Exception as e:
        logger.debug("preheat lyric failed for %s: %s", guid, e)


def _candidate_kbps(item: dict) -> float:
    """同录音候选的估算码率 kbps（file_size*8/duration）；未知返回 0。"""
    try:
        dur = float(item.get("duration_s") or 0)
        size = float(item.get("file_size") or 0)
        if dur > 0 and size > 0:
            return size * 8.0 / dur / 1000.0
    except (TypeError, ValueError):
        pass
    return 0.0


def ordered_stream_alternatives(item: dict) -> list[dict]:
    """musicdl 同录音跨平台候选按音质模式排序。

    high：码率高者优先；balanced：中间码率先向下再向上；smooth：码率低者优先。
    无码率信息时保持原始顺序。
    """
    alts = [
        x for x in (item.get("_alternatives") or [])
        if isinstance(x, dict) and _same_recording(item, x)
    ]
    mode = str(CONF.get("quality_mode") or "high").strip().lower()
    known = sorted({round(_candidate_kbps(x), 1) for x in alts if _candidate_kbps(x) > 0})
    if not known:
        return list(alts)
    if mode == "smooth":
        order = known
    elif mode == "balanced":
        mid = len(known) // 2
        order = [known[mid]] + list(reversed(known[:mid])) + known[mid + 1:]
    else:
        order = list(reversed(known))
    rank = {v: i for i, v in enumerate(order)}
    return sorted(alts, key=lambda x: rank.get(round(_candidate_kbps(x), 1), len(order)))


# ============================================================================
# 方案B：外网直连 CDN（2.6.26）
#
# 在线歌播放时先解析出 CDN 直链，再做一次“裸请求”探测（模拟手机、
# 不带 Referer/UA）：CDN 真的接受直连才 307 把客户端重定向到 CDN 取流
# （绕过 NAS 中转，接近洛雪客户端直连体验、更接近官方速度）。任一步失败
# 都自动回落下面的现有中转路径，起播率不劣于旧路径。
#
# 支持源：lx（洛雪用户源）、netease（网易盒子）；其他源（酷我/咪咕等）
# 无独立可直连地址，继续走中转。音质按插件设置：解析阶段与现有路径完全
# 一致（同档序/同替代曲逻辑），只有交付路径不同。
# 开关：FNMUSIC_WAN_DIRECT（默认 true）；管理页「播放与推荐」可关，.env
# 热重载立即生效；关闭即完全恢复旧行为。
# ============================================================================
_WAN_DIRECT_PROBE_TIMEOUT_S = 3.0
_WAN_DIRECT_RESOLVE_TIMEOUT_S = 12.0


_PROBE_CLIENT: "httpx.AsyncClient | None" = None


def _probe_client() -> httpx.AsyncClient:
    """共享 CDN 探测客户端：复用连接（同 CDN 反复探测不重建 TCP/TLS 会话）。"""
    global _PROBE_CLIENT
    if _PROBE_CLIENT is None or _PROBE_CLIENT.is_closed:
        _PROBE_CLIENT = httpx.AsyncClient(
            follow_redirects=True,
            timeout=_WAN_DIRECT_PROBE_TIMEOUT_S,
            limits=httpx.Limits(max_keepalive_connections=8, keepalive_expiry=30.0),
        )
    return _PROBE_CLIENT


def reset_probe_client() -> None:
    """测试入口：丢弃旧客户端，防止 mock transport 跨用例泄漏。"""
    global _PROBE_CLIENT
    client, _PROBE_CLIENT = _PROBE_CLIENT, None
    if client is not None and not client.is_closed:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass


async def _wan_direct_probe(url: str, range_header: str | None,
                            headers: "dict | None" = None) -> "tuple[bool, str]":
    """“裸请求”（无 Referer/UA，模拟手机）验证 CDN 直链可播。

    传入源专属 headers 时按带请求头口径复验（用于区分防盗链与死链）。
    CDN 若要求源专属请求头（防盗链），裸请求会 403/回文本，手机直连必然不稳，
    此时中转路径（能带正确请求头）比重定向更可靠，由调用方回落。
    """
    req_headers = {"Accept-Encoding": "identity"}
    if range_header:
        req_headers["Range"] = range_header
    if headers:
        for key, value in headers.items():
            if value:
                req_headers[str(key)] = str(value)
    client = _probe_client()
    try:
        resp = await client.send(client.build_request("GET", url, headers=req_headers), stream=True)
    except Exception as exc:
        return False, f"probe {type(exc).__name__}"
    status = resp.status_code
    ctype = resp.headers.get("content-type", "").lower()
    ce = resp.headers.get("content-encoding", "identity").strip().lower()
    first = b""
    try:
        first = await anext(resp.aiter_bytes(), b"")
    except Exception:
        first = b""
    try:
        await resp.aclose()
    except Exception:
        pass
    ok = (
        status in (200, 206)
        and not any(x in ctype for x in ("text/", "json", "html"))
        and ce in ("", "identity")
        and bool(first)
    )
    return ok, f"status={status} ctype={ctype or 'n/a'}"

# TOP[top.wan.direct] 外网直连：探测通过后 302 到音源 CDN（端侧直拉，NAS 只解地址）
async def _wan_direct_redirect(request: Request, guid: str, range_header: str | None):
    """方案B：直链验证通过返回 307 重定向响应；不可用时返回 None（调用方继续现有路径）。"""
    if not CONF.get("wan_direct_enabled"):
        return None
    source = source_from_online_guid(guid)
    if source not in ("lx", "netease"):
        return None
    if not CONF.get({"lx": "lx_enabled", "netease": "netease_enabled"}.get(source, "")):
        return None
    url = None
    try:
        if source == "lx":
            resolved = await asyncio.wait_for(
                resolve_lx_url(get_lx_client(request.app), song_id_from_online_guid(guid)),
                timeout=_WAN_DIRECT_RESOLVE_TIMEOUT_S,
            )
            if resolved:
                url = str(resolved.get("url") or "")
        else:
            url = await asyncio.wait_for(
                resolve_netease_url(
                    get_musicbox_client(request.app),
                    song_id_from_online_guid(guid).split(":")[-1],
                ),
                timeout=_WAN_DIRECT_RESOLVE_TIMEOUT_S,
            )
    except Exception as exc:
        logger.info("[wan-direct] resolve failed for %s: %s", guid, type(exc).__name__)
        return None
    if not url.lower().startswith(("http://", "https://")):
        return None
    if not _lx_url_probe_ok(song_id_from_online_guid(guid), url):
        ok, detail = await _wan_direct_probe(url, range_header)
        if not ok:
            logger.info("[wan-direct] probe rejected for %s (%s): %s", guid, source, detail)
            return None
    else:
        # 2.6.53：预取已按手机口径验证过这条链（新鲜窗口内）→ 跳过 inline 探测
        logger.info("[wan-direct] prefetch-verified, skip inline probe for %s", guid)
    logger.warning("[wan-direct] direct %s (%s) -> %s", guid, source, url[:100])
    return RedirectResponse(url, status_code=307, headers={"Cache-Control": "no-store"})

@app.exception_handler(Exception)
async def _stream_unhandled_to_404(request: Request, exc: Exception) -> Response:
    """2.6.37: 流媒体路径上的未捕获异常一律回 404 干净跳过。

    NAS 现场实测：后台负载把单进程事件循环打满时，流请求会以 500/连接重置
    形式炸出（客户端表现同样是跳歌）。流路径降级成 404（客户端标准跳歌），
    其他路径保留原语义（500 JSON）。
    """
    if str(request.url.path).startswith("/music/api/v1/track/stream"):
        logger.exception("stream request unhandled for %s: %s", request.url.path, exc)
        return JSONResponse(content={"code": 404, "msg": "online source unavailable",
                                     "data": None}, status_code=404)
    return JSONResponse(content={"code": 500, "msg": str(exc)[:200], "data": None},
                        status_code=500)


@app.api_route("/music/api/v1/track/stream", methods=["GET", "HEAD"])
@app.api_route("/music/api/v1/track/stream/{subpath:path}", methods=["GET", "HEAD"])
async def stream_track(request: Request, subpath: str = ""):
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    range_header = request.headers.get("range")
    cached = find_cache_file(guid)
    # ---- 2.6.54 预开命中：当前首播期间后台已把下一首的流打开（CDN 已连上+首包在手）----
    # 必须先于方案B 307 检查：预开流首包在手，比再解析+再探测更快。只针对
    # "GET 从头播"；seek（Range 偏移）不匹配预开，_take_preopen 自动丢弃并
    # 落到方案B/中转；_ext_rendition=1（客户端已持有中转地址）不得中途换流。
    if (not cached and request.method == "GET"
            and request.query_params.get("_ext_rendition") != "1"):
        pre_opened = _take_preopen(guid, range_header, _credential_scope(request))
        if pre_opened:
            resp, owned, ext, info, chunks, first, reconnect_fn, on_death = pre_opened
            _schedule_next_track_prefetch(guid, request)
            _preheat_track_assets(request, guid)
            return stream_tee_response(resp, guid, range_header,
                coro_factory=lambda: _online_info(request, guid, include_lyric=bool(CONF.get("lyric_auto_dl"))),
                client_to_close=owned, resolved_ext=ext, pre_info=info, chunks=chunks, first_chunk=first,
                scan_headers_factory=lambda: copy_incoming_headers(request),
                post_finalize=lambda meta: _auto_lyric_after_finalize(request, guid, meta),
                reconnect=reconnect_fn, on_death=on_death)
    # ---- 方案B：外网直连CDN（2.6.26，FNMUSIC_WAN_DIRECT）----
    # 仅在“无本地缓存 + GET 取音频 + 非锁定 rendition 重请求”时介入：本地磁盘永远
    # 快于 CDN；HEAD 保留现有预热路径（顺带把直链解析进 30 分钟缓存，随后 GET
    # 的解析与探测近乎零成本）；_ext_rendition=1 表示客户端已持有中转地址
    # （字节偏移必须一致），不得中途切到 CDN。任何一步失败都自动回落到
    # 下面的现有中转路径，不影响现有行为。
    if (
        not cached
        and request.method == "GET"
        and request.query_params.get("_ext_rendition") != "1"
    ):
        direct = await _wan_direct_redirect(request, guid, range_header)
        if direct is not None:
            # 外网直连路径也要喂“下一首预取”：否则 307 场景下预取永不触发，
            # 每次切歌都要现解析 + 现探测（2.6.53 补漏）。
            _schedule_next_track_prefetch(guid, request)
            _preheat_track_assets(request, guid)
            return direct
    preview_key = _stream_preview_key(request, guid, range_header)
    preview_opened = _take_stream_preview(preview_key)
    if preview_opened:
        resp, owned, ext, info, chunks, first, reconnect_fn, on_death = preview_opened
        if not should_cache(range_header):
            _register_full_fetch(request, guid)
        _schedule_next_track_prefetch(guid, request)
        _preheat_track_assets(request, guid)
        return stream_tee_response(resp, guid, range_header,
            coro_factory=lambda: _online_info(request, guid, include_lyric=bool(CONF.get("lyric_auto_dl"))),
            client_to_close=owned, resolved_ext=ext, pre_info=info, chunks=chunks, first_chunk=first,
            scan_headers_factory=lambda: copy_incoming_headers(request),
            post_finalize=lambda meta: _auto_lyric_after_finalize(request, guid, meta),
            reconnect=reconnect_fn, on_death=on_death)
    log_stream_probe(request.method, guid, range_header, bool(cached))
    if request.method == "HEAD":
        _schedule_next_track_prefetch(guid, request)
        return await _stream_head_response(request, guid, cached, range_header)
    if cached:
        _schedule_next_track_prefetch(guid, request)
        ext = os.path.splitext(cached)[1].lstrip(".") or "mp3"
        return serve_file_with_range(cached, range_header, media_type_for_ext(ext))
    item, entry = _retained_track(request, guid)
    candidates = [guid]
    # Byte offsets are encoding-specific: do not cross sources on seek/probe.
    if should_cache(range_header) and item and request.query_params.get("_ext_rendition") != "1":
        candidates += [online_guid_from_item(x) for x in ordered_stream_alternatives(item)]
    # 总预算覆盖“多音质递降 + 多源回退 + CDN 建连 + 首包”。飞牛客户端收到
    # 404 就会立刻跳歌，所以宁可在这里多等几秒，也别把还能播的歌判死。
    deadline = asyncio.get_running_loop().time() + float(CONF.get("stream_startup_budget_s", 32.0))
    for candidate in list(dict.fromkeys(candidates))[:4]:
        if not _source_enabled(candidate):
            continue
        for attempt in range(2):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                # 单次解析预算与 musicbox-service 进程内取链（毫秒级）+ 网络抖动余量对齐
                opened = await asyncio.wait_for(_open_online_stream(request, candidate, range_header), timeout=min(32.0, remaining))
            except Exception as exc:
                logger.warning("Stream startup failed for %s: %s", candidate, type(exc).__name__)
                opened = None
            if opened:
                resp, owned, ext, info, chunks, first, reconnect_fn, on_death = opened
                if candidate != guid:
                    # Publish the selected source identity before any audio.
                    # The client owns B's URL for later Range requests even if
                    # this proxy's search session expires; never alias B as A.
                    with anyio.CancelScope(shield=True):
                        await resp.aclose()
                        if owned:
                            await owned.aclose()
                    # Behind the fnOS gateway over a unix socket the request
                    # scheme is always http and the Host header carries no
                    # port, so an absolute URL would strand https/wan clients
                    # on an unreachable address; only a relative Location is
                    # safe for every client entry point.
                    target = request.url.include_query_params(guid=candidate, _ext_rendition="1")
                    location = target.path + (f"?{target.query}" if target.query else "")
                    return RedirectResponse(location, status_code=307, headers={"Cache-Control": "no-store"})
                # Cache the selected source's bytes under its own GUID, never
                # splice a failed stream or alias different encodings for seeks.
                if not should_cache(range_header):
                    # 定长窗口客户端：本响应过不了 tee 写盘门，注册后台整轨下载兜底
                    _register_full_fetch(request, candidate)
                # 当前首已经取到流：顺手预热页内下一首，给切歌留出准备时间。
                _schedule_next_track_prefetch(guid, request)
                _preheat_track_assets(request, guid)
                return stream_tee_response(resp, candidate, range_header,
                    coro_factory=lambda: _online_info(request, candidate, include_lyric=bool(CONF.get("lyric_auto_dl"))),
                    client_to_close=owned,
                    resolved_ext=ext, pre_info=info, chunks=chunks, first_chunk=first,
                    scan_headers_factory=lambda: copy_incoming_headers(request),
                    post_finalize=lambda meta: _auto_lyric_after_finalize(request, candidate, meta),
                    reconnect=reconnect_fn, on_death=on_death)
            if deadline - asyncio.get_running_loop().time() <= 3:
                break
            if attempt == 0:
                # 实测：104 首高风险曲目里有 6 首首次 404，清掉单曲负缓存后
                # 立刻重试即 206。飞牛客户端收到 404 就直接跳歌，不会自己再试，
                # 所以这一枪必须由 proxy 自己补上（原逻辑被 _recover_source
                # 挡在门外，第二次 attempt 实际上从未真正重新解析）。
                _LX_FAIL_COOLDOWN.pop(song_id_from_online_guid(candidate), None)
                await asyncio.sleep(0.25)
                continue
            if not await _recover_source(request, candidate, entry):
                break
    return JSONResponse(content={"code": 404, "msg": "online source unavailable", "data": None}, status_code=404)


async def _transcode_source(request: Request, guid: str) -> "tuple[str | None, dict | None]":
    """转码输入源：已落库/已缓存文件优先（源关了也能转），其次在线直链。"""
    cached = find_cache_file(guid)
    if cached:
        return cached, None
    if not _source_enabled(guid):
        return None, None
    source = source_from_online_guid(guid)
    try:
        if source == "netease":
            url = await resolve_netease_url(
                get_musicbox_client(request.app), song_id_from_online_guid(guid).split(":")[-1])
            return (url or None, None)
        if source == "lx":
            resolved = await resolve_lx_url(get_lx_client(request.app), song_id_from_online_guid(guid))
            if not resolved or not resolved.get("url"):
                # 此前静默返回：调用方发“假列表”被 App 拒收 → 跳歌，
                # 日志里却什么都看不到。至少把真实原因喊出来。
                logger.warning(
                    "transcode source unavailable (lx): %s fail_cd=%s platform_cooled=%s",
                    song_id_from_online_guid(guid),
                    time.time() < _LX_FAIL_COOLDOWN.get(song_id_from_online_guid(guid), 0.0),
                    _lx_platform_cooled(song_id_from_online_guid(guid)),
                )
                return None, None
            headers = {k: str(v) for k, v in (resolved.get("headers") or {}).items()
                       if k.lower() in ("referer", "user-agent")}
            return str(resolved["url"]), headers or None
        url = f"{str(CONF['musicdl_url']).rstrip('/')}/stream?id={quote(song_id_from_online_guid(guid))}&proxy=true"
        if _lossless_is_blacklisted(guid):
            url += "&quality=mp3"
        return url, None
    except Exception as e:  # noqa: BLE001
        logger.warning("transcode source resolve failed for %s: %s", guid, e)
        return None, None


async def _start_transcode_session(request: Request, guid: str) -> "tc.Session | None":
    """启动/复用标准音质转码会话；不可用返回 None（调用方回落单分片桩）。"""
    try:
        info = await _online_info(request, guid) or {}
        duration_s = float(info.get("duration_s") or 0)
        src, headers = await _transcode_source(request, guid)
        if not src:
            return None
        return await tc.ensure_session(
            guid, src, duration_s,
            root=CONF["cache_dir"],
            bitrate=str(CONF.get("transcode_bitrate") or "128k"),
            hls_time=float(CONF.get("transcode_hls_time") or 10),
            max_active=max(1, int(CONF.get("transcode_max_sessions") or 2)),
            max_cache_bytes=max(16, int(CONF.get("transcode_cache_max_mb") or 512)) * 1024 * 1024,
            headers=headers,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("transcode session start failed for %s: %s", guid, e)
        return None


def _transcode_live(guid: str) -> "tc.Session | None":
    """取在跑或磁盘已完整的转码会话（不解析在线源）。"""
    return tc.get_session(guid) or tc.peek_cached(guid, CONF["cache_dir"])


def _legacy_hls_stub(guid: str, info: "dict | None") -> Response:
    """无 ffmpeg / 源不可用时的兜底：单分片=原始文件（历史行为）。"""
    # App 实测不认这种假列表（收到后 1 秒内 quit 跳下一首），
    # 必须留下日志才能统计它出现的频率和对应的歌。
    logger.warning("hls STUB served (no transcode session/source): %s", guid)
    try:
        duration_s = int(float((info or {}).get("duration_s") or 0))
    except (TypeError, ValueError):
        duration_s = 0
    if duration_s <= 0:
        duration_s = 240
    stream_url = f"/music/api/v1/track/stream?guid={quote(guid, safe='')}"
    playlist = (
        "#EXTM3U\n"
        "#EXT-X-VERSION:3\n"
        f"#EXT-X-TARGETDURATION:{max(duration_s, 1)}\n"
        "#EXT-X-PLAYLIST-TYPE:VOD\n"
        "#EXT-X-MEDIA-SEQUENCE:0\n"
        f"#EXTINF:{duration_s:.3f},\n"
        f"{stream_url}\n"
        "#EXT-X-ENDLIST\n"
    )
    return Response(content=playlist, media_type="application/vnd.apple.mpegurl")


def _hls_playlist_base(request: Request) -> str:
    """HLS 分片地址前缀：外网绝对地址 + 主机强制小写。

    反代（Lucky）按 Host 做大小写敏感的域名匹配，而系统播放器不会像 App 的
    网络库那样把域名小写化，导致分片请求在反代层 404、根本到不了这里。
    统一下发小写主机的绝对地址后，无论 App 里填的是大写还是小写都能播。

    地址取反代回传的真实入口：x-forwarded-host 保留原始端口，
    x-real-proto 记录客户端到反代那一跳的协议（x-forwarded-proto 是
    反代到本插件这一跳，通常是 http，不能直接用）。
    """
    host = (
        request.headers.get("x-forwarded-host")
        or request.headers.get("host")
        or ""
    ).split(",")[0].strip().lower()
    if not host:
        return ""
    bare = host.split(":")[0]
    if "." not in bare and not host.startswith("["):
        # localhost / unix-socket 网关场景保持相对地址，避免写出不可达主机
        return ""
    # 只在确认「客户端到反代那一跳是 https」时才下发绝对地址；
    # 其余情况（局域网直连、走 http 入口等）保持相对地址，避免把
    # 分片指到错误协议/端口上反而播不了。
    real_proto = (request.headers.get("x-real-proto") or "").split(",")[0].strip().lower()
    if real_proto != "https":
        return ""
    base_dir = request.url.path.rsplit("/", 1)[0] + "/"
    return f"https://{host}{base_dir}"


_HLS_DEBUG_PATH = os.path.join(CONF.get("cache_dir") or _HOME, "hls_debug.log")


def _hls_debug_note(request: Request, note: str) -> None:
    """把 HLS 播放列表请求的真实转发头写到独立文件（takeover 日志白名单会丢 info 行）。"""
    try:
        path = _HLS_DEBUG_PATH
        if os.path.exists(path) and os.path.getsize(path) > 256 * 1024:
            os.replace(path, path + ".1")
        rec = {
            "t": time.strftime("%Y-%m-%d %H:%M:%S"),
            "path": request.url.path,
            "host": request.headers.get("host", ""),
            "xfh": request.headers.get("x-forwarded-host", ""),
            "xrp": request.headers.get("x-real-proto", ""),
            "xfp": request.headers.get("x-forwarded-proto", ""),
            "xfport": request.headers.get("x-forwarded-port", ""),
            "ua": (request.headers.get("user-agent") or "")[:70],
            "note": note,
        }
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


@app.api_route("/music/api/v1/track/hls/{guid}/preset.m3u8", methods=["GET", "HEAD"])
@app.api_route("/music/api/v1/track/hls/{guid}/{filename}", methods=["GET", "HEAD"])
async def track_hls(request: Request, guid: str, filename: str = "preset.m3u8"):
    guid = resolve_real_guid(guid)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    if filename != "preset.m3u8" and not tc.valid_segment_name(filename):
        return JSONResponse(content={"code": 404, "msg": "not found", "data": None}, status_code=404)

    sess = _transcode_live(guid)
    if sess is None and CONF.get("transcode_enabled") and tc.FFMPEG_BIN:
        # App 有时不先 POST /track/transcode 直接拉 playlist（实测），此处惰性补启
        sess = await _start_transcode_session(request, guid)

    logger.info("hls request: %s %s", guid, filename)
    if sess is not None:
        if filename == "preset.m3u8":
            # 原版行为：会话一启动就立即返回列表（App 只拉一次，必须一次给全），
            # 分片未就绪由下面的 wait_file 按需等待。
            # init.mp4 必须有实际内容再发列表：远程源 ffmpeg 要先下载才能写 init，
            # 不等就发列表 → App 立刻取 init → 0 字节 → 解码失败 → 跳歌。
            await tc.wait_file(os.path.join(sess.directory, tc.INIT_NAME), sess, timeout=25.0)
            _hls_debug_note(request, "relative")
            return Response(content=tc.playlist_text(sess), media_type="application/vnd.apple.mpegurl")
        path = await tc.wait_file(os.path.join(sess.directory, filename), sess, timeout=30.0)
        if path:
            # 原版行为：init 用 audio/mp4，分片用 video/iso.segment，
            # 支持 Range（App 实测就是这么请求的）。
            media = "audio/mp4" if filename == tc.INIT_NAME else "video/iso.segment"
            return serve_file_with_range(path, request.headers.get("range"), media)
        logger.warning("hls segment unavailable: %s %s", guid, filename)
        return JSONResponse(content={"code": 404, "msg": "segment unavailable", "data": None}, status_code=404)

    if filename != "preset.m3u8":
        return JSONResponse(content={"code": 404, "msg": "segment unavailable", "data": None}, status_code=404)
    info = await _online_info(request, guid)
    return _legacy_hls_stub(guid, info)


@app.api_route("/music/api/v1/track/transcode/heartbeat", methods=["GET", "POST"])
@app.api_route("/music/api/v1/track/transcode/quit", methods=["GET", "POST"])
async def track_transcode_session(request: Request):
    guid = await extract_guid_from_body(request)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    if request.url.path.endswith("/heartbeat"):
        tc.heartbeat(guid)
        try:
            await tc.maintain(CONF["cache_dir"], float(CONF.get("transcode_ttl_s") or 90),
                              max(16, int(CONF.get("transcode_cache_max_mb") or 512)) * 1024 * 1024)
        except Exception:  # noqa: BLE001
            pass
    else:
        await tc.quit_session(guid)
    return JSONResponse(content={"code": 0, "msg": "ok", "data": {"guid": guid}})


@app.api_route("/music/api/v1/track/transcode", methods=["GET", "POST"])
async def track_transcode(request: Request):
    guid = await extract_guid_from_body(request)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    if CONF.get("transcode_enabled") and tc.FFMPEG_BIN:
        sess = _transcode_live(guid)
        if sess is None:
            sess = await _start_transcode_session(request, guid)
        if sess is not None and sess.status in ("starting", "running") and not find_cache_file(guid):
            # 标准音质播放不经过 /track/stream，tee 永不触发；挂后台整轨下载保住边听边存
            _register_full_fetch(request, guid)
    return JSONResponse(
        content={
            "code": 0,
            "msg": "ok",
            "status": "success",
            "data": {"guid": guid, "status": "ready"},
        }
    )


# === App 下载（音质偏好=标准 → 转码下载）仿真 ===
# 协议按官方实测对齐（本地歌曲透传抓包，2026-09-29）：
#   prepare POST {trackGUID, quality}  → data{status:"success", errno, errmsg, downloadId}
#   status  GET ?downloadId=           → data{status:"waiting|transcoding|ready|failed",
#                                             errno, errmsg, downloadId, percent}
#   file    GET ?downloadId=           → 音频字节（audio/mpeg，支持断点）
#   delete  POST {downloadId}          → data{downloadId, deleted:true}
# 官方"标准"档 = MP3 320kbps（FLAC 源实测 320067bps）；源已是 mp3 时直接供原文件
# 不做无意义重编码。官方服务不认识在线曲目的伪装 guid，透传必失败；此处仅当
# guid 是在线曲目才接管，本地曲目与未知 downloadId 一律透传官方。
_DL_TASKS: dict[str, dict] = {}
_DL_TASK_TTL_S = 3600.0
_DL_TRANSCODE_SUBDIR = "dltrans"


def _dl_transcode_path(guid: str) -> str:
    return os.path.join(CONF["cache_dir"], _DL_TRANSCODE_SUBDIR, f"{cache_safe_guid(guid)}.mp3")


def _dl_sweep() -> None:
    now = time.monotonic()
    for key, task in list(_DL_TASKS.items()):
        if now - float(task.get("touched", 0)) > _DL_TASK_TTL_S:
            _DL_TASKS.pop(key, None)


async def _dl_body_json(request: Request) -> dict:
    try:
        raw = await request.body()
        if not raw:
            return {}
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _dl_find_task(request: Request, body: dict) -> "dict | None":
    for key in ("downloadId", "taskGuid", "taskId", "id", "guid", "trackGUID"):
        value = request.query_params.get(key) or body.get(key)
        if value:
            hit = _DL_TASKS.get(str(value))
            if hit:
                hit["touched"] = time.monotonic()
                return hit
    return None


def _task_guid_of(task: dict) -> str:
    for key, value in _DL_TASKS.items():
        if value is task:
            task["downloadId"] = key
            return key
    return str(task.get("downloadId") or "")


def _dl_state_word(task: dict) -> str:
    """内部状态 → 官方 status 字段用词（completed=ready）。"""
    state = str(task.get("state") or "waiting")
    return {"completed": "ready"}.get(state, state)


async def _dl_produce(task: dict, cred_headers: dict) -> None:
    """后台产文件：标准档按官方规格 MP3 320k（源已是 mp3 直接供原文件）；原始档供原文件。"""
    guid = task["guid"]
    try:
        src = find_cache_file(guid)
        if not src and _source_enabled(guid):
            # 无本地文件：先走整轨下载管线（带元数据命名/落库，与边听边存同款）
            await _full_fetch_download(guid, cred_headers)
            src = find_cache_file(guid)
        if not src:
            task["state"] = "failed"
            task["errmsg"] = "online source unavailable"
            return
        if task.get("quality") == "original" or os.path.splitext(src)[1].lower() == ".mp3":
            task["path"] = src
        else:
            target = _dl_transcode_path(guid)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            if not os.path.isfile(target):
                task["state"] = "transcoding"
                task["percent"] = 0
                proc = await asyncio.create_subprocess_exec(
                    tc.FFMPEG_BIN, "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
                    "-i", src, "-vn", "-map_metadata", "-1",
                    "-c:a", "libmp3lame", "-b:a", str(CONF.get("transcode_dl_bitrate") or "320k"),
                    "-f", "mp3", target,
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                )
                if await proc.wait() != 0:
                    task["state"] = "failed"
                    task["errmsg"] = "transcode failed"
                    return
            task["path"] = target
        task["state"] = "completed"
        task["percent"] = 100
        try:
            task["size"] = os.path.getsize(task["path"])
        except OSError:
            task["size"] = 0
    except Exception as e:  # noqa: BLE001
        logger.warning("download transcode produce failed for %s: %s", guid, e)
        task["state"] = "failed"
        task["errmsg"] = "internal error"


async def _dl_forward(request: Request):
    """download/* 透传官方；trace_forward=detail 时抓双向往返报文（协议对齐取证）。"""
    if not CONF.get("trace_forward"):
        return await forward_to_upstream(request, get_upstream_client(request.app))
    body = await request.body()
    if body:
        logger.info("[dl-capture] REQ %s %s body=%s", request.method, request.url.path, body[:2000])
    resp = await forward_to_upstream(request, get_upstream_client(request.app))
    captured: list[bytes] = []

    async def _tee():
        async for chunk in resp.body_iterator:
            captured.append(chunk)
            yield chunk
        if captured:
            logger.info("[dl-capture] RESP %s %s status=%s body=%s",
                        request.method, request.url.path, resp.status_code, b"".join(captured)[:2000])

    return StreamingResponse(_tee(), status_code=resp.status_code,
                             headers=filter_headers(resp.headers), media_type=resp.media_type)


@app.api_route("/music/api/v1/download/track/transcode/prepare", methods=["GET", "POST"])
async def dl_transcode_prepare(request: Request):
    body = await _dl_body_json(request)
    if CONF.get("trace_forward"):
        logger.info("[dl-capture] PREPARE req q=%s body=%s", dict(request.query_params), body)
    raw_guid = ""
    for key in ("guid", "trackGUID", "trackGuid"):
        value = request.query_params.get(key) or body.get(key)
        if value:
            raw_guid = str(value)
            break
    guid = resolve_real_guid(raw_guid)
    if not is_online_guid(guid):
        return await _dl_forward(request)
    quality = "standard"
    for key in ("quality", "qualityType"):
        value = str(body.get(key) or request.query_params.get(key) or "").lower()
        if value in ("original", "standard"):
            quality = value
            break
    if body.get("isOriginal") is True:
        quality = "original"
    _dl_sweep()
    download_id = uuid4().hex
    task = {"guid": guid, "quality": quality, "state": "waiting", "percent": 0,
            "downloadId": download_id, "touched": time.monotonic(), "started": time.time()}
    _DL_TASKS[download_id] = task
    asyncio.create_task(_dl_produce(task, copy_incoming_headers(request)))
    return JSONResponse(content={
        "code": 0, "msg": "",
        "data": {"status": "success", "errno": "", "errmsg": "", "downloadId": download_id},
    })


@app.api_route("/music/api/v1/download/track/transcode/status", methods=["GET", "POST"])
async def dl_transcode_status(request: Request):
    body = await _dl_body_json(request)
    task = _dl_find_task(request, body)
    if task is None:
        return await _dl_forward(request)
    return JSONResponse(content={
        "code": 0, "msg": "",
        "data": {"status": _dl_state_word(task), "errno": "",
                 "errmsg": str(task.get("errmsg") or ""), "downloadId": _task_guid_of(task),
                 "percent": int(task.get("percent", 0))},
    })


@app.api_route("/music/api/v1/download/track/transcode/file", methods=["GET", "HEAD"])
async def dl_transcode_file(request: Request):
    body = await _dl_body_json(request)
    task = _dl_find_task(request, body)
    if task is None:
        return await _dl_forward(request)
    path = task.get("path")
    if task["state"] != "completed" or not path or not os.path.isfile(path):
        return JSONResponse(content={"code": 404, "msg": "not ready", "data": None}, status_code=404)
    media = "audio/mpeg" if os.path.splitext(path)[1].lower() == ".mp3" else "audio/mp4"
    fname = os.path.basename(path)
    fn_ext = os.path.splitext(fname)[1]
    quoted = quote(fname, encoding="utf-8")
    ascii_fn = re.sub(r'[^\x20-\x7e]', '_', fname)
    if not ascii_fn.strip() or ascii_fn == fn_ext:
        ascii_fn = f"track{fn_ext}"
    disposition = f'attachment; filename="{ascii_fn}"; filename*=UTF-8\'\'{quoted}'
    return serve_file_with_range(path, request.headers.get("range"), media,
                                 extra_headers={"Content-Disposition": disposition})


@app.api_route("/music/api/v1/download/track/transcode/delete", methods=["GET", "POST", "DELETE"])
async def dl_transcode_delete(request: Request):
    body = await _dl_body_json(request)
    task = _dl_find_task(request, body)
    if task is None:
        return await _dl_forward(request)
    download_id = ""
    for key, value in list(_DL_TASKS.items()):
        if value is task:
            _DL_TASKS.pop(key, None)
            download_id = key
    return JSONResponse(content={
        "code": 0, "msg": "",
        "data": {"downloadId": download_id, "deleted": True},
    })


async def _online_info(request: Request, guid: str, include_lyric: bool = True) -> dict | None:
    override = _CERU_SOURCE_OVERRIDES.get(guid)
    if override and override != guid:
        data = await _online_info(request, override, include_lyric=include_lyric)
        if data:
            return {**data, "id": guid, "source": source_from_online_guid(guid) or data.get("source")}
    retained, entry = _retained_track(request, guid)
    if not _source_enabled(guid):
        return retained
    try:
        data = await asyncio.wait_for(_fetch_online_info(request, guid, include_lyric=include_lyric), timeout=4.0)
        if data:
            return _merge_lx_alt_info(guid, _ceru_enrich_info(guid, data))
        if await _recover_source(request, guid, entry):
            data = await asyncio.wait_for(_fetch_online_info(request, guid, include_lyric=include_lyric), timeout=3.0)
            if data:
                return _merge_lx_alt_info(guid, _ceru_enrich_info(guid, data))
    except Exception:
        pass
    merged_retained = _merge_lx_alt_info(guid, retained or {})
    return merged_retained or retained


def _merge_lx_alt_info(guid: str, data: dict) -> dict:
    """换源只允许补文本元数据，绝不允许用替代源的封面覆盖原歌曲封面。"""
    alt = _lx_alt_meta_for(guid)
    if not alt:
        return data
    merged = dict(data or {})
    for key in ("title", "artist", "album", "duration_s", "ext"):
        if not merged.get(key) and alt.get(key):
            merged[key] = alt[key]
    return merged


def _ceru_enrich_info(guid: str, data: dict) -> dict:
    """Ceru 直连歌曲缺字段时，用原歌曲登记表补齐标题/歌手/封面等。"""
    if not isinstance(data, dict):
        return data
    ceru_song = ceru.song_for_guid(guid)
    if not ceru_song:
        return data
    enriched = dict(data)
    if not str(enriched.get("title") or "").strip():
        enriched["title"] = ceru.song_title(ceru_song)
    if not str(enriched.get("artist") or "").strip():
        enriched["artist"] = ceru.song_artist(ceru_song)
    if not str(enriched.get("album") or "").strip():
        enriched["album"] = ceru.song_album(ceru_song)
    if not enriched.get("duration_s"):
        enriched["duration_s"] = ceru.song_duration_s(ceru_song)
    # 封面与播放源解耦：只要 Ceru 侧有原封面，就始终以它为准，
    # 不允许当前解析平台自带的封面覆盖（否则换源后封面会跟着变）。
    artwork = ceru.song_artwork(ceru_song)
    if artwork:
        enriched["cover_url"] = artwork
    return enriched


async def _fetch_online_info(request: Request, guid: str, include_lyric: bool = True) -> dict | None:
    src = source_from_online_guid(guid)
    if src == "netease":
        musicbox_client = get_musicbox_client(request.app)
        raw_song_id = song_id_from_online_guid(guid)
        song_id = raw_song_id.split(":")[-1]
        try:
            r = await musicbox_client.get(f"/api/v1/song/{song_id}/info", timeout=10.0)
            if r.status_code == 200:
                res_data = r.json()
                if isinstance(res_data, dict) and res_data.get("ok") is not False:
                    data = res_data.get("data")
                    if isinstance(data, dict):
                        name = str(data.get("name") or "")
                        ar = data.get("ar") or []
                        ar_names = []
                        if isinstance(ar, list):
                            for x in ar:
                                if isinstance(x, dict) and x.get("name"):
                                    ar_names.append(str(x["name"]))
                                elif isinstance(x, str):
                                    ar_names.append(x)
                        artist = " / ".join(ar_names)
                        al = data.get("al") or {}
                        album_name = str(al.get("name") or "") if isinstance(al, dict) else ""
                        cover_url = str(al.get("picUrl") or "") if isinstance(al, dict) else ""
                        dt = data.get("dt") or 0
                        duration_s = float(dt) / 1000.0 if dt else 0.0
                        sq = data.get("sq")
                        hr = data.get("hr")
                        h = data.get("h") or {}
                        ext = "flac" if (sq or hr) else "mp3"
                        size_obj = sq or h or {}
                        file_size = int(size_obj.get("size", 0) or 0) if isinstance(size_obj, dict) else 0

                        lyric_text = ""
                        if include_lyric:
                            try:
                                lr = await musicbox_client.get(f"/api/v1/song/{song_id}/lyric", timeout=10.0)
                                if lr.status_code == 200:
                                    l_res = lr.json()
                                    if isinstance(l_res, dict) and l_res.get("ok") is not False:
                                        l_data = l_res.get("data")
                                        if isinstance(l_data, dict):
                                            lyric_text = str(l_data.get("lyric") or "").strip()
                            except Exception as l_err:
                                logger.warning("musicbox lyric fetch in _online_info failed for %s: %s", guid, l_err)

                        return {
                            "id": f"netease:{song_id}",
                            "source": "netease",
                            "title": name,
                            "artist": artist,
                            "album": album_name,
                            "cover_url": cover_url,
                            "duration_s": duration_s,
                            "ext": ext,
                            "file_size": file_size,
                            "lyric": lyric_text,
                        }
        except Exception as e:
            logger.warning("musicbox /info failed for %s: %s", guid, e)
        return None

    if src == "lx":
        lx_client = get_lx_client(request.app)
        song_id = song_id_from_online_guid(guid)
        try:
            r = await lx_client.get("/api/v1/track/info", params={"id": song_id}, timeout=10.0)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict) and data.get("ok") is not False:
                    inner = data.get("data")
                    if isinstance(inner, dict):
                        lyric_text = str(inner.get("lyric") or "").strip()
                        if not lyric_text and include_lyric:
                            try:
                                lr = await lx_client.get(
                                    "/api/v1/track/lyric",
                                    params={
                                        "id": song_id,
                                        # 2.6.36: 同步路径只做播放平台直连解析（亚秒级）；
                                        # 拿不到词由 _ensure_lyric_backfill 后台全模式回补。
                                        "fast": "1",
                                        "title": str(inner.get("title") or ""),
                                        "artist": str(inner.get("artist") or ""),
                                    },
                                    timeout=10.0,
                                )
                                if lr.status_code == 200:
                                    l_res = lr.json()
                                    if isinstance(l_res, dict) and l_res.get("ok") is not False:
                                        lyric_text = str((l_res.get("data") or {}).get("lyric") or "").strip()
                            except Exception as l_err:
                                logger.warning("lxmusic lyric fetch failed for %s: %s", guid, l_err)
                        return {
                            "id": song_id,
                            "source": "lx",
                            "lx_source": str(inner.get("lx_source") or ""),
                            "title": str(inner.get("title") or ""),
                            "artist": str(inner.get("artist") or ""),
                            "album": str(inner.get("album") or ""),
                            "cover_url": str(inner.get("cover_url") or ""),
                            "duration_s": float(inner.get("duration_s") or 0),
                            "ext": str(inner.get("ext") or "mp3") or "mp3",
                            "file_size": int(inner.get("file_size") or 0),
                            "lyric": lyric_text,
                        }
        except Exception as e:
            logger.warning("lxmusic /info failed for %s: %s", guid, e)
        return None

    musicdl_client = get_musicdl_client(request.app)
    song_id = song_id_from_online_guid(guid)
    try:
        r = await musicdl_client.get("/info", params={"id": song_id}, timeout=10.0)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and data.get("ok") is not False:
                return data
    except Exception as e:
        logger.warning("musicdl /info failed for %s: %s", guid, e)
    return None


@app.get("/music/api/v1/lyric/list")
@app.get("/music/api/v1/lyric/list/{subpath:path}")
async def lyric_list(request: Request, subpath: str = ""):
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    lyric_text = await resolve_online_lyric(request, guid)
    return JSONResponse(content=disguise_client_json(build_lyric_list_payload(guid, lyric_text)))


@app.get("/music/api/v1/track/lyrics")
@app.get("/music/api/v1/track/lyrics/{subpath:path}")
@app.get("/music/api/v1/detail/lyrics/{subpath:path}")
async def track_lyrics(request: Request, subpath: str = ""):
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    lyric_text = await resolve_online_lyric(request, guid)
    if lyric_text:
        res = {"code": 0, "msg": "ok", "data": {"guid": guid, "lyric": lyric_text}}
        set_by_path(res, CONF["lyric_field"], lyric_text)
        return JSONResponse(content=disguise_client_json(res))
    return empty_ok()


@app.get("/music/api/v1/track/metadata")
@app.get("/music/api/v1/track/metadata/{subpath:path}")
@app.get("/music/api/v1/track/audio-info")
async def track_metadata(request: Request, subpath: str = ""):
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    data = await _online_info(request, guid) or stub_online_info(guid)
    cached_lyric = read_lyric_cache(guid)
    if cached_lyric:
        data = {**data, "lyric": cached_lyric}
    elif data.get("lyric"):
        write_lyric_cache(
            guid,
            str(data.get("lyric") or ""),
            title=str(data.get("title") or ""),
            artist=str(data.get("artist") or ""),
        )
    if not str(data.get("lyric") or "").strip():
        # fast=1 拿不到词：后台全模式回补（播放页元数据轮询是自然触发点）
        _ensure_lyric_backfill(request, guid, data)
    payload = build_metadata_payload(guid, data)
    # Ceru 曲：播放页元数据对齐列表行契约（同 coverId/原封面/标星口径），
    # 否则播放页用新值覆盖列表已渲染状态，星与封面都会「闪现后消失」。
    ceru_song = ceru.song_for_guid(guid) if _ceru_on() else None
    if ceru_song is not None:
        _apply_ceru_metadata_fields(guid, ceru_song, payload)
    if await _track_is_favorite(request, guid, data):
        if isinstance(payload.get("data"), dict):
            payload["data"]["isFavorite"] = True
            if isinstance(payload["data"].get("track"), dict):
                payload["data"]["track"]["isFavorite"] = True
    return JSONResponse(content=disguise_client_json(payload))


# === 封面兜底链（2.6.36 统一 tail）：cover_url 302 → 源 CDN 直构（kw rid / qq albummid）→
# 跨源搜索补齐（/api/v1/cover/search，落 cover_cache）→ 网易 cloudsearch 补全（仅启用时）→
# 本地缓存文件内嵌图 → 本地占位图池。
# 在线曲目封面永不 404（空封面是客户端裂图的主要来源）。 ===
_COVER_CDN_CACHE: dict[str, tuple[str, float]] = {}
_COVER_CDN_TTL_S = 7 * 86400.0
_KW_TEXT_COVER_HOST = dailyrec.KW_TEXT_COVER_HOST


def _cover_url_unusable(url: str) -> bool:
    """客户端无法直接取到的封面地址：为空、酷我文本封面、内网地址。

    内网地址典型来源：其他设备把本机 fnMusic 的封面端点 URL（如
    http://192.168.x.x:5666/music/api/v1/static/cover?coverId=...）作为 artwork 写进
    同步服务器——无会话访问 401、外网完全不可达。这类地址若 302 给客户端，
    会反复裂图/闪占位图；必须走兜底链（源封面/本地缓存/占位图池）。
    """
    if not url:
        return True
    if _KW_TEXT_COVER_HOST in url:
        return True
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return True
    if not host:
        return True
    if host == "localhost" or host.endswith((".local", ".home.arpa")):
        return True
    parts = host.split(".")
    if parts[0].isdigit():
        first = int(parts[0])
        if first in (10, 127, 192, 169):
            return True
        if first == 172 and len(parts) > 1 and parts[1].isdigit() and 16 <= int(parts[1]) <= 31:
            return True
    return False
_PLACEHOLDER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "covers")
_PLACEHOLDER_COUNT = 6
_PLACEHOLDER_CACHE: dict[str, bytes] = {}
# 1x1 灰点：占位图文件意外缺失时的最终兜底，保证响应仍是合法图片
_TINY_GRAY_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc\xf8\xcf\xc0"
    b"\xf0\x1f\x00\x05\x05\x02\x00_\xc8\xe7A\x00\x00\x00\x00IEND\xaeB`\x82"
)


def _cover_cache_get(key: str) -> str:
    hit = _COVER_CDN_CACHE.get(key)
    if hit and time.time() - hit[1] < _COVER_CDN_TTL_S:
        return hit[0]
    _COVER_CDN_CACHE.pop(key, None)
    return ""


def _cover_cache_put(key: str, url: str) -> None:
    if len(_COVER_CDN_CACHE) > 2000:
        _COVER_CDN_CACHE.clear()
    _COVER_CDN_CACHE[key] = (url, time.time())


_COVER_CACHE_DIR = (
    os.environ.get("FNMUSIC_COVER_CACHE_DIR") or os.path.join(_HOME, "cover_cache")
)
# 2.6.36: 封面缓存永久保留、不随时间过期——同一首歌补到的图就一直用。
# 清理只走歌曲删除流程：favorite-track/delete 末尾 _purge_cover_cache(guid)。


def _cover_cache_path(guid: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", str(guid or ""))
    return os.path.join(_COVER_CACHE_DIR, f"{safe}.img")


def _read_cover_cache(guid: str) -> "tuple[bytes, str] | None":
    path = _cover_cache_path(guid)
    try:
        if not os.path.exists(path):
            return None
        data = open(path, "rb").read()
        mime = _sniff_image_mime(data)
        if data and mime:
            return data, mime
    except OSError:
        pass
    return None


def _write_cover_cache(guid: str, data: bytes, mime: str) -> None:
    if not data or not mime:
        return
    path = _cover_cache_path(guid)
    part = f"{path}.{uuid4().hex[:8]}.part"
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(part, "wb") as f:
            f.write(data)
        os.replace(part, path)
    except OSError as e:
        logger.debug("cover cache write failed for %s: %s", guid, e)
        try:
            os.remove(part)
        except OSError:
            pass


def _purge_cover_cache(guid: str) -> None:
    try:
        os.remove(_cover_cache_path(guid))
    except OSError:
        pass


async def _cached_cover_response(guid: str, url: str) -> Response:
    cached = _read_cover_cache(guid)
    if cached:
        data, mime = cached
        return Response(content=data, media_type=mime, headers={"Cache-Control": "public, max-age=86400"})
    try:
        data, mime = await asyncio.to_thread(download_cover_bytes, url)
    except Exception:
        data, mime = None, ""
    if data and mime:
        _write_cover_cache(guid, data, mime)
        return Response(content=data, media_type=mime, headers={"Cache-Control": "public, max-age=86400"})
    return RedirectResponse(url, status_code=302)


def _kw_rid_from_guid(guid: str) -> str:
    """酷我 rid：lx 平台 online:lx:kw:<rid>；musicdl online:kuwo:<rid>。"""
    parts = (guid or "").split(":")
    if len(parts) >= 4 and parts[1] == "lx" and parts[2] == "kw":
        return parts[3]
    if len(parts) == 3 and parts[1] in ("kw", "kuwo"):
        return parts[2]
    return ""


def _cover_cdn_client() -> httpx.AsyncClient:
    # 直构 CDN 探测用独立短超时客户端；transport 仅供测试注入 MockTransport
    kwargs: dict = {"timeout": 5.0, "follow_redirects": True}
    if _COVER_CDN_TRANSPORT is not None:
        kwargs["transport"] = _COVER_CDN_TRANSPORT
    return httpx.AsyncClient(**kwargs)


_COVER_CDN_TRANSPORT: "httpx.AsyncBaseTransport | None" = None


_KW_BYID_NEGATIVE: dict[str, float] = {}
_KW_BYID_NEGATIVE_TTL_S = 6 * 3600.0
_KW_BYID_DAILY_BUDGET = int(os.environ.get("FNMUSIC_KW_BYID_DAILY_BUDGET", "500"))
_KW_BYID_CALLS: dict[str, Any] = {"date": "", "count": 0}


def _kw_byid_charge() -> bool:
    """酷我按 ID 取图每日额度（官方接口有访问限制，超限当天不再打）。"""
    today = time.strftime("%Y-%m-%d")
    if _KW_BYID_CALLS["date"] != today:
        _KW_BYID_CALLS["date"] = today
        _KW_BYID_CALLS["count"] = 0
    if _KW_BYID_CALLS["count"] >= _KW_BYID_DAILY_BUDGET:
        return False
    _KW_BYID_CALLS["count"] += 1
    return True


async def _kw_cover_by_songinfo(rid: str) -> str:
    """酷我按 ID 取图（与手机端同思路：知道 ID 就按 ID 查，不做关键词搜索）。

    问 lxmusic-service 要而不是自己打酷我：那条 songinfoandlrc 一次调用同时给
    封面与歌词，服务侧带 7 天缓存与合流，补封面顺带把歌词也补好，官方接口
    每首歌只打一次（此前两边各打一次，白烧一倍额度）。
    """
    try:
        async with _cover_cdn_client() as client:
            r = await client.get(
                f"{CONF['lx_url'].rstrip('/')}/api/v1/track/songinfo",
                params={"id": f"lx:kw:{rid}"},
                timeout=12.0,
            )
        if r.status_code != 200:
            return ""
        data = (r.json() or {}).get("data") or {}
        pic = str(data.get("pic") or "").strip()
        return pic if pic.startswith("http") else ""
    except Exception as e:  # noqa: BLE001
        logger.debug("kw songinfo cover resolve failed for rid=%s: %s", rid, e)
        return ""


async def _kw_cover_by_artistpic(rid: str) -> str:
    """酷我 artistpicserver 按 rid 取图（接口返回文本，需解析出图片地址）。"""
    url = (
        "https://artistpicserver.kuwo.cn/pic?corp=kuwo&type=rid_pic"
        f"&pictype=500&size=500&rid={rid}"
    )
    try:
        async with _cover_cdn_client() as client:
            r = await client.get(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.kuwo.cn/"},
            )
        if r.status_code != 200:
            return ""
        m = re.search(r"https?://[^\s'\"<>]+?\.(?:jpg|jpeg|png|webp)", r.text, re.IGNORECASE)
        if not m:
            return ""
        return m.group(0)
    except Exception as e:
        logger.debug("kw artistpic resolve failed for rid=%s: %s", rid, e)
        return ""


async def _kw_cover_by_rid(rid: str) -> str:
    """酷我按 ID 取封面：先 songinfoandlrc（按 ID 查，1 次轻量请求），
    再 artistpicserver 兜底。未命中记 6h 负缓存 + 每日额度限制。"""
    rid = str(rid or "").strip()
    if not rid.isdigit():
        return ""
    cached = _cover_cache_get(f"kw:{rid}")
    if cached:
        return cached
    if _KW_BYID_NEGATIVE.get(rid, 0.0) > time.monotonic():
        return ""
    if not _kw_byid_charge():
        return ""
    real = await _kw_cover_by_songinfo(rid)
    if not real:
        real = await _kw_cover_by_artistpic(rid)
    if real:
        _cover_cache_put(f"kw:{rid}", real)
        return real
    _KW_BYID_NEGATIVE[rid] = time.monotonic() + _KW_BYID_NEGATIVE_TTL_S
    return ""


def _qq_cover_by_albummid(albummid: "str | None") -> str:
    mid = re.sub(r"[^0-9A-Za-z]", "", str(albummid or ""))
    if len(mid) < 8:
        return ""
    return f"https://y.gtimg.cn/music/photo_new/T002R800x800M000{mid}.jpg?max_age=2592000"


async def _enrich_cover_via_netease(request: Request, guid: str, data: dict) -> str:
    """musicdl/lx 空封面 → 网易 cloudsearch 同名曲补全（FNMUSIC_COVER_ENRICH=1）。"""
    if not CONF.get("cover_enrich", True):
        return ""
    title = str((data or {}).get("title") or "").strip()
    if not title:
        return ""
    artist = str((data or {}).get("artist") or "").strip()
    key = f"wy:{title.casefold()}|{artist.casefold()}"
    cached = _cover_cache_get(key)
    if cached:
        return cached
    musicbox_client = get_musicbox_client(request.app)
    keyword = " ".join(x for x in (artist, title) if x)
    target = ""
    try:
        r = await musicbox_client.get(
            "/api/v1/search",
            params={"keyword": keyword, "limit": 5, "type": "song"},
            timeout=8.0,
        )
        if r.status_code != 200:
            return ""
        payload = r.json()
        raw = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(raw, list):
            return ""
        title_l = title.casefold()
        artist_l = artist.casefold()
        for it in raw:
            if not isinstance(it, dict):
                continue
            it_title = str(it.get("song_name") or it.get("title") or "")
            if it_title.casefold() != title_l:
                continue
            it_artist = str(it.get("artist") or "")
            if artist_l and artist_l not in it_artist.casefold() and it_artist.casefold() not in artist_l:
                continue
            sid = str(it.get("song_id") or it.get("id") or "")
            if not sid:
                continue
            d = await musicbox_client.get("/api/v1/songs/detail", params={"ids": sid}, timeout=8.0)
            if d.status_code == 200:
                dj = d.json()
                dl = dj.get("data") if isinstance(dj, dict) else None
                first = dl[0] if isinstance(dl, list) and dl and isinstance(dl[0], dict) else {}
                target = str(first.get("album_pic_url") or "")
            break
    except Exception as e:
        logger.debug("cover enrich via netease failed for %s: %s", guid, e)
        return ""
    if target:
        _cover_cache_put(key, target)
    return target


def _embedded_cover_bytes(guid: str) -> "tuple[bytes, str] | None":
    """本地边听边存缓存文件的内嵌专辑图（mutagen 读 ID3/FLAC/MP4 封面）。"""
    path = find_cache_file(guid)
    if not path:
        return None
    try:
        from mutagen import File as MutagenFile

        mf = MutagenFile(path)
        tags = getattr(mf, "tags", None)
        if tags is None:
            return None
        pics: list = []
        if hasattr(tags, "pictures"):  # FLAC/APE
            pics = list(tags.pictures or [])
        elif hasattr(tags, "getall"):  # ID3
            pics = list(tags.getall("APIC") or [])
        elif hasattr(tags, "get"):
            covr = tags.get("covr")  # MP4
            pics = list(covr) if covr else []
        for pic in pics:
            data = getattr(pic, "data", None)
            if not data:
                continue
            mime = str(getattr(pic, "mime", "") or "")
            if "/" not in mime:
                mime = "image/jpeg"
            return bytes(data), mime
    except Exception as e:
        logger.debug("embedded cover extract failed for %s: %s", guid, e)
    return None


# === 2.6.36 跨源封面补齐：播放源给不出封面时，用服务端 /api/v1/cover/search
# 并行搜 5 平台（与桌面端 coverResolver 同款匹配/打分算法），命中即下载落
# cover_cache。未命中记 6h 负缓存，避免同一首歌反复打搜索端点。 ===
_COVER_SEARCH_NEGATIVE: dict[str, float] = {}
_COVER_SEARCH_NEGATIVE_TTL_S = 6 * 3600.0


def _cover_search_meta(guid: str, data: "dict | None") -> "dict | None":
    d = data if isinstance(data, dict) else {}
    title = str(d.get("title") or "").strip()
    artist = str(d.get("artist") or "").strip()
    album = str(d.get("album") or "").strip()
    try:
        duration_s = float(d.get("duration_s") or 0)
    except (TypeError, ValueError):
        duration_s = 0.0
    if not (title or artist):
        song = ceru.song_for_guid(guid)
        if song:
            title = title or ceru.song_title(song)
            artist = artist or ceru.song_artist(song)
            album = album or ceru.song_album(song)
            if not duration_s:
                duration_s = ceru.song_duration_s(song)
    if not title:
        return None
    source = str(d.get("lx_source") or "").strip().lower()
    src_id = ""
    if not source:
        parts = str(guid or "").split(":")
        if len(parts) >= 4 and parts[1] == "lx":
            source = parts[2]
            src_id = parts[3]
    if not src_id:
        raw_id = str(d.get("id") or "")
        if raw_id.startswith("lx:") and raw_id.split(":")[1] == source:
            src_id = raw_id.split(":")[2]
    return {
        "title": title, "artist": artist, "album": album,
        "duration_s": duration_s, "source": source, "src_id": src_id,
    }


async def _cross_source_cover_response(request: Request, guid: str, data: "dict | None") -> "Response | None":
    """跨源封面搜索命中 → 下载落 cover_cache 直接 200；任何失败一律 None（fail-soft）。"""
    cached = _read_cover_cache(guid)
    if cached:
        content, mime = cached
        return Response(content=content, media_type=mime,
                        headers={"Cache-Control": "public, max-age=86400"})
    meta = _cover_search_meta(guid, data)
    if not meta:
        return None
    now = time.monotonic()
    last_fail = _COVER_SEARCH_NEGATIVE.get(guid)
    if last_fail is not None and now - last_fail < _COVER_SEARCH_NEGATIVE_TTL_S:
        return None
    global _COVER_SEARCH_ACTIVE
    if _COVER_SEARCH_ACTIVE >= _COVER_SEARCH_MAX:
        return None  # 在途上限：本轮放弃（不记负缓存，下个请求/页还会试）
    _COVER_SEARCH_ACTIVE += 1
    try:
        lx_client = get_lx_client(request.app)
        try:
            r = await lx_client.get(
                "/api/v1/cover/search",
                params={
                    "title": meta["title"],
                    "artist": meta["artist"],
                    "album": meta["album"],
                    "duration": f"{meta['duration_s']:.1f}",
                    "source": meta.get("source") or "",
                },
                timeout=13.0,
            )
            payload = r.json() if r.status_code == 200 else {}
        except Exception as e:
            logger.debug("cross-source cover search failed for %s: %s", guid, e)
            _COVER_SEARCH_NEGATIVE[guid] = now
            return None
        best: "dict | None" = None
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            best = payload["data"]
        cover = str((best or {}).get("cover_url") or "").strip()
        if not cover or _cover_url_unusable(cover):
            _COVER_SEARCH_NEGATIVE[guid] = now
            return None
        try:
            got = await asyncio.to_thread(download_cover_bytes, cover)
        except Exception as e:
            logger.debug("cross-source cover download failed for %s: %s", guid, e)
            got = None
        if not got:
            _COVER_SEARCH_NEGATIVE[guid] = now
            return None
        content, mime = got
        _write_cover_cache(guid, content, mime)
        logger.info("cross-source cover fill ok for %s (via %s)", guid, (best or {}).get("lx_source") or "")
        return Response(content=content, media_type=mime,
                        headers={"Cache-Control": "public, max-age=86400"})
    finally:
        _COVER_SEARCH_ACTIVE -= 1


async def _cover_fallback_tail(request: Request, guid: str, data: "dict | None") -> "Response | None":
    """封面兜底链统一 tail：源 CDN 直构 → 跨源搜索补齐 → 网易补全（仅启用时）→ 内嵌图。
    全未命中返回 None，由调用方回落占位图（保持原行为）。"""
    direct = ""
    kw_rid = _kw_rid_from_guid(guid)
    if kw_rid:
        direct = await _kw_cover_by_rid(kw_rid)
    if not direct:
        direct = _qq_cover_by_albummid((data or {}).get("albummid"))
    if direct:
        return RedirectResponse(direct, status_code=302)
    filled = await _cross_source_cover_response(request, guid, data)
    if filled is not None:
        return filled
    if CONF.get("netease_enabled"):
        # 网易补全走 cloudsearch，单次 8s+ 预算；未启用时直接跳过（不空耗）
        enriched = await _enrich_cover_via_netease(request, guid, data or {})
        if enriched:
            return RedirectResponse(enriched, status_code=302)
    embedded = _embedded_cover_bytes(guid)
    if embedded:
        art, mime = embedded
        _write_cover_cache(guid, art, mime)
        return Response(content=art, media_type=mime,
                        headers={"Cache-Control": "public, max-age=86400"})
    return None


def _placeholder_cover_response(guid: str) -> Response:
    """占位图池确定性选取（guid 哈希）。

    故意不随图带缓存：同一个封面地址后续可能换成真图（封面补全/多端同步后），
    占位图一旦缓存就会把封面钉死在灰图上，观感变成「闪一下然后再也不变」。"""
    digest = hashlib.sha256(str(guid or "").encode()).hexdigest()
    name = f"placeholder-{int(digest[:8], 16) % _PLACEHOLDER_COUNT}.png"
    content = _PLACEHOLDER_CACHE.get(name)
    if content is None:
        try:
            with open(os.path.join(_PLACEHOLDER_DIR, name), "rb") as f:
                content = f.read()
            _PLACEHOLDER_CACHE[name] = content
        except OSError:
            logger.warning("占位图缺失: %s", os.path.join(_PLACEHOLDER_DIR, name))
            content = _TINY_GRAY_PNG
    return Response(
        content=content,
        media_type="image/png",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.api_route("/music/api/v1/static/cover", methods=["GET", "HEAD"])
@app.api_route("/music/api/v1/static/cover/{subpath:path}", methods=["GET", "HEAD"])
async def static_cover(request: Request, subpath: str = ""):
    guid = extract_guid(request, subpath if is_online_guid(subpath) else None)
    if not guid and subpath.startswith("online:"):
        guid = subpath
    guid = _CERU_SOURCE_OVERRIDES.get(guid, guid)
    playlist_kind = dailyrec.online_playlist_kind(guid)
    if playlist_kind:
        upstream_client = get_upstream_client(request.app)
        is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
        if not is_authed and auth_resp is not None:
            return auth_resp
        cached = dailyrec.load_daily_cache(user_guid, dailyrec.today_key(), playlist_kind)
        tracks = (cached or {}).get("tracks") or []
        picked = dailyrec.pick_playlist_cover_track(tracks)
        picked_guid = str((picked or {}).get("guid") or "")
        if not is_online_guid(picked_guid):
            cover_id = str((picked or {}).get("coverId") or "")
            if picked and cover_id and not is_online_guid(cover_id):
                # 本地曲目封面：coverId 是真实官方封面 guid，透传官方静态封面端点
                upstream_client = get_upstream_client(request.app)
                headers = copy_incoming_headers(request)
                cover_req = upstream_client.build_request(
                    request.method,
                    f"/music/api/v1/static/cover?coverId={quote(cover_id, safe='')}",
                    headers=headers,
                )
                cover_resp = await upstream_client.send(cover_req, stream=True)
                cover_headers = filter_headers(
                    cover_resp.headers, exclude_keys={"content-length", "content-encoding"}
                )

                async def _cover_stream():
                    try:
                        async for chunk in cover_resp.aiter_bytes():
                            yield chunk
                    finally:
                        await cover_resp.aclose()

                return StreamingResponse(
                    _cover_stream(), status_code=cover_resp.status_code, headers=cover_headers
                )
            # 歌单里没有可用封面：不显示图标，客户端回落自带默认样式
            return Response(status_code=404)
        guid = picked_guid
    # 网易账号歌单封面：登记的封面直链直接 302（兼容重启后反查表未重建的裸假 id）
    nm_pl_id = nmpl.playlist_id_from_cover_request(guid, request.query_params.get("coverId") or subpath)
    if nm_pl_id:
        cover = nmpl.cover_url_for(nm_pl_id)
        if cover:
            return RedirectResponse(cover, status_code=302)
        return Response(status_code=404)
    ceru_pid = _ceru_playlist_id_from_guid(guid) or _ceru_playlist_id_from_guid(
        resolve_real_guid(request.query_params.get("coverId") or subpath)
    )
    if ceru_pid:
        cover = _CERU_PLAYLIST_COVERS.get(ceru_pid, "")
        if cover:
            return await _cached_cover_response(guid, cover)
        return Response(status_code=404)
    imp_pid = plimp.imp_playlist_id_from_guid(guid) or plimp.imp_playlist_id_from_guid(
        resolve_real_guid(request.query_params.get("coverId") or subpath)
    )
    if imp_pid:
        # 导入歌单封面：用导入时拿到的平台封面直链，不落音源配额
        cover = _IMP_PLAYLIST_COVERS.get(imp_pid, "")
        if not cover:
            cover = str((plimp.read_record(imp_pid) or {}).get("cover") or "")
            if cover:
                _IMP_PLAYLIST_COVERS[imp_pid] = cover
        if cover:
            return await _cached_cover_response(guid, cover)
        return Response(status_code=404)
    if not is_online_guid(guid):
        return await forward_to_upstream(request, get_upstream_client(request.app))

    if _COVER_VERSION_RE.search(guid) and not _COVER_ENTITY_VERSION_RE.search(guid):
        base_guid = _COVER_VERSION_RE.sub("", guid)
        # 导入歌单曲目：导入时已存平台封面直链，直接命中缓存链，不打音源
        imp_cover = _IMP_SONG_COVERS.get(base_guid, "")
        if imp_cover:
            return await _cached_cover_response(f"{base_guid}:imp", imp_cover)
        # Ceru 歌曲的封面跟“当前能播的源”解耦：换源只换音频，绝不能把
        # 播放源的封面顶上来。先用 Ceru 原封面，取不到才回退播放源封面。
        ceru_song = ceru.song_for_guid(base_guid)
        artwork = ceru.song_artwork(ceru_song) if ceru_song else ""
        if not _cover_url_unusable(artwork):
            # 独立缓存键：历史上同一 guid 已按播放源封面落过盘，直接复用
            # 会把旧源封面吐回来（这正是列表对、正在播放错的成因）。
            return await _cached_cover_response(f"{base_guid}:ceru", artwork)
        # 2.6.37: 封面请求只取元数据，不顺带取词（每页 50 个封面请求的隐性放大）
        data = await _online_info(request, base_guid, include_lyric=False)
        cover = str((data or {}).get("cover_url") or "")
        if not _cover_url_unusable(cover):
            return await _cached_cover_response(base_guid, cover)
        # 2.6.36: 播放源无封面 → 直构 CDN / 跨源搜索补齐 / 网易 / 内嵌图兜底链
        tail = await _cover_fallback_tail(request, base_guid, data)
        return tail if tail is not None else _placeholder_cover_response(base_guid)

    if _COVER_ENTITY_VERSION_RE.search(guid):
        base_guid = _COVER_ENTITY_VERSION_RE.sub("", guid)
        # 2.6.37: 封面请求只取元数据，不顺带取词（每页 50 个封面请求的隐性放大）
        data = await _online_info(request, base_guid, include_lyric=False)
        artwork = str((data or {}).get("cover_url") or "")
        if not artwork:
            ceru_song = ceru.song_for_guid(base_guid)
            artwork = ceru.song_artwork(ceru_song) if ceru_song else ""
        if not _cover_url_unusable(artwork):
            return await _cached_cover_response(base_guid, artwork)
        # 2.6.36: 播放源无封面 → 统一兜底链
        tail = await _cover_fallback_tail(request, base_guid, data)
        return tail if tail is not None else _placeholder_cover_response(base_guid)

    # 全屏播放页请求的是专辑/艺人封面（guid 以 :album / :artist 结尾）。
    # Ceru 歌曲重建后 Fake 专辑表可能为空，这里直接回退到歌曲自身的封面。
    if guid.endswith(":album") or guid.endswith(":artist"):
        base_guid = guid.rsplit(":", 1)[0]
        ceru_song = ceru.song_for_guid(base_guid)
        artwork = ceru.song_artwork(ceru_song) if ceru_song else ""
        if not _cover_url_unusable(artwork):
            return await _cached_cover_response(f"{base_guid}:ceru", artwork)
        # 2.6.37: 封面请求只取元数据，不顺带取词（每页 50 个封面请求的隐性放大）
        data = await _online_info(request, base_guid, include_lyric=False)
        artwork = str((data or {}).get("cover_url") or "")
        if not _cover_url_unusable(artwork):
            return await _cached_cover_response(base_guid, artwork)
        # 2.6.36: 播放源无封面 → 统一兜底链
        tail = await _cover_fallback_tail(request, base_guid, data)
        return tail if tail is not None else _placeholder_cover_response(base_guid)

    # 专辑锚点 guid（/search/album 在线专辑的 coverId）：登记时存了封面直链，直接 302
    album_entry = album_entry_from_real_guid(guid)
    if album_entry is not None:
        cover = str((album_entry.get("item") or {}).get("cover_url") or "")
        if not _cover_url_unusable(cover):
            return RedirectResponse(cover, status_code=302)
        return _placeholder_cover_response(guid)

    # 2.6.37: 封面请求只取元数据，不顺带取词
    data = await _online_info(request, guid, include_lyric=False)
    cover = str((data or {}).get("cover_url") or "")
    # Ceru 歌曲：正在播放页走的是裸 guid 分支，这里同样优先 Ceru 原封面，
    # 与列表(:cover5)/专辑(:album)分支保持一致，封面与播放源解耦。
    if _cover_url_unusable(cover):
        ceru_song = ceru.song_for_guid(guid)
        artwork = ceru.song_artwork(ceru_song) if ceru_song else ""
        if not _cover_url_unusable(artwork):
            return await _cached_cover_response(f"{guid}:ceru", artwork)
    # ① 已知直链封面直接 302；酷我文本封面（artistpicserver 返回的是文本页）除外
    if not _cover_url_unusable(cover):
        return await _cached_cover_response(guid, cover)

    # ② 2.6.36 统一 tail：直构 CDN → 跨源搜索补齐 → 网易补全（仅启用时）→ 内嵌图
    tail = await _cover_fallback_tail(request, guid, data)
    if tail is not None:
        return tail

    # ⑤ 本地占位图池：在线曲目封面永不 404
    return _placeholder_cover_response(guid)


# === online favorites ===

_FAV_LOCK = asyncio.Lock()


def sanitize_user_guid(guid: str | None) -> str:
    """过滤文件名合法字符 [A-Za-z0-9-_]，非法字符替换为 _；为空则返回 'shared'。"""
    raw = str(guid or "").strip()
    safe = re.sub(r"[^A-Za-z0-9\-_]", "_", raw)
    return safe or "shared"


def user_fav_path(user_guid: str) -> str:
    fav_dir = CONF.get("fav_dir") or os.path.join(_HOME, "online_favorites")
    safe_name = sanitize_user_guid(user_guid)
    return os.path.join(fav_dir, f"{safe_name}.json")


def load_online_favorites(user_guid: str) -> list[dict]:
    path = user_fav_path(user_guid)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("items"), list):
                return data["items"]
            if isinstance(data, list):
                return data
    except Exception as e:
        logger.warning("Failed to load online favorites for %s from %s: %s", user_guid, path, e)
    return []


def save_online_favorites(user_guid: str, items: list[dict]) -> bool:
    path = user_fav_path(user_guid)
    parent = os.path.dirname(path) or "."
    part_path = f"{path}.{uuid4().hex[:8]}.part"
    try:
        os.makedirs(parent, exist_ok=True)
        with open(part_path, "w", encoding="utf-8") as f:
            json.dump({"items": items}, f, ensure_ascii=False, indent=2)
        os.replace(part_path, path)
        return True
    except Exception as e:
        logger.warning("Failed to save online favorites for %s to %s: %s", user_guid, path, e)
        if os.path.exists(part_path):
            try:
                os.remove(part_path)
            except Exception:
                pass
        return False


def official_track_template(official_list: list) -> dict | None:
    """取一条真实官方 track 作结构模板：App 端反序列化严格，在线条目字段集需与官方对齐。"""
    for item in official_list:
        if isinstance(item, dict):
            g = str(item.get("guid") or "")
            if g and not is_online_guid(g):
                return deepcopy(item)
    return None


def _snapshot_to_info(track: dict) -> dict:
    """把落盘的 track 对象（最终形状）还原成 build_online_track 的 info 输入形状。

    收藏/历史快照是最终 track 对象（artists 数组、duration 毫秒、audioSpec），
    重建时需映射回 artist/duration_s/ext；info 形状的历史快照字段已对齐则原样保留。
    """
    info = dict(track)
    artists = track.get("artists")
    if not info.get("artist") and isinstance(artists, list) and artists and isinstance(artists[0], dict):
        info["artist"] = artists[0].get("name") or ""
    album = track.get("album")
    if isinstance(album, dict):
        info["album"] = album.get("name") or ""
    if not info.get("duration_s") and track.get("duration"):
        try:
            info["duration_s"] = float(track["duration"]) / 1000.0
        except (TypeError, ValueError):
            pass
    spec = track.get("audioSpec")
    if isinstance(spec, dict):
        info.setdefault("ext", spec.get("format") or "mp3")
        info.setdefault("file_size", spec.get("size") or 0)
        if not info.get("cover_url"):
            info["cover_url"] = track.get("coverUrl") or track.get("cover_url") or ""
    return info


def build_favorite_track_obj(
    guid: str,
    info: dict | None = None,
    created_at: int | None = None,
    template: dict | None = None,
) -> dict:
    raw_info = dict(info or {})
    raw_info.setdefault("id", song_id_from_online_guid(guid))
    raw_info.setdefault("source", source_from_online_guid(guid))
    vo = build_online_track(raw_info)

    now = int(time.time())
    ts = created_at or now

    # App 端解析严格：artists 永不为空、album.name 永不为空，避免整列表被丢弃。
    # 官方习惯：artist.coverId 为 null（无独立歌手封面），不能填歌曲 guid。
    artist_name = vo.get("artist") or "未知艺术家"
    artists_list = [
        {
            "guid": f"{guid}:artist",
            "name": artist_name,
            "coverId": None,
            "createdAt": ts,
            "updatedAt": ts,
        }
    ]

    album_name = (
        vo.get("albumName")
        or (vo.get("album", {}).get("name") if isinstance(vo.get("album"), dict) else "")
        or "未知专辑"
    )
    # 官方 album 对象没有 artists 键；releaseDate/barcode 缺省为 null 而非 0/""
    album_obj = {
        "guid": f"{guid}:album",
        "name": album_name,
        "coverId": guid,
        "releaseDate": None,
        "barcode": None,
        "createdAt": ts,
        "updatedAt": ts,
    }

    audio_spec = vo.get("audioSpec") or {}

    obj = {
        "guid": guid,
        "title": vo.get("title") or "",
        "duration": vo.get("duration") or 0,
        "isFavorite": True,
        "isCue": False,
        "genres": [],
        "artists": artists_list,
        "album": album_obj,
        "audioSpec": audio_spec,
        "accessStatus": 0,
        "coverId": guid,
        "year": None,
        "discNo": None,
        "trackNo": None,
        "isrc": None,
        "createdAt": ts,
        "updatedAt": ts,
    }

    if isinstance(template, dict) and template:
        # 以官方真实条目为结构底版，再用在线字段全覆盖：本侧未构造的官方字段
        # 保留官方结构，避免 App 因缺字段整列表解析失败。
        merged = deepcopy(template)
        merged.update(obj)
        return merged
    return obj


async def _probe_upstream_auth(request: Request, client: httpx.AsyncClient) -> tuple[bool, str, Response | None]:
    """向上游探测用户是否已登录。复用当前请求 headers。
    返回 (is_authed, user_guid, error_response)。
    """
    headers = copy_incoming_headers(request)
    try:
        probe_req = client.build_request("GET", "/music/api/v1/user/me", headers=headers)
        probe_resp = await client.send(probe_req)
        resp_headers = filter_headers(probe_resp.headers, exclude_keys={"content-length", "content-encoding"})

        if probe_resp.status_code == 401:
            return False, "", Response(
                content=probe_resp.content,
                status_code=401,
                headers=resp_headers,
                media_type=probe_resp.headers.get("content-type"),
            )

        if probe_resp.status_code == 200:
            try:
                probe_json = probe_resp.json()
                if isinstance(probe_json, dict) and probe_json.get("code") == 99999:
                    return False, "", JSONResponse(
                        content=probe_json,
                        status_code=200,
                        headers=resp_headers,
                    )
                if isinstance(probe_json, dict) and probe_json.get("code") == 0:
                    data = probe_json.get("data")
                    if isinstance(data, dict) and data.get("guid"):
                        return True, str(data["guid"]), None
                    logger.warning("user/me response missing data.guid, falling back to 'shared': %s", probe_json)
                    return True, "shared", None
            except Exception as e:
                logger.warning("Failed to parse user/me json response: %s", e)
                return True, "shared", None
            return True, "shared", None

        # 其他非 200/401 状态码，上游异常
        return True, "shared", None
    except Exception as e:
        logger.warning("Upstream auth probe failed: %s", e)
        # 探测异常时保守放行
        return True, "shared", None


async def _online_favorite_set(request: Request) -> set[str]:
    """当前用户在线收藏 guid 集合（探测失败回退 shared/空集，绝不抛错）。"""
    try:
        upstream_client = get_upstream_client(request.app)
        is_authed, user_guid, _resp = await _probe_upstream_auth(request, upstream_client)
        if not is_authed:
            return set()
        async with _FAV_LOCK:
            return {str(it.get("guid") or "") for it in load_online_favorites(user_guid)}
    except Exception:
        return set()


async def _best_effort_online_info(request: Request, guid: str) -> dict:
    """尽力获取在线曲目元数据：拉不到时从缓存反查兜底，绝不为 None（收藏/歌单快照共用）。"""
    info = await _online_info(request, guid)
    if info:
        return info
    cached_lyric = read_lyric_cache(guid)
    title = ""
    artist = ""
    cached_media = find_cache_file(guid)
    if cached_media:
        base = os.path.splitext(os.path.basename(cached_media))[0]
        if " - " in base:
            artist, title = base.split(" - ", 1)
        else:
            title = base
    return {
        "id": song_id_from_online_guid(guid),
        "source": source_from_online_guid(guid),
        "title": title,
        "artist": artist,
        "lyric": cached_lyric,
    }


@app.post("/music/api/v1/favorite-track/create")
# TOP[top.fav] 收藏创建：本地多用户隔离 + ceru 多端写回（未注册歌按主键合成）
async def favorite_track_create(request: Request):
    upstream_client = get_upstream_client(request.app)
    try:
        body = await request.json()
    except Exception:
        body = {}

    guid = ""
    if isinstance(body, dict):
        guid = resolve_real_guid(str(body.get("trackGUID") or body.get("guid") or "").strip())

    if not is_online_guid(guid):
        return await forward_to_upstream(request, upstream_client)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    now = int(time.time())
    info = await _best_effort_online_info(request, guid)
    track_obj = build_favorite_track_obj(guid, info, created_at=now)

    saved = False
    autobind = bool(CONF.get("fav_auto_bind"))
    async with _FAV_LOCK:
        try:
            items = load_online_favorites(user_guid)
            # 查重
            idx = next((i for i, it in enumerate(items) if it.get("guid") == guid), None)
            if idx is not None:
                # 幂等更新（重新收藏视为新操作，官方绑定开关开启时补 pending 标记）
                items[idx]["track"] = track_obj
                if autobind:
                    items[idx]["bind"] = "pending"
            else:
                item = {
                    "guid": guid,
                    "createdAt": now,
                    "track": track_obj,
                }
                if autobind:
                    item["bind"] = "pending"
                items.append(item)
            save_online_favorites(user_guid, items)
            saved = True
        except Exception as e:
            logger.warning("Error updating online favorites for user %s: %s", user_guid, e)

    if saved:
        _register_fav_autobind(request, guid, user_guid)

    if _ceru_on():
        ceru_song = ceru.song_for_guid(guid)
        if ceru_song is None:
            # 新搜索到的在线歌曲（不在同步服务器歌单里）：合成歌曲记录并登记，
            # 收藏才能同步到桌面/安卓端，后续取消收藏也可按主键反查
            ceru_song = _ceru_song_from_guid(guid, info)
            if ceru_song is not None:
                ceru.register_guid(guid, ceru_song)
        if ceru_song:
            try:
                await ceru.favorite_create(ceru_song)
            except Exception as e:
                logger.warning("cerusync favorite create failed for %s: %s", guid, e)
            # 标星：若该曲不在 favorites 语义歌单（刚取消过、或本机首次标星同步曲），
            # 用 operation 加回，服务器广播给其他设备；已在歌单则跳过（服务器侧
            # 重复 add 幂等，但先查缓存可省掉无谓的服务器写入与序号消耗）。
            try:
                track_key = ceru.song_track_key(ceru_song)
                if track_key:
                    for pid in await ceru.favorites_playlist_ids():
                        songs = await ceru.list_playlist_songs(pid)
                        ident = _fav_identity(ceru.song_title(ceru_song),
                                            ceru.song_artist(ceru_song))
                        dur = ceru.song_duration_s(ceru_song)
                        # 同一首歌可能已经用另一个平台的主键收藏过：再 add 一次
                        # 就是往「我的喜欢」里塞两条重复曲目
                        already = any(
                            isinstance(s, dict) and (
                                ceru.song_track_key(s) == track_key
                                or (ident
                                    and _fav_identity(ceru.song_title(s),
                                        ceru.song_artist(s)) == ident
                                    and _fav_same_song(ceru.song_duration_s(s), dur))
                            )
                            for s in songs
                        )
                        if not already:
                            await ceru.playlist_song_operation(pid, "add", track_key, song=ceru_song)
                            ceru.invalidate_playlist_tracks(pid)
                _forget_ceru_favorites_index()
            except Exception as e:
                logger.warning("cerusync star writeback failed for %s: %s", guid, e)

    return JSONResponse(content={"code": 0, "msg": "", "data": None})


@app.post("/music/api/v1/favorite-track/delete")
# TOP[top.fav.delete] 取消收藏：主键/身份找回 → operation remove + /favorites DELETE
async def favorite_track_delete(request: Request):
    upstream_client = get_upstream_client(request.app)
    try:
        body = await request.json()
    except Exception:
        body = {}

    guid = ""
    raw_guid = ""
    if isinstance(body, dict):
        raw_guid = str(body.get("trackGUID") or body.get("guid") or "").strip()
        guid = resolve_real_guid(raw_guid)

    if not is_online_guid(guid):
        # 官方绑定已完成的歌曲本地映射已删，客户端旧会话可能仍持伪装 id 取消
        # 收藏：登记命中则翻译成官方 guid 撤销官方收藏；未命中照旧透传官方
        hit = _lookup_bind_any_user(raw_guid)
        if hit and hit[2].get("fav"):
            bound_user, bound_guid, entry = hit
            ok = await _official_upstream_write(
                "POST", "/music/api/v1/favorite-track/delete",
                copy_incoming_headers(request), {"trackGUID": entry["official"]},
            )
            if ok:
                _forget_bind(bound_user, bound_guid, fav=True)
            return JSONResponse(content={"code": 0, "msg": "", "data": None})
        return await forward_to_upstream(request, upstream_client)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    if _ceru_on():
        ceru_song = ceru.song_for_guid(guid)
        track_key = ceru.song_track_key(ceru_song) if ceru_song else ""
        match_info = None
        if ceru_song is None:
            # 注册表里没有这首歌（换源播放 / 搜到的另一平台版本 / 服务重启）：
            # 按 guid 主键与歌名+歌手在「我的喜欢」里找回，否则取消标星没有落点
            ceru_song, track_key = await _ceru_favorite_match(guid)
            if ceru_song is None:
                # 主键对不上才需要元数据：按歌名+歌手跨平台认同一首歌
                match_info = await _best_effort_online_info(request, guid)
                ceru_song, track_key = await _ceru_favorite_match(guid, match_info)
            if ceru_song is None:
                # 仍未命中：按 guid 主键合成后按键删除（operation 只在服务器
                # 「我的喜欢」确含该曲时才真正写入；/favorites 删除未命中为 no-op）
                ceru_song = _ceru_song_from_guid(guid, match_info)
                track_key = ceru.song_track_key(ceru_song) if ceru_song else ""
        if ceru_song:
            try:
                await ceru.favorite_delete(ceru_song)
            except Exception as e:
                logger.warning("cerusync favorite delete failed for %s: %s", guid, e)
            # 取消标星：回写到 favorites 语义歌单（如“我的喜欢”）。/favorites 是
            # 独立存储、不会联动歌单，必须走 operation 协议，服务器才会向其他设备
            # 广播删除事件；只对缓存里确认包含该曲的歌单操作，避免无谓写入。
            try:
                if track_key:
                    for pid in await ceru.favorites_playlist_ids():
                        songs = await ceru.list_playlist_songs(pid)
                        if any(ceru.song_track_key(s) == track_key for s in songs):
                            await ceru.playlist_song_operation(pid, "remove", track_key)
                            # 服务器已同步应用：立即失效本地曲目缓存，用户刷新即见最新
                            # （不等同步事件/TTL，否则刚取消的歌刷新后还在）
                            ceru.invalidate_playlist_tracks(pid)
                _forget_ceru_favorites_index()
            except Exception as e:
                logger.warning("cerusync unstar writeback failed for %s: %s", guid, e)

    async with _FAV_LOCK:
        try:
            items = load_online_favorites(user_guid)
            items = [it for it in items if it.get("guid") != guid]
            save_online_favorites(user_guid, items)
        except Exception as e:
            logger.warning("Error deleting from online favorites for user %s: %s", user_guid, e)

    entry = lookup_bind_entry(user_guid, guid)
    if entry and entry.get("fav"):
        # 该曲已完成官方绑定（旧会话假 id）：同步撤销官方收藏
        if await _official_upstream_write(
            "POST", "/music/api/v1/favorite-track/delete",
            copy_incoming_headers(request), {"trackGUID": entry["official"]},
        ):
            _forget_bind(user_guid, guid, fav=True)

    _purge_cover_cache(guid)
    return JSONResponse(content={"code": 0, "msg": "", "data": None})


@app.get("/music/api/v1/favorite-track/list")
async def favorite_track_list(request: Request):
    upstream_client = get_upstream_client(request.app)
    url_path = request.url.path
    if request.url.query:
        url_path = f"{url_path}?{request.url.query}"
    headers = copy_incoming_headers(request)

    req = upstream_client.build_request("GET", url_path, headers=headers)
    upstream_resp = await upstream_client.send(req)
    resp_headers = filter_headers(upstream_resp.headers, exclude_keys={"content-length", "content-encoding"})

    if upstream_resp.status_code != 200:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    try:
        upstream_json = upstream_resp.json()
    except Exception:
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    if not isinstance(upstream_json, dict) or upstream_json.get("code") != 0:
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    # 探测当前用户身份。官方列表已取回成功（同一组请求头），此时探测被拒
    # （如 App 一次性票据已被首次请求消耗）不能回传鉴权错误导致整列表空白，
    # 降级为仅返回官方列表。
    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed:
        logger.warning("favorite list degraded to official-only: auth probe rejected after official list ok")
        return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)

    # 成功获取官方列表，合并本地在线收藏
    data = upstream_json.get("data")
    if not isinstance(data, dict):
        data = {"list": [], "total": 0}
        upstream_json["data"] = data

    official_list = data.get("list")
    if not isinstance(official_list, list):
        official_list = []
        data["list"] = official_list

    # 飞牛音乐前端收藏列表依赖 isFavorite=True 状态判断，遍历补齐官方列表中可能缺失的字段
    for item in official_list:
        if isinstance(item, dict):
            item["isFavorite"] = True

    official_total = data.get("total")
    if not isinstance(official_total, int):
        official_total = len(official_list)

    async with _FAV_LOCK:
        try:
            fav_items = load_online_favorites(user_guid)
        except Exception as e:
            logger.warning("Error reading online favorites for list for user %s: %s", user_guid, e)
            fav_items = []

    # 自愈：bind=pending 的收藏在此补发官方绑定/下载（含代理重启后的恢复）
    _maybe_retry_official_binds(request, user_guid, fav_items=fav_items)

    # 官方真实条目作为结构模板：App 端解析严格，在线条目字段集需与官方对齐；
    # 旧数据存的快照也统一重建，避免历史遗留形状继续下发。
    template = official_track_template(official_list)

    # 按 createdAt 倒序
    fav_items_sorted = sorted(fav_items, key=lambda x: x.get("createdAt", 0), reverse=True)
    online_tracks = []
    seen_guids: set[str] = set()
    for it in fav_items_sorted:
        g = str(it.get("guid") or "")
        if not g:
            continue
        seen_guids.add(g)
        snapshot = it.get("track") if isinstance(it.get("track"), dict) else None
        obj = build_favorite_track_obj(g, _snapshot_to_info(snapshot) if snapshot else None, created_at=it.get("createdAt"), template=template)
        obj["isFavorite"] = True
        online_tracks.append(disguise_client_json(obj))

    # Ceru 同步服务器中的 song 收藏同样并入红心列表；此处构建会登记 guid→song，
    # 用户在飞牛取消收藏时 favorite-track/delete 才能反查并回写服务器。
    if _ceru_on():
        try:
            ceru_songs = await _ceru_favorite_songs()
            for fav in ceru_songs:
                obj = await _ceru_favorite_obj(fav, template=template)
                if obj and str(obj.get("guid") or "") not in seen_guids:
                    seen_guids.add(str(obj["guid"]))
                    online_tracks.append(disguise_client_json(obj))
        except Exception as e:
            logger.warning("cerusync favorite list inject failed: %s", e)

    data["list"] = official_list + online_tracks
    data["total"] = official_total + len(online_tracks)
    _remember_list_order(upstream_json)
    _warm_page_head(online_tracks)

    return JSONResponse(content=upstream_json, status_code=upstream_resp.status_code, headers=resp_headers)


# === daily recommend + play history ===

_HISTORY_LOCK = asyncio.Lock()
_DAILY_TASKS: dict[str, asyncio.Task] = {}  # key: f"{user}:{kind}:{day}"
# 空结果构建冷却：key 同 _DAILY_TASKS，value=该次空结果的 builtAt（见 _ensure_daily_task）
_EMPTY_BUILD_COOLDOWN: dict[str, float] = {}
_EMPTY_BUILD_COOLDOWN_S = 300.0


def _prune_stale_daily_tasks(day: str) -> None:
    stale = [k for k in list(_DAILY_TASKS) if not str(k).endswith(f":{day}")]
    for k in stale:
        old = _DAILY_TASKS.pop(k, None)
        if old is not None and not old.done():
            old.cancel()
    for k in [k for k in _EMPTY_BUILD_COOLDOWN if not str(k).endswith(f":{day}")]:
        _EMPTY_BUILD_COOLDOWN.pop(k, None)


def _recommend_kind_enabled(kind: str) -> bool:
    if kind == "hot":
        return bool(CONF.get("recommend_hot", True))
    return bool(CONF.get("recommend_daily", True))


def _recommend_kinds_enabled() -> list[str]:
    """按开关返回要注入的推荐歌单类型（顺序即歌单列表顺序）。"""
    return [k for k in ("daily", "hot") if _recommend_kind_enabled(k)]


def _is_shared_user_guid(user_guid: str) -> bool:
    """身份探测未解析到真实用户 guid（保守回落 shared）的会话。"""
    return str(user_guid or "").strip() == "shared"


def _recommend_injectable_kinds(user_guid: str) -> list[str]:
    """按用户身份返回可注入（可构建）的推荐歌单类型，注入/生成侧统一判定。

    issue #25 多用户隔离：_probe_upstream_auth 失败/解析异常/guid 缺失时所有用户
    都回落 user_guid="shared"。数据来源结论（recommend.py）：
      - daily 是个性化歌单——种子取个人 play_history（本地+在线）与个人收藏，
        网易日推也是已登录账号的个性化内容；shared 会话注入会让身份探测失败的
        所有用户共享同一份"每日推荐"，混合历史生成的歌单跨用户泄露，必须跳过。
      - hot 是公共榜单——构建链（网易热歌榜/lx 免登录榜单）不读任何个人历史与
        收藏、不做按用户排除，内容与用户身份无关（缓存仅按用户分目录存放，
        不含任何用户数据），shared 注入无隔离风险。
    生成侧同样收口：seeds 只按各自 user_guid 文件读取，shared 历史
    （play_history/shared.json）不会进入任何真实用户的 daily 种子；shared 的
    daily 经本判定在 _peek/_load 处直接短路，绝不触发构建。
    """
    kinds = _recommend_kinds_enabled()
    if _is_shared_user_guid(user_guid):
        return [k for k in kinds if k == "hot"]
    return kinds


async def _ensure_daily_task(request: Request, user_guid: str, kind: str = "daily") -> asyncio.Task:
    day = dailyrec.today_key()
    _prune_stale_daily_tasks(day)
    key = f"{user_guid}:{kind}:{day}"
    task = _DAILY_TASKS.get(key)
    if task is not None and not task.done():
        return task
    if task is not None and task.done():
        try:
            if task.exception() is None:
                result = task.result()
                if isinstance(result, dict) and len(result.get("tracks") or []) >= dailyrec.PLAYLIST_SIZE:
                    return task
                if isinstance(result, dict) and not result.get("tracks"):
                    # 空结果冷却：音源全挂/LLM 不可用时构建产出 0 首，缓存为空会让
                    # 每次打开列表都重新触发一轮构建（反复打搜索与 LLM）。5 分钟内
                    # 不重建，冷却过后自动重试（缓存非空时本分支不会到达）。
                    built_at = result.get("builtAt")
                    last = float(built_at) if isinstance(built_at, (int, float)) else time.time()
                    if time.time() - last < _EMPTY_BUILD_COOLDOWN_S:
                        _EMPTY_BUILD_COOLDOWN[key] = last
                        return task
                    _EMPTY_BUILD_COOLDOWN[key] = last
        except (asyncio.CancelledError, Exception):
            pass
    if time.time() - _EMPTY_BUILD_COOLDOWN.get(key, 0.0) < _EMPTY_BUILD_COOLDOWN_S:
        # 冷却期内：不重复构建（上方已把可返回的 done task 返回；此处是异常/无结果的兜底）
        done = _DAILY_TASKS.get(key)
        if done is not None:
            return done
    _EMPTY_BUILD_COOLDOWN.pop(key, None)
    async with _FAV_LOCK:
        favs = load_online_favorites(user_guid)
    task = asyncio.create_task(
        dailyrec.get_or_build_daily(
            user_guid=user_guid,
            musicdl_client=get_musicdl_client(request.app) if CONF.get("musicdl_enabled") else None,
            musicbox_client=get_musicbox_client(request.app) if CONF["netease_enabled"] else None,
            llm_http=get_llm_client(request.app) if dailyrec.llm_enabled() else None,
            build_track=build_online_track,
            netease_enabled=CONF["netease_enabled"],
            favorite_items=favs,
            lx_client=get_lx_client(request.app) if CONF.get("lx_enabled") else None,
            lx_enabled=bool(CONF.get("lx_enabled")),
            lx_sources=CONF.get("lx_sources") or None,
            recommend_hot=bool(CONF.get("recommend_hot", True)),
            recommend_daily=bool(CONF.get("recommend_daily", True)),
            kind=kind,
        )
    )
    _DAILY_TASKS[key] = task
    return task


async def _wait_progressive_bundle(
    request: Request, user_guid: str, kind: str, timeout_s: float,
) -> dict:
    """issue #23 渐进上架：后台构建期间轮询磁盘 checkpoint，出现任意一首即返回。

    recommend 构建已改为逐首 checkpoint（2s 节流落盘），这里 0.5s 轮询一次：
    歌单/曲目列表首屏必有歌（构建仍在继续，用户重进列表可见增长），不再
    空等整链构建完成。task 完成则返回最终结果；超时仍无任何曲目回占位歌单。
    """
    day = dailyrec.today_key()
    task = await _ensure_daily_task(request, user_guid, kind)
    deadline = asyncio.get_running_loop().time() + timeout_s
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=min(remaining, 0.5))
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.warning("daily recommend build failed: %s", e)
            break
        cached = dailyrec.load_daily_cache(user_guid, day, kind)
        if cached and cached.get("tracks"):
            return cached
        if task.done():
            break
    cached = dailyrec.load_daily_cache(user_guid, day, kind)
    if cached and cached.get("tracks"):
        return cached
    return dailyrec.empty_daily_bundle(user_guid, kind)


async def _peek_daily_bundle(request: Request, user_guid: str, kind: str = "daily") -> dict:
    """歌单列表用：有缓存立刻返回；否则等渐进 checkpoint，最多 2s，超时仍返回占位歌单。"""
    if kind not in _recommend_injectable_kinds(user_guid):
        # 开关关闭或身份探测失败（shared）的个性化 daily：注入与生成一并跳过
        return dailyrec.empty_daily_bundle(user_guid, kind)
    day = dailyrec.today_key()
    dailyrec.purge_stale_daily_cache(user_guid, day)
    cached = dailyrec.load_daily_cache(user_guid, day, kind)
    if cached and cached.get("tracks"):
        return cached
    return await _wait_progressive_bundle(request, user_guid, kind, timeout_s=2.0)


async def _load_daily_bundle(request: Request, user_guid: str, kind: str = "daily") -> dict:
    if kind not in _recommend_injectable_kinds(user_guid):
        # 同 _peek_daily_bundle：shared 会话不构建/不下发个性化 daily（issue #25）
        return dailyrec.empty_daily_bundle(user_guid, kind)
    day = dailyrec.today_key()
    dailyrec.purge_stale_daily_cache(user_guid, day)
    cached = dailyrec.load_daily_cache(user_guid, day, kind)
    if cached and cached.get("tracks"):
        return cached
    return await _wait_progressive_bundle(request, user_guid, kind, timeout_s=20.0)


# === playlist tracks（官方歌单内的在线附加条目）===
# 官方 add-track/remove-track 对 DB 中不存在的 track guid 返回 100002，
# 在线曲目（客户端持有伪装 32-hex 假 id）直达官方必被拒。与收藏三接口同款
# 分工：在线条目记本地 JSON（按用户分文件，桶键=官方歌单 guid），官方条目
# 照旧透传官方；读取接口透传官方信封后合并下发。

_PLT_LOCK = asyncio.Lock()


def plt_dir() -> str:
    return CONF.get("plt_dir") or os.path.join(_HOME, "playlist_tracks")


def plt_dir_has_data() -> bool:
    """目录里存在任一用户文件才启用读接口拦截路径，否则纯透传（零开销零风险）。"""
    try:
        return any(name.endswith(".json") for name in os.listdir(plt_dir()))
    except OSError:
        return False


def user_plt_path(user_guid: str) -> str:
    return os.path.join(plt_dir(), f"{sanitize_user_guid(user_guid)}.json")


def load_playlist_tracks(user_guid: str) -> dict[str, list[dict]]:
    """返回 {歌单guid: [{"guid","addedAt","track"}, ...]}。"""
    path = user_plt_path(user_guid)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("items"), dict):
            return {str(k): v for k, v in data["items"].items() if isinstance(v, list)}
    except Exception as e:
        logger.warning("Failed to load playlist tracks for %s from %s: %s", user_guid, path, e)
    return {}


def save_playlist_tracks(user_guid: str, items: dict[str, list[dict]]) -> bool:
    path = user_plt_path(user_guid)
    parent = os.path.dirname(path) or "."
    part_path = f"{path}.{uuid4().hex[:8]}.part"
    try:
        os.makedirs(parent, exist_ok=True)
        with open(part_path, "w", encoding="utf-8") as f:
            json.dump({"items": items}, f, ensure_ascii=False, indent=2)
        os.replace(part_path, path)
        return True
    except Exception as e:
        logger.warning("Failed to save playlist tracks for %s to %s: %s", user_guid, path, e)
        if os.path.exists(part_path):
            try:
                os.remove(part_path)
            except Exception:
                pass
        return False


def playlist_track_counts(user_guid: str) -> dict[str, int]:
    return {k: len(v) for k, v in load_playlist_tracks(user_guid).items()}


async def _parse_playlist_track_body(request: Request) -> tuple[str, list[str]]:
    """解析 add/remove-track 的 {"guid": 歌单, "trackGUIDs": [...]}（官方契约，批量）。"""
    try:
        body = json.loads((await request.body()).decode("utf-8") or "{}")
    except Exception:
        body = {}
    if not isinstance(body, dict):
        return "", []
    playlist_guid = str(body.get("guid") or "").strip()
    raw = body.get("trackGUIDs")
    track_guids = [str(g or "").strip() for g in raw if str(g or "").strip()] if isinstance(raw, list) else []
    return playlist_guid, track_guids


async def _forward_official_playlist_tracks(
    request: Request, client: httpx.AsyncClient, action: str,
    playlist_guid: str, official: list[str],
) -> Response | None:
    """混合批次中官方部分先行。返回 None=官方成功，否则返回官方错误响应（本地不动）。"""
    headers = copy_incoming_headers(request)
    req = client.build_request(
        "POST",
        f"/music/api/v1/playlist/{action}",
        headers=headers,
        json={"guid": playlist_guid, "trackGUIDs": official},
    )
    resp = await client.send(req)
    resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})
    if resp.status_code != 200:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    try:
        payload = resp.json()
    except Exception:
        payload = None
    if not (isinstance(payload, dict) and payload.get("code") == 0):
        return JSONResponse(content=payload or {"code": -1, "msg": "upstream error", "data": None})
    return None


# === Write-back of online songs for sync playlists / imported playlists ======================================
# The App "add to playlist" picker can show Ceru sync playlists and imported playlists, which are not official playlists:
# passing fake ids straight through to the official server gets them rejected, and the old logic's silent absorb
# manifested as "tapped but nothing happened". Here we write online songs back to the sync server via the LWW
# operation protocol (visible across desktop/Android multi-device), or append them to the local JSON of the
# imported playlist (visible in-place within the playlist).

async def _ceru_playlist_add_tracks(request: Request, playlist_guid: str, track_guids: list[str]) -> JSONResponse:
    """Add tracks to a Ceru sync playlist: write online songs into the sync server via LWW add."""
    if not _ceru_on():
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    pid = _ceru_playlist_id_from_guid(playlist_guid)
    online = [g for g in (resolve_real_guid(g) for g in track_guids) if is_online_guid(g)]
    if not online:
        # Official guids do not belong to the sync server: keep absorbing (cannot pass through)
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    songs = await ceru.list_playlist_songs(pid)
    is_fav = pid in await ceru.favorites_playlist_ids()
    handled = 0
    for g in online:
        song = await _ceru_sync_song(request, g)
        if not song:
            continue
        key = ceru.song_track_key(song)
        if not key:
            continue
        # Skip if already in the playlist (including another platform primary key for the same song), avoiding duplicate entries
        ident = _fav_identity(ceru.song_title(song), ceru.song_artist(song))
        dur = ceru.song_duration_s(song)
        if any(
            isinstance(x, dict) and (
                ceru.song_track_key(x) == key
                or (ident
                    and _fav_identity(ceru.song_title(x), ceru.song_artist(x)) == ident
                    and _fav_same_song(ceru.song_duration_s(x), dur))
            )
            for x in songs
        ):
            handled += 1
            continue
        res = await ceru.playlist_song_operation(pid, "add", key, song=song)
        if res is None:
            logger.warning("cerusync playlist add failed: %s -> %s", playlist_guid, key)
            continue
        handled += 1
        if is_fav:
            try:
                await ceru.favorite_create(song)
            except Exception as e:
                logger.warning("cerusync favorite create failed for %s: %s", g, e)
    if handled:
        ceru.invalidate_playlist_tracks(pid)
        if is_fav:
            _forget_ceru_favorites_index()
    if handled:
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    return JSONResponse(content={"code": -1, "msg": "Failed to add to sync playlist, please try again later", "data": None})


async def _ceru_playlist_remove_tracks(request: Request, playlist_guid: str, track_guids: list[str]) -> JSONResponse:
    """Remove from a Ceru sync playlist: LWW remove, idempotent (treated as success if the song is not on the server)."""
    if not _ceru_on():
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    pid = _ceru_playlist_id_from_guid(playlist_guid)
    online = [g for g in (resolve_real_guid(g) for g in track_guids) if is_online_guid(g)]
    if not online:
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    songs = await ceru.list_playlist_songs(pid)
    present = {ceru.song_track_key(x) for x in songs if isinstance(x, dict)}
    is_fav = pid in await ceru.favorites_playlist_ids()
    removed = 0
    for g in online:
        key = next((k for k in _guid_track_keys(g) if k in present), "")
        if not key:
            continue
        res = await ceru.playlist_song_operation(pid, "remove", key)
        if res is None:
            logger.warning("cerusync playlist remove failed: %s -> %s", playlist_guid, key)
            continue
        removed += 1
        if is_fav:
            song = next((x for x in songs if isinstance(x, dict) and ceru.song_track_key(x) == key), None)
            if song is not None:
                try:
                    await ceru.favorite_delete(song)
                except Exception as e:
                    logger.warning("cerusync favorite delete failed for %s: %s", g, e)
    if removed:
        ceru.invalidate_playlist_tracks(pid)
        if is_fav:
            _forget_ceru_favorites_index()
    return JSONResponse(content={"code": 0, "msg": "", "data": None})


def _imp_song_key(song: dict) -> tuple[str, str]:
    return (str(song.get("source") or "").lower(), str(song.get("id") or ""))


# TOP[top.imp.add] 导入歌单加歌：本地 JSON 按平台主键追加，查重幂等
async def _imp_playlist_add_tracks(request: Request, playlist_guid: str, track_guids: list[str]) -> JSONResponse:
    """Add tracks to an imported playlist: dedupe online songs by platform primary key, then append to the local JSON."""
    pid = plimp.imp_playlist_id_from_guid(playlist_guid)
    record = plimp.read_record(pid)
    if not record:
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    existing = {_imp_song_key(x) for x in record.get("songs") or [] if isinstance(x, dict)}
    added = 0
    for g in (resolve_real_guid(t) for t in track_guids):
        if not is_online_guid(g):
            continue
        keys = _guid_track_keys(g)
        if not keys:
            continue
        platform, ident = keys[0].split(":", 1)
        if platform == "lx" or not ident or (platform, ident) in existing:
            continue
        info = await _best_effort_online_info(request, g)
        title = str(info.get("title") or "").strip()
        if not title:
            continue
        song = plimp.make_song(
            platform, ident, title,
            str(info.get("artist") or "").strip(),
            str(info.get("album") or info.get("albumName") or "").strip(),
            _fav_duration_s(info.get("duration_s")),
            str(info.get("cover_url") or "").strip(),
        )
        if not song:
            continue
        record.setdefault("songs", []).append(song)
        existing.add((platform, ident))
        added += 1
    if added:
        try:
            plimp.write_record(record)
        except Exception as e:
            logger.warning("Error appending imported playlist %s: %s", pid, e)
    return JSONResponse(content={"code": 0, "msg": "", "data": None})


async def _imp_playlist_remove_tracks(playlist_guid: str, track_guids: list[str]) -> JSONResponse:
    """Remove from an imported playlist: delete from the local JSON by platform primary key, idempotent."""
    pid = plimp.imp_playlist_id_from_guid(playlist_guid)
    record = plimp.read_record(pid)
    if not record:
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    wanted: set[tuple[str, str]] = set()
    for g in (resolve_real_guid(t) for t in track_guids):
        if not is_online_guid(g):
            continue
        for k in _guid_track_keys(g):
            platform, _, ident = k.partition(":")
            if platform != "lx" and ident:
                wanted.add((platform, ident))
    before = len(record.get("songs") or [])
    kept = [x for x in record.get("songs") or []
            if not (isinstance(x, dict) and _imp_song_key(x) in wanted)]
    if len(kept) != before:
        try:
            record["songs"] = kept
            plimp.write_record(record)
        except Exception as e:
            logger.warning("Error removing from imported playlist %s: %s", pid, e)
    return JSONResponse(content={"code": 0, "msg": "", "data": None})


@app.post("/music/api/v1/playlist/add-track")
# TOP[top.playlist.write] 歌单加歌/删歌：官方透传 + 本地桶 + ceru LWW + 导入 JSON
async def playlist_add_track(request: Request):
    """往歌单加曲目：在线条目记本地存储（幂等，重复加刷新 addedAt 对齐官方语义）。"""
    upstream_client = get_upstream_client(request.app)
    playlist_guid, track_guids = await _parse_playlist_track_body(request)
    if not playlist_guid:
        return await forward_to_upstream(request, upstream_client)

    playlist_guid = resolve_real_guid(playlist_guid)
    if dailyrec.is_recommend_playlist_guid(playlist_guid):
        # 推荐歌单是按天重建的虚拟歌单，保持只读：吸收请求，避免伪装 id 直达官方被拒
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    if nmpl.is_nm_playlist_guid(playlist_guid):
        # 网易账号歌单只读（不回写网易）：同样吸收请求
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    if _is_ceru_playlist_guid(playlist_guid):
        # Ceru 同步歌单：在线歌曲经 LWW operation 协议写回同步服务器（多端可见）
        return await _ceru_playlist_add_tracks(request, playlist_guid, track_guids)
    if plimp.is_imp_playlist_guid(playlist_guid):
        # 导入歌单：在线歌曲追加进本地 JSON，歌单内原位可见
        return await _imp_playlist_add_tracks(request, playlist_guid, track_guids)

    online = [g for g in (resolve_real_guid(g) for g in track_guids) if is_online_guid(g)]
    if not online:
        # 全官方（或空列表）：原样透传，官方语义兜底（含参数校验错误回传）
        return await forward_to_upstream(request, upstream_client)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    official = [g for g in (resolve_real_guid(g) for g in track_guids) if not is_online_guid(g)]
    if official:
        err = await _forward_official_playlist_tracks(request, upstream_client, "add-track", playlist_guid, official)
        if err is not None:
            return err

    now = int(time.time())
    snapshots = {}
    for g in online:
        snapshots[g] = build_favorite_track_obj(g, await _best_effort_online_info(request, g), created_at=now)

    saved = False
    autobind = bool(CONF.get("fav_auto_bind"))
    async with _PLT_LOCK:
        try:
            items = load_playlist_tracks(user_guid)
            bucket = items.setdefault(playlist_guid, [])
            for g in online:
                idx = next((i for i, it in enumerate(bucket) if it.get("guid") == g), None)
                if idx is not None:
                    bucket[idx]["addedAt"] = now
                    bucket[idx]["track"] = snapshots[g]
                else:
                    entry = {"guid": g, "addedAt": now, "track": snapshots[g]}
                    if autobind:
                        entry["bind"] = "pending"
                    bucket.append(entry)
            save_playlist_tracks(user_guid, items)
            saved = True
        except Exception as e:
            logger.warning("Error updating playlist tracks for user %s: %s", user_guid, e)

    if saved:
        for g in online:
            _register_fav_autobind(request, g, user_guid, playlist_guid=playlist_guid)

    return JSONResponse(content={"code": 0, "msg": "", "data": None})


@app.post("/music/api/v1/playlist/remove-track")
async def playlist_remove_track(request: Request):
    """从歌单移除曲目：在线条目删本地存储（幂等），官方条目照旧透传。"""
    upstream_client = get_upstream_client(request.app)
    playlist_guid, track_guids = await _parse_playlist_track_body(request)
    if not playlist_guid:
        return await forward_to_upstream(request, upstream_client)

    playlist_guid = resolve_real_guid(playlist_guid)
    if dailyrec.is_recommend_playlist_guid(playlist_guid):
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    if nmpl.is_nm_playlist_guid(playlist_guid):
        return JSONResponse(content={"code": 0, "msg": "", "data": None})
    if _is_ceru_playlist_guid(playlist_guid):
        # Ceru 同步歌单：LWW remove 写回同步服务器，幂等
        return await _ceru_playlist_remove_tracks(request, playlist_guid, track_guids)
    if plimp.is_imp_playlist_guid(playlist_guid):
        # 导入歌单：按主键从本地 JSON 移除
        return await _imp_playlist_remove_tracks(playlist_guid, track_guids)

    online = [g for g in (resolve_real_guid(g) for g in track_guids) if is_online_guid(g)]
    if not online:
        return await forward_to_upstream(request, upstream_client)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    official = [g for g in (resolve_real_guid(g) for g in track_guids) if not is_online_guid(g)]
    # 旧会话假 id 中已完成官方绑定的：翻译成官方 guid 一并从官方歌单移除
    bound_pairs: "list[tuple[str, str]]" = []
    for g in online:
        entry = lookup_bind_entry(user_guid, g)
        if entry and playlist_guid in (entry.get("playlists") or []):
            bound_pairs.append((g, str(entry.get("official") or "")))
    official += [og for _, og in bound_pairs if og]
    if official:
        err = await _forward_official_playlist_tracks(request, upstream_client, "remove-track", playlist_guid, official)
        if err is not None:
            return err
        for g, _og in bound_pairs:
            _forget_bind(user_guid, g, playlist_guid=playlist_guid)

    online_set = set(online)
    async with _PLT_LOCK:
        try:
            items = load_playlist_tracks(user_guid)
            bucket = items.get(playlist_guid)
            if bucket:
                kept = [it for it in bucket if it.get("guid") not in online_set]
                if kept:
                    items[playlist_guid] = kept
                else:
                    items.pop(playlist_guid, None)
                save_playlist_tracks(user_guid, items)
        except Exception as e:
            logger.warning("Error removing playlist tracks for user %s: %s", user_guid, e)

    return JSONResponse(content={"code": 0, "msg": "", "data": None})


@app.post("/music/api/v1/playlist/delete")
async def playlist_delete(request: Request):
    """删除歌单：透传官方；成功后清掉本地为该歌单存的在线附加条目（防孤儿数据）。"""
    upstream_client = get_upstream_client(request.app)
    try:
        _body = json.loads((await request.body()).decode("utf-8") or "{}")
    except Exception:
        _body = {}
    _pl_guid = str(_body.get("guid") or "").strip() if isinstance(_body, dict) else ""
    if nmpl.is_nm_playlist_guid(resolve_real_guid(_pl_guid)):
        # 网易账号歌单只读：吸收删除（不透传官方必被拒的假 id），下次刷新卡片仍在
        return JSONResponse(content={"code": 0, "msg": "ok", "data": None})
    if _is_ceru_playlist_guid(resolve_real_guid(_pl_guid)):
        # Ceru 同步歌单只读：吸收删除
        return JSONResponse(content={"code": 0, "msg": "ok", "data": None})
    imp_del = plimp.imp_playlist_id_from_guid(resolve_real_guid(_pl_guid))
    if imp_del:
        # 导入歌单存在本地：真删除（不透传官方，假 id 必被拒）
        plimp.delete(imp_del)
        _IMP_PLAYLIST_COVERS.pop(imp_del, None)
        return JSONResponse(content={"code": 0, "msg": "ok", "data": None})
    envelope = await fetch_upstream_envelope(request, upstream_client)
    if isinstance(envelope, Response):
        return envelope
    headers = envelope.pop("_ext_headers", {})

    if envelope.get("code") == 0:
        try:
            body = json.loads((await request.body()).decode("utf-8") or "{}")
        except Exception:
            body = {}
        playlist_guid = str(body.get("guid") or "").strip() if isinstance(body, dict) else ""
        resolved = resolve_real_guid(playlist_guid)
        if playlist_guid and not dailyrec.is_recommend_playlist_guid(resolved) and plt_dir_has_data():
            is_authed, user_guid, _ = await _probe_upstream_auth(request, upstream_client)
            if is_authed:
                async with _PLT_LOCK:
                    try:
                        items = load_playlist_tracks(user_guid)
                        if items.pop(resolved, None) is not None or items.pop(playlist_guid, None) is not None:
                            save_playlist_tracks(user_guid, items)
                    except Exception as e:
                        logger.warning("Error purging playlist tracks for user %s: %s", user_guid, e)
                # 官方已写入该歌单的绑定登记同步清理，防孤儿转发
                _forget_bind_playlist(user_guid, resolved)

    return JSONResponse(content=envelope, headers=headers)


def _playlist_public_fields(record: dict, tracks: list | None = None) -> dict:
    # 封面取曲在下发时重算（兼容当天旧缓存），并伪装成官方 track_+32hex 形态：
    # 官方 App 按 id 格式过滤，online: 原样下发的 coverId 不会被渲染成图标。
    # 本地曲目的 coverId 是真实官方封面 guid：原样下发（封面端点透传官方）。
    cover = ""
    picked = dailyrec.pick_playlist_cover_track(tracks)
    if picked:
        cover = str(picked.get("coverId") or picked.get("guid") or "")
    if not cover:
        cover = str(record.get("coverId") or record.get("guid") or "")
    if cover.startswith("online:"):
        cover = "track_" + fake_official_guid(cover)
    return {
        "guid": record.get("guid"),
        "name": record.get("name") or "每日推荐",
        "coverId": cover,
        "createdAt": int(record.get("createdAt") or time.time()),
        "updatedAt": int(record.get("updatedAt") or time.time()),
        "trackCount": int(record.get("trackCount") or 0),
        "isDaily": True,
    }


# === Ceru 自部署同步服务接入 ===

_CERU_PLAYLIST_COVERS: dict[str, str] = {}
_CERU_SOURCE_OVERRIDES: dict[str, str] = {}


def _ceru_ts(value: Any) -> int:
    """Ceru 时间戳兼容：数字直接取整，ISO 字符串转 epoch 秒。"""
    if value is None or value == "":
        return int(time.time())
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        return int(time.time())


def _canonical_ceru_base(guid: str) -> str:
    """把歌曲/专辑/艺人/版本化封面 ID 收敛回原歌曲 guid。"""
    base = str(guid or "")
    while True:
        for suffix in (":cover3", ":cover2", ":album:cover3", ":album:cover2", ":artist:cover3", ":artist:cover2", ":album", ":artist"):
            if base.endswith(suffix):
                base = base[: -len(suffix)]
                break
        else:
            return base


def _ceru_on() -> bool:
    return bool(CONF.get("ceru_sync_enabled") and ceru.enabled())


def _ceru_active_source() -> str:
    if CONF.get("lx_enabled"):
        return "lx"
    if CONF.get("netease_enabled"):
        return "netease"
    if CONF.get("musicdl_enabled"):
        return "musicdl"
    return ""


async def _ceru_search_fallback(keyword: str, limit: int) -> list[dict]:
    """当前音源按标题+歌手搜索候选条目（跨源歌单的兜底映射）。"""
    if not keyword:
        return []
    source = _ceru_active_source()
    try:
        if source == "lx":
            return await fetch_lx_search(get_lx_client(app), keyword, limit)
        if source == "netease":
            return await _musicbox_search_request(get_musicbox_client(app), keyword, limit) or []
        if source == "musicdl":
            data = await fetch_musicdl_search(get_musicdl_client(app), keyword, limit)
            if isinstance(data, dict):
                return data.get("items") or []
    except Exception as e:
        logger.debug("cerusync search fallback failed (%s): %s", source, e)
    return []


def _ceru_pick_fallback(candidates: list[dict], song: dict) -> dict | None:
    title = re.sub(r"[\s\-—·・&/、,，|]", "", ceru.song_title(song)).lower()
    artist = re.sub(r"[\s\-—·・&/、,，|]", "", ceru.song_artist(song)).lower()
    duration = ceru.song_duration_s(song)
    best = None
    best_score = -1.0
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        c_title = re.sub(r"[\s\-—·・&/、,，|]", "", str(candidate.get("title") or candidate.get("name") or "")).lower()
        c_artist = re.sub(r"[\s\-—·・&/、,，|]", "", str(candidate.get("artist") or candidate.get("singer") or "")).lower()
        score = 0.0
        if title and c_title:
            if title == c_title:
                score += 100
            elif title in c_title or c_title in title:
                score += 40
        if artist and c_artist:
            if artist == c_artist:
                score += 50
            elif artist in c_artist or c_artist in artist:
                score += 20
        try:
            c_duration = float(candidate.get("duration_s") or 0)
        except (TypeError, ValueError):
            c_duration = 0.0
        if duration > 0 and c_duration > 0:
            delta = abs(duration - c_duration)
            if delta <= 3:
                score += 30
            elif delta <= 10:
                score += 12
            elif delta > 30:
                score -= 30
        # 同平台优先，减少跨平台版本差异。
        cand_source = str(candidate.get("lx_source") or ceru.song_source(candidate)).lower()
        if ceru.song_source(song) == cand_source:
            score += 5
        # 酷狗同步源只有数字 audio_id，搜索回来的是真 hash。命中同一个
        # audio_id 说明是同一首，给最高权重，避免同歌名不同版本被选中。
        if ceru.song_source(song) == "kg":
            want_sid = str(ceru.song_id(song) or "")
            cand_audio = str(
                candidate.get("audio_id") or candidate.get("album_audio_id") or ""
            )
            if want_sid and cand_audio and want_sid == cand_audio:
                score += 200
        if score > best_score:
            best = candidate
            best_score = score
    return best if best is not None else (candidates[0] if candidates else None)


def _ceru_item_matches_song(item: dict, song: dict) -> bool:
    """缓存/候选必须歌名一致、歌手匹配、时长接近，避免同歌手错歌被长期缓存。"""
    if not isinstance(item, dict):
        return False
    norm = lambda value: re.sub(r"[\s\-—·・&/、,，|]", "", str(value or "")).lower()
    want_title = norm(ceru.song_title(song))
    want_artist = norm(ceru.song_artist(song))
    got_title = norm(item.get("title") or item.get("name"))
    got_artist = norm(item.get("artist") or item.get("singer"))
    if not want_title or got_title != want_title:
        return False
    if want_artist and got_artist and not (
        want_artist == got_artist or want_artist in got_artist or got_artist in want_artist
    ):
        return False
    want_duration = ceru.song_duration_s(song)
    try:
        got_duration = float(item.get("duration_s") or 0)
    except (TypeError, ValueError):
        got_duration = 0.0
    if want_duration > 0 and got_duration > 0 and abs(want_duration - got_duration) > 20:
        return False
    return True


def _ceru_item_for_song(song: dict) -> dict:
    """Ceru 歌曲 → 当前音源 item。不发起网络，先做确定性的直连映射。"""
    source = _ceru_active_source()
    if source == "lx":
        return ceru.lx_item_for_song(song) or {}
    if source == "netease" and ceru.song_source(song) == "wy":
        sid = ceru.song_id(song)
        if sid:
            return {
                "id": f"netease:{sid}",
                "source": "netease",
                "title": ceru.song_title(song),
                "artist": ceru.song_artist(song),
                "album": ceru.song_album(song),
                "duration_s": ceru.song_duration_s(song),
                "ext": "mp3",
                "cover_url": ceru.song_artwork(song),
                "lyric": "",
            }
    return {}


async def _ceru_build_track(song: dict) -> dict | None:
    """把 Ceru 歌曲转成可播放的 online VO，并登记 guid→song 反查。"""
    if not isinstance(song, dict):
        return None
    item = _ceru_resolve_item_for_song(song)
    if not item or not item.get("id"):
        keyword = f"{ceru.song_title(song)} {ceru.song_artist(song)}".strip()
        if not keyword:
            return None
        picked = _ceru_pick_fallback(await _ceru_search_fallback(keyword, 20), song)
        item = picked if picked and _ceru_item_matches_song(picked, song) else {}
        if item:
            _ceru_store_lx_item(song, item)
    if not item or not item.get("id"):
        return None
    try:
        track = build_online_track(item)
    except Exception as e:
        logger.debug("cerusync build track failed: %s", e)
        return None
    guid = str(track.get("guid") or "")
    if guid:
        # 封面与播放解耦：VO 自带的 cover_url 一律改写为 Ceru 原封面，
        # 播放源只提供音频，绝不允许把搜索候选的封面带进正在播放页。
        artwork = ceru.song_artwork(song)
        if artwork:
            track["cover_url"] = artwork
            track["coverUrl"] = artwork
            track["coverURL"] = artwork
        # 列表/ Dock / 全屏是三条不同的封面请求路径；旧占位图会被客户端
        # 缓存 24h。这里给 Ceru 曲目的封面 ID 升一版，强制重新取真实封面。
        shared_cover_id = f"{guid}:cover5"
        track["coverId"] = shared_cover_id
        album_obj = track.get("album")
        if isinstance(album_obj, dict):
            album_obj["coverId"] = shared_cover_id
        ceru.register_guid(guid, song)
    return track


def _ceru_light_track(song: dict) -> dict | None:
    """列表用轻量 VO：只做确定性直连映射，不发起任何网络搜索。

    映射失败的歌曲用占位 item 保留在列表里（guid 仍可播：点播时
    _open_online_stream 会按标题+歌手懒搜索解析），避免“trackCount
    与实际列表对不上、后页全是空洞、点播放没声”。
    """
    if not isinstance(song, dict):
        return None
    item = _ceru_resolve_item_for_song(song)
    if not item or not item.get("id"):
        src = ceru.song_source(song) or "kg"
        sid = ceru.song_id(song) or ""
        if src not in ("kg", "wy", "tx", "mg", "kw"):
            src = "kg"
        # 占位 id：直连解析必失败，但点播时的懒搜索(fallback)可用标题找回。
        # Ceru song_id 常藏在 raw 里(song.get('id')多为空)，用 song_track_key
        # +标题专辑哈希做唯一键：同一首歌 guid 稳定、不同歌不串、翻页不丢。
        raw = song.get("raw") if isinstance(song.get("raw"), dict) else {}
        raw_key = (
            str(song.get("id") or "").strip()
            or str(raw.get("hash") or "").strip()
            or str(raw.get("songmid") or "").strip()
            or str(song.get("songmid") or "").strip()
            or str(raw.get("rid") or raw.get("song_id") or raw.get("copyrightId") or "").strip()
        )
        sid_key = sid.strip() or raw_key or hashlib.md5(
            f"{ceru.song_title(song)}|{ceru.song_artist(song)}|{ceru.song_album(song)}".encode("utf-8")
        ).hexdigest()[:16]
        uniq = f"{ceru.song_track_key(song)}|{sid_key}|{ceru.song_title(song)}|{ceru.song_album(song)}"
        placeholder = "ph" + hashlib.md5(uniq.encode("utf-8")).hexdigest()[:30]
        item = {
            "id": f"lx:{src}:{placeholder}",
            "source": "lx",
            "lx_source": src,
            "title": ceru.song_title(song),
            "artist": ceru.song_artist(song),
            "album": ceru.song_album(song),
            "duration_s": ceru.song_duration_s(song),
            "ext": "mp3",
            "cover_url": ceru.song_artwork(song),
            "file_size": 0,
            "lyric": "",
            "_ceru_placeholder": True,
        }
    else:
        # 直连命中的条目也统一用 Ceru 原封面，避免播放源封面混入。
        artwork = ceru.song_artwork(song)
        if artwork:
            item = dict(item)
            item["cover_url"] = artwork
    try:
        track = build_online_track(item)
    except Exception as e:
        logger.debug("cerusync light track failed: %s", e)
        return None
    guid = str(track.get("guid") or "")
    if not guid:
        return None
    artwork = ceru.song_artwork(song)
    if artwork:
        track["cover_url"] = artwork
        track["coverUrl"] = artwork
        track["coverURL"] = artwork
    shared_cover_id = f"{guid}:cover5"
    track["coverId"] = shared_cover_id
    album_obj = track.get("album")
    if isinstance(album_obj, dict):
        album_obj["coverId"] = shared_cover_id
    ceru.register_guid(guid, song)
    return track


async def _ceru_favorite_obj(song: dict, template: dict | None = None) -> dict | None:
    """Ceru song 收藏 → 收藏列表 VO（登记 guid 以便取消收藏回写）。"""
    item = _ceru_resolve_item_for_song(song)
    if not item or not item.get("id"):
        keyword = f"{ceru.song_title(song)} {ceru.song_artist(song)}".strip()
        if not keyword:
            return None
        picked = _ceru_pick_fallback(await _ceru_search_fallback(keyword, 20), song)
        item = picked if picked and _ceru_item_matches_song(picked, song) else {}
        if item:
            _ceru_store_lx_item(song, item)
    if not item or not item.get("id"):
        return None
    try:
        track = build_online_track(item)
    except Exception:
        return None
    guid = str(track.get("guid") or "")
    if not guid:
        return None
    ceru.register_guid(guid, song)
    obj = build_favorite_track_obj(guid, item, created_at=int(time.time()), template=template)
    # 封面与播放解耦：收藏 VO 同样用 Ceru 原封面。
    artwork = ceru.song_artwork(song)
    if artwork:
        obj["cover_url"] = artwork
        obj["coverUrl"] = artwork
        obj["coverURL"] = artwork
    shared_cover_id = f"{guid}:cover5"
    obj["coverId"] = shared_cover_id
    album_obj = obj.get("album")
    if isinstance(album_obj, dict):
        album_obj["coverId"] = shared_cover_id
    return obj


async def _ceru_cards(request: Request) -> list[dict]:
    """Ceru 歌单摘要卡片（含“我的喜欢”），返回给 playlist/list 注入。"""
    if not _ceru_on():
        return []
    logger.info("cerusync _ceru_cards called")
    try:
        playlists = await ceru.list_playlists()
    except Exception as e:
        logger.debug("cerusync playlist list failed: %s", e)
        return []
    cards: list[dict] = []
    for pl in playlists:
        pid = str(pl.get("id") or "")
        if not pid:
            continue
        guid = f"online:playlist:ceru:{pid}"
        cover = str(pl.get("coverUrl") or pl.get("cover") or "")
        if cover:
            _CERU_PLAYLIST_COVERS[pid] = cover
        cover_id = "track_" + fake_official_guid(guid)
        cards.append({
            "guid": guid,
            "name": str(pl.get("title") or pl.get("name") or "未命名歌单"),
            "coverId": cover_id,
            "createdAt": _ceru_ts(pl.get("createdAt")),
            "updatedAt": _ceru_ts(pl.get("updatedAt")),
            "trackCount": int(pl.get("total") or 0),
            "isDaily": True,
        })
    if cards:
        ceru.schedule_prefetch(_ceru_build_track_sync, cards)
    logger.info("cerusync playlist list loaded: %d cards", len(cards))
    return cards


def _ceru_build_track_sync(song: dict) -> dict | None:
    """schedule_prefetch 同步回调：绕过 async 上下文做一次轻量直连映射。"""
    item = _ceru_item_for_song(song)
    if not item or not item.get("id"):
        return None
    try:
        track = build_online_track(item)
    except Exception:
        return None
    guid = str(track.get("guid") or "")
    if guid:
        ceru.register_guid(guid, song)
    return track


def _ceru_playlist_id_from_guid(guid: str) -> str:
    if not guid.startswith("online:playlist:ceru:"):
        return ""
    return guid[len("online:playlist:ceru:") :]


def _is_ceru_playlist_guid(guid: str) -> bool:
    return bool(_ceru_playlist_id_from_guid(guid or ""))


async def _ceru_playlist_card(pid: str) -> dict | None:
    playlists = await ceru.list_playlists()
    for pl in playlists:
        if str(pl.get("id") or "") == pid:
            return {
                "guid": f"online:playlist:ceru:{pid}",
                "name": str(pl.get("title") or pl.get("name") or "未命名歌单"),
                "coverId": "track_" + fake_official_guid(f"online:playlist:ceru:{pid}"),
                "createdAt": _ceru_ts(pl.get("createdAt")),
                "updatedAt": _ceru_ts(pl.get("updatedAt")),
                "trackCount": int(pl.get("total") or 0),
                "isDaily": True,
            }
    return None


async def _ceru_playlist_tracks(pid: str) -> list[dict]:
    """Ceru 歌单歌曲 → 可播 VO（按服务器顺序）。"""
    songs = await ceru.list_playlist_songs(pid)
    tracks: list[dict] = []
    for song in songs:
        track = await _ceru_build_track(song)
        if track and track.get("guid"):
            tracks.append(track)
    return tracks


async def _ceru_song_count(pid: str) -> int:
    """歌单歌曲数（优先服务器 total，避免为计数构建全部 VO）。"""
    playlists = await ceru.list_playlists()
    for pl in playlists:
        if str(pl.get("id") or "") == pid:
            try:
                return int(pl.get("total") or 0)
            except (TypeError, ValueError):
                pass
    songs = await ceru.list_playlist_songs(pid)
    return len(songs)


async def _ceru_playlist_tracks_page(pid: str, start: int, size: int) -> tuple[list[dict], int]:
    """只把当前页的歌曲构建成可播 VO，避免大歌单整单转换。"""
    songs = await ceru.list_playlist_songs(pid)
    total = len(songs)
    page_songs = songs[start:start + size]
    # 列表只做轻量直连映射，不发起搜索：未映射的用占位保留，
    # 点播时 _open_online_stream 再懒搜索解析，避免大歌单整单搜索打爆限流。
    tracks: list[dict] = []
    for song in page_songs:
        try:
            track = _ceru_light_track(song)
        except Exception:
            track = None
        if isinstance(track, dict) and track.get("guid"):
            tracks.append(track)
    _remember_ceru_page(tracks)
    # 不整页预解析：洛雪用户源有硬限流，一开歌单批量请求会把后续正常播放
    # 全部打成失败。只预热页首 1 首，用一次额度换首播接近秒开。
    prefetch_ids = [
        song_id_from_online_guid(str(t.get("guid") or ""))
        for t in tracks[:1]
        if str(t.get("guid") or "").startswith("online:lx:")
    ]
    if prefetch_ids:
        schedule_lx_prefetch(prefetch_ids)
    return tracks, total


# ------------------------------------------------------------------ 导入歌单 ---
# 洛雪 JSON / 酷狗·酷我·QQ·网易云·咪咕 分享链接导入的本地歌单（一单一文件）。
# 列表阶段零上游请求：曲目只做确定性直连映射（音源 id 已知），封面用导入时
# 存的直链，点播时才真正取链，避免一开歌单就把上游按天配额吃光。

_IMP_PLAYLIST_COVERS: dict[str, str] = {}
_IMP_SONG_COVERS: dict[str, str] = {}
_IMP_SONG_COVER_CAP = 4000


def _imp_cover_register(guid: str, cover: str) -> None:
    """登记「曲目 guid → 导入时保存的封面直链」，供 /static/cover 零请求命中。"""
    if not guid or not cover:
        return
    if len(_IMP_SONG_COVERS) >= _IMP_SONG_COVER_CAP:
        _IMP_SONG_COVERS.clear()
    _IMP_SONG_COVERS[guid] = cover


def _imp_card_from_summary(summary: dict) -> dict:
    guid = str(summary.get("guid") or "")
    pid = str(summary.get("id") or "")
    if pid:
        _IMP_PLAYLIST_COVERS[pid] = str(summary.get("cover") or "")
    return {
        "guid": guid,
        "name": str(summary.get("name") or "导入歌单"),
        "coverId": "track_" + fake_official_guid(guid),
        "createdAt": int(summary.get("created_at") or 0) or int(time.time()),
        "updatedAt": int(summary.get("updated_at") or 0) or int(time.time()),
        "trackCount": int(summary.get("count") or 0),
        # 同热门/每日推荐/网易/Ceru：客户端只对 isDaily 的 online: 歌单拉曲目列表
        "isDaily": True,
    }


def _imp_cards() -> list[dict]:
    cards: list[dict] = []
    for summary in plimp.list_records():
        try:
            cards.append(_imp_card_from_summary(summary))
        except Exception as e:  # noqa: BLE001
            logger.debug("playlist import card failed: %s", e)
    return cards


def _imp_playlist_card(pid: str) -> dict | None:
    record = plimp.read_record(pid)
    if not record:
        return None
    return _imp_card_from_summary(plimp.summary_of(record))


def _imp_light_track(song: dict) -> dict | None:
    """导入歌曲 → 可播 VO（不搜索、不取链）。"""
    if not isinstance(song, dict):
        return None
    try:
        track = build_online_track(plimp.song_item(song))
    except Exception as e:  # noqa: BLE001
        logger.debug("playlist import light track failed: %s", e)
        return None
    guid = str(track.get("guid") or "")
    if not guid:
        return None
    cover = str(song.get("cover") or "").strip()
    if cover:
        track["cover_url"] = cover
        track["coverUrl"] = cover
        track["coverURL"] = cover
    shared_cover_id = f"{guid}:cover5"
    track["coverId"] = shared_cover_id
    album_obj = track.get("album")
    if isinstance(album_obj, dict):
        album_obj["coverId"] = shared_cover_id
    _imp_cover_register(guid, cover)
    return track


def _imp_playlist_tracks_page(pid: str, start: int, size: int) -> tuple[list[dict], int]:
    songs, total = plimp.tracks_page(pid, start, size)
    tracks: list[dict] = []
    for song in songs:
        try:
            track = _imp_light_track(song)
        except Exception:  # noqa: BLE001
            track = None
        if isinstance(track, dict) and track.get("guid"):
            tracks.append(track)
    # 只预热页首 1 首（同 Ceru）：一次额度换首播接近秒开，其余等点播
    prefetch_ids = [
        song_id_from_online_guid(str(t.get("guid") or ""))
        for t in tracks[:1]
        if str(t.get("guid") or "").startswith("online:lx:")
    ]
    if prefetch_ids:
        schedule_lx_prefetch(prefetch_ids)
    return tracks, total


async def _ceru_favorites_track_keys() -> set[str]:
    """favorites 语义歌单（「我的喜欢」）全部歌曲的 trackKey 集合（走歌单缓存）。"""
    index = await _ceru_favorites_index()
    return set(index["keys"])


# 「我的喜欢」存的是收藏当时那个平台的主键（tx/kw/wy/kg/mg + 平台内 id）。
# 同一首歌换个平台播主键必然不一样，而 guid→歌曲注册表是内存态（服务重启就空），
# 所以标星判断不能只查注册表：guid 主键推导 + 歌名歌手身份兜底都得有。
_CERU_SOURCE_ALIASES = {
    "netease": "wy", "musicbox": "wy", "kugou": "kg", "kuwo": "kw",
    "qq": "tx", "tencent": "tx", "yqq": "tx", "migu": "mg",
}
_CERU_FAV_INDEX_TTL_S = 30.0
# {"ts": 单调时钟, "keys": trackKey 集合, "by_key": trackKey→歌曲, "idents": 身份→歌曲列表}
_CERU_FAV_INDEX: dict = {"ts": 0.0, "keys": set(), "by_key": {}, "idents": {}}
_FAV_FT_SPLIT = re.compile(r"[/、,&;；+]|\bfeat(?:uring)?\.?|\bft\.?", re.IGNORECASE)


def _fav_norm_text(text: Any) -> str:
    """歌名/歌手归一化：去掉空白与标点符号，只留文字和数字并转小写。

    跨平台的写法差异（空格、全半角括号、点号）不改变歌曲身份；而翻唱/伴奏/Live
    这类版本后缀是文字，会保留下来，因此不会被并成同一首。"""
    out = []
    for ch in str(text or ""):
        if ch.isspace() or unicodedata.category(ch)[0] in ("P", "S", "Z", "C"):
            continue
        out.append(ch.lower())
    return "".join(out)


def _fav_norm_artist(text: Any) -> str:
    """歌手按集合比较：「A/B」与「B/A」、feat. 前后缀顺序差异算同一组。"""
    names = {_fav_norm_text(part) for part in _FAV_FT_SPLIT.split(str(text or ""))}
    return "|".join(sorted(name for name in names if name))


def _fav_identity(title: Any, artist: Any) -> str:
    song_name = _fav_norm_text(title)
    artist_key = _fav_norm_artist(artist)
    return f"{song_name}\u0001{artist_key}" if song_name and artist_key else ""


def _fav_duration_s(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _fav_same_song(fav_dur: float, dur: float) -> bool:
    """时长闸门：两边都认得出时长时要差在 5s 内；有一边认不出则不因此否决。"""
    return not fav_dur or not dur or abs(fav_dur - dur) <= 5.0


def _identity_fields(item: Any) -> tuple[str, str, float]:
    """任意一种曲目形态（内部 item / 客户端 track / Ceru 歌曲）都能取到身份三要素。"""
    if not isinstance(item, dict):
        return "", "", 0.0
    title = str(item.get("title") or item.get("name") or "")
    artist = str(item.get("artist") or item.get("singer") or "")
    if not artist and isinstance(item.get("artists"), list) and item["artists"]:
        first = item["artists"][0]
        if isinstance(first, dict):
            artist = str(first.get("name") or "")
    dur = _fav_duration_s(item.get("duration_s"))
    if dur <= 0:
        ms = _fav_duration_s(
            item.get("duration_ms") or item.get("durationMs") or item.get("duration")
        )
        dur = ms / 1000.0 if ms > 1000 else ms
    if dur <= 0:
        dur = ceru.song_duration_s(item)
    return title, artist, dur


def _merge_identity_fields(*items: Any) -> tuple[str, str, float]:
    """按优先级合并身份字段：第一个非空值胜出。"""
    fields = [_identity_fields(item) for item in items]
    title = next((t for t, _a, _d in fields if t), "")
    artist = next((a for _t, a, _d in fields if a), "")
    duration_s = next((d for _t, _a, d in fields if d), 0.0)
    return title, artist, duration_s


def _guid_track_keys(guid: str) -> list[str]:
    """在线 guid → 「我的喜欢」trackKey（{source}:{id}）候选，不依赖内存注册表。

    online:lx:tx:HASH → tx:HASH（收藏里存的就是它）；同时给出原样拼接，兼容
    source 本身就是 lx 的同步曲。netease/kugou 这类源名先翻译成洛雪平台码。"""
    parts = [p for p in str(guid or "").split(":") if p]
    if len(parts) < 3 or parts[0] != "online":
        return []
    # 只有源名大小写无关；平台内主键（QQ 的 000Opyfj4bDwYX、洛雪的 hash）大小写敏感
    head = parts[1].lower()
    seg = [_CERU_SOURCE_ALIASES.get(head, head)] + list(parts[2:])
    keys = []
    if seg[0] == "lx" and len(seg) >= 3:
        keys.append(f"{seg[1]}:{':'.join(seg[2:])}")
    keys.append(":".join(seg))
    return [k for k in dict.fromkeys(keys) if k.split(":", 1)[-1].strip()]


def _ceru_song_from_guid(guid: str, info: dict | None) -> dict | None:
    """Online guid + track metadata -> Ceru song shape (synthesize when the song is not yet in the sync server's playlists).

    After a synthesized record is written to the server, other devices will reverse-lookup
    to a playable Luoxue item via the same primary key (lx_item_for_song), which is consistent
    with the playback path for synced tracks. Without a song title, the entry has no meaning, so return None."""
    keys = _guid_track_keys(guid)
    if not keys:
        return None
    platform, ident = keys[0].split(":", 1)
    if platform == "lx" or not ident:
        return None
    data = info if isinstance(info, dict) else {}
    title = str(data.get("title") or "").strip()
    if not title:
        return None
    artist = str(data.get("artist") or "").strip() or "Unknown Artist"
    album = str(data.get("album") or data.get("albumName") or "").strip()
    dur = _fav_duration_s(data.get("duration_s"))
    if dur <= 0:
        ms = _fav_duration_s(data.get("duration") or data.get("durationMs"))
        dur = ms / 1000.0 if ms > 1000 else ms
    interval = f"{int(dur) // 60:02d}:{int(dur) % 60:02d}" if dur > 0 else "00:00"
    cover = str(data.get("cover_url") or data.get("cover") or "").strip()
    song = {
        "id": ident,
        "songmid": ident,
        "source": platform,
        "title": title,
        "name": title,
        "artist": artist,
        "singer": artist,
        "album": album,
        "albumName": album,
        "interval": interval,
        "durationText": interval,
        "img": cover,
        "artworkUrl": cover,
    }
    if platform == "kg":
        song["hash"] = ident
    return song


async def _ceru_sync_song(request: Request, guid: str) -> dict | None:
    """Song to write into the sync server: prefer a registry hit; synthesize on a miss and register it."""
    song = ceru.song_for_guid(guid)
    if song is None:
        song = _ceru_song_from_guid(guid, await _best_effort_online_info(request, guid))
        if song is not None:
            ceru.register_guid(guid, song)
    return song


async def _ceru_favorites_index() -> dict:
    """favorites 歌单索引：trackKey 集合 / trackKey→歌曲 / 身份→歌曲列表。

    索引本身 30s 记忆；底下 list_playlist_songs 另有 300s 缓存并在标星写入后失效，
    上千首歌不必每首重算一遍。"""
    now = time.monotonic()
    if now - float(_CERU_FAV_INDEX.get("ts") or 0.0) < _CERU_FAV_INDEX_TTL_S:
        return _CERU_FAV_INDEX
    keys: set[str] = set()
    by_key: dict[str, dict] = {}
    idents: dict[str, list[dict]] = {}
    for song in await _ceru_favorite_songs():
        if not isinstance(song, dict):
            continue
        key = ceru.song_track_key(song)
        if key:
            keys.add(key)
            by_key.setdefault(key, song)
        ident = _fav_identity(ceru.song_title(song), ceru.song_artist(song))
        if ident:
            idents.setdefault(ident, []).append(song)
    # 空索引不记忆：同步服务偶发拉不到时，不能让红心集体消失 30s（底下的
    # list_playlist_songs 自带 300s 缓存，不命中的代价只是一次内存读）
    if keys or idents:
        _CERU_FAV_INDEX.update({"ts": now, "keys": keys, "by_key": by_key,
                               "idents": idents})
    else:
        _CERU_FAV_INDEX.update({"ts": 0.0, "keys": keys, "by_key": by_key,
                               "idents": idents})
    return _CERU_FAV_INDEX


def _forget_ceru_favorites_index() -> None:
    """标星写入后作废索引，本机立即看到最新状态。"""
    _CERU_FAV_INDEX.update({"ts": 0.0, "keys": set(), "by_key": {}, "idents": {}})


async def _ceru_favorite_hit(candidates: list[str], title: str, artist: str,
                             duration_s: float) -> bool:
    """这首歌是否已在「我的喜欢」里：先按主键，再按歌名+歌手（跨平台主键不同）。"""
    index = await _ceru_favorites_index()
    if any(key and key in index["keys"] for key in candidates):
        return True
    for song in index["idents"].get(_fav_identity(title, artist), []):
        if _fav_same_song(ceru.song_duration_s(song), duration_s):
            return True
    return False


async def _ceru_favorite_match(guid: str, info: Any = None) -> "tuple[dict | None, str]":
    """注册表里没有这首歌时（换源播放、搜到的另一平台版本、服务重启），按 guid
    主键与歌名+歌手在「我的喜欢」里找回对应歌曲，返回 (歌曲, trackKey)。"""
    index = await _ceru_favorites_index()
    for key in _guid_track_keys(guid):
        song = index["by_key"].get(key)
        if song is not None:
            return song, key
    title, artist, duration_s = _merge_identity_fields(info)
    for song in index["idents"].get(_fav_identity(title, artist), []):
        if _fav_same_song(ceru.song_duration_s(song), duration_s):
            return song, ceru.song_track_key(song)
    return None, ""


async def _stamp_ceru_favorite_flags(tracks: list[dict], user_guid: str) -> None:
    """补齐 ceru 歌单曲目的 isFavorite：飞牛前端按曲目该字段决定是否显示标星。

    官方歌单视图同口径（本地收藏集合成员判断）；这里再并入「我的喜欢」歌单的
    全部歌曲（收藏 tab 注入的就是它），两边观感一致。打开「我的喜欢」本身时
    每首曲目都命中，整单默认标星。
    判断不依赖 guid→歌曲注册表（内存态，服务重启即空）：先按 guid 推导主键，
    再按歌名+歌手在「我的喜欢」里找，所以换源播放 / 搜到的另一平台版本也能标星。"""
    try:
        fav_guids = {str(it.get("guid") or "") for it in load_online_favorites(user_guid)}
    except Exception:
        fav_guids = set()
    ceru_on = _ceru_on()
    for track in tracks:
        if not isinstance(track, dict):
            continue
        guid = str(track.get("guid") or "")
        if not guid:
            continue
        if guid in fav_guids:
            track["isFavorite"] = True
            continue
        if not ceru_on:
            continue
        try:
            title, artist, duration_s = _identity_fields(track)
            if not await _ceru_favorite_hit(_guid_track_keys(guid), title, artist, duration_s):
                # 行上的字段不够时（歌单轻量映射只有部分元数据）再查一次注册表
                song = ceru.song_for_guid(guid)
                if song is None:
                    continue
                key = ceru.song_track_key(song)
                alt_title, alt_artist, alt_dur = _merge_identity_fields(song, track)
                if not await _ceru_favorite_hit(
                        _guid_track_keys(guid) + ([key] if key else []),
                        alt_title, alt_artist, alt_dur):
                    continue
            track["isFavorite"] = True
        except Exception:
            continue


def _apply_ceru_metadata_fields(guid: str, song: dict, payload: dict) -> None:
    """播放页元数据与列表行用同一封面契约（同 coverId、同原封面）。

    build_metadata_payload 用裸 guid 做 coverId，列表行用 {guid}:cover5：播放页收到
    新标识会丢弃已渲染封面重新拉取，造成「秒出来→消失→再出来」的闪动；标识与
    URL 保持一致客户端才能直接复用。"""
    data = payload.get("data") if isinstance(payload.get("data"), dict) else None
    if data is None:
        return
    artwork = ceru.song_artwork(song)
    cover_id = f"{guid}:cover5"
    targets = [data]
    for key in ("track", "album"):
        if isinstance(data.get(key), dict):
            targets.append(data[key])
    for obj in targets:
        obj["coverId"] = cover_id
        if artwork:
            obj["cover_url"] = artwork
            obj["coverUrl"] = artwork
            obj["coverURL"] = artwork


async def _track_is_favorite(request: Request, guid: str, info: Any = None) -> bool:
    """曲目是否标星：本地收藏 ∪「我的喜欢」（favorites 语义歌单）全部歌曲。

    收藏 tab 与歌单视图都是这个口径；播放页元数据若只查本地收藏，会把列表里已
    标星的同步曲判为未收藏，观感是「亮一下红色再取消掉」。
    不只看 guid 字符串：「我的喜欢」存的是收藏当时那个平台的主键，同一首歌换
    平台播（换源兜底、搜索结果里的另一平台版本）主键就变了，于是能播却不显红心。
    现在先按 guid 推导主键，再按歌名+歌手（+时长闸门）认同一首歌。"""
    if guid in await _online_favorite_set(request):
        return True
    if not _ceru_on():
        return False
    # 先用元数据里的身份字段试一次：命中就不必去查注册表（miss 会读盘）
    title, artist, duration_s = _identity_fields(info)
    if await _ceru_favorite_hit(_guid_track_keys(guid), title, artist, duration_s):
        return True
    song = ceru.song_for_guid(guid)
    if song is None:
        return False
    key = ceru.song_track_key(song)
    alt_title, alt_artist, alt_dur = _merge_identity_fields(song, info)
    return await _ceru_favorite_hit(
        _guid_track_keys(guid) + ([key] if key else []), alt_title, alt_artist, alt_dur)


async def _ceru_favorite_songs() -> list[dict]:
    """“我的喜欢”语义歌单的完整歌曲（用于红心列表，含歌手/专辑/时长）。"""
    try:
        playlists = await ceru.list_playlists()
    except Exception:
        return []
    for pl in playlists:
        title = str(pl.get("title") or pl.get("name") or "")
        if pl.get("semanticType") == "favorites" or title in ("我的喜欢", "我的收藏"):
            try:
                return await ceru.list_playlist_songs(str(pl.get("id") or ""))
            except Exception:
                return []
    return []


@app.get("/music/api/v1/playlist/list")
@app.get("/music/api/v1/playlist/list/{subpath:path}")
# TOP[top.playlist.view] 歌单列表/曲目视图：官方歌单 + ceru/导入/推荐/网易注入
async def playlist_list(request: Request):
    upstream_client = get_upstream_client(request.app)
    envelope = await fetch_upstream_envelope(request, upstream_client)
    if isinstance(envelope, Response):
        return envelope
    headers = envelope.pop("_ext_headers", {})
    if envelope.get("code") != 0:
        return JSONResponse(content=envelope, headers=headers)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed:
        return auth_resp or JSONResponse(content=envelope, headers=headers)

    kinds = _recommend_injectable_kinds(user_guid)
    # 网易账号歌单：实例级内容（同热门推荐），shared 会话同样可见
    nm_on = bool(CONF.get("netease_my_playlists")) and bool(CONF.get("netease_enabled"))
    ceru_on = _ceru_on()
    try:
        imp_summaries = plimp.list_records()
    except Exception as e:  # noqa: BLE001
        logger.warning("playlist import list failed: %s", e)
        imp_summaries = []
    logger.info("cerusync playlist_list debug: authed=%s user=%s kinds=%s nm=%s ceru=%s imp=%s",
                is_authed, user_guid, kinds, nm_on, ceru_on, len(imp_summaries))
    if not kinds and not nm_on and not ceru_on and not imp_summaries:
        # 注入源全部关闭/无内容（或 shared 会话无任何可注入类型）：不注入
        return JSONResponse(content=envelope, headers=headers)

    data = envelope.get("data")
    if not isinstance(data, dict):
        data = {"list": [], "total": 0}
        envelope["data"] = data
    official = data.get("list")
    if not isinstance(official, list):
        official = []
        data["list"] = official
    recs: list[dict] = []
    for kind in kinds:
        try:
            bundle = await _peek_daily_bundle(request, user_guid, kind)
        except Exception as e:
            logger.warning("%s recommend list inject failed: %s", kind, e)
            continue
        tracks = bundle.get("tracks") or []
        if kind == "hot" and not tracks:
            # 热门歌单构建失败/无可用榜单时不挂空壳（每日推荐保留占位等待后台生成）
            continue
        rec = _playlist_public_fields(bundle.get("playlist") or {}, tracks)
        rec["trackCount"] = len(tracks)
        recs.append(rec)
    nm_cards: list[dict] = []
    if nm_on:
        try:
            nm_cards = await nmpl.peek_summaries(get_musicbox_client(request.app))
            for card in nm_cards:
                g = str(card.get("guid") or "")
                if is_online_guid(g):
                    # 登记 guid/封面假 id 反查（coverId 与 guid 同源 md5）
                    fake_official_guid(g)
        except Exception as e:
            logger.warning("netease my-playlists inject failed: %s", e)
            nm_cards = []
        if nm_cards:
            nmpl.schedule_prefetch(get_musicbox_client(request.app), build_online_track, nm_cards)
    ceru_cards: list[dict] = []
    if ceru_on:
        try:
            ceru_cards = await _ceru_cards(request)
            for card in ceru_cards:
                g = str(card.get("guid") or "")
                if is_online_guid(g):
                    fake_official_guid(g)
        except Exception as e:
            logger.warning("cerusync playlist list inject failed: %s", e)
            ceru_cards = []
    imp_cards: list[dict] = []
    for summary in imp_summaries:
        try:
            imp_cards.append(_imp_card_from_summary(summary))
        except Exception as e:  # noqa: BLE001
            logger.debug("playlist import card inject failed: %s", e)
    official = [
        it for it in official
        if not (isinstance(it, dict) and dailyrec.is_recommend_playlist_guid(str(it.get("guid") or "")))
    ]
    data["list"] = recs + nm_cards + ceru_cards + imp_cards + official
    total = data.get("total")
    data["total"] = (
        (total if isinstance(total, int) else len(official))
        + len(recs) + len(nm_cards) + len(ceru_cards) + len(imp_cards)
    )
    return JSONResponse(content=envelope, headers=headers)


@app.get("/music/api/v1/playlist/detail")
async def playlist_detail(request: Request):
    guid = str(request.query_params.get("guid") or "").strip()
    ceru_pid = _ceru_playlist_id_from_guid(guid) or _ceru_playlist_id_from_guid(resolve_real_guid(guid))
    logger.info("cerusync detail debug: raw_guid=%s ceru_pid=%s on=%s", guid, ceru_pid, _ceru_on())
    if ceru_pid and _ceru_on():
        logger.info("cerusync playlist_detail guid=%s pid=%s", guid, ceru_pid)
        card = await _ceru_playlist_card(ceru_pid)
        if card is None:
            return JSONResponse(content={"code": -1, "msg": "playlist not found", "data": None})
        card["trackCount"] = await _ceru_song_count(ceru_pid)
        return JSONResponse(content={"code": 0, "msg": "ok", "data": card})
    imp_pid = plimp.imp_playlist_id_from_guid(guid) or plimp.imp_playlist_id_from_guid(resolve_real_guid(guid))
    if imp_pid:
        card = _imp_playlist_card(imp_pid)
        if card is None:
            return JSONResponse(content={"code": -1, "msg": "playlist not found", "data": None})
        return JSONResponse(content={"code": 0, "msg": "ok", "data": card})
    nm_pl_id = nmpl.nm_playlist_id_from_guid(guid) or nmpl.nm_playlist_id_from_guid(resolve_real_guid(guid))
    if nm_pl_id:
        # 网易账号歌单：只读虚拟歌单，卡片从摘要（内存→磁盘）取；trackCount 用
        # 已缓存曲目数更准（可播过滤后可能少于网易侧计数）
        card = nmpl.card_for(nm_pl_id)
        if card is None:
            return JSONResponse(content={"code": -1, "msg": "playlist not found", "data": None})
        tracks = nmpl.cached_tracks(nm_pl_id)
        if tracks is not None:
            card = {**card, "trackCount": len(tracks)}
        return JSONResponse(content={"code": 0, "msg": "ok", "data": card})
    kind = dailyrec.online_playlist_kind(guid)
    if not kind:
        # 官方歌单：本地存在在线附加条目才拦截修正 trackCount，否则纯透传
        if not plt_dir_has_data():
            return await forward_to_upstream(request, get_upstream_client(request.app))
        upstream_client = get_upstream_client(request.app)
        envelope = await fetch_upstream_envelope(request, upstream_client)
        if isinstance(envelope, Response):
            return envelope
        headers = envelope.pop("_ext_headers", {})
        if envelope.get("code") != 0:
            return JSONResponse(content=envelope, headers=headers)
        # 官方明细已成功，此时探测被拒不能回传鉴权错误，降级为官方原样（同收藏列表）
        is_authed, user_guid, _ = await _probe_upstream_auth(request, upstream_client)
        if not is_authed:
            return JSONResponse(content=envelope, headers=headers)
        extras = load_playlist_tracks(user_guid).get(resolve_real_guid(guid)) or []
        if extras:
            data = envelope.get("data")
            if isinstance(data, dict):
                tc = data.get("trackCount")
                data["trackCount"] = (tc if isinstance(tc, int) else 0) + len(extras)
        return JSONResponse(content=envelope, headers=headers)

    upstream_client = get_upstream_client(request.app)
    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp
    bundle = await _load_daily_bundle(request, user_guid, kind)
    rec = _playlist_public_fields(bundle.get("playlist") or {}, bundle.get("tracks") or [])
    rec["trackCount"] = len(bundle.get("tracks") or [])
    return JSONResponse(content={"code": 0, "msg": "ok", "data": rec})


@app.get("/music/api/v1/playlist/batch-detail")
async def playlist_batch_detail(request: Request):
    raw = request.query_params.get("guids") or request.query_params.get("guid") or ""
    guids = [g.strip() for g in raw.split(",") if g.strip()]
    recommend_ids = [g for g in guids if dailyrec.is_recommend_playlist_guid(g)]
    ceru_ids = [
        pid for pid in (
            _ceru_playlist_id_from_guid(g) or _ceru_playlist_id_from_guid(resolve_real_guid(g))
            for g in guids
        ) if pid
    ]
    nm_ids = [
        pid for pid in (
            nmpl.nm_playlist_id_from_guid(g) or nmpl.nm_playlist_id_from_guid(resolve_real_guid(g))
            for g in guids
        ) if pid
    ]
    imp_ids = [
        pid for pid in (
            plimp.imp_playlist_id_from_guid(g) or plimp.imp_playlist_id_from_guid(resolve_real_guid(g))
            for g in guids
        ) if pid
    ]
    if not recommend_ids and not nm_ids and not ceru_ids and not imp_ids and not plt_dir_has_data():
        return await forward_to_upstream(request, get_upstream_client(request.app))

    upstream_client = get_upstream_client(request.app)
    rest = [
        g for g in guids
        if not dailyrec.is_recommend_playlist_guid(g)
        and not nmpl.is_nm_playlist_guid(resolve_real_guid(g))
        and not _is_ceru_playlist_guid(resolve_real_guid(g))
        and not plimp.is_imp_playlist_guid(resolve_real_guid(g))
    ]
    official_list: list = []
    if rest:
        headers = copy_incoming_headers(request)
        req = upstream_client.build_request(
            "GET",
            f"/music/api/v1/playlist/batch-detail?guids={quote(','.join(rest), safe=',')}",
            headers=headers,
        )
        resp = await upstream_client.send(req)
        if resp.status_code == 200:
            try:
                payload = resp.json()
                if isinstance(payload, dict) and payload.get("code") == 0:
                    data = payload.get("data") or {}
                    if isinstance(data, dict) and isinstance(data.get("list"), list):
                        official_list = data["list"]
                    elif isinstance(data, list):
                        official_list = data
            except Exception:
                official_list = []

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp
    # 官方歌单 trackCount 并上本地在线附加条目
    if official_list:
        counts = playlist_track_counts(user_guid)
        for it in official_list:
            if isinstance(it, dict):
                extra = counts.get(str(it.get("guid") or ""))
                if extra:
                    tc = it.get("trackCount")
                    it["trackCount"] = (tc if isinstance(tc, int) else 0) + extra
    recs: list[dict] = []
    for g in recommend_ids:
        kind = dailyrec.online_playlist_kind(g) or "daily"
        bundle = await _load_daily_bundle(request, user_guid, kind)
        rec = _playlist_public_fields(bundle.get("playlist") or {}, bundle.get("tracks") or [])
        rec["trackCount"] = len(bundle.get("tracks") or [])
        recs.append(rec)
    for pid in nm_ids:
        card = nmpl.card_for(pid)
        if card is not None:
            tracks = nmpl.cached_tracks(pid)
            if tracks is not None:
                card = {**card, "trackCount": len(tracks)}
            recs.append(card)
    for pid in ceru_ids:
        card = await _ceru_playlist_card(pid)
        if card is not None:
            card["trackCount"] = await _ceru_song_count(pid)
            recs.append(card)
    for pid in imp_ids:
        card = _imp_playlist_card(pid)
        if card is not None:
            recs.append(card)
    return JSONResponse(content={"code": 0, "msg": "ok", "data": {"list": recs + official_list}})


def _preheat_playlist_page_covers(request: Request, page_tracks: list) -> None:
    """2.6.36 列表级封面回补：当页前 20 首里缺可用封面的曲目，后台走首播预热链。

    「我的喜欢」这类歌单空封面/内网地址曲目多（实测 1388 首里 169 首空封面），
    不等用户点播、按页后台补齐（每页最多 5 首、并发 3），客户端刷新即见真封面。
    """
    targets = []
    for t in (page_tracks or [])[:20]:
        if not isinstance(t, dict):
            continue
        t_guid = str(t.get("guid") or "")
        if not is_online_guid(t_guid) or t_guid in _PREHEAT_INFLIGHT:
            continue
        if _read_cover_cache(t_guid):
            continue
        cover = str(t.get("cover_url") or t.get("coverUrl") or "")
        if cover and not _cover_url_unusable(cover):
            continue
        targets.append(t_guid)
        if len(targets) >= 3:  # 2.6.37: 每页最多 3 首，页内预热不压主路径
            break
    if not targets:
        return

    async def _run() -> None:
        sem = asyncio.Semaphore(3)

        async def _one(g: str) -> None:
            async with sem:
                try:
                    _preheat_track_assets(request, g)
                except Exception:  # noqa: BLE001
                    pass

        await asyncio.gather(*(_one(g) for g in targets))

    try:
        asyncio.create_task(_run())
    except RuntimeError:
        pass


@app.get("/music/api/v1/track/playlist-detail/list")
async def playlist_track_list(request: Request):
    guid = str(
        request.query_params.get("playlistGUID")
        or request.query_params.get("playlistGuid")
        or request.query_params.get("guid")
        or ""
    ).strip()
    kind = dailyrec.online_playlist_kind(guid)
    ceru_pid = _ceru_playlist_id_from_guid(guid) or _ceru_playlist_id_from_guid(resolve_real_guid(guid))
    logger.info("cerusync track_list debug: raw_guid=%s ceru_pid=%s on=%s", guid, ceru_pid, _ceru_on())
    if ceru_pid and _ceru_on():
        logger.info("cerusync playlist_track_list guid=%s pid=%s", guid, ceru_pid)
        logger.info("cerusync track_list raw_query=%s", request.url.query)
        upstream_client = get_upstream_client(request.app)
        is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
        if not is_authed and auth_resp is not None:
            return auth_resp
        try:
            page = max(int(request.query_params.get("page") or 1), 1)
        except (TypeError, ValueError):
            page = 1
        try:
            size = int(request.query_params.get("size") or 50)
        except (TypeError, ValueError):
            size = 50
        if size == -1:
            # “播放全部/返回全部”：仍走分页轻量映射，上限 500 首，避免整单
            # 搜索把洛雪源限流打爆导致点播放没声。
            size = 500
        if size < 1:
            size = 50
        if size > 500:
            size = 500
        start = (page - 1) * size
        logger.info("cerusync track_list parsed page=%s size=%s start=%s", page, size, start)
        page_tracks, total = await _ceru_playlist_tracks_page(ceru_pid, start, size)
        page_tracks = dailyrec.stamp_playlist_tracks(page_tracks)
        await _stamp_ceru_favorite_flags(page_tracks, user_guid)
        _preheat_playlist_page_covers(request, page_tracks)
        logger.info("cerusync track_list loaded: pid=%s total=%d page=%d", ceru_pid, total, len(page_tracks))
        return JSONResponse(
            content=disguise_client_json({
                "code": 0,
                "msg": "ok",
                "data": {"list": page_tracks, "total": total, "sort": request.query_params.get("sort") or ""},
            })
        )
    nm_pl_id = nmpl.nm_playlist_id_from_guid(guid) or nmpl.nm_playlist_id_from_guid(resolve_real_guid(guid))
    if nm_pl_id:
        # 网易账号歌单曲目：可播过滤后的 VO 分页下发（与推荐歌单同款分页语义）
        upstream_client = get_upstream_client(request.app)
        is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
        if not is_authed and auth_resp is not None:
            return auth_resp
        tracks = dailyrec.stamp_playlist_tracks(
            await nmpl.load_tracks(get_musicbox_client(request.app), nm_pl_id, build_online_track)
        )
        try:
            page = max(int(request.query_params.get("page") or 1), 1)
        except (TypeError, ValueError):
            page = 1
        try:
            size = int(request.query_params.get("size") or 50)
        except (TypeError, ValueError):
            size = 50
        if size == -1:
            size = max(len(tracks), 1)
        if size < 1:
            size = 50
        start = (page - 1) * size
        page_tracks = tracks[start:start + size]
        payload = {
            "code": 0,
            "msg": "ok",
            "data": {"list": page_tracks, "total": len(tracks), "sort": request.query_params.get("sort") or ""},
        }
        _remember_list_order(payload)
        _warm_page_head(page_tracks)
        return JSONResponse(content=disguise_client_json(payload))
    imp_pid = plimp.imp_playlist_id_from_guid(guid) or plimp.imp_playlist_id_from_guid(resolve_real_guid(guid))
    if imp_pid:
        # 导入歌单曲目：本地 JSON 分页下发，列表阶段零上游请求
        upstream_client = get_upstream_client(request.app)
        is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
        if not is_authed and auth_resp is not None:
            return auth_resp
        try:
            page = max(int(request.query_params.get("page") or 1), 1)
        except (TypeError, ValueError):
            page = 1
        try:
            size = int(request.query_params.get("size") or 50)
        except (TypeError, ValueError):
            size = 50
        if size == -1:
            # 「播放全部」：上限 500，与 Ceru 同口径，避免整单构建拖慢首屏
            size = 500
        if size < 1:
            size = 50
        if size > 500:
            size = 500
        start = (page - 1) * size
        page_tracks, total = _imp_playlist_tracks_page(imp_pid, start, size)
        page_tracks = dailyrec.stamp_playlist_tracks(page_tracks)
        _preheat_playlist_page_covers(request, page_tracks)
        return JSONResponse(
            content=disguise_client_json({
                "code": 0,
                "msg": "ok",
                "data": {"list": page_tracks, "total": total, "sort": request.query_params.get("sort") or ""},
            })
        )
    if not kind:
        # 官方歌单：本地存在在线附加条目才拦截合并，否则纯透传
        if not plt_dir_has_data():
            return await forward_to_upstream(request, get_upstream_client(request.app))
        upstream_client = get_upstream_client(request.app)
        envelope = await fetch_upstream_envelope(request, upstream_client)
        if isinstance(envelope, Response):
            return envelope
        headers = envelope.pop("_ext_headers", {})
        if envelope.get("code") != 0:
            return JSONResponse(content=envelope, headers=headers)
        # 官方列表已成功，此时探测被拒不能回传鉴权错误，降级为官方原样（同收藏列表）
        is_authed, user_guid, _ = await _probe_upstream_auth(request, upstream_client)
        if not is_authed:
            return JSONResponse(content=envelope, headers=headers)
        resolved_pl_guid = resolve_real_guid(guid)
        extras = load_playlist_tracks(user_guid).get(resolved_pl_guid) or []
        if not extras:
            return JSONResponse(content=envelope, headers=headers)
        # 自愈：bind=pending 的歌单条目在此补发官方绑定/下载
        _maybe_retry_official_binds(request, user_guid, plt_map={resolved_pl_guid: extras})

        data = envelope.get("data")
        if not isinstance(data, dict):
            data = {"list": [], "total": 0}
            envelope["data"] = data
        official_list = data.get("list") if isinstance(data.get("list"), list) else []
        official_total = data.get("total") if isinstance(data.get("total"), int) else len(official_list)

        # 在线附加条目接在官方条目之后（addedAt 倒序），逻辑区间
        # [official_total, official_total+len)；按请求页窗口切片，翻页不重不漏
        extras_sorted = sorted(extras, key=lambda x: x.get("addedAt", 0), reverse=True)
        fav_guids = {str(it.get("guid") or "") for it in load_online_favorites(user_guid)}
        template = official_track_template(official_list)
        online_objs = []
        for it in extras_sorted:
            g = str(it.get("guid") or "")
            if not g:
                continue
            snapshot = it.get("track") if isinstance(it.get("track"), dict) else None
            obj = build_favorite_track_obj(
                g, _snapshot_to_info(snapshot) if snapshot else None,
                created_at=it.get("addedAt"), template=template,
            )
            obj["isFavorite"] = g in fav_guids
            online_objs.append(obj)

        try:
            page = max(int(request.query_params.get("page") or 1), 1)
        except (TypeError, ValueError):
            page = 1
        try:
            size = int(request.query_params.get("size") or 50)
        except (TypeError, ValueError):
            size = 50
        if size == -1:
            online_page = online_objs
        else:
            if size < 1:
                size = 50
            start = (page - 1) * size
            lo = max(0, start - official_total)
            hi = max(0, start + size - official_total)
            online_page = online_objs[lo:hi]

        data["list"] = official_list + online_page
        data["total"] = official_total + len(online_objs)
        _remember_list_order(envelope)
        _warm_page_head(official_list + online_page)
        return JSONResponse(content=disguise_client_json(envelope), headers=headers)


    upstream_client = get_upstream_client(request.app)
    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp
    bundle = await _load_daily_bundle(request, user_guid, kind)
    tracks = dailyrec.stamp_playlist_tracks(list(bundle.get("tracks") or []))
    try:
        page = max(int(request.query_params.get("page") or 1), 1)
    except (TypeError, ValueError):
        page = 1
    try:
        size = int(request.query_params.get("size") or 50)
    except (TypeError, ValueError):
        size = 50
    if size == -1:
        # -1 是"返回全部"的约定值，须在 <1 兜底之前归一，否则永远到不了全量分支
        size = max(len(tracks), 1)
    if size < 1:
        size = 50
    start = (page - 1) * size
    page_tracks = tracks[start:start + size]
    payload = {
        "code": 0,
        "msg": "ok",
        "data": {"list": page_tracks, "total": len(tracks), "sort": request.query_params.get("sort") or ""},
    }
    _remember_list_order(payload)
    _warm_page_head(page_tracks)
    return JSONResponse(content=disguise_client_json(payload))


# === album detail（在线曲目伪装专辑详情，issue #22）===
# 曲目 VO 的 album.guid 形如 "online:<src>:<id>:album"，disguise 后客户端拿到
# 官方 32-hex 形态；点击专辑时客户端请求官方专辑详情端点，原样转发官方必返回
# "无此专辑"。此处拦截分源适配：netease 经 musicbox 调真实专辑接口；musicdl/lx
# 从保留的搜索缓存/收藏/历史聚合同专辑名曲目合成；无法合成返回业务错误，
# 绝不透传官方"无此专辑"。官方专辑（guid 非 fake）原样转发不受影响。
#
# 取证说明：仓库内（proxy/static、webui-service/static、docs、tests）未找到客户端
# 专辑跳调端点的直接证据；官方 API 命名风格为 /music/api/v1/<资源>/detail?guid=
# （playlist/detail 同款），故按 /music/api/v1/album 前缀 + 携带 guid 参数保守
# 拦截——反解不到已登记伪装专辑的请求一律原样转发官方，行为与官方直连一致。


def _artist_names_joined(row: dict) -> str:
    """原生 netease 行的 ar/artists 列表 → "A / B" 拼接。"""
    ar = row.get("ar") or row.get("artists") or []
    if not isinstance(ar, list):
        return ""
    return " / ".join(str(a.get("name") or "") for a in ar if isinstance(a, dict) and a.get("name"))


def _album_detail_payload(album_guid: str, album_name: str, artist: str, tracks: list[dict]) -> dict:
    """官方专辑详情 VO：字段结构对齐 track VO 的 artists/album 对象风格。"""
    ts = int(time.time())
    artist_name = str(artist or "").strip() or "未知艺术家"
    artists_list = [{
        "guid": f"{album_guid}:artist",
        "name": artist_name,
        "coverId": None,
        "createdAt": ts,
        "updatedAt": ts,
    }]
    # 封面取第一首曲目 guid（/static/cover 按曲目封面解析），无曲目回落专辑自身
    cover_id = str((tracks[0] or {}).get("guid") or album_guid) if tracks else album_guid
    album_obj = {
        "guid": album_guid,
        "name": str(album_name or "").strip() or "未知专辑",
        "artists": artists_list,
        "coverId": cover_id,
        "releaseDate": None,
        "barcode": None,
        "createdAt": ts,
        "updatedAt": ts,
        "trackCount": len(tracks),
        "tracks": dailyrec.stamp_playlist_tracks(tracks),
    }
    payload = {"code": 0, "msg": "ok", "data": album_obj}
    _remember_list_order(payload)
    _warm_page_head(tracks)
    return payload


async def _netease_album_detail_payload(request: Request, entry: dict) -> dict | None:
    """netease 专辑：曲目详情解析真实专辑 ID，经 musicbox /api/v1/album/{id} 拉曲目列表。

    entry 带 album_id（/search/album 在线专辑登记）时跳过 song/info 二跳直达。
    """
    if not CONF.get("netease_enabled"):
        return None
    musicbox_client = get_musicbox_client(request.app)
    track_guid = str(entry.get("track_guid") or "")
    album_id = str(entry.get("album_id") or "")
    album_name = str(entry.get("album") or "")
    cover_url = ""
    if not album_id:
        song_id = song_id_from_online_guid(track_guid).rsplit(":", 1)[-1]
        try:
            r = await musicbox_client.get(f"/api/v1/song/{song_id}/info", timeout=10.0)
            if r.status_code == 200:
                body = r.json()
                if isinstance(body, dict) and body.get("ok") is not False:
                    data = body.get("data")
                    if isinstance(data, dict):
                        al = data.get("al") if isinstance(data.get("al"), dict) else {}
                        album_id = str(al.get("id") or "")
                        album_name = album_name or str(al.get("name") or "")
                        cover_url = str(al.get("picUrl") or "")
        except Exception as e:
            logger.warning("musicbox song info for album detail failed (%s): %s", track_guid, e)
    if not album_id:
        return None

    rows: list = []
    try:
        r = await musicbox_client.get(f"/api/v1/album/{album_id}", timeout=25.0)
        if r.status_code == 200:
            body = r.json()
            if isinstance(body, dict):
                rows = body.get("data") if isinstance(body.get("data"), list) else []
            elif isinstance(body, list):
                rows = body
    except Exception as e:
        logger.warning("musicbox album detail failed for %s: %s", album_id, e)

    # 行归一化：兼容 dig_info 形状（song_name/artist/duration 秒）与原生形状（name/ar/dt 毫秒）
    items: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("song_id") or row.get("id") or "")
        if not sid:
            continue
        title = str(row.get("song_name") or row.get("name") or "")
        artist = str(row.get("artist") or "") or _artist_names_joined(row)
        try:
            dur_f = float(row.get("duration") or row.get("dt") or 0)
        except (TypeError, ValueError):
            dur_f = 0.0
        items.append({
            "id": f"netease:{sid}",
            "source": "netease",
            "title": title,
            "artist": artist,
            "album": album_name,
            "duration_s": dur_f / 1000.0 if dur_f > 10000 else dur_f,
            "ext": "flac" if (row.get("has_sq") or row.get("sq") or row.get("has_hr")) else "mp3",
            "cover_url": cover_url,
        })
    if not items:
        return None
    tracks = [build_online_track(it) for it in items]
    return _album_detail_payload(f"{track_guid}:album", album_name, str(tracks[0].get("artist") or ""), tracks)


def _user_album_snapshots(user_guid: str) -> list[dict]:
    """当前用户的收藏/播放历史/歌单附加曲目快照（仅本用户，绝不扫其他用户数据）。"""
    out: list[dict] = []
    for loader in (load_online_favorites, dailyrec.load_online_play_history):
        try:
            for it in loader(user_guid) or []:
                snapshot = it.get("track") if isinstance(it, dict) and isinstance(it.get("track"), dict) else None
                if snapshot:
                    out.append(snapshot)
        except Exception:
            continue
    try:
        for bucket in load_playlist_tracks(user_guid).values():
            for it in bucket:
                snapshot = it.get("track") if isinstance(it, dict) and isinstance(it.get("track"), dict) else None
                if snapshot:
                    out.append(snapshot)
    except Exception:
        pass
    return out


def _normalize_album_candidate(it: dict) -> dict:
    """聚合候选归一为 info 形状：VO 快照还原，搜索条目原样。"""
    if isinstance(it.get("artists"), list) or isinstance(it.get("audioSpec"), dict):
        return _snapshot_to_info(it)
    return dict(it)


async def _aggregate_album_detail_payload(request: Request, entry: dict) -> dict | None:
    """musicdl/lx 专辑：无真实专辑接口，从内存搜索缓存 + 当前用户收藏/历史聚合同名曲目合成。"""
    album_name = str(entry.get("album") or "").strip()
    src = str(entry.get("source") or "")
    track_guid = str(entry.get("track_guid") or "")
    if not album_name:
        # 无专辑名无法按专辑聚合（搜索结果常缺专辑字段），走业务错误
        return None

    anchor = entry.get("item") if isinstance(entry.get("item"), dict) else {}
    candidates: list[dict] = [anchor] if anchor else []
    # ① 内存搜索缓存：音源目录数据，与用户身份无关
    _clean_search_cache()
    for cache_entry in _SEARCH_CACHE.values():
        for it in cache_entry.get("items") or []:
            if isinstance(it, dict):
                candidates.append(it)
    # ② 当前用户的收藏/历史/歌单附加快照（探测失败则跳过磁盘源，只用缓存）
    upstream_client = get_upstream_client(request.app)
    try:
        is_authed, user_guid, _resp = await _probe_upstream_auth(request, upstream_client)
    except Exception:
        is_authed, user_guid = False, ""
    if is_authed and user_guid:
        candidates.extend(_user_album_snapshots(user_guid))

    infos: list[dict] = []
    seen_guids: set[str] = set()
    for it in candidates:
        if not isinstance(it, dict) or not it:
            continue
        try:
            g = online_guid_from_item(it)
        except Exception:
            continue
        if not g or not is_online_guid(g) or g in seen_guids:
            continue
        if source_from_online_guid(g) != src:
            continue
        info = _normalize_album_candidate(it)
        if str(info.get("album") or "").strip() != album_name:
            continue
        seen_guids.add(g)
        infos.append(info)
    if not infos:
        return None
    tracks = [build_online_track(info) for info in infos]
    return _album_detail_payload(f"{track_guid}:album", album_name, str(tracks[0].get("artist") or ""), tracks)


# 专辑详情合成缓存（fake guid → (过期时间, payload)）：客户端进专辑页会先调
# /album/detail 再调 /track/album-detail/list，避免同专辑合成两次；TTL 短，仅省重复请求
_ALBUM_PAYLOAD_CACHE: dict[str, tuple[float, dict]] = {}
_ALBUM_PAYLOAD_TTL_S = 120.0


async def _synthesize_album_payload(request: Request, entry: dict, raw_guid: str) -> "dict | None":
    """按源合成专辑详情 payload（带 120s TTL 缓存）；合成失败返回 None。"""
    now = time.monotonic()
    cached = _ALBUM_PAYLOAD_CACHE.get(raw_guid)
    if cached and cached[0] > now:
        return cached[1]
    src = str(entry.get("source") or "")
    payload = None
    try:
        if src == "netease":
            payload = await _netease_album_detail_payload(request, entry)
        else:
            payload = await _aggregate_album_detail_payload(request, entry)
    except Exception as e:
        logger.warning("album detail adapt failed for %s: %s", raw_guid, e)
    if payload is not None:
        if len(_ALBUM_PAYLOAD_CACHE) > 256:
            _ALBUM_PAYLOAD_CACHE.clear()
        _ALBUM_PAYLOAD_CACHE[raw_guid] = (now + _ALBUM_PAYLOAD_TTL_S, payload)
    return payload


def _album_list_envelope(request: Request, tracks: list[dict]) -> dict:
    """官方专辑曲目列表信封：与 /track/playlist-detail/list 同款 {list,total,sort}。"""
    try:
        page = max(int(request.query_params.get("page") or 1), 1)
    except (TypeError, ValueError):
        page = 1
    try:
        size = int(request.query_params.get("size") or 50)
    except (TypeError, ValueError):
        size = 50
    if size == -1:
        size = max(len(tracks), 1)
    if size < 1:
        size = 50
    start = (page - 1) * size
    return {
        "code": 0,
        "msg": "ok",
        "data": {
            "list": tracks[start:start + size],
            "total": len(tracks),
            "sort": request.query_params.get("sort") or "",
        },
    }


@app.api_route("/music/api/v1/album", methods=["GET", "HEAD"])
@app.api_route("/music/api/v1/album/{subpath:path}", methods=["GET", "HEAD"])
async def album_detail(request: Request, subpath: str = ""):
    raw_guid = str(
        request.query_params.get("guid")
        or request.query_params.get("albumGUID")
        or request.query_params.get("albumGuid")
        or request.query_params.get("id")
        or request.query_params.get("albumId")
        or ""
    ).strip()
    entry = resolve_fake_album(raw_guid)
    if entry is None:
        # 官方专辑或未知 guid：原样转发官方，官方专辑请求不受影响
        return await forward_to_upstream(request, get_upstream_client(request.app))

    payload = await _synthesize_album_payload(request, entry, raw_guid)
    if payload is not None:
        return JSONResponse(content=disguise_client_json(payload))
    logger.info("album detail not synthesizable: source=%s album=%r guid=%s", entry.get("source"), entry.get("album"), raw_guid)
    return JSONResponse(content={"code": -1, "msg": "该音源暂不支持专辑详情", "data": None})


@app.api_route("/music/api/v1/track/album-detail/list", methods=["GET", "HEAD"])
async def track_album_detail_list(request: Request):
    """专辑页曲目列表（官方 2.5 版客户端在 /album/detail 之后调用）。

    官方专辑原样透传；在线伪装专辑复用专辑详情合成结果分页下发，
    合成失败返回业务错误空列表，绝不透传官方"无此专辑"。
    """
    raw_guid = str(
        request.query_params.get("albumGUID")
        or request.query_params.get("albumGuid")
        or request.query_params.get("guid")
        or request.query_params.get("id")
        or ""
    ).strip()
    entry = resolve_fake_album(raw_guid)
    if entry is None:
        return await forward_to_upstream(request, get_upstream_client(request.app))

    payload = await _synthesize_album_payload(request, entry, raw_guid)
    if payload is None:
        return JSONResponse(content={"code": -1, "msg": "该音源暂不支持专辑详情", "data": None})
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    tracks = list(data.get("tracks") or [])
    return JSONResponse(content=disguise_client_json(_album_list_envelope(request, tracks)))


# === /search/album 在线专辑合并（issue #22，官方 2.5 版客户端专辑搜索 Tab）===
# 官方端点搜的是本地曲库专辑；这里透传官方结果后合并在线专辑：
#   - netease：musicbox type=album 真实专辑搜索，登记真实 album_id 直达详情
#   - musicdl/lx：无专辑接口，按关键词搜曲目后按"专辑名+源"聚合合成，
#     代表曲目 guid 注册伪装专辑（详情走聚合适配）
# 官方专辑永远排在前面；同名专辑（与官方结果撞名）不重复注入。

_ALBUM_SEARCH_LIMIT = 10


def _album_list_obj(album_guid: str, album_name: str, artist: str, cover_ref: str, track_count: int) -> dict:
    """专辑列表/搜索结果 VO（与 _album_detail_payload 同字段风格，无 tracks）。"""
    ts = int(time.time())
    artist_name = str(artist or "").strip() or "未知艺术家"
    return {
        "guid": album_guid,
        "name": str(album_name or "").strip() or "未知专辑",
        "artists": [{
            "guid": f"{album_guid}:artist",
            "name": artist_name,
            "coverId": None,
            "createdAt": ts,
            "updatedAt": ts,
        }],
        "coverId": cover_ref or album_guid,
        "releaseDate": None,
        "barcode": None,
        "createdAt": ts,
        "updatedAt": ts,
        "trackCount": int(track_count or 0),
    }


def _normalize_netease_album_row(row: dict) -> "tuple[str, str, str, str] | None":
    """网易专辑搜索行（CLI 与 web 兜底两种形状）→ (album_id, 名称, 歌手, 封面)。"""
    album_id = str(row.get("album_id") or row.get("albumId") or row.get("id") or "")
    name = str(row.get("album_name") or row.get("name") or row.get("title") or "").strip()
    if not album_id or album_id in ("0", "None") or not name:
        return None
    ar = row.get("artists") or row.get("artist") or []
    if isinstance(ar, list):
        artist = " / ".join(str(a.get("name") or "") for a in ar if isinstance(a, dict) and a.get("name"))
    else:
        artist = str(ar or "")
    cover = str(row.get("pic_url") or row.get("picUrl") or row.get("coverImgUrl") or row.get("cover") or "")
    return album_id, name, artist, cover


async def _netease_album_search_rows(request: Request, keyword: str, limit: int) -> list[dict]:
    if not CONF.get("netease_enabled"):
        return []
    client = get_musicbox_client(request.app)
    try:
        r = await client.get(
            "/api/v1/search",
            params={"keyword": keyword, "type": "album", "limit": max(limit, 5)},
            timeout=8.0,
        )
    except Exception as e:
        logger.debug("netease album search failed: %s", e)
        return []
    if r.status_code != 200:
        return []
    body = r.json() if r.headers.get("content-type", "").startswith(("application/json", "text/json")) else {}
    rows = body.get("data") if isinstance(body, dict) else None
    if not isinstance(rows, list):
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        norm = _normalize_netease_album_row(row)
        if norm is None:
            continue
        album_id, name, artist, cover = norm
        if album_id in seen:
            continue
        seen.add(album_id)
        # 锚点 guid 仅供专辑伪装登记/反解，不对应任何真实曲目；
        # 形态对齐曲目锚点约定：专辑 guid = 锚点 + ":album"（register 内部拼）
        anchor = f"online:netease:album:{album_id}"
        register_fake_album(anchor, name, {"cover_url": cover} if cover else {}, album_id=album_id, persist=True)
        out.append(_album_list_obj(f"{anchor}:album", name, artist, f"{anchor}:album", 0))
        if len(out) >= limit:
            break
    return out


async def _aggregate_album_search_rows(request: Request, keyword: str, limit: int) -> list[dict]:
    """musicdl/lx：关键词搜曲目后按"专辑名+源"聚合合成专辑卡片。"""
    jobs: list = []
    if CONF.get("musicdl_enabled"):
        jobs.append(("musicdl", get_musicdl_client(request.app).get(
            "/search", params={"keyword": keyword, "limit": 20}, timeout=6.0,
        )))
    if CONF.get("lx_enabled"):
        params: dict = {"keyword": keyword, "limit": 20}
        lx_sources = CONF.get("lx_sources") or None
        if lx_sources:
            params["sources"] = ",".join(lx_sources)
        jobs.append(("lx", get_lx_client(request.app).get("/api/v1/search", params=params, timeout=6.0)))

    async def _run(coro):
        try:
            return await coro
        except Exception as e:
            logger.debug("album aggregate source search failed: %s", e)
            return None

    groups: "dict[tuple[str, str], list[dict]]" = {}
    for fut in asyncio.as_completed([_run(coro) for _name, coro in jobs]):
        r = await fut
        if r is None or r.status_code != 200:
            continue
        try:
            rows = r.json().get("items")
        except Exception:
            continue
        if not isinstance(rows, list):
            continue
        for it in rows:
            if not isinstance(it, dict):
                continue
            # lx 行的 id 是裸平台 id（如 kg:xxx），补 online: 前缀统一 guid 形态
            raw_id = str(it.get("id") or "")
            if raw_id and not raw_id.startswith(("online:", "lx:")):
                it = dict(it, id=f"lx:{raw_id}" if str(it.get("source") or "") == "lx" else raw_id)
            try:
                guid = online_guid_from_item(it)
            except Exception:
                continue
            src = source_from_online_guid(guid)
            album_name = str(it.get("album") or "").strip()
            if not guid or not is_online_guid(guid) or not album_name:
                continue
            groups.setdefault((src, album_name), []).append(it)

    out: list[dict] = []
    # 每组取曲目数最多的前 N 个专辑；代表曲目注册伪装专辑（详情走聚合适配）
    for (src, album_name), items in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        rep = items[0]
        try:
            rep_guid = online_guid_from_item(rep)
        except Exception:
            continue
        register_fake_album(rep_guid, album_name, rep, persist=True)
        out.append(_album_list_obj(
            f"{rep_guid}:album", album_name,
            str(rep.get("artist") or ""), rep_guid, len(items),
        ))
        if len(out) >= limit:
            break
    return out


async def _online_album_search(request: Request, keyword: str, limit: int) -> list[dict]:
    """在线专辑搜索：netease 真实接口优先，musicdl/lx 聚合补充（并行）。"""
    netease_coro = _netease_album_search_rows(request, keyword, limit)
    agg_coro = _aggregate_album_search_rows(request, keyword, max(limit // 2, 3))
    results = await asyncio.gather(netease_coro, agg_coro, return_exceptions=True)
    out: list[dict] = []
    seen: set[str] = set()
    for part in results:
        if isinstance(part, Exception):
            logger.debug("online album search branch failed: %s", part)
            continue
        for obj in part or []:
            name_key = str(obj.get("name") or "").strip().lower()
            if name_key and name_key in seen:
                continue
            seen.add(name_key)
            out.append(obj)
    return out[:limit]


@app.api_route("/music/api/v1/search/album", methods=["GET", "HEAD"])
@app.api_route("/music/api/v1/search/album/{subpath:path}", methods=["GET", "HEAD"])
async def search_album(request: Request, subpath: str = ""):
    upstream_client = get_upstream_client(request.app)
    envelope = await fetch_upstream_envelope(request, upstream_client)
    if not isinstance(envelope, dict) or envelope.get("code") != 0:
        # 官方错误（含未登录 401 信封）原样透传，不吞官方语义
        return envelope if not isinstance(envelope, dict) else JSONResponse(content=envelope)

    keyword = str(request.query_params.get("q") or request.query_params.get("keyword") or request.query_params.get("wd") or "").strip()
    online: list[dict] = []
    if keyword:
        try:
            online = await asyncio.wait_for(
                _online_album_search(request, keyword, _ALBUM_SEARCH_LIMIT), timeout=10.0,
            )
        except asyncio.TimeoutError:
            logger.debug("online album search timed out for %r", keyword)
        except Exception as e:
            logger.warning("online album search failed: %s", e)

    if online:
        data = envelope.get("data")
        if not isinstance(data, dict):
            data = {}
            envelope["data"] = data
        lst = data.get("list") if isinstance(data.get("list"), list) else None
        if lst is None:
            data["list"] = lst = []
        official_names = {
            str(x.get("name") or "").strip().lower()
            for x in lst if isinstance(x, dict)
        }
        add = [o for o in online if str(o.get("name") or "").strip().lower() not in official_names]
        data["list"] = lst + add
        if isinstance(data.get("total"), int):
            data["total"] = data["total"] + len(add)
        elif "total" not in data:
            data["total"] = len(data["list"])
    return JSONResponse(content=disguise_client_json(envelope))


@app.post("/music/api/v1/event/report")
async def event_report(request: Request):
    upstream_client = get_upstream_client(request.app)
    raw = await request.body()
    try:
        body = json.loads(raw.decode("utf-8") or "{}") if raw else {}
    except Exception:
        body = {}
    events = body.get("events") if isinstance(body, dict) else None
    online_plays: list[tuple[str, dict]] = []
    other_events: list = []
    if isinstance(events, list):
        for ev in events:
            if not isinstance(ev, dict):
                continue
            et = str(ev.get("eventType") or ev.get("type") or "")
            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}
            guid = resolve_real_guid(str(payload.get("trackGUID") or payload.get("guid") or ""))
            if et in ("track_play", "TrackPlay") and is_online_guid(guid):
                online_plays.append((guid, payload))
            else:
                other_events.append(ev)
    else:
        return await forward_to_upstream(request, upstream_client)

    if online_plays:
        is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
        if is_authed:
            async with _HISTORY_LOCK:
                for guid, payload in online_plays:
                    info = stub_online_info(guid)
                    title = str(info.get("title") or "").strip()
                    artist = str(info.get("artist") or "").strip()
                    album = ""
                    duration_s = None
                    if payload:
                        # 客户端上报若带元数据（App/Web 字段名可能不同）优先采信，
                        # 避免历史条目只有 guid、标题歌手为空。专辑可能是对象，先解开。
                        p_title, p_artist, p_album = _tag_fields({
                            "title": payload.get("title"),
                            "name": payload.get("name"),
                            "artist": payload.get("artist") or payload.get("artistName"),
                            "artists": payload.get("artists"),
                            "album": payload.get("album"),
                            "albumName": payload.get("albumName"),
                        })
                        title = p_title or title
                        artist = p_artist or artist
                        album = p_album or album
                        raw_dur = payload.get("duration") or payload.get("durationMs") or payload.get("duration_ms")
                        try:
                            dv = float(raw_dur)
                            duration_s = dv / 1000.0 if dv > 10000 else dv
                        except (TypeError, ValueError):
                            duration_s = None
                    if not title or not artist or not album:
                        # 事件不带元数据时从内存搜索会话补齐（播放时会话必在）
                        retained, _entry = _retained_track(request, guid)
                        if retained:
                            r_title, r_artist, r_album = _tag_fields(retained)
                            title = title or r_title
                            artist = artist or r_artist
                            album = album or r_album
                            if not duration_s:
                                try:
                                    duration_s = float(retained.get("duration_s") or 0) or None
                                except (TypeError, ValueError):
                                    duration_s = None
                        if not title or not artist or not album:
                            served = _lookup_playlist_cache_track(guid)
                            if served:
                                s_title, s_artist, s_album = _tag_fields(served)
                                title = title or s_title
                                artist = artist or s_artist
                                album = album or s_album
                    cached = find_cache_file(guid)
                    if cached and (not title or not artist):
                        base = os.path.splitext(os.path.basename(cached))[0]
                        if " - " in base:
                            file_artist, file_title = base.split(" - ", 1)
                            artist = artist or file_artist
                            title = title or file_title
                        else:
                            title = title or base
                    snapshot = {
                        "guid": guid,
                        "title": title,
                        "artist": artist,
                        "source": source_from_online_guid(guid),
                    }
                    if album:
                        snapshot["album"] = album
                    if duration_s:
                        snapshot["duration_s"] = duration_s
                    dailyrec.record_online_play(user_guid, guid, snapshot)
        elif auth_resp is not None and not other_events:
            return auth_resp

    if other_events:
        headers = copy_incoming_headers(request)
        fwd = dict(body)
        fwd["events"] = other_events
        req = upstream_client.build_request(
            "POST",
            "/music/api/v1/event/report",
            headers=headers,
            content=json.dumps(fwd).encode("utf-8"),
        )
        resp = await upstream_client.send(req)
        resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    return JSONResponse(content={"code": 0, "msg": "ok", "data": None})


@app.get("/music/api/v1/play-history/list")
async def play_history_list(request: Request):
    upstream_client = get_upstream_client(request.app)
    envelope = await fetch_upstream_envelope(request, upstream_client)
    if isinstance(envelope, Response):
        return envelope
    headers = envelope.pop("_ext_headers", {})
    if envelope.get("code") != 0:
        return JSONResponse(content=envelope, headers=headers)

    # 官方历史已取回成功（同一组请求头），此时探测被拒不回传鉴权错误，
    # 降级为仅返回官方列表（同收藏列表）。
    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed:
        logger.warning("play history list degraded to official-only: auth probe rejected after official list ok")
        return JSONResponse(content=envelope, headers=headers)

    data = envelope.get("data")
    if not isinstance(data, dict):
        data = {"list": [], "total": 0}
        envelope["data"] = data
    official = data.get("list")
    if not isinstance(official, list):
        official = []
        data["list"] = official

    async with _HISTORY_LOCK:
        online_items = dailyrec.load_online_play_history(user_guid)
    fav_set = {str(it.get("guid") or "") for it in load_online_favorites(user_guid)}
    template = official_track_template(official)
    online_tracks = []
    for it in reversed(online_items):
        guid = str(it.get("guid") or "")
        if not guid:
            continue
        track = it.get("track") if isinstance(it.get("track"), dict) else {}
        info = _snapshot_to_info(track) if track else None
        if not (info or {}).get("title"):
            # 历史快照可能没带元数据：从内存搜索会话补齐（无网络开销）
            retained, _entry = _retained_track(request, guid)
            if retained:
                r_title, r_artist, r_album = _tag_fields(retained)
                info = {
                    **(info or {}),
                    "title": r_title,
                    "artist": r_artist,
                    "album": r_album,
                    "duration_s": retained.get("duration_s") or 0,
                    "ext": retained.get("ext") or "",
                }
        if not (info or {}).get("title"):
            # Ceru 歌单/收藏歌曲的标题映射是持久化的，历史快照缺字段时从这里补。
            ceru_song = ceru.song_for_guid(guid)
            if ceru_song:
                info = {
                    **(info or {}),
                    "title": ceru.song_title(ceru_song),
                    "artist": ceru.song_artist(ceru_song),
                    "album": ceru.song_album(ceru_song),
                    "duration_s": ceru.song_duration_s(ceru_song),
                    "cover_url": (info or {}).get("cover_url") or ceru.song_artwork(ceru_song),
                }
        obj = build_favorite_track_obj(guid, info, created_at=int(it.get("playedAt") or time.time()), template=template)
        obj["isFavorite"] = guid in fav_set
        online_tracks.append(disguise_client_json(obj))

    seen = {str(x.get("guid")) for x in official if isinstance(x, dict)}
    merged_online = [t for t in online_tracks if t.get("guid") not in seen]
    data["list"] = merged_online + official
    official_total = data.get("total")
    if not isinstance(official_total, int):
        official_total = len(official)
    data["total"] = official_total + len(merged_online)
    return JSONResponse(content=envelope, headers=headers)


async def _forward_official_play_history_delete(
    request: Request, client: httpx.AsyncClient, official: list[str],
) -> Response | None:
    """混合批次中官方部分先行。返回 None=官方成功，否则返回官方错误响应（本地不动）。"""
    req = client.build_request(
        "POST",
        "/music/api/v1/play-history/delete",
        headers=copy_incoming_headers(request),
        json={"trackGUIDs": official},
    )
    resp = await client.send(req)
    resp_headers = filter_headers(resp.headers, exclude_keys={"content-length", "content-encoding"})
    if resp.status_code != 200:
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    try:
        payload = resp.json()
    except Exception:
        payload = None
    if not (isinstance(payload, dict) and payload.get("code") == 0):
        return JSONResponse(content=payload or {"code": -1, "msg": "upstream error", "data": None})
    return None


@app.api_route("/music/api/v1/play-history/delete", methods=["POST", "DELETE"])
async def play_history_delete(request: Request):
    """删除播放历史：online 条目删本地存储，官方条目转发官方后端。

    列表是代理拼的（list 合并本地在线历史），删除闭环也必须在代理完成，否则
    带（伪装成官方 32-hex 的）在线 id 的删除请求直达官方被拒。官方契约是批量
    字段 trackGUIDs（前端单条移除与"编辑"多选移除共用，官方后端 binding:"required"），
    兼容单数 trackGUID/guid（含 query 透传）；混合批次官方部分先行，官方失败
    本地不动，与 playlist/remove-track 同款分工。
    """
    upstream_client = get_upstream_client(request.app)
    try:
        body = await request.json()
    except Exception:
        body = {}

    raw = body.get("trackGUIDs") if isinstance(body, dict) else None
    if isinstance(raw, list) and raw:
        candidates = [str(g or "").strip() for g in raw]
    else:
        single = ""
        if isinstance(body, dict):
            single = str(body.get("trackGUID") or body.get("guid") or "").strip()
        if not single:
            single = str(request.query_params.get("trackGUID") or request.query_params.get("guid") or "").strip()
        candidates = [single] if single else []

    resolved = [resolve_real_guid(g) for g in candidates if g]
    online = [g for g in resolved if is_online_guid(g)]
    if not online:
        return await forward_to_upstream(request, upstream_client)

    is_authed, user_guid, auth_resp = await _probe_upstream_auth(request, upstream_client)
    if not is_authed and auth_resp is not None:
        return auth_resp

    official = [g for g in resolved if not is_online_guid(g)]
    if official:
        err = await _forward_official_play_history_delete(request, upstream_client, official)
        if err is not None:
            return err

    async with _HISTORY_LOCK:
        for guid in online:
            try:
                dailyrec.remove_online_play(user_guid, guid)
            except Exception as e:
                logger.warning("Error deleting from online play history for user %s: %s", user_guid, e)

    return JSONResponse(content={"code": 0, "msg": "", "data": None})


# 官方端点取证：未拦截路径 -> 转发次数（仅内存，重启清零；用于首见告警与采样）
_FORWARDED_PATH_STATS: dict[str, int] = {}


def _trace_forwarded_endpoint(request: Request) -> None:
    """catch_all 透传前的官方端点取证日志。

    未拦截的 /music/api 请求：同一路径首见打 INFO（官方 App 更新引入新端点时
    日志立刻可见），之后每 50 次采样一条；FNMUSIC_TRACE_FORWARD=detail 逐条记录。
    """
    path = request.url.path
    if not path.startswith("/music/api/"):
        return
    count = _FORWARDED_PATH_STATS.get(path, 0) + 1
    _FORWARDED_PATH_STATS[path] = count
    if CONF.get("trace_forward") or count == 1 or count % 50 == 0:
        logger.info(
            "forward-unhandled %s %s (#%d) —— 官方端点未被代理拦截，仅透传",
            request.method,
            path,
            count,
        )


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def catch_all(request: Request, full_path: str):
    _trace_forwarded_endpoint(request)
    return await forward_to_upstream(request, get_upstream_client(request.app))
