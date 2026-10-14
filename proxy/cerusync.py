# TOP[top.ceru] 多端同步核心：同步服务器协议、歌单/收藏写回、guid 注册表、多级缓存
"""Ceru 自部署同步服务接入：歌单、收藏双向同步。

fnOS 音乐扩展在这里扮演 Ceru 同步服务的一个“设备”。它用 Web 管理页生成的
配对码（或已有的 accessToken）登录服务，然后：

* 拉取歌单（含“我的喜欢”语义歌单）与歌曲顺序；
* 把 Ceru 歌曲映射成当前音源可播放的 online track（优先洛雪直连映射）；
* 把飞牛侧的收藏 / 取消收藏回写到同步服务，桌面端、安卓端随之同步。

本模块不 import proxy.app，避免循环依赖。音源搜索回退由 app 注入回调完成。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Callable
from uuid import uuid4

import httpx

logger = logging.getLogger("proxy.cerusync")

_SUMMARY_TTL_S = 60.0
_TRACKS_TTL_S = 300.0
_FAVORITES_TTL_S = 120.0
_FETCH_TIMEOUT_S = 12.0
_TRACKS_FETCH_TIMEOUT_S = 45.0
_MAX_PLAYLISTS = 500
_MAX_TRACKS_PER_PLAYLIST = 5000

_LX_PLATFORMS = {"kg", "wy", "tx", "mg", "kw"}
_HEX32_RE = re.compile(r"[0-9a-f]{32}", re.IGNORECASE)

_http_client: httpx.AsyncClient | None = None
_http_base_url: str = ""
_state: dict[str, Any] | None = None
_summary_cache: dict[str, Any] | None = None
_summary_disk_stale = False
_tracks_cache: dict[str, tuple[list, float]] = {}
_tracks_disk_stale: set[str] = set()
_favorites_cache: dict[str, tuple[dict, float]] = {}
_favorites_disk_stale = False
_load_tasks: dict[str, asyncio.Task] = {}
_prefetch_tasks: set[asyncio.Task] = set()

# fnOS online guid -> Ceru 歌曲（收藏回写反查用）。
_guid_song_registry: dict[str, dict] = {}
_guid_persist_last = 0.0
_GUID_PERSIST_INTERVAL_S = 5.0


def home_dir() -> str:
    env = (os.environ.get("FNMUSIC_HOME") or "").strip()
    if env:
        return env
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def state_dir() -> str:
    return os.environ.get("FNMUSIC_CERU_SYNC_DIR") or os.path.join(home_dir(), "ceru_sync")


def _state_path() -> str:
    return os.path.join(state_dir(), "state.json")


def _disk_dir() -> str:
    return os.path.join(state_dir(), "cache")


def config() -> dict[str, Any]:
    host = (os.environ.get("FNMUSIC_CERU_SYNC_HOST") or "").strip()
    port = (os.environ.get("FNMUSIC_CERU_SYNC_PORT") or "").strip()
    use_https = os.environ.get("FNMUSIC_CERU_SYNC_USE_HTTPS", "false").lower() in ("true", "1", "yes")
    url = (os.environ.get("FNMUSIC_CERU_SYNC_URL") or "").strip().rstrip("/")
    if host:
        host = host.strip()
        if "://" in host:
            # 容错：用户手滑填了 scheme，以显式 scheme 为准，再剥掉路径
            scheme, _, rest = host.partition("://")
            host = rest.split("/", 1)[0]
            use_https = scheme.lower() == "https"
        else:
            host = host.split("/", 1)[0].strip()
        if ":" in host and not host.startswith("["):
            host_part, _, port_part = host.partition(":")
            if port_part.isdigit():
                host = host_part
                port = port_part
        try:
            port_num = int(port) if port else 31231
        except ValueError:
            port_num = 31231
        url = f"{'https' if use_https else 'http'}://{host}:{port_num}"
    return {
        "enabled": os.environ.get("FNMUSIC_CERU_SYNC_ENABLED", "false").lower() in ("true", "1", "yes"),
        "url": url,
        "host": host,
        "port": port,
        "use_https": use_https,
        "pair_code": (os.environ.get("FNMUSIC_CERU_SYNC_PAIR_CODE") or "").strip(),
        "token": (os.environ.get("FNMUSIC_CERU_SYNC_TOKEN") or "").strip(),
        "device_id": (os.environ.get("FNMUSIC_CERU_SYNC_DEVICE_ID") or "").strip(),
        "username": (os.environ.get("FNMUSIC_CERU_SYNC_USERNAME") or "").strip(),
        "password": (os.environ.get("FNMUSIC_CERU_SYNC_PASSWORD") or "").strip(),
        "include_favorites_playlist": os.environ.get("FNMUSIC_CERU_SYNC_FAVORITES", "true").lower() in ("true", "1", "yes"),
        "prefetch": os.environ.get("FNMUSIC_CERU_SYNC_PREFETCH", "true").lower() in ("true", "1", "yes"),
    }


def enabled() -> bool:
    return bool(config()["enabled"] and config()["url"])


def _load_state() -> dict[str, Any]:
    global _state
    if _state is not None:
        return _state
    path = _state_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        _state = data if isinstance(data, dict) else {}
    except Exception:
        _state = {}
    return _state


def _save_state(data: dict[str, Any]) -> None:
    global _state
    _state = data
    path = _state_path()
    parent = os.path.dirname(path) or "."
    part = f"{path}.{uuid4().hex[:8]}.part"
    try:
        os.makedirs(parent, exist_ok=True)
        with open(part, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(part, path)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
    except Exception as e:
        logger.warning("cerusync state write failed: %s", e)
        if os.path.exists(part):
            try:
                os.remove(part)
            except Exception:
                pass


def _atomic_write_json(path: str, payload: Any) -> None:
    parent = os.path.dirname(path) or "."
    part = f"{path}.{uuid4().hex[:8]}.part"
    try:
        os.makedirs(parent, exist_ok=True)
        with open(part, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(part, path)
    except Exception as e:
        logger.warning("cerusync cache write failed: %s", e)
        if os.path.exists(part):
            try:
                os.remove(part)
            except Exception:
                pass


def _load_disk_json(path: str, ttl_s: float) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    saved = data.get("savedAt")
    if not (isinstance(saved, (int, float)) and time.time() - saved < ttl_s):
        return None
    return data.get("payload")


def _disk_playlists_path() -> str:
    return os.path.join(_disk_dir(), "playlists.json")


def _disk_songs_path(playlist_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", str(playlist_id or ""))
    return os.path.join(_disk_dir(), f"pl-{safe}.json")


def _disk_favorites_path() -> str:
    return os.path.join(_disk_dir(), "favorites.json")


def _guid_map_path() -> str:
    return os.path.join(_disk_dir(), "guid-map.json")


def device_id() -> str:
    cfg = config()
    if cfg["device_id"]:
        return cfg["device_id"]
    state = _load_state()
    existing = state.get("deviceId")
    if not existing:
        existing = f"fnos-{uuid4().hex[:12]}"
        _save_state({**state, "deviceId": existing})
    return existing


def get_client() -> httpx.AsyncClient:
    global _http_client, _http_base_url
    base_url = config()["url"]
    if _http_client is None or _http_client.is_closed or _http_base_url != base_url:
        _http_client = httpx.AsyncClient(
            base_url=base_url,
            timeout=_FETCH_TIMEOUT_S,
            follow_redirects=True,
        )
        _http_base_url = base_url
    return _http_client


async def close() -> None:
    global _http_client, _http_base_url
    if _http_client is not None and not _http_client.is_closed:
        await _http_client.aclose()
    _http_client = None
    _http_base_url = ""


def _token() -> str:
    cfg = config()
    if cfg["token"]:
        return cfg["token"]
    state = _load_state()
    token = str(state.get("accessToken") or "")
    expires = state.get("expiresAt") or 0
    if token and isinstance(expires, (int, float)) and expires > time.time() * 1000:
        return token
    return ""


async def _auth(force: bool = False) -> str:
    cfg = config()
    token = _token()
    if token and not force:
        return token

    pair_code = cfg["pair_code"]
    username = cfg["username"]
    password = cfg["password"]
    if not pair_code and not (username and password):
        return ""

    client = get_client()
    if pair_code:
        resp = await client.post("/auth/pair", json={"pairCode": pair_code})
    else:
        resp = await client.post("/auth/login", json={"username": username, "password": password})
    if resp.status_code != 200:
        logger.debug("cerusync auth failed: %s", resp.status_code)
        return ""
    data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    if not isinstance(data, dict):
        return ""
    access = str(data.get("accessToken") or "")
    if not access:
        return ""
    state = _load_state()
    _save_state({
        **state,
        "accessToken": access,
        "expiresAt": data.get("expiresAt") or (int(time.time() * 1000) + 30 * 86400 * 1000),
        "user": data.get("user") or state.get("user"),
    })
    return access


async def _api(method: str, path: str, body: Any = None, *, retry_auth: bool = True) -> Any:
    token = await _auth()
    if not token:
        return None
    client = get_client()
    headers = {"Authorization": f"Bearer {token}"}
    try:
        if method.upper() == "GET":
            resp = await client.get(path, headers=headers)
        elif method.upper() == "POST":
            resp = await client.post(path, headers=headers, json=body)
        elif method.upper() == "DELETE":
            resp = await client.request("DELETE", path, headers=headers, json=body)
        else:
            resp = await client.request(method, path, headers=headers, json=body)
    except Exception as e:
        logger.debug("cerusync request failed %s %s: %s", method, path, e)
        return None
    if resp.status_code == 401 and retry_auth:
        await _auth(force=True)
        return await _api(method, path, body, retry_auth=False)
    if resp.status_code != 200:
        return None
    try:
        return resp.json()
    except Exception:
        return None


# ------------------------------------------------------------------ 数据拉取 --

async def list_playlists() -> list[dict]:
    """Ceru 歌单摘要列表（按服务器 position 排序）。失败回落磁盘，不抛异常。"""
    global _summary_cache, _summary_disk_stale
    now = time.monotonic()
    if _summary_cache and now - _summary_cache["saved_at"] < _SUMMARY_TTL_S:
        return list(_summary_cache["items"])

    disk = _load_disk_json(_disk_playlists_path(), 24 * 3600)
    if isinstance(disk, list):
        if _summary_disk_stale:
            # 同步事件后不能把旧磁盘摘要伪装成新内存缓存；先同步拉一次，
            # 只有服务器不可用时才退回磁盘旧数据。
            await _fetch_playlists()
            if _summary_cache is not None:
                return list(_summary_cache.get("items") or [])
        _summary_cache = {"items": disk, "saved_at": now}
        _schedule_refresh_playlists()
        return list(disk)
    return await _fetch_playlists()


async def _fetch_playlists() -> list[dict]:
    global _summary_cache, _summary_disk_stale
    payload = await _api("GET", "/playlists")
    if not isinstance(payload, list):
        cached = _summary_cache or {}
        return list((cached or {}).get("items") or [])
    items = [p for p in payload if isinstance(p, dict)]
    items.sort(key=lambda p: (p.get("position") if isinstance(p.get("position"), int) else 0, p.get("createdAt") or ""))
    _summary_cache = {"items": items, "saved_at": time.monotonic()}
    _summary_disk_stale = False
    _atomic_write_json(_disk_playlists_path(), {"savedAt": int(time.time()), "payload": items})
    return list(items)


def _schedule_refresh_playlists() -> None:
    try:
        task = asyncio.get_running_loop().create_task(_fetch_playlists())
    except RuntimeError:
        return
    _prefetch_tasks.add(task)
    task.add_done_callback(_prefetch_tasks.discard)


async def list_playlist_songs(playlist_id: str) -> list[dict]:
    pid = str(playlist_id or "")
    key = pid
    hit = _tracks_cache.get(key)
    if hit and time.monotonic() < hit[1]:
        return list(hit[0])
    existing = _load_tasks.get(key)
    if existing is not None and not existing.done():
        return await asyncio.shield(existing)

    disk = _load_disk_json(_disk_songs_path(pid), 24 * 3600)
    if isinstance(disk, list):
        if pid in _tracks_disk_stale:
            songs = await _fetch_songs(pid)
            if pid not in _tracks_disk_stale:
                return list(songs)
        _tracks_cache[key] = (disk, time.monotonic() + _TRACKS_TTL_S)
        try:
            refresh = asyncio.get_running_loop().create_task(_fetch_songs(pid))
        except RuntimeError:
            refresh = None
        if refresh is not None:
            _prefetch_tasks.add(refresh)
            refresh.add_done_callback(_prefetch_tasks.discard)
        return list(disk)

    task = asyncio.get_running_loop().create_task(_fetch_songs(pid))
    _load_tasks[key] = task
    try:
        return await asyncio.shield(task)
    finally:
        _load_tasks.pop(key, None)


async def _fetch_songs(playlist_id: str) -> list[dict]:
    global _tracks_disk_stale
    payload = await _api("GET", f"/playlists/{playlist_id}/songs?limit={_MAX_TRACKS_PER_PLAYLIST}")
    songs: list[dict] = []
    valid_payload = False
    if isinstance(payload, dict):
        raw = payload.get("songs")
        if raw is None:
            raw = payload.get("list")
        if isinstance(raw, list):
            songs = [s for s in raw if isinstance(s, dict)]
            valid_payload = True
    if valid_payload:
        _tracks_disk_stale.discard(playlist_id)
        _tracks_cache[playlist_id] = (songs, time.monotonic() + _TRACKS_TTL_S)
        _atomic_write_json(_disk_songs_path(playlist_id), {"savedAt": int(time.time()), "payload": songs})
        return list(songs)
    disk = _load_disk_json(_disk_songs_path(playlist_id), 24 * 3600)
    return list(disk) if isinstance(disk, list) else []


async def list_favorites() -> dict[str, list[dict]]:
    """拉取 song / playlist 两类收藏。返回 {"song": [...], "playlist": [...]}。"""
    key = "favorites"
    hit = _favorites_cache.get(key)
    if hit and time.monotonic() < hit[1]:
        return {k: list(v) for k, v in hit[0].items()}
    global _favorites_disk_stale
    result = {"song": [], "playlist": []}
    valid_payload = False
    for entity_type in ("song", "playlist"):
        payload = await _api("GET", f"/favorites?entityType={entity_type}")
        if isinstance(payload, dict) and isinstance(payload.get("items"), list):
            result[entity_type] = [f for f in payload["items"] if isinstance(f, dict)]
            valid_payload = True
    if valid_payload:
        _favorites_disk_stale = False
        _favorites_cache[key] = (result, time.monotonic() + _FAVORITES_TTL_S)
        _atomic_write_json(_disk_favorites_path(), {"savedAt": int(time.time()), "payload": result})
    else:
        disk = _load_disk_json(_disk_favorites_path(), 24 * 3600)
        if isinstance(disk, dict):
            result = {"song": disk.get("song") or [], "playlist": disk.get("playlist") or []}
    return result


async def register_device() -> None:
    if not enabled():
        return
    token = await _auth()
    if not token:
        return
    await _api("POST", "/sync/register", {"deviceId": device_id(), "deviceName": "fnOS 音乐", "deviceType": "fnos"})


def invalidate_caches() -> None:
    global _summary_cache, _favorites_cache, _summary_disk_stale, _favorites_disk_stale
    _summary_cache = None
    _summary_disk_stale = True
    _favorites_cache = {}
    _favorites_disk_stale = True
    _tracks_disk_stale.update(_tracks_cache.keys())
    _tracks_cache.clear()
    _guid_song_registry.clear()


def invalidate_playlist_tracks(playlist_id: str) -> None:
    """立即失效指定歌单的曲目缓存（LWW 操作成功后调用）。

    /playlist-song-operations 由服务器同步应用（同一请求内推进 revision），本地
    300s 曲目缓存若不等同步事件/TTL 过期就失效，用户在取消标星后立即刷新仍会
    命中旧缓存，观感是「取消了但不消失」。"""
    pid = str(playlist_id or "")
    if not pid:
        return
    _tracks_cache.pop(pid, None)
    _tracks_disk_stale.add(pid)


async def sync_poll_loop() -> None:
    """低频轮询服务器增量事件，有变化就让下次列表请求重新拉取。"""
    revision = 0
    await register_device()
    while True:
        if not enabled():
            await asyncio.sleep(10.0)
            continue
        try:
            payload = await _api("GET", f"/sync?sinceRevision={revision}")
            if isinstance(payload, dict):
                next_revision = payload.get("nextRevision")
                if isinstance(next_revision, (int, float)):
                    next_revision = int(next_revision)
                else:
                    next_revision = revision
                changed = bool(payload.get("events")) or bool(payload.get("requiresRefresh"))
                # revision 前进本身就是变更证据；部分服务端不会同时带 events/requiresRefresh。
                changed = changed or next_revision > revision
                if changed:
                    invalidate_caches()
                if next_revision > revision:
                    revision = next_revision
        except Exception as e:
            logger.debug("cerusync poll failed: %s", e)
        await asyncio.sleep(2.0)


# ------------------------------------------------------------------ 收藏回写 --

# TOP[top.ceru.fav] /favorites 独立收藏存储写入（不与「我的喜欢」歌单联动，需配合 op）
async def favorite_create(song: dict) -> dict | None:
    """把一个 Ceru 歌曲（含回写时间戳）写入服务器 song 收藏。"""
    if not enabled() or not song:
        return None
    entity_id = str(song.get("id") or song.get("songmid") or "")
    if not entity_id:
        return None
    occurred_at = int(time.time() * 1000)
    body = {
        "entityType": "song",
        "entityId": entity_id,
        "title": song.get("title") or song.get("name") or "未命名歌曲",
        "source": song.get("source") or "",
        "metadata": {"occurredAtMs": occurred_at, "title": song.get("title") or ""},
        "occurredAtMs": occurred_at,
    }
    return await _api("POST", "/favorites", body)


async def favorite_delete(song: dict) -> dict | None:
    if not enabled() or not song:
        return None
    entity_id = str(song.get("id") or song.get("songmid") or "")
    if not entity_id:
        return None
    body = {"entityType": "song", "entityId": entity_id, "occurredAtMs": int(time.time() * 1000)}
    return await _api("DELETE", "/favorites", body)


async def favorite_delete_by_entity_id(entity_id: str) -> dict | None:
    if not enabled() or not entity_id:
        return None
    return await _api("DELETE", "/favorites", {"entityType": "song", "entityId": entity_id, "occurredAtMs": int(time.time() * 1000)})

# ------------------------------------------------------------------ favorites 歌单操作回写 --

# 服务器把 semanticType=favorites 的歌单（“我的喜欢”）视为收藏本体：歌单曲目
# 只能走 /playlist-song-operations（LWW 多端协议）增删，直接改歌单曲目会被
# 服务器拒绝（favorites-require-operation）。/favorites 是另一套独立存储，
# 不会联动“我的喜欢”，因此标星/取消标星的多端同步必须走 operation。


FAVORITES_SEMANTIC = "favorites"


async def favorites_playlist_ids() -> list[str]:
    """语义为 favorites 的歌单 id 列表（如“我的喜欢”）。"""
    try:
        playlists = await list_playlists()
    except Exception:
        return []
    out: list[str] = []
    for pl in playlists or []:
        if not isinstance(pl, dict):
            continue
        semantic = str(pl.get("semanticType") or pl.get("semantic_type") or "")
        title = str(pl.get("title") or pl.get("name") or "")
        if semantic == FAVORITES_SEMANTIC or title in ("我的喜欢", "我的收藏"):
            pid = str(pl.get("id") or "")
            if pid and pid not in out:
                out.append(pid)
    return out


def _op_sequence_next(track_key: str) -> int:
    """同设备对同一 trackKey 的单调递增序号（LWW 判定用），持久化到 state。"""
    state = _load_state()
    seqs = state.get("operationSequences")
    if not isinstance(seqs, dict):
        seqs = {}
    next_seq = int(seqs.get(track_key) or 0) + 1
    seqs[track_key] = next_seq
    if len(seqs) > 2000:  # 防止 map 无限增长：只保留较新的条目
        for k in list(seqs)[: len(seqs) - 2000]:
            seqs.pop(k, None)
    try:
        _save_state({**state, "operationSequences": seqs})
    except Exception:
        logger.debug("operation sequence persist failed", exc_info=True)
    return next_seq


# TOP[top.ceru.op] 歌单曲目 LWW 增删：operationId/deviceId/sequence，服务器广播给其他设备
async def playlist_song_operation(
    playlist_id: str,
    action: str,
    track_key: str,
    song: dict | None = None,
) -> dict | None:
    """对 favorites 歌单做 LWW 增删（add/remove），服务器广播事件给其他设备。"""
    if not enabled() or not playlist_id or not track_key:
        return None
    body: dict[str, Any] = {
        "operationId": uuid4().hex,
        "deviceId": device_id(),
        "playlistId": playlist_id,
        "action": action,
        "sequence": _op_sequence_next(track_key),
        "trackKey": track_key,
        "occurredAtMs": int(time.time() * 1000),
    }
    if action == "add" and song:
        body["song"] = song
    return await _api("POST", "/playlist-song-operations", body)


# ------------------------------------------------------------------ 歌曲映射 --

def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def song_source(song: dict) -> str:
    raw = song.get("raw") if isinstance(song.get("raw"), dict) else {}
    return (_as_text(song.get("source")) or _as_text(raw.get("source")) or "local").lower()


def song_id(song: dict) -> str:
    raw = song.get("raw") if isinstance(song.get("raw"), dict) else {}
    value = _as_text(song.get("id")) or _as_text(song.get("songmid")) or _as_text(raw.get("songmid")) or _as_text(raw.get("hash"))
    src = song_source(song)
    if src and value.lower().startswith(src + ":"):
        value = value[len(src) + 1 :].strip()
    return value


def song_track_key(song: dict) -> str:
    return f"{song_source(song)}:{song_id(song)}"


def song_title(song: dict) -> str:
    return _as_text(song.get("title")) or _as_text(song.get("name"))


def song_artist(song: dict) -> str:
    return _as_text(song.get("artist")) or _as_text(song.get("singer"))


def song_album(song: dict) -> str:
    return _as_text(song.get("album")) or _as_text(song.get("albumName"))


def song_duration_s(song: dict) -> float:
    raw = song.get("raw") if isinstance(song.get("raw"), dict) else {}
    value = song.get("duration_s")
    if value in (None, "", 0):
        value = song.get("durationText") or song.get("interval") or raw.get("interval")
    if value is None:
        return 0.0
    text = str(value).strip()
    if ":" in text:
        parts = text.split(":")
        try:
            return int(parts[0]) * 60 + int(parts[1])
        except (ValueError, IndexError):
            return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def song_artwork(song: dict) -> str:
    return _as_text(song.get("artworkUrl")) or _as_text(song.get("img"))


def lx_item_for_song(song: dict) -> dict | None:
    """Ceru 歌曲 -> 洛雪 item（直接映射，无网络）。不支持或信息不足返回 None。"""
    src = song_source(song)
    if src not in _LX_PLATFORMS:
        return None
    sid = song_id(song)
    if not sid:
        return None
    raw = song.get("raw") if isinstance(song.get("raw"), dict) else {}

    identifier = sid
    if src == "kg":
        candidate = _as_text(song.get("hash")) or _as_text(raw.get("hash"))
        # 酷狗播放主键必须是真的 32 位 file hash；Ceru 导入歌单里常见只有
        # audio_id 没有 hash，继续直连会被源脚本判成无效 ID（500/no URL）。
        # 返回 None 让上层按标题+歌手重新搜索，拿到安卓版同款 hash 再播放。
        if not _HEX32_RE.fullmatch(candidate):
            return None
        identifier = candidate
    if not identifier:
        return None

    item: dict[str, Any] = {
        "id": f"lx:{src}:{identifier}",
        "source": "lx",
        "lx_source": src,
        "title": song_title(song),
        "artist": song_artist(song),
        "album": song_album(song),
        "duration_s": song_duration_s(song),
        "ext": "mp3",
        "cover_url": song_artwork(song),
        "file_size": 0,
        "lyric": "",
    }
    if src == "kg":
        item["hash"] = identifier
    elif src == "tx":
        item["songmid"] = identifier
        media = _as_text(raw.get("strMediaMid")) or _as_text(raw.get("str_media_mid"))
        if media:
            item["str_media_mid"] = media
    elif src == "mg":
        item["copyright_id"] = _as_text(raw.get("copyrightId")) or identifier
    elif src == "kw":
        item["rid"] = identifier
    elif src == "wy":
        item["song_id"] = identifier
    return item


# TOP[top.ceru.registry] guid→歌曲注册表（内存态，重启即空；靠平台主键/歌名+歌手兜底）
def register_guid(guid: str, song: dict) -> None:
    global _guid_persist_last
    if guid:
        _guid_song_registry[guid] = dict(song)
        now = time.monotonic()
        if now - _guid_persist_last >= _GUID_PERSIST_INTERVAL_S:
            _guid_persist_last = now
            flush_guid_map()


def flush_guid_map() -> None:
    """强制把当前 guid→歌曲映射完整落盘（批量登记后调用）。"""
    try:
        existing = _load_disk_json(_guid_map_path(), 365 * 86400.0)
        merged = dict(existing) if isinstance(existing, dict) else {}
        merged.update(_guid_song_registry)
        _guid_song_registry.update(merged)
        _atomic_write_json(_guid_map_path(), {
            "savedAt": int(time.time()),
            "payload": merged,
        })
    except Exception:
        pass


def song_for_guid(guid: str) -> dict | None:
    hit = _guid_song_registry.get(guid)
    if hit:
        return hit
    data = _load_disk_json(_guid_map_path(), 365 * 86400.0)
    if isinstance(data, dict):
        _guid_song_registry.update({k: v for k, v in data.items() if isinstance(v, dict)})
        return _guid_song_registry.get(guid)
    return None


def registered_guids() -> list[str]:
    """返回所有已持久化的 Ceru guid，供代理启动时重建假 id 反查表。"""
    data = _load_disk_json(_guid_map_path(), 365 * 86400.0)
    if isinstance(data, dict):
        _guid_song_registry.update({k: v for k, v in data.items() if isinstance(v, dict)})
    return list(_guid_song_registry.keys())


def clear_guid_registry() -> None:
    _guid_song_registry.clear()


def reset_for_test() -> None:
    global _http_client, _http_base_url, _state, _summary_cache, _favorites_cache, _summary_disk_stale, _favorites_disk_stale
    _http_client = None
    _http_base_url = ""
    _state = None
    _summary_disk_stale = False
    _summary_cache = None
    _favorites_cache = {}
    _tracks_cache.clear()
    _tracks_disk_stale.clear()
    _favorites_disk_stale = False
    _guid_song_registry.clear()
    for task in list(_load_tasks.values()) + list(_prefetch_tasks):
        if not task.done():
            task.cancel()
    _load_tasks.clear()
    _prefetch_tasks.clear()


def schedule_prefetch(build_track: Callable[[dict], dict | None], cards: list[dict]) -> None:
    """串行后台预取各歌单曲目，避免首次点开一个上千首歌单时阻塞。"""
    if not config()["prefetch"]:
        return
    pending = [str(c.get("id") or "") for c in cards if str(c.get("id") or "")]
    if not pending:
        return

    async def _runner() -> None:
        for pid in pending:
            try:
                songs = await list_playlist_songs(pid)
                for song in songs:
                    if isinstance(song, dict):
                        track = build_track(song)
                        if track and track.get("guid"):
                            register_guid(str(track["guid"]), song)
            except Exception as e:
                logger.debug("cerusync prefetch failed (%s): %s", pid, e)

    try:
        task = asyncio.get_running_loop().create_task(_runner())
    except RuntimeError:
        return
    _prefetch_tasks.add(task)
    task.add_done_callback(_prefetch_tasks.discard)
