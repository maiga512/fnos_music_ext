# TOP[top.imp] 导入歌单：洛雪导出 JSON + 五平台公开歌单链接 → 本地 JSON → 注入音乐页
"""导入外部歌单：洛雪歌单文件 + 公开歌单链接 → 音乐页只读虚拟歌单。

两条来源（与安卓版树夏music 的导入能力对齐）：
1) 洛雪音乐导出 JSON：playList_v2 / playListPart_v2 / allData_v2 / 裸歌单数组，
   字段兼容形状取自 Shuxia Music 的 LxMusicPlaylistFormat；
2) 平台公开歌单链接或分享口令：QQ音乐 / 网易云 / 酷我 / 酷狗 / 咪咕，
   链接识别规则与取列表接口逐条对齐安卓 LinkParser + MusicApiService.fetch*PlaylistDirect。

落盘 <data>/imported_playlists/<id>.json，每首歌一条
{source, id, name, singer, album, interval, cover}，source/id 就是洛雪主键
（kg=hash、tx=songmid、kw=rid、mg=copyrightId、wy=songId），因此播放完全复用
既有洛雪取链链路，本模块不碰取流。

音乐页歌单 guid 为 online:playlist:imp:<id>，与 nm/ceru 歌单同款只读语义：
在音乐页往里加歌、移歌、删除都不回写来源。列表侧只做确定性直连映射，绝不整单
搜索：洛雪用户源有按天配额与限流，开一屏搜一屏会把正常播放打成失败。
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import uuid

import httpx

logger = logging.getLogger("proxy.playlist_import")

IMP_PREFIX = "online:playlist:imp:"
MAX_SONGS = 2000                # 单个导入歌单落盘上限（超出截断并在名字上标注）
_FETCH_TIMEOUT_S = 20.0

# 平台分享链接里的中文名 → 洛雪平台码
PLATFORM_BY_LABEL = {
    "网易云": "wy", "网易云音乐": "wy",
    "QQ音乐": "tx", "qq音乐": "tx",
    "酷我音乐": "kw", "酷我": "kw",
    "酷狗音乐": "kg", "酷狗": "kg",
    "咪咕音乐": "mg", "咪咕": "mg",
}
LABEL_BY_PLATFORM = {"wy": "网易云", "tx": "QQ音乐", "kw": "酷我音乐",
                     "kg": "酷狗音乐", "mg": "咪咕音乐"}
LX_SOURCES = ("wy", "tx", "kw", "kg", "mg")

# 链接识别顺序与安卓 LinkParser 一致：网易云 → QQ → 酷我 → 酷狗 → 咪咕。
# 每组的第一个捕获组就是歌单 id（短链组捕获到的是跳转码，需先展开）。
_LINK_PATTERNS: list[tuple[str, list[re.Pattern]]] = [
    ("wy", [
        re.compile(r"music\.163\.com/playlist\?id=(\d+)"),
        # 新版客户端分享的是路径形式：music.163.com/playlist/3778678/
        re.compile(r"music\.163\.com/(?:m/|#/)?playlist/(\d+)"),
        re.compile(r"music\.163\.com/#/playlist\?id=(\d+)"),
        re.compile(r"music\.163\.com/m/playlist\?id=(\d+)"),
        re.compile(r"y\.music\.163\.com.*[?&]id=(\d+)"),
        re.compile(r"music\.163\.com.*[?&]id=(\d+)"),
    ]),
    ("tx", [
        re.compile(r"y\.qq\.com/n/ryqq/playlist/(\d+)"),
        re.compile(r"i\.y\.qq\.com/n2/m/share/details/taoge\.html\?.*id=(\d+)"),
        re.compile(r"qq\.com.*[?&]id=(\d{7,})"),
        re.compile(r"_qq.*[&?]id=(\d{7,})"),
        re.compile(r"[&?]id=(\d{7,}).*ADTAG"),
    ]),
    ("kw", [
        re.compile(r"kuwo\.cn/playlist_detail/(\d+)"),
        re.compile(r"m\.kuwo\.cn/newh5app/playlist_detail/(\d+)"),
        re.compile(r"kuwo\.cn.*playlist.*?(\d{5,})"),
        re.compile(r"m\.kuwo\.cn.*?(\d{5,})"),
    ]),
    ("kg", [
        re.compile(r"kugou\.com/songlist/([A-Za-z0-9_-]+)"),
        re.compile(r"m\.kugou\.com/.*?listid=(\d+)"),
        re.compile(r"kugou\.com.*special.*?(\d{5,})"),
        re.compile(r"m\.kugou\.com.*?(\d{5,})"),
    ]),
    ("mg", [
        re.compile(r"music\.migu\.cn/v3/music/playlist/(\d+)"),
        re.compile(r"music\.migu\.cn/v5/#/playlist\?playlistId=(\d+)"),
        re.compile(r"music\.migu\.cn.*[?&]playlistId=(\d+)"),
        re.compile(r"music\.migu\.cn.*[?&]id=(\d+)"),
        re.compile(r"h5\.nf\.migu\.cn/.*/playlist/.*[?&]id=(\d+)"),
    ]),
]
_URL_RE = re.compile(r"https?://[^\s<>\"{}|\\^`\[\]]+")
_SHORT_LINK_RE = re.compile(r"(?:163cn\.tv|c\.y\.qq\.com/base/fcgi-bin/u|m\.kugou\.com/t)/([A-Za-z0-9]+)")


def imp_playlist_id_from_guid(guid: str) -> str:
    if not str(guid or "").startswith(IMP_PREFIX):
        return ""
    return str(guid)[len(IMP_PREFIX):]


def is_imp_playlist_guid(guid: str) -> bool:
    return bool(imp_playlist_id_from_guid(guid or ""))


# ------------------------------------------------------------------ 时长/清洗 ---

def parse_interval(value) -> float:
    """各种时长写法 → 秒。支持秒数、毫秒、"3:25"、"1:02:03"。"""
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value).strip()
        if not text:
            return 0.0
        if ":" in text:
            parts = []
            for piece in text.split(":"):
                try:
                    parts.append(float(piece))
                except ValueError:
                    return 0.0
            if len(parts) == 2:
                return max(parts[0] * 60 + parts[1], 0.0)
            if len(parts) == 3:
                return max(parts[0] * 3600 + parts[1] * 60 + parts[2], 0.0)
            return 0.0
        try:
            number = float(text)
        except ValueError:
            return 0.0
    if number < 0:
        return 0.0
    # 毫秒级数值（网易 duration 常为毫秒）：明显超过 2 小时的秒数按毫秒处理
    if number > 7200:
        number /= 1000.0
    return round(number, 2)


def _text(*values) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _cover(*values) -> str:
    """只接受 http(s) 图片地址，顺手把洛雪/酷狗的 {size} 占位换成 400。"""
    for value in values:
        url = _text(value)
        if url.startswith("//"):
            url = "https:" + url
        if url.lower().startswith(("http://", "https://")):
            return url.replace("{size}", "400")
    return ""


def make_song(source: str, song_id: str, name: str, singer: str = "", album: str = "",
              interval=0, cover: str = "") -> dict | None:
    src = _text(source).lower()
    if src not in LX_SOURCES:
        return None
    title = _text(name)
    if not title:
        return None
    return {
        "source": src,
        "id": _text(song_id),
        "name": title,
        "singer": _text(singer) or "未知",
        "album": _text(album),
        "interval": parse_interval(interval),
        "cover": _cover(cover),
    }


def song_item(song: dict) -> dict:
    """导入歌曲 → 代理在线条目（build_online_track 的入参形状）。"""
    src = _text(song.get("source")).lower() or "kg"
    sid = _text(song.get("id"))
    interval = parse_interval(song.get("interval"))
    if not sid:
        # 没有平台主键时用标题+歌手做稳定占位键：直连解析必失败，
        # 点播时代理会按标题+歌手懒搜索找回（与 Ceru 占位同一套路）。
        uniq = f"{_text(song.get('name'))}|{_text(song.get('singer'))}|{_text(song.get('album'))}"
        sid = "ph" + hashlib.md5(uniq.encode("utf-8")).hexdigest()[:30]
    return {
        "id": f"lx:{src}:{sid}",
        "source": "lx",
        "lx_source": src,
        "title": _text(song.get("name")),
        "artist": _text(song.get("singer")) or "未知",
        "album": _text(song.get("album")),
        "duration_s": interval,
        "ext": "mp3",
        "cover_url": _cover(song.get("cover")),
        "file_size": 0,
        "lyric": "",
    }


# ------------------------------------------------------------------ 存储 ---

_DIR_CACHE: list[str] = []


def set_dir(path: str) -> None:
    """代理启动时注入存储目录（同 nm_playlists_cache 的做法）。"""
    if path and (not _DIR_CACHE or _DIR_CACHE[0] != path):
        _DIR_CACHE.clear()
        _DIR_CACHE.append(path)


def imp_dir() -> str:
    path = _DIR_CACHE[0] if _DIR_CACHE else os.path.join(
        os.environ.get("FNMUSIC_HOME", "."), "imported_playlists")
    os.makedirs(path, exist_ok=True)
    return path


def _path(pid: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "", str(pid or ""))
    return os.path.join(imp_dir(), f"{safe}.json")


def read_record(pid: str) -> dict | None:
    try:
        with open(_path(pid), "r", encoding="utf-8") as fh:
            record = json.load(fh)
    except (OSError, ValueError):
        return None
    if isinstance(record, dict) and isinstance(record.get("songs"), list):
        return record
    return None


def write_record(record: dict) -> dict:
    songs = [s for s in (record.get("songs") or []) if isinstance(s, dict) and _text(s.get("name"))]
    record["songs"] = songs
    record["count"] = len(songs)
    record.setdefault("id", "imp" + uuid.uuid4().hex[:12])
    record.setdefault("created_at", int(time.time()))
    record["updated_at"] = int(time.time())
    path = _path(record["id"])
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(record, fh, ensure_ascii=False)
    os.replace(tmp, path)
    return record


def list_records() -> list[dict]:
    """全部导入歌单（不含歌曲正文，按导入时间倒序）。"""
    out: list[dict] = []
    try:
        names = os.listdir(imp_dir())
    except OSError:
        return out
    for name in names:
        if not name.endswith(".json") or name.endswith(".tmp"):
            continue
        record = read_record(name[:-5])
        if not record:
            continue
        out.append(_summary(record))
    out.sort(key=lambda item: item.get("created_at") or 0, reverse=True)
    return out


def delete(pid: str) -> bool:
    try:
        os.remove(_path(pid))
    except OSError:
        return False
    return True


def _summary(record: dict) -> dict:
    return {
        "id": str(record.get("id") or ""),
        "guid": IMP_PREFIX + str(record.get("id") or ""),
        "name": _text(record.get("name")) or "导入歌单",
        "cover": _cover(record.get("cover")),
        "origin": _text(record.get("origin")),
        "origin_id": _text(record.get("origin_id")),
        "platform": _text(record.get("platform")),
        "count": int(record.get("count") or len(record.get("songs") or [])),
        "created_at": int(record.get("created_at") or 0),
        "updated_at": int(record.get("updated_at") or 0),
        "refreshable": bool(_text(record.get("origin")) == "link" and _text(record.get("origin_id"))),
    }


def summary_of(record: dict) -> dict:
    """对外暴露的记录摘要（代理注入歌单卡片时复用）。"""
    return _summary(record)


def tracks_page(pid: str, start: int, size: int) -> tuple[list[dict], int]:
    record = read_record(pid)
    if not record:
        return [], 0
    songs = record.get("songs") or []
    start = max(int(start or 0), 0)
    size = max(int(size or 50), 1)
    return songs[start:start + size], len(songs)


# ------------------------------------------------------------------ 链接识别 ---

def parse_link(text: str) -> tuple[str, str] | None:
    """分享文本/链接 → (平台码, 歌单 id)。识别不出返回 None。"""
    raw = _text(text)
    if not raw:
        return None
    candidates = [raw] + _URL_RE.findall(raw)
    for hop in range(2):                      # 第二轮处理展开后的短链
        for candidate in candidates:
            for platform, patterns in _LINK_PATTERNS:
                for pattern in patterns:
                    match = pattern.search(candidate)
                    if match:
                        return platform, normalize_playlist_id(platform, match.group(1))
        expanded = [_text(url) for url in _SHORT_LINK_RE.findall("\n".join(candidates))]
        if not expanded or hop == 0:
            break
        candidates = expanded
    return None


def normalize_playlist_id(platform: str, raw: str) -> str:
    """酷狗分享链接里的 gcid_/id_ 是前缀而非 id 本身，取列表接口只认后面那段。"""
    value = _text(raw)
    if _text(platform).lower() == "kg":
        for prefix in ("gcid_", "gc_", "id_"):
            if value.lower().startswith(prefix):
                return value[len(prefix):]
    return value


def _shorten_songs(songs: list[dict], name: str) -> tuple[str, list[dict]]:
    if len(songs) <= MAX_SONGS:
        return name, songs
    return f"{name}（前{MAX_SONGS}首）", songs[:MAX_SONGS]


# ------------------------------------------------------------------ 洛雪 JSON ---

def decode_lx_json(text: str) -> list[dict]:
    """洛雪导出 JSON → [{name, source, source_list_id, songs:[...]}]。

    兼容 playList_v2 / playListPart_v2 / allData_v2 / allData、userList 容器、
    裸歌单数组，以及更早版本导出的未包装歌单。
    """
    raw = _text(text)
    if not raw:
        return []
    try:
        node = json.loads(raw)
    except ValueError:
        return []
    if isinstance(node, list):
        docs = _decode_array(node)
    elif isinstance(node, dict):
        docs = _decode_object(node)
    else:
        docs = []
    return [doc for doc in docs if doc.get("songs")]


def _decode_array(items: list) -> list[dict]:
    first = items[0] if items and isinstance(items[0], dict) else None
    if first and ("list" in first or "songs" in first):
        return [_decode_playlist(item) for item in items if isinstance(item, dict)]
    if first and "source" in first:           # 裸歌曲数组
        return [{"name": "导入歌单", "source": _text(first.get("source")),
                 "source_list_id": "", "songs": _decode_songs(items, _text(first.get("source")))}]
    return []


def _decode_object(root: dict) -> list[dict]:
    kind = _text(root.get("type"))
    if kind == "playListPart_v2":
        data = root.get("data")
        return [_decode_playlist(data)] if isinstance(data, dict) else []
    if kind == "playList_v2":
        data = root.get("data")
        return [_decode_playlist(it) for it in data if isinstance(it, dict)] if isinstance(data, list) else []
    if kind in ("allData_v2", "allData"):
        data = root.get("playList")
        return [_decode_playlist(it) for it in data if isinstance(it, dict)] if isinstance(data, list) else []

    # 少数导出/二次保存的文件把歌单直接放在键里（没有 type 字段），一并认下：
    # {"playList_v2":[...]} / {"allData_v2":{"playList":[...]}} / {"playList":[...]}
    for key in ("playList_v2", "playListPart_v2", "playList", "allData_v2", "allData"):
        value = root.get(key)
        if isinstance(value, list):
            return [_decode_playlist(it) for it in value if isinstance(it, dict)]
        if isinstance(value, dict):
            nested = value.get("playList")
            if isinstance(nested, list):
                return [_decode_playlist(it) for it in nested if isinstance(it, dict)]
            return [_decode_playlist(value)]

    docs: list[dict] = []
    container_needed = any(key in root for key in ("defaultList", "loveList", "tempList", "userList"))
    if container_needed:
        for key, default_name, default_id in (
            ("defaultList", "试听列表", "default"), ("loveList", "我的收藏", "love"),
        ):
            doc = _decode_container(root.get(key), default_name, default_id)
            if doc:
                docs.append(doc)
        users = root.get("userList")
        if isinstance(users, list):
            docs.extend(_decode_array(users))
        doc = _decode_container(root.get("tempList"), "临时列表", "temp")
        if doc:
            docs.append(doc)
        return [item for item in docs if item.get("songs")]
    if "list" in root or "songs" in root:
        return [_decode_playlist(root)]
    return []


def _decode_container(value, default_name: str, default_id: str) -> dict | None:
    if isinstance(value, dict):
        return _decode_playlist(value, default_name)
    if isinstance(value, list):
        return {"id": default_id, "name": default_name, "source": "",
                "source_list_id": "", "songs": _decode_songs(value, "")}
    return None


def _decode_playlist(node: dict, default_name: str = "导入歌单") -> dict:
    source = _text(node.get("source")).lower()
    songs = node.get("list")
    if not isinstance(songs, list):
        songs = node.get("songs")
    return {
        "id": _text(node.get("id")) or "imported",
        "name": _text(node.get("name"), node.get("title")) or default_name,
        "source": source,
        "source_list_id": _text(node.get("sourceListId")),
        "songs": _decode_songs(songs or [], source),
    }


def _decode_songs(items: list, playlist_source: str) -> list[dict]:
    songs: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for index, raw in enumerate(items):
        if not isinstance(raw, dict):
            continue
        song = _decode_song(raw, playlist_source, index)
        if not song:
            continue
        key = (song["source"], song["id"] or song["name"])
        if key in seen:
            continue
        seen.add(key)
        songs.append(song)
    return songs[:MAX_SONGS]


def _decode_song(node: dict, playlist_source: str, index: int) -> dict | None:
    source = (_text(node.get("source")) or playlist_source).lower()
    meta = node.get("meta") if isinstance(node.get("meta"), dict) else {}
    song_id = _text(
        node.get("songmid"), node.get("platformId"), meta.get("songId"),
        str(node.get("id") or "").removeprefix(f"{source}_"),
    )
    interval = node.get("interval")
    if interval is None:
        interval = node.get("durationMs", node.get("duration"))
    return make_song(
        source, song_id,
        _text(node.get("name"), node.get("title")),
        _text(node.get("singer"), node.get("artist")) or "未知",
        _text(node.get("albumName"), node.get("album"), meta.get("albumName")),
        interval,
        _text(node.get("img"), node.get("coverUrl"), meta.get("picUrl")),
    )


# ------------------------------------------------------------------ 平台抓取 ---

async def _get_json(client: httpx.AsyncClient, url: str, headers: dict):
    try:
        resp = await client.get(url, headers=headers, timeout=_FETCH_TIMEOUT_S)
        if resp.status_code != 200:
            return None
        text = resp.text or ""
    except httpx.HTTPError as exc:
        logger.info("导入歌单请求失败 %s: %s", url[:90], type(exc).__name__)
        return None
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        return json.loads(text[start:end + 1])     # 兼容 jsonp 包裹
    except ValueError:
        return None


async def _fetch_tx(client: httpx.AsyncClient, pid: str) -> dict | None:
    headers = {"Referer": "https://y.qq.com", "User-Agent":
               "Mozilla/5.0 (Linux; Android 12) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36"}
    urls = (
        f"https://c.y.qq.com/v8/fcg-bin/fcg_v8_playlist_cp.fcg?newsong=1&id={pid}&format=json&inCharset=utf8&outCharset=utf-8",
        "https://i.y.qq.com/qzone-music/fcg-bin/fcg_ucc_getcdinfo_byids_cp.fcg?type=1&json=1"
        f"&utf8=1&onlysong=0&new_format=1&disstid={pid}&format=json",
    )
    for url in urls:
        node = await _get_json(client, url, headers)
        if not isinstance(node, dict):
            continue
        data = node.get("data") if isinstance(node.get("data"), dict) else {}
        cdlist = node.get("cdlist") or data.get("cdlist")
        cd = cdlist[0] if isinstance(cdlist, list) and cdlist and isinstance(cdlist[0], dict) else data
        if not isinstance(cd, dict):
            continue
        top = data.get("topinfo") if isinstance(data.get("topinfo"), dict) else {}
        raw_dirinfo = cd.get("dirinfo")
        dirinfo = {}
        if isinstance(raw_dirinfo, str) and raw_dirinfo.strip().startswith("{"):
            try:
                dirinfo = json.loads(raw_dirinfo)
            except ValueError:
                dirinfo = {}
        name = _text(cd.get("dissname"), dirinfo.get("title"), top.get("ListName"),
                     cd.get("title")) or "QQ音乐歌单"
        cover = _cover(cd.get("logo"), top.get("pic"), top.get("pic_v12"),
                       top.get("headPic_v12"), top.get("headPic"), dirinfo.get("picurl"))
        rows = cd.get("songlist") or data.get("songlist") or data.get("list")
        if not isinstance(rows, list) or not rows:
            continue
        songs: list[dict] = []
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            item = raw.get("data") if isinstance(raw.get("data"), dict) else (
                raw.get("songInfo") if isinstance(raw.get("songInfo"), dict) else raw)
            mid = _text(item.get("songmid"), item.get("songMid"), item.get("mid"))
            album_mid = _text(item.get("albummid"), item.get("albumMid"), item.get("albumPMid"),
                              (item.get("album") or {}).get("mid") if isinstance(item.get("album"), dict) else "")
            singers = item.get("singer")
            artist = "/".join(
                _text(row.get("name")) for row in singers
                if isinstance(row, dict) and _text(row.get("name"))
            ) if isinstance(singers, list) else ""
            song = make_song(
                "tx", mid,
                _text(item.get("songname"), item.get("title"), item.get("name")),
                artist or "未知",
                _text(item.get("albumname"), item.get("albumName"),
                      (item.get("album") or {}).get("name") if isinstance(item.get("album"), dict) else ""),
                item.get("interval"),
                f"https://y.gtimg.cn/music/photo_new/T002R300x300M000{album_mid}.jpg" if album_mid else "",
            )
            if song:
                songs.append(song)
        if songs:
            name, songs = _shorten_songs(songs, name)
            return {"name": name, "cover": cover, "songs": songs, "platform": "tx"}
    return None


async def _fetch_kw(client: httpx.AsyncClient, pid: str) -> dict | None:
    url = ("http://nplserver.kuwo.cn/pl.svc?op=getlistinfo&pn=0&rn=1000&encode=utf8"
           f"&keyset=pl2012&identity=kuwo&pcmp4=1&vipver=MUSIC_9.0.5.0_W1&newver=1&pid={pid}")
    node = await _get_json(client, url, {"Referer": "http://www.kuwo.cn/"})
    if not isinstance(node, dict):
        return None
    rows = node.get("musiclist") or node.get("musicList")
    if not isinstance(rows, list):
        return None
    songs: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        rid = _text(row.get("id"), row.get("rid"), str(row.get("musicrid") or "").removeprefix("MUSIC_"))
        song = make_song(
            "kw", rid, _text(row.get("name"), row.get("songName")),
            _text(row.get("artist"), row.get("artistName")) or "未知",
            _text(row.get("album"), row.get("albumName")),
            row.get("duration"),
            _cover(row.get("pic"), row.get("albumpic"), node.get("pic"), node.get("pic300")),
        )
        if song:
            songs.append(song)
    if not songs:
        return None
    name = _text(node.get("title"), node.get("name")) or "酷我歌单"
    name, songs = _shorten_songs(songs, name)
    return {"name": name, "cover": _cover(node.get("pic"), node.get("pic300"), node.get("img")),
            "songs": songs, "platform": "kw"}


async def _fetch_kg(client: httpx.AsyncClient, pid: str) -> dict | None:
    url = f"http://www2.kugou.kugou.com/yueku/v9/special/single/{pid}-5-9999.html"
    try:
        resp = await client.get(url, headers={"Referer": "https://www.kugou.com/"},
                                timeout=_FETCH_TIMEOUT_S)
        body = resp.text or ""
        if resp.status_code != 200 or not body:
            return None
    except httpx.HTTPError:
        return None
    match = re.search(r"global\.data\s*=\s*(\[.*?\]);", body, re.S)
    if not match:
        return None
    try:
        rows = json.loads(match.group(1))
    except ValueError:
        return None
    name_match = re.search(r'name:\s*"([^"]+)"', body, re.S)
    cover_match = re.search(r'pic:\s*"([^"]+)"', body, re.S)
    cover = _cover(cover_match.group(1) if cover_match else "")
    songs: list[dict] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        trans = row.get("trans_param") if isinstance(row.get("trans_param"), dict) else {}
        song = make_song(
            "kg", _text(row.get("hash"), row.get("hash_320"), row.get("hash_flac")),
            _text(row.get("songname"), str(row.get("filename") or "").split(" - ")[0]),
            _text(row.get("singername")) or "未知",
            _text(row.get("album_name"), row.get("album")),
            row.get("duration"),
            _cover(trans.get("union_cover"), cover),
        )
        if song:
            songs.append(song)
    if not songs:
        return None
    name = (name_match.group(1).replace('\\"', '"') if name_match else "") or "酷狗歌单"
    name, songs = _shorten_songs(songs, name)
    return {"name": name, "cover": cover, "songs": songs, "platform": "kg"}


def _migu_cover(value) -> str:
    url = _text(value)
    if url.startswith("/"):
        return "https://d.musicapp.migu.cn" + url
    return _cover(url)


def _artist_from(rows) -> str:
    if not isinstance(rows, list):
        return ""
    names = [_text(row.get("name"), row.get("singerName"))
             for row in rows if isinstance(row, dict)]
    return "/".join([name for name in names if name])


async def _fetch_mg(client: httpx.AsyncClient, pid: str) -> dict | None:
    headers = {"Referer": "https://m.music.migu.cn/", "User-Agent":
               "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 "
               "(KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1"}
    info = await _get_json(
        client,
        f"https://c.musicapp.migu.cn/MIGUM3.0/resource/playlist/v2.0?playlistId={pid}",
        headers)
    if not isinstance(info, dict) or _text(info.get("code")) != "000000":
        return None
    meta = info.get("data") if isinstance(info.get("data"), dict) else {}
    img = meta.get("imgItem") if isinstance(meta.get("imgItem"), dict) else {}
    name = _text(meta.get("title")) or "咪咕歌单"
    cover = _cover(img.get("img"), meta.get("image"), meta.get("pic"))
    songs: list[dict] = []
    seen: set[str] = set()
    total = 1 << 30
    page = 1
    while len(songs) < total and page <= 20:
        node = await _get_json(
            client,
            "https://app.c.nf.migu.cn/MIGUM3.0/resource/playlist/song/v2.0"
            f"?pageNo={page}&pageSize=50&playlistId={pid}", headers)
        if not isinstance(node, dict) or _text(node.get("code")) != "000000":
            break
        data = node.get("data") if isinstance(node.get("data"), dict) else {}
        total = int(data.get("totalCount") or 0) or len(songs)
        rows = data.get("songList")
        if not isinstance(rows, list) or not rows:
            break
        for row in rows:
            if not isinstance(row, dict):
                continue
            copyright_id = _text(row.get("copyrightId"), row.get("songId"), row.get("id"))
            if not copyright_id or copyright_id in seen:
                continue
            seen.add(copyright_id)
            song = make_song(
                "mg", copyright_id,
                _text(row.get("songName"), row.get("name")),
                _artist_from(row.get("singerList")) or _artist_from(row.get("artists")) or "未知",
                _text(row.get("album"), row.get("albumName")),
                row.get("duration"),
                _migu_cover(row.get("img3")) or _migu_cover(row.get("img2"))
                or _migu_cover(row.get("img1")),
            )
            if song:
                songs.append(song)
        if len(rows) < 50:
            break
        page += 1
    if not songs:
        return None
    name, songs = _shorten_songs(songs, name)
    return {"name": name, "cover": cover, "songs": songs, "platform": "mg"}


async def _fetch_wy(client: httpx.AsyncClient, pid: str) -> dict | None:
    """网易云公开歌单：playlist/detail 拿名字封面与曲目，缺详情时批量补。

    对齐安卓的做法但不碰 linuxapi/weapi 加密：公开歌单用官方 Web 接口足够，
    需要登录的私密歌单不在本功能范围内。
    """
    headers = {"Referer": "https://music.163.com/", "Origin": "https://music.163.com",
               "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                             "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
               "Cookie": "os=pc"}
    playlist = None
    for url in (f"https://music.163.com/api/playlist/detail?id={pid}",
                f"https://music.163.com/api/v6/playlist/detail?id={pid}&n=100000"):
        node = await _get_json(client, url, headers)
        if not isinstance(node, dict):
            continue
        for candidate in (node.get("result"), node.get("playlist"), node):
            if isinstance(candidate, dict) and (candidate.get("tracks") or candidate.get("trackIds")):
                playlist = candidate
                break
        if playlist:
            break
    if not playlist:
        return None
    name = _text(playlist.get("name")) or "网易云歌单"
    cover = _cover(playlist.get("coverImgUrl"), playlist.get("coverUrl"), playlist.get("picUrl"))
    tracks = playlist.get("tracks") if isinstance(playlist.get("tracks"), list) else []
    by_id = {}
    for song in _wy_songs(tracks):
        by_id.setdefault(song["id"], song)
    track_ids = [
        _text(row.get("id")) for row in (playlist.get("trackIds") or [])
        if isinstance(row, dict) and _text(row.get("id"))
    ]
    missing = [pid for pid in track_ids if pid not in by_id]
    if missing:
        # v6 详情只带前若干首：只补没拿到的那些（每批 200 首），不重复要已有的
        for song in await _wy_details(client, headers, missing[:MAX_SONGS]):
            by_id.setdefault(song["id"], song)
    order = track_ids or list(by_id)
    songs = [by_id[pid] for pid in order if pid in by_id] or list(by_id.values())
    if not songs:
        return None
    name, songs = _shorten_songs(songs, name)
    return {"name": name, "cover": cover, "songs": songs, "platform": "wy"}


def _wy_songs(rows: list) -> list[dict]:
    songs: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        album = row.get("album") if isinstance(row.get("album"), dict) else {}
        song = make_song(
            "wy", _text(row.get("id"), row.get("songId")),
            _text(row.get("name"), row.get("title")),
            _artist_from(row.get("artists")) or _text(row.get("artistName")) or "未知",
            _text(album.get("name")),
            row.get("duration"),
            _cover(album.get("picUrl"), album.get("blurPicUrl"), row.get("coverUrl")),
        )
        if song:
            songs.append(song)
    return songs


async def _wy_details(client: httpx.AsyncClient, headers: dict, ids: list[str]) -> list[dict]:
    """api/song/detail 批量补详情（每批 200 首，公开接口，无需加密）。"""
    songs: list[dict] = []
    for start in range(0, len(ids), 200):
        batch = ids[start:start + 200]
        joined = ",".join(str(item) for item in batch)
        node = await _get_json(client, f"https://music.163.com/api/song/detail?ids=[{joined}]", headers)
        if not isinstance(node, dict):
            continue
        songs.extend(_wy_songs(node.get("songs") or []))
        if len(songs) >= MAX_SONGS:
            break
    return songs[:MAX_SONGS]


_PLATFORM_FETCHERS = {"tx": _fetch_tx, "kw": _fetch_kw, "kg": _fetch_kg,
                      "mg": _fetch_mg, "wy": _fetch_wy}


async def fetch_platform(client: httpx.AsyncClient, platform: str, playlist_id: str) -> dict | None:
    fetcher = _PLATFORM_FETCHERS.get(_text(platform).lower())
    if not fetcher or not _text(playlist_id):
        return None
    try:
        result = await fetcher(client, str(playlist_id).strip())
    except Exception as exc:  # noqa: BLE001
        logger.warning("导入歌单抓取异常 %s/%s: %s", platform, playlist_id, exc)
        return None
    if not result or not result.get("songs"):
        return None
    return result


# ------------------------------------------------------------------ 导入入口 ---

async def import_from_text(client: httpx.AsyncClient, text: str,
                           name: str = "") -> tuple[list[dict], str]:
    """一段用户输入（链接/口令/洛雪 JSON 全文）→ 落盘的导入歌单列表。

    返回 (records, error)；error 非空表示没导入成任何一首。
    """
    raw = _text(text)
    if not raw:
        return [], "请输入歌单链接或粘贴洛雪导出的歌单内容"
    stripped = raw.lstrip()
    if stripped[:1] in ("{", "["):
        docs = decode_lx_json(raw)
        if docs:
            records = []
            for doc in docs:
                songs = doc["songs"]
                title = _text(name, doc.get("name")) or "导入歌单"
                title, songs = _shorten_songs(songs, title)
                cover = next((song["cover"] for song in songs if song.get("cover")), "")
                records.append(write_record({
                    "name": title, "cover": cover, "origin": "lxjson",
                    "origin_id": _text(doc.get("source_list_id")),
                    "platform": _text(doc.get("source")),
                    "songs": songs,
                }))
            if records:
                return [_summary(item) for item in records], ""
            return [], "洛雪歌单内容里没有找到可播曲目"
    parsed = parse_link(raw)
    if not parsed:
        return [], "无法识别歌单链接，请粘贴完整分享链接或洛雪导出的 JSON 内容"
    platform, playlist_id = parsed
    data = await fetch_platform(client, platform, playlist_id)
    if not data:
        label = LABEL_BY_PLATFORM.get(platform, platform)
        return [], f"{label}歌单读取失败：请确认歌单为公开状态且链接完整"
    title = _text(name, data.get("name")) or f"{LABEL_BY_PLATFORM.get(platform, platform)}歌单"
    songs = data["songs"]
    title, songs = _shorten_songs(songs, title)
    record = write_record({
        "name": title, "cover": _cover(data.get("cover")), "origin": "link",
        "origin_id": playlist_id, "platform": platform, "songs": songs,
    })
    return [_summary(record)], ""


async def refresh(client: httpx.AsyncClient, pid: str) -> tuple[dict | None, str]:
    """重新抓取来源覆盖曲目（仅链接来源可刷新，洛雪文件导入的没有远端可回源）。"""
    record = read_record(pid)
    if not record:
        return None, "导入歌单不存在"
    if _text(record.get("origin")) != "link" or not _text(record.get("origin_id")):
        return None, "该歌单来自本地文件，没有可回源的链接"
    data = await fetch_platform(client, _text(record.get("platform")), _text(record.get("origin_id")))
    if not data:
        return None, "来源歌单读取失败，已保留原有曲目"
    title = _text(record.get("name"), data.get("name")) or "导入歌单"
    songs = data["songs"]
    title, songs = _shorten_songs(songs, title)
    record["name"] = title
    record["cover"] = _cover(data.get("cover")) or _cover(record.get("cover"))
    record["songs"] = songs
    return _summary(write_record(record)), ""
