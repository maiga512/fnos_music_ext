# TOP[top.lxsvc.runtime] 洛雪自定义源运行时：脚本校验 + Node 沙箱 + musicUrl 解析（含源版本号）
"""洛雪自定义源运行时：脚本下载/校验 + Node 沙箱进程管理 + musicUrl 解析。

对齐洛雪桌面版自定义源规范（https://lxmusic.toside.cn/desktop/custom-source）：
- 脚本必须以 ``/* ... */`` 头部注释块开头，元数据字段 @name/@description/@version/@author/@homepage；
- 脚本通过 ``globalThis.lx`` 与宿主交互，事件仅 inited / request / updateAlert；
- 非 local 源的 actions 仅支持 musicUrl（搜索/歌词/封面/榜单走本服务内置平台接口）。

与 js/bridge.js 的进程协议见该文件头注释。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import signal
import shutil
import sysconfig
import tempfile
import time
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import httpx

logger = logging.getLogger("lxmusic_service.runtime")


_LANE_HOST_RE = re.compile(r"https?://([A-Za-z0-9._-]+)")


def script_lane_key(script: str) -> str:
    """把同一个上游运营商的脚本折叠到同一条取链队列上。

    限流是按中转域名（多数还按 API key）计的：聆澜赞助版 v8.5/v8.8 和池里的
    重复条目打的都是 source.shiqianjiang.cn 同一个账号，全豆要的三份副本同属
    一套聚合链路。若按"条目 id"分道，等于对同一台上游主机开好几路并发，
    正好是我们实测到的高并发限流。这里取脚本里出现最多的域名作为签名；
    混淆后取不到域名的脚本（独家音源）退回按脚本内容哈希分道。
    """
    text = script or ""
    counts: "dict[str, int]" = {}
    for host in _LANE_HOST_RE.findall(text):
        key = host.lower()
        counts[key] = counts.get(key, 0) + 1
    if counts:
        return "host:" + max(counts, key=lambda k: (counts[k], k))
    return "script:" + hashlib.sha1(text.encode("utf-8", "ignore")).hexdigest()[:16]

SCRIPT_MAX_BYTES = 9_000_000
MAX_REDIRECTS = 3
# 服务端中转型脚本（如 pdone/lx-music-source 系）init 期间要先联系自家 API 服务器，
# 实测 1.4~6s、弱网更久；10s 会把"慢而活"的源掐死在激活环节，
# 30s 对齐桌面版"不设硬超时"的宽裕度，又不至于让激活请求无限挂起
INIT_TIMEOUT_S = 30.0
MUSIC_PLATFORMS = ("kw", "kg", "tx", "wy", "mg")

_META_LIMITS = {"name": 24, "description": 36, "author": 56, "homepage": 1024, "version": 36}
_HEADER_BLOCK_RE = re.compile(r"^/\*[\S\s]+?\*/")
_META_LINE_RE = re.compile(r"^\s?\*\s?@(\w+)\s(.+)$", re.M)
_URL_RE = re.compile(r"^https?://", re.I)
_FILE_URL_RE = re.compile(r"^file://", re.I)
# 上传脚本落盘目录（容器内 /data/lxmusic/uploads，随数据卷持久化）
UPLOAD_DIR_NAME = "uploads"

# tier（本服务内部档位）→ 期望的脚本档位（洛雪规范 qualitys 取值）
_TIER_QUALITY_PREFERENCE = {
    "lossless": ["flac", "flac24bit", "hires"],
    "high": ["320k"],
    "standard": ["128k"],
}


class SourceError(Exception):
    """带错误类别的源操作失败；category ∈ download/invalid/init/no_platform/resolve。"""

    def __init__(self, category: str, message: str):
        super().__init__(message)
        self.category = category


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.environ.get(name, "") or default))
    except (TypeError, ValueError):
        return default


def _resolve_node_bin() -> str:
    """解析 Node 可执行文件：默认 ``node`` 时优先取真正的 ELF 二进制。

    nodejs-wheel 这类打包会把 ``/usr/local/bin/node`` 做成 Python 包装脚本：
    每个音源多驻留一个解释器进程（实测 1.5~8MB/个），且包装进程被 SIGTERM
    时不会带真实 node 子进程一起退出，孤儿 node 会重挂到 init 常驻
    （issue #29 的"多进程叠加"与此同源）。解析到非 ELF 时尝试
    nodejs_wheel 内置二进制；``LX_NODE_BIN`` 显式指定时原样尊重。
    """
    raw = os.environ.get("LX_NODE_BIN", "").strip() or "node"
    if raw != "node":
        return raw
    path = shutil.which(raw)
    if not path:
        return raw
    try:
        with open(path, "rb") as fh:
            if fh.read(4) == b"\x7fELF":
                return raw
    except OSError:
        return raw
    try:
        wheel_bin = Path(sysconfig.get_path("purelib")) / "nodejs_wheel" / "bin" / "node"
    except Exception:  # noqa: BLE001
        return raw
    if wheel_bin.is_file() and os.access(wheel_bin, os.X_OK):
        return str(wheel_bin)
    return raw


def reap_orphan_bridges(
    bridge_path: "str | Path | None" = None, *, proc_root: "str | Path" = "/proc"
) -> list[int]:
    """清扫父进程已死亡的残留 bridge 子进程（issue #29「多进程叠加」根因）。

    服务被 SIGKILL/断电等异常终止时，Node 子进程被内核重挂到 PID 1 继续常驻；
    重启后 SourceManager 只管自己拉起的新进程，孤儿便在 ps 里叠加（#29 实测
    三个同名进程、etime 各为 26h/12m/4m，即多次异常重启的累计残留）。

    只清理同时满足以下条件的进程，其余一律不动：
      1. cmdline 恰好引用本服务 bridge.js 的绝对路径（不同部署路径互不影响）；
      2. 父进程已不存在（或已是 PID 1 托孤）——父进程尚在的不确定归属进程保守跳过；
      3. 不是本进程自己的孩子（孩子由 UserSource 生命周期管理）。
    proc_root 参数仅供测试注入伪 /proc 树。
    """
    try:
        target = str(
            (Path(bridge_path) if bridge_path else Path(__file__).resolve().parent / "js" / "bridge.js")
            .resolve()
        )
    except OSError:
        return []
    me = os.getpid()
    root = Path(proc_root)
    killed: list[int] = []
    try:
        entries = list(root.iterdir())
    except OSError:
        return []
    for entry in entries:
        name = entry.name
        if not name.isdigit() or int(name) == me:
            continue
        try:
            args = (entry / "cmdline").read_bytes().split(b"\0")
            if not any(
                os.path.realpath(a.decode("utf-8", "replace")) == target for a in args if a
            ):
                continue
            stat_text = (entry / "stat").read_text()
            ppid = int(stat_text[stat_text.rindex(")") + 2:].split()[1])
            if ppid == me:
                continue
            if ppid != 1 and (root / str(ppid)).exists():
                continue
            os.kill(int(name), signal.SIGTERM)
            killed.append(int(name))
            logger.warning("reaped orphan lx bridge process pid=%s (ppid=%s)", name, ppid)
        except (OSError, ValueError, IndexError):
            continue
    return killed


def parse_script_meta(script: str) -> dict:
    """解析脚本头部元数据；头部块或 @name 缺失视为无效脚本（对齐桌面版校验）。"""
    header = _HEADER_BLOCK_RE.match(script or "")
    if header is None:
        raise SourceError("invalid", "自定义源脚本必须以 /* ... */ 头部注释块开头")
    meta: dict[str, str] = {}
    for key, value in _META_LINE_RE.findall(header.group(0)):
        limit = _META_LIMITS.get(key)
        if limit is None:
            continue
        value = value.strip()
        meta[key] = (value[:limit] + "...") if len(value) > limit else value
    if not meta.get("name"):
        raise SourceError("invalid", "脚本头部必须声明 @name 元数据")
    return meta


def is_source_url(url: str) -> bool:
    """合法源地址：http(s):// 或 file://（本服务数据目录内的上传脚本）。"""
    raw = (url or "").strip()
    return bool(_URL_RE.match(raw) or _FILE_URL_RE.match(raw))


def _validate_script_text(script: str) -> None:
    """脚本文本统一校验：UTF-8 已由调用方解码；头部元数据必须合法。"""
    if len(script.encode("utf-8")) > SCRIPT_MAX_BYTES:
        raise SourceError("download", "脚本超过 9MB 大小上限")
    parse_script_meta(script)


async def download_script(url: str) -> str:
    """下载/读取源脚本（重定向≤3、≤9MB、UTF-8），并完成头部校验。

    file:// URL 直接读本地文件——仅用于本服务数据目录（uploads/）内的
    上传脚本与 state 缓存，路径已在落盘时净化。
    """
    url = (url or "").strip()
    if _FILE_URL_RE.match(url):
        path = url[len("file://"):]
        if not path.startswith("/"):
            raise SourceError("download", "file:// 路径必须是绝对路径")
        try:
            raw = Path(path).read_bytes()
        except OSError as exc:
            raise SourceError("download", f"读取脚本文件失败: {exc}") from exc
        try:
            script = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SourceError("invalid", "脚本不是有效的 UTF-8 文本") from exc
        _validate_script_text(script)
        return script
    if not _URL_RE.match(url):
        raise SourceError("download", "源地址必须以 http:// 、https:// 或 file:// 开头")
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            max_redirects=MAX_REDIRECTS,
            timeout=httpx.Timeout(20.0, connect=8.0),
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"},
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            declared = resp.headers.get("content-length", "")
            if declared.isdigit() and int(declared) > SCRIPT_MAX_BYTES:
                raise SourceError("download", "脚本超过 9MB 大小上限")
            raw = resp.content
    except SourceError:
        raise
    except httpx.TooManyRedirects as exc:
        raise SourceError("download", "重定向次数超过 3 次") from exc
    except Exception as exc:  # noqa: BLE001
        raise SourceError("download", f"下载失败: {exc}") from exc
    if len(raw) > SCRIPT_MAX_BYTES:
        raise SourceError("download", "脚本超过 9MB 大小上限")
    try:
        script = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SourceError("invalid", "脚本不是有效的 UTF-8 文本") from exc
    parse_script_meta(script)
    return script


_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._\-\u4e00-\u9fff]+")


def sanitize_upload_filename(filename: str) -> str:
    """上传文件名净化：basename、去控制字符、强制 .js 后缀。"""
    name = os.path.basename((filename or "").strip())
    name = _SAFE_FILENAME_RE.sub("_", name).strip("._") or "source"
    if not name.lower().endswith(".js"):
        name = f"{name}.js"
    return name[:120]


def save_upload(state_dir: "str | Path", filename: str, script: str) -> "tuple[str, str]":
    """把上传脚本写入数据目录 uploads/，返回 (容器内绝对路径, file:// URL)。

    同名冲突时追加时间戳；调用方负责先做脚本校验（parse_script_meta）。
    """
    _validate_script_text(script)
    uploads = Path(state_dir) / UPLOAD_DIR_NAME
    uploads.mkdir(parents=True, exist_ok=True)
    name = sanitize_upload_filename(filename)
    target = uploads / name
    if target.exists():
        stem = target.stem
        target = uploads / f"{stem}-{int(time.time() * 1000) % 10_000_000_000}.js"
    target.write_text(script, encoding="utf-8")
    return str(target), f"file://{target}"


def script_quality_for_tier(tier: str, declared: list[str]) -> "str | None":
    """tier → 脚本声明中最合适的档位；无匹配返回 None（由上层降档重试）。"""
    for quality in _TIER_QUALITY_PREFERENCE.get(tier or "standard", []):
        if quality in declared:
            return quality
    return None


def build_music_info(item: dict, platform: str) -> dict:
    """本服务条目 → 洛雪脚本期望的 musicInfo 结构。

    主键约定（与主流洛雪源一致）：kg 用 hash，其余平台用 songmid；
    tx 用 strMediaMid，mg 用 copyrightId，一并冗余提供。"""
    identifier = str(item.get("_identifier") or "")

    def seconds_str(seconds: Any) -> str:
        # 洛雪官方 musicInfo.interval 是纯秒数字符串（如 "253"），不是 MM:SS
        try:
            total = int(float(seconds) or 0)
        except (TypeError, ValueError):
            total = 0
        return str(total)

    info = {
        "songmid": identifier,
        "songId": identifier,
        "name": str(item.get("title") or ""),
        "singer": str(item.get("artist") or ""),
        "source": platform,
        "interval": seconds_str(item.get("duration_s")),
        "albumName": str(item.get("album") or ""),
        "meta": {
            "songId": identifier,
            "albumName": str(item.get("album") or ""),
            "picUrl": str(item.get("cover_url") or "") or None,
        },
    }
    # identifier 即 "lx:<platform>:<identifier>" 的平台主键：条目缺字段时兜底，
    # 保证脚本总能拿到本平台的规范主键（kg 读 hash、kw 读 rid……）
    platform_key = {
        "kg": ("hash", item.get("hash") or identifier),
        "tx": ("songmid", item.get("songmid") or identifier),
        "mg": ("copyrightId", item.get("copyright_id") or identifier),
        "kw": ("rid", item.get("rid") or identifier),
        "wy": ("songId", item.get("song_id") or identifier),
    }.get(platform)
    if platform_key and platform_key[1]:
        info[platform_key[0]] = str(platform_key[1])
    if platform == "kg" and item.get("hash"):
        info["songmid"] = str(item["hash"])  # 部分脚本读 songmid
    # 社区源（六音酷狗、全豆要 QQ）按官方 musicInfo 读这些别名。
    # QQ 的 songmid 与 file.media_mid 经常不是同一个值，不能互相顶替。
    info["id"] = info.get("songmid") or identifier
    album_id = str(item.get("album_id") or "").strip()
    if album_id and album_id not in ("0", "None"):
        info["albumId"] = album_id
    media_mid = str(item.get("str_media_mid") or item.get("media_mid") or "").strip()
    if media_mid:
        info["strMediaMid"] = media_mid
    return info


class UserSource:
    """一个已加载的用户自定义源：持久 Node 子进程 + 请求多路复用。"""

    def __init__(self, script: str, meta: dict, *, script_dir: "str | None" = None):
        self.script = script
        self.meta = meta
        self.node_bin = _resolve_node_bin()
        self._script_dir = Path(script_dir or tempfile.gettempdir())
        self._start_lock = asyncio.Lock()
        self.last_used = 0.0
        self._script_path: "Path | None" = None
        self._proc: "asyncio.subprocess.Process | None" = None
        self._pending: dict[str, asyncio.Future] = {}
        self._write_lock = asyncio.Lock()
        self._reader_task: "asyncio.Task | None" = None
        self._stderr_task: "asyncio.Task | None" = None
        self._next_id = 0
        self._started = False
        self._inited_event = asyncio.Event()
        self._inited_payload: Any = None
        self._init_error: "str | None" = None
        self.inited: dict = {}
        self.platforms: dict[str, dict] = {}
        self.started_at = 0.0
        self.update_alert: Any = None
        # 上游取链闸门的分道标识：同一家运营商（同一个中转域名）共用一条道
        self.lane_key = script_lane_key(script)

    # ------------------------------------------------------------------ 生命周期

    async def start(self) -> None:
        if self._started:
            return
        # 每次（重新）启动用全新事件：stop() 会把旧事件置位以解阻在途等待，
        # 不复位的话重启会直接读到旧 inited payload，新进程初始化失败也照样"成功"
        self._inited_event = asyncio.Event()
        self._inited_payload = None
        self._init_error = None
        bridge = Path(__file__).resolve().parent / "js" / "bridge.js"
        if not bridge.exists():
            raise SourceError("init", f"bridge script missing: {bridge}")
        try:
            self._script_dir.mkdir(parents=True, exist_ok=True)
            self._script_path = self._script_dir / f"lx-source-{os.getpid()}-{int(time.time() * 1000)}.js"
            self._script_path.write_text(self.script, encoding="utf-8")
        except OSError as exc:
            raise SourceError("init", f"无法写入脚本缓存: {exc}") from exc
        try:
            # start_new_session：wrapper+node 自成进程组，stop() 按组清理
            # （wrapper 脚本不转发 SIGTERM 时真实 node 也不会漏成孤儿）
            self._proc = await asyncio.create_subprocess_exec(
                self.node_bin,
                str(bridge),
                str(self._script_path),
                json.dumps(self.meta, ensure_ascii=False),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except FileNotFoundError as exc:
            raise SourceError("init", f"未找到 Node.js 运行时（{self.node_bin}）") from exc
        except Exception as exc:  # noqa: BLE001
            raise SourceError("init", f"启动 Node 沙箱失败: {exc}") from exc
        self._reader_task = asyncio.create_task(self._read_loop())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        try:
            self.inited = await asyncio.wait_for(self._wait_inited(), INIT_TIMEOUT_S)
        except asyncio.TimeoutError as exc:
            await self.stop()
            raise SourceError("init", f"脚本 {INIT_TIMEOUT_S:.0f}s 内未完成初始化（inited）") from exc
        except SourceError:
            await self.stop()
            raise
        sources = self.inited.get("sources")
        platforms: dict[str, dict] = {}
        if isinstance(sources, dict):
            for key, info in sources.items():
                code = str(key or "").lower()
                if code not in MUSIC_PLATFORMS or not isinstance(info, dict):
                    continue
                actions = [str(a) for a in (info.get("actions") or [])]
                if "musicUrl" not in actions:
                    continue
                platforms[code] = {
                    "name": str(info.get("name") or key),
                    "actions": actions,
                    "qualitys": [str(q) for q in (info.get("qualitys") or []) if q],
                }
        if not platforms:
            await self.stop()
            raise SourceError(
                "init", "脚本未声明任何可用的音乐平台（需 kw/kg/tx/wy/mg 之一且 actions 含 musicUrl）"
            )
        self.platforms = platforms
        self._started = True
        self.started_at = time.time()
        logger.info(
            "lx user source loaded: %s v%s (platforms=%s)",
            self.meta.get("name"), self.meta.get("version"), ",".join(sorted(platforms)),
        )

    async def _wait_inited(self) -> dict:
        await self._inited_event.wait()
        if self._init_error:
            raise SourceError("init", self._init_error)
        payload = self._inited_payload
        if not isinstance(payload, dict) or payload.get("status") is False:
            message = "脚本初始化失败（inited status=false）"
            if isinstance(payload, dict) and payload.get("message"):
                message = f"{message}: {payload['message']}"
            raise SourceError("init", message)
        return payload

    async def stop(self) -> None:
        self._started = False
        self._inited_event.set()
        cancelled = [t for t in (self._reader_task, self._stderr_task) if t is not None]
        for task in cancelled:
            task.cancel()
        if cancelled:
            # 必须等旧任务彻底退出：它们的退出代码仍可能触碰 self 状态，
            # 与紧随其后的 start()（新事件/新进程）形成竞态
            await asyncio.gather(*cancelled, return_exceptions=True)
        self._reader_task = None
        self._stderr_task = None
        proc = self._proc
        if proc is not None:
            pid = getattr(proc, "pid", None)
            # 仅当本进程是自建进程组组长（start_new_session）时按组杀，
            # 否则退回逐进程信号，避免误杀宿主进程组
            pgid = None
            if pid is not None:
                try:
                    candidate = os.getpgid(pid)
                except (OSError, TypeError):
                    candidate = None
                if candidate == pid:
                    pgid = candidate
            if proc.returncode is None:
                try:
                    if pgid is not None:
                        os.killpg(pgid, signal.SIGTERM)
                    else:
                        proc.terminate()
                except (ProcessLookupError, PermissionError):
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), 3.0)
                except asyncio.TimeoutError:
                    try:
                        if pgid is not None:
                            os.killpg(pgid, signal.SIGKILL)
                        else:
                            proc.kill()
                    except (ProcessLookupError, PermissionError, OSError):
                        pass
                    try:
                        await asyncio.wait_for(proc.wait(), 3.0)
                    except asyncio.TimeoutError:
                        logger.warning("lx bridge pid=%s 仍在运行（SIGKILL 后），请人工检查", pid)
        self._proc = None
        self._fail_pending("source stopped")
        if self._script_path is not None and self._script_path.exists():
            try:
                self._script_path.unlink()
            except OSError:
                pass
            self._script_path = None

    # ------------------------------------------------------------------ 协议读写

    async def _read_loop(self) -> None:
        proc = self._proc
        # 事件快照：stop()→start() 重启时旧 reader 可能晚于新事件创建才退出，
        # 只允许它操作自己这一轮的事件，避免污染新进程的 init 等待
        event = self._inited_event
        if proc is None or proc.stdout is None:
            return
        try:
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except Exception:  # noqa: BLE001
                    continue
                if isinstance(msg, dict):
                    self._dispatch(msg, event)
        finally:
            self._fail_pending("node process exited")
            if not event.is_set():
                self._init_error = "Node 进程提前退出"
                event.set()

    async def _read_stderr(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if text:
                    logger.warning("lx bridge stderr: %s", text[:500])
        except Exception:  # noqa: BLE001
            pass

    def _dispatch(self, msg: dict, event: "asyncio.Event | None" = None) -> None:
        event = event if event is not None else self._inited_event
        mtype = msg.get("type")
        if mtype == "event":
            name = msg.get("name")
            payload = msg.get("payload")
            if name == "inited":
                if not event.is_set():
                    self._inited_payload = payload
                    event.set()
            elif name == "fatal":
                if not event.is_set():
                    self._init_error = str((payload or {}).get("error") or "bridge fatal")
                    event.set()
                else:
                    self._fail_pending("bridge fatal")
            elif name == "updateAlert":
                self.update_alert = payload
                logger.info("lx source update alert: %s", str(payload or "")[:200])
        elif mtype == "log":
            level = str(msg.get("level") or "info")
            message = str(msg.get("message") or "")
            bound = getattr(logger, "warning" if level in ("warn", "error") else "info", logger.info)
            bound("[%s] %s", self.meta.get("name") or "lx-source", message[:500])
        elif mtype == "pong":
            pass
        elif "id" in msg:
            fut = self._pending.pop(str(msg["id"]), None)
            if fut is None or fut.done():
                return
            if msg.get("ok"):
                fut.set_result(msg.get("result"))
            else:
                fut.set_exception(SourceError("resolve", str(msg.get("error") or "script request failed")))

    def _fail_pending(self, reason: str) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(SourceError("resolve", reason))
        self._pending.clear()

    @property
    def running(self) -> bool:
        return self._started and self._proc is not None and self._proc.returncode is None

    def qualitys(self, platform: str) -> list[str]:
        return list(self.platforms.get(platform, {}).get("qualitys") or [])

    def music_platforms(self) -> list[str]:
        return [p for p in MUSIC_PLATFORMS if p in self.platforms]

    async def music_url(self, music_info: dict, quality: "str | None", *, platform: str,
                        timeout: float = 10.0) -> str:
        """调用脚本 musicUrl；quality 为洛雪规范档位（128k/320k/flac/...）。"""
        if not self.running:
            # 空闲回收/定期轮换后首个请求触发懒重建（冷启动 1-3s），链路不断；
            # 从未初始化成功的源不做懒启动，避免每次请求都重试 30s init
            if self.started_at > 0:
                async with self._start_lock:
                    if not self.running:
                        await self.start()
            if not self.running:
                raise SourceError("init", "源运行时未就绪")
        self.last_used = time.time()
        self._next_id += 1
        req_id = f"r{self._next_id}"
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        payload = {
            "type": "request",
            "id": req_id,
            "source": platform,
            "action": "musicUrl",
            "info": {"type": quality, "musicInfo": music_info},
        }
        try:
            async with self._write_lock:
                proc = self._proc
                if proc is None or proc.stdin is None:
                    raise SourceError("init", "源运行时未就绪")
                proc.stdin.write((json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"))
                await proc.stdin.drain()
            result = await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError as exc:
            self._pending.pop(req_id, None)
            raise SourceError("resolve", f"musicUrl 解析超时（{timeout:.0f}s）") from exc
        url = str(result or "")
        if not url.startswith(("http://", "https://")):
            raise SourceError("resolve", "脚本未返回有效的 http 直链")
        return url

    async def recycle(self) -> None:
        """温和轮换：停止后重建（供内存守护使用；轮换期请求走懒重建/下家音源）。"""
        await self.stop()
        async with self._start_lock:
            if not self.running:
                await self.start()

    def describe(self) -> dict:
        # issue #29：暴露 bridge 进程 pid 与运行时长，便于用户上报"多进程/CPU 占用"时
        # 直接对照 healthz 与宿主 ps 输出（正常恒为单进程；多进程=容器/服务被外部拉起多次）
        pid = self._proc.pid if self._proc is not None else None
        uptime_s = round(time.time() - self.started_at, 1) if self.started_at else None
        return {
            "name": self.meta.get("name") or "",
            "version": self.meta.get("version") or "",
            "author": self.meta.get("author") or "",
            "description": self.meta.get("description") or "",
            "homepage": self.meta.get("homepage") or "",
            "platforms": {
                code: {"name": info["name"], "qualitys": info["qualitys"]}
                for code, info in sorted(self.platforms.items())
            },
            "running": self.running,
            "started_at": self.started_at,
            "pid": pid,
            "uptime_s": uptime_s,
            "update_alert": self.update_alert,
        }


class SourceManager:
    """当前激活源管理：state.json 持久化（URL + 脚本缓存），env 仅作首次种子。"""

    def __init__(self, state_dir: "str | None" = None, seed_url: str = ""):
        base = state_dir or os.environ.get("LX_DATA_DIR") or "/data/lxmusic"
        try:
            self.state_dir = Path(base)
            self.state_dir.mkdir(parents=True, exist_ok=True)
            _probe = self.state_dir / ".write-probe"
            _probe.write_text("", encoding="utf-8")
            _probe.unlink()
        except OSError:
            self.state_dir = Path(tempfile.mkdtemp(prefix="lxmusic-state-"))
            logger.warning("lx data dir %s not writable, falling back to %s", base, self.state_dir)
        self.state_path = self.state_dir / "state.json"
        self.script_cache = self.state_dir / "source.js"
        self.seed_url = (seed_url or os.environ.get("LX_SOURCE_URL") or "").strip()
        self._runtime: "UserSource | None" = None
        self._lock = asyncio.Lock()
        self.active_url = ""
        self.last_error = ""

    # ------------------------------------------------------------------ 状态

    def read_state(self) -> dict:
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except (OSError, ValueError):
            return {}

    def _write_state(self, url: str, script: str) -> None:
        try:
            self.state_path.write_text(
                json.dumps({"url": url, "activated_at": time.time()}, ensure_ascii=False),
                encoding="utf-8",
            )
            self.script_cache.write_text(script, encoding="utf-8")
        except OSError as exc:
            logger.warning("cannot persist lx source state: %s", exc)

    # ------------------------------------------------------------------ 激活/加载

    async def activate(self, url: str, *, script: "str | None" = None) -> UserSource:
        """校验并切换到给定 URL 的源（成功后旧运行时立即停用）。"""
        async with self._lock:
            script = script if script is not None else await download_script(url)
            meta = parse_script_meta(script)
            runtime = UserSource(script, meta, script_dir=str(self.state_dir))
            await runtime.start()
            old = self._runtime
            self._runtime = runtime
            self.active_url = url
            self.last_error = ""
            self._write_state(url, script)
            if old is not None:
                await old.stop()
            return runtime

    async def load(self) -> None:
        """服务启动时加载：state.json 的 URL 优先，env 为种子；下载失败回退缓存脚本。"""
        # issue #29：先清扫上次异常退出（SIGKILL/断电）留下的孤儿 bridge，再拉起新进程，
        # 避免"多个同名 bridge.js 叠加常驻高 CPU"复发。活跃父进程下的进程一律不动。
        try:
            reap_orphan_bridges()
        except Exception as exc:  # noqa: BLE001
            logger.warning("orphan bridge reaper failed (ignored): %s", type(exc).__name__)
        async with self._lock:
            if self._runtime is not None:
                return
            state = self.read_state()
            url = str(state.get("url") or "").strip() or self.seed_url
            if not url:
                logger.info("no lx source configured (state/env both empty)")
                return
            script = None
            try:
                script = await download_script(url)
            except SourceError as exc:
                self.last_error = f"{exc.category}: {exc}"
                logger.warning("lx source download failed (%s), trying cache", exc)
                if self.script_cache.exists():
                    try:
                        script = self.script_cache.read_text(encoding="utf-8")
                        parse_script_meta(script)
                    except (OSError, SourceError, UnicodeDecodeError):
                        script = None
            if script is None:
                logger.warning("lx source unavailable: no script (url=%s)", url[:120])
                return
            try:
                meta = parse_script_meta(script)
                runtime = UserSource(script, meta, script_dir=str(self.state_dir))
                await runtime.start()
                self._runtime = runtime
                self.active_url = url
                logger.info("lx source restored from %s", "cache" if self.last_error else "download")
            except SourceError as exc:
                self.last_error = f"{exc.category}: {exc}"
                logger.warning("lx source init failed: %s", exc)

    async def shutdown(self) -> None:
        async with self._lock:
            runtime = self._runtime
            self._runtime = None
            if runtime is not None:
                await runtime.stop()

    def get(self) -> "UserSource | None":
        runtime = self._runtime
        if runtime is not None and runtime.running:
            return runtime
        return None

    def _cached_script(self) -> str:
        try:
            return self.script_cache.read_text(encoding="utf-8")
        except OSError:
            return ""

    async def check_update(self) -> dict:
        """重新下载当前源 URL，只比较 hash，不改运行态。"""
        runtime = self.get()
        alert = runtime.update_alert if runtime is not None else None
        url = (
            str((alert or {}).get("updateUrl") or "").strip()
            if isinstance(alert, dict)
            else ""
        ) or (self.active_url or self.seed_url or "").strip()
        if not url:
            return {"available": False, "reason": "no_source_url"}
        try:
            latest = await download_script(url)
        except Exception as exc:  # noqa: BLE001
            return {"available": False, "error": str(exc)[:300]}
        current = self._cached_script()
        current_meta = parse_script_meta(current) if current else {}
        current_hash = hashlib.sha256(current.encode("utf-8")).hexdigest() if current else ""
        latest_hash = hashlib.sha256(latest.encode("utf-8")).hexdigest()
        latest_meta = parse_script_meta(latest)
        return {
            "available": latest_hash != current_hash,
            "url": url,
            "current_hash": current_hash,
            "latest_hash": latest_hash,
            "meta": latest_meta,
            "current_meta": current_meta,
            "current_version": current_meta.get("version", ""),
            "latest_version": latest_meta.get("version", ""),
        }

    async def update(self) -> dict:
        runtime = self.get()
        alert = runtime.update_alert if runtime is not None else None
        url = (
            str((alert or {}).get("updateUrl") or "").strip()
            if isinstance(alert, dict)
            else ""
        ) or (self.active_url or self.seed_url or "").strip()
        if not url:
            return {"updated": False, "reason": "no_source_url"}
        try:
            latest = await download_script(url)
        except Exception as exc:  # noqa: BLE001
            return {"updated": False, "error": str(exc)[:300]}
        current = self._cached_script()
        if current and hashlib.sha256(current.encode("utf-8")).hexdigest() == hashlib.sha256(latest.encode("utf-8")).hexdigest():
            return {"updated": False, "reason": "already_latest", **self.describe()}
        await self.activate(url, script=latest)
        return {"updated": True, **self.describe()}

    def describe(self) -> dict:
        runtime = self.get()
        meta = {}
        try:
            cached = self._cached_script()
            if cached:
                meta = parse_script_meta(cached)
        except Exception:
            pass
        source = runtime.describe() if runtime is not None else {
            "name": meta.get("name", ""), "version": meta.get("version", ""),
            "author": meta.get("author", ""), "description": meta.get("description", ""),
            "homepage": meta.get("homepage", ""), "platforms": {}, "running": False,
        }
        # script_hash：与源池同一算法（sha256 脚本正文），管理页用它把
        # 「主源」与「附加源」合并成一份列表时去重，避免同一脚本显示两行
        script_hash = ""
        try:
            cached = self._cached_script()
            if cached:
                script_hash = hashlib.sha256(cached.encode("utf-8")).hexdigest()
        except Exception:
            script_hash = ""
        return {
            "configured": bool(self.active_url or self.seed_url),
            "url": self.active_url or self.seed_url,
            "initialized": runtime is not None,
            "last_error": self.last_error,
            "script_hash": script_hash,
            "meta": meta,
            "source": source,
        }


def memory_guard_enabled() -> bool:
    """音源常驻内存守护开关（默认全关，保持现有行为）。

    - LX_SOURCE_IDLE_REAP_S > 0：空闲超过阈值的音源进程被停止以释放内存；
      下次播放时懒重建（冷启动 1-3s），播放链路不变、不中断活跃解析。
    - LX_SOURCE_MAX_UPTIME_S > 0：运行时长超过阈值的空闲音源进程被轮换重建，
      兜住脚本自身缓存膨胀（实测个别音源进程数日内可增长数十 MB）。
    """
    return (
        _env_int("LX_SOURCE_IDLE_REAP_S", 0) > 0
        or _env_int("LX_SOURCE_MAX_UPTIME_S", 0) > 0
    )


async def run_memory_guard(get_runtimes: "Callable[[], list[UserSource]]") -> None:
    """后台守护：仅对"近期无请求"的音源生效，永不中断正在进行的解析。"""
    while True:
        idle_s = _env_int("LX_SOURCE_IDLE_REAP_S", 0)
        max_uptime_s = _env_int("LX_SOURCE_MAX_UPTIME_S", 0)
        if idle_s > 0 or max_uptime_s > 0:
            now = time.time()
            try:
                runtimes = list(get_runtimes())
            except Exception as exc:  # noqa: BLE001
                runtimes = []
                logger.warning("memory guard: 枚举音源运行时失败: %s", exc)
            for rt in runtimes:
                name = (rt.meta or {}).get("name") or rt.script[:24]
                try:
                    if not rt.running:
                        continue
                    idle_for = now - rt.last_used
                    if idle_s > 0 and idle_for >= idle_s:
                        logger.info(
                            "lx source %s 空闲 %.0fs >= %ds，回收常驻进程", name, idle_for, idle_s
                        )
                        await rt.stop()
                    elif (
                        max_uptime_s > 0
                        and now - rt.started_at >= max_uptime_s
                        and idle_for >= min(300.0, float(max_uptime_s))
                    ):
                        logger.info(
                            "lx source %s 运行 %.0fh 超阈值，轮换常驻进程",
                            name, (now - rt.started_at) / 3600.0,
                        )
                        await rt.recycle()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("memory guard: %s 处理失败: %s", name, exc)
        await asyncio.sleep(120)


class MultiSourceManager:
    """附加洛雪源池：同一时间可启用多个脚本，播放时按顺序逐个尝试。"""

    def __init__(self, state_dir: "str | Path"):
        self.state_dir = Path(state_dir)
        self.sources_path = self.state_dir / "sources.json"
        self.scripts_dir = self.state_dir / "sources"
        self._entries: dict[str, dict] = {}
        self._runtimes: dict[str, UserSource] = {}
        self._loaded_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def _read_sources(self) -> list[dict]:
        try:
            data = json.loads(self.sources_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return []
        rows = data.get("sources") if isinstance(data, dict) else []
        return [dict(row) for row in rows if isinstance(row, dict) and row.get("id")]

    def _write_sources(self) -> None:
        try:
            self.scripts_dir.mkdir(parents=True, exist_ok=True)
            rows = []
            for entry in sorted(self._entries.values(), key=lambda e: int(e.get("position") or 0)):
                rows.append({
                    "id": entry["id"],
                    "name": entry.get("name") or "",
                    "url": entry.get("url") or "",
                    "enabled": bool(entry.get("enabled")),
                    "auto_update": bool(entry.get("auto_update")),
                    "position": int(entry.get("position") or 0),
                    "script_file": entry.get("script_file") or "",
                    "script_hash": entry.get("script_hash") or "",
                    "added_at": entry.get("added_at") or int(time.time()),
                })
            self.sources_path.write_text(
                json.dumps({"version": 1, "sources": rows}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.warning("cannot persist multi-source state: %s", exc)

    def _script_path(self, entry: dict) -> Path:
        return self.scripts_dir / str(entry.get("script_file") or f"{entry['id']}.js")

    async def _start_entry(self, entry: dict) -> None:
        if self._runtimes.get(entry["id"]) is not None and self._runtimes[entry["id"]].running:
            return
        path = self._script_path(entry)
        try:
            script = path.read_text(encoding="utf-8")
            meta = parse_script_meta(script)
            runtime = UserSource(script, meta, script_dir=str(self.scripts_dir))
            await runtime.start()
            self._runtimes[entry["id"]] = runtime
            entry["name"] = entry.get("name") or meta.get("name") or ""
            entry["last_error"] = ""
        except Exception as exc:  # noqa: BLE001
            entry["last_error"] = str(exc)[:300]
            logger.warning("additional lx source %s start failed: %s", entry["id"], exc)

    async def wait_loaded(self, timeout: float = 5.0) -> bool:
        try:
            await asyncio.wait_for(self._loaded_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def load(self) -> None:
        try:
            async with self._lock:
                for row in self._read_sources():
                    self._entries[row["id"]] = row
                for entry in sorted(self._entries.values(), key=lambda e: int(e.get("position") or 0)):
                    if not entry.get("script_hash"):
                        try:
                            script = self._script_path(entry).read_text(encoding="utf-8")
                            entry["script_hash"] = hashlib.sha256(script.encode("utf-8")).hexdigest()
                        except OSError:
                            pass
                # 元数据先发布；脚本进程初始化不能阻塞源列表接口。
                self._loaded_event.set()
                for entry in sorted(self._entries.values(), key=lambda e: int(e.get("position") or 0)):
                    if entry.get("enabled"):
                        await self._start_entry(entry)
                self._write_sources()
        finally:
            self._loaded_event.set()

    async def add(self, url: str, *, script: "str | None" = None, name: str = "") -> dict:
        async with self._lock:
            script = script if script is not None else await download_script(url)
            meta = parse_script_meta(script)
            script_hash = hashlib.sha256(script.encode("utf-8")).hexdigest()
            for existing in self._entries.values():
                if existing.get("script_hash") and existing["script_hash"] == script_hash:
                    existing_name = existing.get("name") or existing.get("id") or "未知音源"
                    raise SourceError(
                        "duplicate",
                        f"“{existing_name}”已在音源池中（脚本内容相同）。重复添加同一脚本"
                        "不会提高播放成功率，只会多占一个常驻 Node 进程；如需换新版请对已有条目点「更新」。",
                    )
            source_id = "src_" + hashlib.sha1(
                f"{url}|{time.time()}|{uuid4().hex}".encode("utf-8")
            ).hexdigest()[:12]
            filename = f"{source_id}.js"
            self.scripts_dir.mkdir(parents=True, exist_ok=True)
            (self.scripts_dir / filename).write_text(script, encoding="utf-8")
            entry = {
                "id": source_id,
                "name": str(name or meta.get("name") or "").strip(),
                "url": url,
                "enabled": True,
                "auto_update": False,
                "position": max([int(e.get("position") or 0) for e in self._entries.values()] or [-1]) + 1,
                "script_file": filename,
                "script_hash": script_hash,
                "added_at": int(time.time()),
                "last_error": "",
            }
            self._entries[source_id] = entry
            await self._start_entry(entry)
            self._write_sources()
            return self._describe_entry(entry)

    async def remove(self, source_id: str) -> bool:
        async with self._lock:
            entry = self._entries.pop(source_id, None)
            if entry is None:
                return False
            runtime = self._runtimes.pop(source_id, None)
            if runtime is not None:
                await runtime.stop()
            try:
                self._script_path(entry).unlink()
            except OSError:
                pass
            self._write_sources()
            return True

    async def set_enabled(self, source_id: str, enabled: bool) -> "dict | None":
        async with self._lock:
            entry = self._entries.get(source_id)
            if entry is None:
                return None
            entry["enabled"] = bool(enabled)
            if enabled:
                await self._start_entry(entry)
            else:
                runtime = self._runtimes.pop(source_id, None)
                if runtime is not None:
                    await runtime.stop()
            self._write_sources()
            return self._describe_entry(entry)

    async def set_auto_update(self, source_id: str, enabled: bool) -> "dict | None":
        async with self._lock:
            entry = self._entries.get(source_id)
            if entry is None:
                return None
            entry["auto_update"] = bool(enabled)
            self._write_sources()
            return self._describe_entry(entry)

    async def check_update(self, source_id: str) -> "dict | None":
        entry = self._entries.get(source_id)
        if entry is None:
            return None
        runtime = self._runtimes.get(source_id)
        alert = runtime.update_alert if runtime is not None else None
        url = (
            str((alert or {}).get("updateUrl") or "").strip()
            if isinstance(alert, dict)
            else ""
        ) or str(entry.get("url") or "").strip()
        if not url:
            return {"available": False, "reason": "no_source_url"}
        try:
            latest = await download_script(url)
        except Exception as exc:  # noqa: BLE001
            return {"available": False, "error": str(exc)[:300]}
        latest_hash = hashlib.sha256(latest.encode("utf-8")).hexdigest()
        current_hash = str(entry.get("script_hash") or "")
        latest_meta = parse_script_meta(latest)
        current_meta = {}
        try:
            current_meta = parse_script_meta(self._script_path(entry).read_text(encoding="utf-8"))
        except Exception:
            pass
        available = latest_hash != current_hash
        result = {
            "available": available,
            "url": url,
            "current_hash": current_hash,
            "latest_hash": latest_hash,
            "meta": latest_meta,
            "current_meta": current_meta,
            "current_version": current_meta.get("version", ""),
            "latest_version": latest_meta.get("version", ""),
            "auto_update": bool(entry.get("auto_update")),
        }
        if available and entry.get("auto_update"):
            updated = await self.update(source_id)
            return {**result, "auto_applied": True, "update": updated}
        return result

    async def update(self, source_id: str) -> "dict | None":
        async with self._lock:
            entry = self._entries.get(source_id)
            if entry is None:
                return None
            runtime = self._runtimes.get(source_id)
            alert = runtime.update_alert if runtime is not None else None
            url = (
                str((alert or {}).get("updateUrl") or "").strip()
                if isinstance(alert, dict)
                else ""
            ) or str(entry.get("url") or "").strip()
            if not url:
                return {"updated": False, "reason": "no_source_url"}
            try:
                latest = await download_script(url)
            except Exception as exc:  # noqa: BLE001
                return {"updated": False, "error": str(exc)[:300]}
            latest_hash = hashlib.sha256(latest.encode("utf-8")).hexdigest()
            if latest_hash == str(entry.get("script_hash") or ""):
                return {"updated": False, "reason": "already_latest", "source": self._describe_entry(entry)}
            meta = parse_script_meta(latest)
            path = self._script_path(entry)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(latest, encoding="utf-8")
            runtime = self._runtimes.pop(source_id, None)
            if runtime is not None:
                await runtime.stop()
            entry.update({
                "script_hash": latest_hash,
                "name": entry.get("name") or meta.get("name") or "",
                "last_error": "",
                "url": url,
            })
            if entry.get("enabled"):
                await self._start_entry(entry)
            self._write_sources()
            return {"updated": True, "source": self._describe_entry(entry)}

    async def reorder(self, source_ids: list[str]) -> None:
        async with self._lock:
            positions = {sid: idx for idx, sid in enumerate(source_ids)}
            for sid, entry in self._entries.items():
                if sid in positions:
                    entry["position"] = positions[sid]
            self._write_sources()

    def _describe_entry(self, entry: dict) -> dict:
        runtime = self._runtimes.get(entry["id"])
        meta = {}
        try:
            meta = parse_script_meta(self._script_path(entry).read_text(encoding="utf-8"))
        except Exception:
            pass
        source = runtime.describe() if runtime is not None and runtime.running else {
            "name": meta.get("name", ""), "version": meta.get("version", ""),
            "author": meta.get("author", ""), "description": meta.get("description", ""),
            "homepage": meta.get("homepage", ""), "platforms": {}, "running": False,
        }
        return {
            "id": entry["id"],
            "name": entry.get("name") or meta.get("name") or "",
            "url": entry.get("url") or "",
            "enabled": bool(entry.get("enabled")),
            "auto_update": bool(entry.get("auto_update")),
            "position": int(entry.get("position") or 0),
            "script_hash": entry.get("script_hash") or "",
            "running": bool(runtime is not None and runtime.running),
            "last_error": entry.get("last_error") or "",
            "update_alert": runtime.update_alert if runtime is not None and runtime.running else None,
            "meta": meta,
            "source": source,
        }

    def describe_all(self) -> list[dict]:
        return [
            self._describe_entry(entry)
            for entry in sorted(self._entries.values(), key=lambda e: int(e.get("position") or 0))
        ]

    def runtimes_for(self, platform: str) -> list["tuple[str, UserSource]"]:
        out = []
        for entry in sorted(self._entries.values(), key=lambda e: int(e.get("position") or 0)):
            if not entry.get("enabled"):
                continue
            runtime = self._runtimes.get(entry["id"])
            if runtime is not None and runtime.running and platform in runtime.platforms:
                out.append((entry["id"], runtime))
        return out

    def all_runtimes(self) -> list["tuple[str, UserSource]"]:
        out = []
        for entry in sorted(self._entries.values(), key=lambda e: int(e.get("position") or 0)):
            if not entry.get("enabled"):
                continue
            runtime = self._runtimes.get(entry["id"])
            if runtime is not None and runtime.running:
                out.append((entry["id"], runtime))
        return out

    def configured(self) -> bool:
        return bool(self._entries)

    async def shutdown(self) -> None:
        async with self._lock:
            runtimes = list(self._runtimes.values())
            self._runtimes.clear()
            for runtime in runtimes:
                await runtime.stop()
