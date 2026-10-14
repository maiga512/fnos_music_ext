#!/usr/bin/env python3
# TOP[top.webui] 飞牛桌面网关 socket → 本机 WebUI(8774) 转发 + host-file 例外端点
"""把飞牛桌面网关的 Unix socket 原样转到本机 WebUI。

应用中心按 ui/config 的 gatewaySocket 连接
<应用目录>/fnmusic-ext.sock，把 https://<主机>:<桌面端口>/app/fnmusic-ext/
转到这个 socket。WebUI 自己监听 127.0.0.1:8774，并认识 /app/fnmusic-ext 前缀。

本模块还处理一个例外端点：POST /app/fnmusic-ext/api/host-file——浏览器把
NAS 上选中的 .js 源脚本路径发来，由宿主侧（本进程，root）代读文件内容。
该端点只在桌面网关链路里存在（直连 8774 不经过这里），并要求网关注入的
X-Trim-Isadmin: true。其余请求一律原样转发，不动字节。
"""
from __future__ import annotations

import json
import os
import signal
import socket
import sys
import threading
from pathlib import Path

UPSTREAM = ("127.0.0.1", 8774)
SOCK_NAME = "fnmusic-ext.sock"
PID_FILE = Path("/run/fnmusic-ext/webui-gateway.pid")
# create_connection 的 timeout 会留在套接字上，之后的 recv 也受它限制。
# 洛雪源校验要几十秒才有响应；沿用 5 秒会让桌面网关中途拆连接，nginx 回 502。
CONNECT_TIMEOUT = 5
RELAY_TIMEOUT = 180

HOST_FILE_PATHS = ("/app/fnmusic-ext/api/host-file", "/api/host-file")
HOST_FILE_MAX_BODY = 1 << 20          # 请求体上限 1MB（只装一个路径字符串）
HOST_FILE_MAX_BYTES = 9_000_000       # 与 lxmusic SCRIPT_MAX_BYTES 对齐

# --- 导入歌单：宿主侧端点（同上 host-file 例外的第二条链路） -----------------
# 导入歌单落盘在宿主的数据根（FNMUSIC_HOME/imported_playlists），而 WebUI 在
# 容器里只挂了 /repo，看不到那个目录。代理读的就是这个目录，所以增删查全部在
# 本进程（宿主 root）直接做，不再绕一次网络，也不必为此新加一条数据卷。
IMPORT_BASES = ("/app/fnmusic-ext/api/playlists", "/api/playlists")
IMPORT_MAX_BODY = 8 << 20             # 洛雪 JSON 歌单可达数 MB
IMPORT_TIMEOUT_S = 90.0               # 单个歌单导入（含平台翻页）总上限

_PROXY_DIR = Path(__file__).resolve().parent
if str(_PROXY_DIR) not in sys.path:
    sys.path.insert(0, str(_PROXY_DIR))

try:
    from env_merge import parse_env_file
except Exception as _env_exc:  # noqa: BLE001 - 读不到 .env 时退化到进程环境
    parse_env_file = None  # type: ignore
    _ENV_MERGE_IMPORT_ERROR = str(_env_exc)

try:
    import playlist_import as plimp  # noqa: E402
except Exception as _exc:  # noqa: BLE001 - 缺依赖时网关照常转发，只禁用本端点
    plimp = None
    _PLIMP_IMPORT_ERROR = str(_exc)

_BASE_DIR = _PROXY_DIR.parent


def socket_path_for(base: Path) -> Path:
    """fpk 布局是 <应用目录>/repo。socket 必须放在应用目录下，桌面网关才找得到。"""
    parent = base.resolve().parent
    if (parent / "ui").is_dir() and base.name == "repo":
        return parent / SOCK_NAME
    run = Path("/run/fnmusic-ext")
    run.mkdir(parents=True, exist_ok=True)
    return run / SOCK_NAME


def _relay(src: socket.socket, dst: socket.socket, prefix: bytes = b"") -> None:
    try:
        if prefix:
            dst.sendall(prefix)
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for sock in (src, dst):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def _read_request_head(client: socket.socket) -> "tuple[bytes, dict[str, str]] | None":
    """读到 HTTP 头部结束；返回 (头部原始字节, 解析出的头)。超时/格式异常返回 None。"""
    buf = b""
    try:
        while b"\r\n\r\n" not in buf:
            if len(buf) > 65536:
                return None
            chunk = client.recv(8192)
            if not chunk:
                return None
            buf += chunk
    except OSError:
        return None
    head, _, rest = buf.partition(b"\r\n\r\n")
    headers: dict[str, str] = {}
    lines = head.decode("latin-1", "replace").split("\r\n")
    for line in lines[1:]:
        if ":" in line:
            k, _, v = line.partition(":")
            headers[k.strip().lower()] = v.strip()
    if rest:
        headers["__rest__"] = rest.decode("latin-1", "replace")
    headers["__raw__"] = head.decode("latin-1", "replace")
    return head + b"\r\n\r\n" + rest, headers


def _read_full_body(client: socket.socket, headers: dict[str, str], head_raw: bytes,
                    max_body: int = HOST_FILE_MAX_BODY) -> "bytes | None":
    """按 Content-Length 收齐 POST 体（小请求：仅路径字符串）。"""
    try:
        length = int(headers.get("content-length", "0"))
    except ValueError:
        return None
    if length < 0 or length > max_body:
        return None
    head_end = head_raw.find(b"\r\n\r\n") + 4
    body = head_raw[head_end:]
    while len(body) < length:
        chunk = client.recv(65536)
        if not chunk:
            return None
        body += chunk
    return body[:length]


def _http_response(status: int, reason: str, payload: bytes, content_type: str) -> bytes:
    head = (
        f"HTTP/1.1 {status} {reason}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(payload)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("latin-1")
    return head + payload


def _json_response(status: int, obj: dict) -> bytes:
    return _http_response(status, "OK" if status == 200 else "Error",
                          json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                          "application/json; charset=utf-8")


def handle_host_file(client: socket.socket, headers: dict[str, str], head_raw: bytes) -> None:
    """POST /api/host-file：校验 admin + .js 路径，读文件内容回 JSON {script}。"""
    try:
        if headers.get("x-trim-isadmin", "").lower() != "true":
            client.sendall(_json_response(403, {"ok": False, "error": "仅管理员可读取主机文件"}))
            return
        body = _read_full_body(client, headers, head_raw)
        if body is None:
            client.sendall(_json_response(400, {"ok": False, "error": "请求体无效"}))
            return
        try:
            req = json.loads(body.decode("utf-8"))
            path = str(req.get("path") or "")
        except Exception:  # noqa: BLE001
            client.sendall(_json_response(400, {"ok": False, "error": "请求体必须是 JSON"}))
            return
        target = Path(path)
        if not path.startswith("/") or ".." in target.parts:
            client.sendall(_json_response(400, {"ok": False, "error": "路径必须是绝对路径且不含 .."}))
            return
        if not path.lower().endswith(".js"):
            client.sendall(_json_response(400, {"ok": False, "error": "只支持读取 .js 后缀文件"}))
            return
        try:
            st = target.stat()
        except OSError:
            client.sendall(_json_response(404, {"ok": False, "error": f"文件不存在: {path}"}))
            return
        if not os.path.isfile(str(target)) or st.st_size > HOST_FILE_MAX_BYTES:
            client.sendall(_json_response(400, {"ok": False, "error": "不是常规文件或超过 9MB 上限"}))
            return
        try:
            script = target.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            client.sendall(_json_response(400, {"ok": False, "error": f"读取失败: {exc}"}))
            return
        client.sendall(_json_response(200, {"ok": True, "script": script}))
    except OSError:
        pass
    finally:
        try:
            client.close()
        except OSError:
            pass


def serve(sock_path: Path, upstream: tuple[str, int] = UPSTREAM) -> None:
    sock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        sock_path.unlink()
    except FileNotFoundError:
        pass
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    os.chmod(sock_path, 0o666)
    server.listen(64)
    while True:
        client, _addr = server.accept()
        threading.Thread(target=_handle, args=(client, upstream), daemon=True).start()


def _env_value(env_path: Path, key: str) -> str:
    if parse_env_file is None:
        return ""
    try:
        kv, _other = parse_env_file(env_path)
    except Exception:  # noqa: BLE001 - 文件缺失/编码异常都按未配置处理
        return ""
    for name, value in reversed(kv):
        if name == key:
            return (value or "").strip()
    return ""


def import_store_dir() -> str:
    """导入歌单目录：与宿主代理同一个目录（.env 优先，其次进程环境）。"""
    env_path = _BASE_DIR / ".env"
    key = "FNMUSIC_IMPORT_PLAYLISTS_DIR"
    home_key = "FNMUSIC_HOME"
    path = _env_value(env_path, key) or (os.environ.get(key) or "").strip()
    if not path:
        home = _env_value(env_path, home_key) or (os.environ.get(home_key) or "").strip()
        path = os.path.join(home or str(_BASE_DIR), "imported_playlists")
    return path


def _request_line(head_raw: bytes) -> tuple[str, str]:
    try:
        line = head_raw.split(b"\r\n", 1)[0].decode("latin-1", "replace")
    except Exception:  # noqa: BLE001
        return "", ""
    parts = line.split()
    if len(parts) < 2:
        return "", ""
    return parts[0].upper(), parts[1].split("?", 1)[0]


def _import_route(head_raw: bytes) -> dict | None:
    """识别导入歌单端点 → {action, id}；不是这些端点返回 None（原样转发）。"""
    method, path = _request_line(head_raw)
    if not path:
        return None
    base = ""
    for candidate in IMPORT_BASES:
        if path == candidate or path.startswith(candidate + "/"):
            base = candidate
            break
    if not base:
        return None
    rest = path[len(base):].strip("/")          # imported / import / imported/refresh
    if method == "GET" and rest == "imported":
        return {"action": "list"}
    if method != "POST":
        return None
    if rest == "import":
        return {"action": "import"}
    parts = rest.split("/")
    if len(parts) == 2 and parts[0] == "imported" and parts[1] in ("refresh", "delete"):
        return {"action": parts[1]}
    return None


def _run_import(coro):
    import asyncio

    async def _main():
        import httpx

        timeout = httpx.Timeout(IMPORT_TIMEOUT_S, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            return await asyncio.wait_for(coro(client), IMPORT_TIMEOUT_S)

    return asyncio.run(_main())


def handle_import_api(route: dict, client: socket.socket, headers: dict[str, str],
                      head_raw: bytes) -> None:
    """导入歌单的增删查：本进程（宿主 root）直接读写数据根里的 JSON。"""
    try:
        if headers.get("x-trim-isadmin", "").lower() != "true":
            client.sendall(_json_response(403, {"ok": False, "error": "仅管理员可管理导入歌单"}))
            return
        if plimp is None:
            client.sendall(_json_response(503, {
                "ok": False,
                "error": f"导入模块不可用: {_PLIMP_IMPORT_ERROR}",
            }))
            return
        plimp.set_dir(import_store_dir())
        action = route.get("action")
        body = b"{}"
        if action != "list":
            payload = _read_full_body(client, headers, head_raw, IMPORT_MAX_BODY)
            if payload is None:
                client.sendall(_json_response(400, {"ok": False, "error": "请求体无效"}))
                return
            body = payload
        try:
            req = json.loads(body.decode("utf-8") or "{}")
            if not isinstance(req, dict):
                req = {}
        except Exception:  # noqa: BLE001
            client.sendall(_json_response(400, {"ok": False, "error": "请求体必须是 JSON"}))
            return

        if action == "list":
            client.sendall(_json_response(200, {"ok": True, "data": plimp.list_records()}))
            return
        if action == "import":
            text = str(req.get("text") or "")
            name = str(req.get("name") or "")

            async def _do_import(http_client):
                return await plimp.import_from_text(http_client, text, name)

            summaries, error = _run_import(_do_import)
            if error and not summaries:
                client.sendall(_json_response(400, {"ok": False, "error": error}))
                return
            client.sendall(_json_response(200, {"ok": True, "data": summaries, "warning": error}))
            return
        pid = str(req.get("id") or "").strip()
        if not pid:
            client.sendall(_json_response(400, {"ok": False, "error": "缺少歌单 id"}))
            return
        if action == "refresh":
            async def _do_refresh(http_client):
                return await plimp.refresh(http_client, pid)

            summary, error = _run_import(_do_refresh)
            if summary is None:
                client.sendall(_json_response(400, {"ok": False, "error": error or "刷新失败"}))
                return
            client.sendall(_json_response(200, {"ok": True, "data": summary}))
            return
        if action == "delete":
            if not plimp.delete(pid):
                client.sendall(_json_response(404, {"ok": False, "error": "歌单不存在"}))
                return
            client.sendall(_json_response(200, {"ok": True, "data": {"id": pid}}))
            return
        client.sendall(_json_response(404, {"ok": False, "error": "未知操作"}))
    except Exception as exc:  # noqa: BLE001 - 网关必须始终回包，不能让连接悬着
        try:
            client.sendall(_json_response(500, {"ok": False, "error": f"处理失败: {exc}"}))
        except OSError:
            pass
    finally:
        try:
            client.close()
        except OSError:
            pass


def _is_host_file_request(head_raw: bytes) -> bool:
    try:
        request_line = head_raw.split(b"\r\n", 1)[0].decode("latin-1", "replace")
    except Exception:  # noqa: BLE001
        return False
    parts = request_line.split()
    if len(parts) < 2 or parts[0].upper() != "POST":
        return False
    return parts[1] in HOST_FILE_PATHS


def _handle(client: socket.socket, upstream: tuple[str, int]) -> None:
    head = _read_request_head(client)
    if head is None:
        client.close()
        return
    head_raw, headers = head
    route = _import_route(head_raw)
    if route is not None:
        handle_import_api(route, client, headers, head_raw)
        return
    if _is_host_file_request(head_raw):
        handle_host_file(client, headers, head_raw)
        return
    try:
        remote = socket.create_connection(upstream, timeout=CONNECT_TIMEOUT)
        remote.settimeout(RELAY_TIMEOUT)
    except OSError:
        client.close()
        return
    left = threading.Thread(target=_relay, args=(client, remote, head_raw), daemon=True)
    right = threading.Thread(target=_relay, args=(remote, client), daemon=True)
    left.start()
    right.start()
    left.join()
    right.join()
    client.close()
    remote.close()


def _stop_previous() -> None:
    if not PID_FILE.is_file():
        return
    try:
        pid = int(PID_FILE.read_text().strip())
    except ValueError:
        return
    if pid == os.getpid():
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass


def _daemonize() -> None:
    if os.fork() > 0:
        os._exit(0)
    os.setsid()
    if os.fork() > 0:
        os._exit(0)
    os.chdir("/")
    devnull = os.open(os.devnull, os.O_RDWR)
    for fd in (0, 1, 2):
        try:
            os.dup2(devnull, fd)
        except OSError:
            pass


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    base = Path(args[0]) if args else Path(__file__).resolve().parent.parent
    global _BASE_DIR
    _BASE_DIR = base
    path = socket_path_for(base)
    _stop_previous()
    _daemonize()
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()))
    try:
        serve(path)
    finally:
        try:
            PID_FILE.unlink()
        except OSError:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
