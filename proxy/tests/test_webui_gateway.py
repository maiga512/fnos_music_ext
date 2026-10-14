"""桌面网关 socket 把 HTTP 原样转到 WebUI 端口；/api/host-file 由网关本地处理。"""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "webui_gateway", Path(__file__).resolve().parents[1] / "webui_gateway.py")
_MOD = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MOD)
serve = _MOD.serve
socket_path_for = _MOD.socket_path_for


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        body = self.path.encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def _start_gateway(tmp_path: Path, port: int) -> Path:
    sock_path = tmp_path / "fnmusic-ext.sock"
    errors = []

    def _run():
        try:
            serve(sock_path, ("127.0.0.1", port))
        except Exception as exc:  # noqa: BLE001 — 断言里带出线程失败原因
            errors.append(exc)

    threading.Thread(target=_run, daemon=True).start()
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    last = None
    for _ in range(50):
        try:
            client.connect(str(sock_path))
            break
        except OSError as exc:
            last = exc
            time.sleep(0.02)
    else:
        raise AssertionError(f"socket 未就绪: {last}; thread={errors}")
    client.close()
    return sock_path


def _request(sock_path: Path, raw: bytes, timeout: float = 5) -> bytes:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(sock_path))
    client.sendall(raw)
    data = b""
    client.settimeout(timeout)
    try:
        while True:
            chunk = client.recv(4096)
            if not chunk:
                break
            data += chunk
    except socket.timeout:
        pass
    finally:
        client.close()
    return data


def _post_host_file(path: str, is_admin: str = "true") -> bytes:
    body = json.dumps({"path": path}).encode()
    return (
        "POST /app/fnmusic-ext/api/host-file HTTP/1.1\r\n"
        "Host: localhost\r\n"
        f"X-Trim-Isadmin: {is_admin}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode() + body


def test_socket_path_for_fpk_layout(tmp_path: Path):
    app = tmp_path / "fnmusic-ext"
    (app / "ui").mkdir(parents=True)
    repo = app / "repo"
    repo.mkdir()
    assert socket_path_for(repo) == app / "fnmusic-ext.sock"


class _SlowHandler(BaseHTTPRequestHandler):
    """响应故意晚于旧的 5 秒套接字超时，用来守住校验请求不被中途拆掉。"""

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length:
            self.rfile.read(length)
        time.sleep(6)
        body = b'{"ok":true}'
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def test_slow_upstream_is_not_cut_at_connect_timeout(tmp_path: Path):
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _SlowHandler)
    port = upstream.server_address[1]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    sock_path = _start_gateway(tmp_path, port)
    raw = (
        b"POST /app/fnmusic-ext/api/lx/verify HTTP/1.1\r\n"
        b"Host: localhost\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
    )
    data = _request(sock_path, raw, timeout=12)
    upstream.shutdown()
    assert b"200" in data.split(b"\r\n", 1)[0]
    assert b'{"ok":true}' in data


def test_socket_forwards_desktop_path(tmp_path: Path):
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = upstream.server_address[1]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    sock_path = _start_gateway(tmp_path, port)
    data = _request(sock_path, b"GET /app/fnmusic-ext/ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
    upstream.shutdown()
    assert b"/app/fnmusic-ext/" in data


def test_host_file_requires_admin(tmp_path: Path):
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = upstream.server_address[1]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    sock_path = _start_gateway(tmp_path, port)
    data = _request(sock_path, _post_host_file("/tmp/x.js", is_admin="false"))
    upstream.shutdown()
    assert b"403" in data.split(b"\r\n", 1)[0]
    assert "仅管理员".encode() in data


def test_host_file_validates_path_and_reads_content(tmp_path: Path):
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = upstream.server_address[1]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    sock_path = _start_gateway(tmp_path, port)

    script = "/*\n * @name 测试\n */\nvar a = 1;\n"
    js = tmp_path / "source.js"
    js.write_text(script, encoding="utf-8")

    # 非法：相对路径 / .. / 非 .js / 不存在
    for bad in ("relative/x.js", "/tmp/../etc/passwd", str(tmp_path / "a.txt"), str(tmp_path / "nope.js")):
        data = _request(sock_path, _post_host_file(bad))
        assert b"400" in data.split(b"\r\n", 1)[0] or b"404" in data.split(b"\r\n", 1)[0], bad

    # 正常读取
    data = _request(sock_path, _post_host_file(str(js)))
    head, _, body = data.partition(b"\r\n\r\n")
    assert b"200" in head.split(b"\r\n", 1)[0]
    payload = json.loads(body.decode())
    assert payload["ok"] is True
    assert payload["script"] == script
    upstream.shutdown()


def test_host_file_only_post_intercepted(tmp_path: Path):
    """GET /api/host-file 不是代读端点，走正常转发。"""
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = upstream.server_address[1]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    sock_path = _start_gateway(tmp_path, port)
    data = _request(sock_path, b"GET /app/fnmusic-ext/api/host-file HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    upstream.shutdown()
    # 上游原样回显请求路径（未拦截）
    assert b"/app/fnmusic-ext/api/host-file" in data


# ------------------------------------------------ 导入歌单端点（宿主侧本地处理）
def _short_dir(name: str) -> Path:
    """AF_UNIX 路径上限 104 字节，pytest 的 tmp_path 在 macOS 上太长，另开短目录。"""
    base = Path("/tmp") / f"fmimp_{name}_{os.getpid()}"
    shutil.rmtree(base, ignore_errors=True)
    (base / "fnmusic-data" / "imported_playlists").mkdir(parents=True)
    return base


def _imp_env(tmp_path: Path, monkeypatch, name: str = "base") -> "tuple[Path, Path]":
    """把网关指向临时应用目录：数据根从该目录的 .env 读，和宿主代理一致。

    返回 (应用目录, 数据目录)。socket 另用短路径目录（第三个返回值由调用方取）。
    """
    base = _short_dir(name)
    data_dir = base / "fnmusic-data" / "imported_playlists"
    (base / ".env").write_text(
        "FNMUSIC_HOME='%s'\nFNMUSIC_IMPORT_PLAYLISTS_DIR='%s'\n" % (base / "fnmusic-data", data_dir),
        encoding="utf-8",
    )
    monkeypatch.setattr(_MOD, "_BASE_DIR", base)
    _MOD.plimp.set_dir(str(data_dir))
    return base, data_dir


def _imp_request(path: str, method: str = "GET", body: dict | None = None,
                 is_admin: str = "true") -> bytes:
    payload = json.dumps(body or {}).encode()
    head = (
        f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n"
        f"X-Trim-Isadmin: {is_admin}\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n"
    ).encode()
    return head + payload


def _payload(data: bytes) -> dict:
    _head, _, body = data.partition(b"\r\n\r\n")
    return json.loads(body.decode())


def test_import_store_dir_reads_env_data_root(tmp_path: Path, monkeypatch):
    _base, data_dir = _imp_env(tmp_path, monkeypatch, "dir")
    assert _MOD.import_store_dir() == str(data_dir)


def test_import_routes_are_recognized():
    ok = {
        b"GET /app/fnmusic-ext/api/playlists/imported HTTP/1.1": {"action": "list"},
        b"GET /api/playlists/imported?x=1 HTTP/1.1": {"action": "list"},
        b"POST /app/fnmusic-ext/api/playlists/import HTTP/1.1": {"action": "import"},
        b"POST /app/fnmusic-ext/api/playlists/imported/refresh HTTP/1.1": {"action": "refresh"},
        b"POST /app/fnmusic-ext/api/playlists/imported/delete HTTP/1.1": {"action": "delete"},
    }
    for head, expected in ok.items():
        assert _MOD._import_route(head + b"\r\n\r\n") == expected, head
    # 其余一律原样转发（不能把 WebUI 自己的接口吃掉）
    for head in (b"GET /app/fnmusic-ext/api/config HTTP/1.1",
                 b"GET /app/fnmusic-ext/api/playlists/other HTTP/1.1",
                 b"DELETE /app/fnmusic-ext/api/playlists/imported/x HTTP/1.1",
                 b"POST /app/fnmusic-ext/api/playlists/imported/unknown HTTP/1.1"):
        assert _MOD._import_route(head + b"\r\n\r\n") is None, head


def test_import_list_served_on_host_without_upstream(tmp_path: Path, monkeypatch):
    """列表/删除都由网关直接读写数据根：上游即便在也不该收到这些请求。"""
    _base, data_dir = _imp_env(tmp_path, monkeypatch, "list")
    sock_dir = _short_dir("listsock")
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = upstream.server_address[1]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    sock_path = _start_gateway(sock_dir, port)

    record = _MOD.plimp.write_record({
        "id": "imp001", "name": "老歌收藏", "cover": "http://img/c.jpg",
        "origin": "link", "origin_id": "123", "platform": "tx",
        "songs": [{"source": "tx", "id": "A1", "name": "晴天", "singer": "周杰伦"}],
    })
    assert (data_dir / f"{record['id']}.json").is_file()

    data = _request(sock_path, _imp_request("/app/fnmusic-ext/api/playlists/imported"))
    payload = _payload(data)
    assert payload["ok"] is True
    assert payload["data"][0]["name"] == "老歌收藏"
    assert payload["data"][0]["count"] == 1
    assert payload["data"][0]["refreshable"] is True

    data = _request(sock_path, _imp_request("/app/fnmusic-ext/api/playlists/imported/delete",
                                           "POST", {"id": "imp001"}))
    assert _payload(data)["ok"] is True
    assert not (data_dir / "imp001.json").exists()
    assert _payload(_request(sock_path, _imp_request(
        "/app/fnmusic-ext/api/playlists/imported")))["data"] == []
    # 上游回显里不含这些路径 = 一次都没转发
    echoed = _request(sock_path, b"GET /app/fnmusic-ext/api/status HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    assert b"/app/fnmusic-ext/api/status" in echoed
    upstream.shutdown()


def test_import_endpoints_require_admin(tmp_path: Path, monkeypatch):
    _base, _data = _imp_env(tmp_path, monkeypatch, "admin")
    sock_dir = _short_dir("adminsock")
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = upstream.server_address[1]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    sock_path = _start_gateway(sock_dir, port)
    for method, path, body in (
        ("GET", "/app/fnmusic-ext/api/playlists/imported", None),
        ("POST", "/app/fnmusic-ext/api/playlists/import", {"text": "x"}),
    ):
        data = _request(sock_path, _imp_request(path, method, body, is_admin="false"))
        assert b"403" in data.split(b"\r\n", 1)[0], path
        assert _payload(data)["ok"] is False
    upstream.shutdown()


def test_import_validates_body_and_id(tmp_path: Path, monkeypatch):
    _base, _data = _imp_env(tmp_path, monkeypatch, "bad")
    sock_dir = _short_dir("badsock")
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = upstream.server_address[1]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    sock_path = _start_gateway(sock_dir, port)

    raw = (b"POST /app/fnmusic-ext/api/playlists/imported/delete HTTP/1.1\r\n"
           b"Host: x\r\nX-Trim-Isadmin: true\r\nContent-Length: 3\r\nConnection: close\r\n\r\nnot")
    assert _payload(_request(sock_path, raw))["ok"] is False
    assert b"400" in _request(sock_path, _imp_request(
        "/app/fnmusic-ext/api/playlists/imported/delete", "POST", {})).split(b"\r\n", 1)[0]
    assert b"404" in _request(sock_path, _imp_request(
        "/app/fnmusic-ext/api/playlists/imported/delete", "POST", {"id": "ghost"})).split(b"\r\n", 1)[0]
    upstream.shutdown()


def test_import_text_imports_and_refreshes(tmp_path: Path, monkeypatch):
    """走一次真实解析链：洛雪 JSON 文本 → 落盘 → 刷新（链接型）。"""
    _base, _data = _imp_env(tmp_path, monkeypatch, "text")
    sock_dir = _short_dir("textsock")
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = upstream.server_address[1]
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    sock_path = _start_gateway(sock_dir, port)

    lx_json = json.dumps({"playList_v2": [{
        "name": "深夜电台", "id": "lx99",
        "list": [{"songmid": "S1", "name": "Night", "singer": "DJ",
                  "interval": 200, "source": "tx"}],
    }]}, ensure_ascii=False).encode()
    body = json.dumps({"text": lx_json.decode()}).encode()
    raw = (
        "POST /app/fnmusic-ext/api/playlists/import HTTP/1.1\r\n"
        "Host: x\r\nX-Trim-Isadmin: true\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % len(body)
    ).encode() + body
    data = _request(sock_path, raw, timeout=20)
    payload = _payload(data)
    assert payload["ok"] is True, data[:400]
    assert payload["data"][0]["name"] == "深夜电台"
    assert payload["data"][0]["count"] == 1
    assert payload["data"][0]["refreshable"] is False   # JSON 导入无上游可刷
    upstream.shutdown()
